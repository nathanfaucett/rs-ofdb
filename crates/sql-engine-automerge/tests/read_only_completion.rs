use std::{
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use engine::{Engine, EngineError, EngineResult, InMemoryKernel, Kernel, KernelTransaction};
use engine_automerge::AutomergeRowCodec;
use engine_redb::RedbKernel;
use futures::{Stream, executor::block_on};
use query::{QueryResult, Translator};
use sql_translator::SqlTranslator;
use value::{Row, Value};

#[derive(Default)]
struct Completions {
    commits: AtomicUsize,
    rollbacks: AtomicUsize,
    writes: AtomicUsize,
}

impl Completions {
    fn take(&self) -> (usize, usize, usize) {
        (
            self.commits.swap(0, Ordering::Relaxed),
            self.rollbacks.swap(0, Ordering::Relaxed),
            self.writes.swap(0, Ordering::Relaxed),
        )
    }
}

struct ObservedKernel<K> {
    inner: K,
    completions: Arc<Completions>,
}

struct ObservedTransaction<T> {
    inner: T,
    completions: Arc<Completions>,
}

impl<K: Kernel> Kernel for ObservedKernel<K> {
    type Transaction = ObservedTransaction<K::Transaction>;

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        Ok(ObservedTransaction {
            inner: self.inner.transaction().await?,
            completions: Arc::clone(&self.completions),
        })
    }
}

impl<T: KernelTransaction> KernelTransaction for ObservedTransaction<T> {
    async fn ensure_table(&mut self, table: &str) -> EngineResult<()> {
        self.completions.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.ensure_table(table).await
    }

    async fn drop_table(&mut self, table: &str) -> EngineResult<()> {
        self.completions.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.drop_table(table).await
    }

    async fn get_bytes(&self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.inner.get_bytes(table, key).await
    }

    fn scan_bytes<'a>(
        &'a self,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + Send + 'a {
        self.inner.scan_bytes(table)
    }

    fn scan_bytes_range<'a>(
        &'a self,
        table: &'a str,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + Send + 'a {
        self.inner.scan_bytes_range(table, range)
    }

    async fn put_bytes(&mut self, table: &str, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        self.completions.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.put_bytes(table, key, value).await
    }

    async fn remove_bytes(&mut self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.completions.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.remove_bytes(table, key).await
    }

    async fn commit(self) -> EngineResult<()> {
        self.inner.commit().await?;
        self.completions.commits.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn rollback(self) -> EngineResult<()> {
        self.inner.rollback().await?;
        self.completions.rollbacks.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

async fn execute<K: Kernel>(
    engine: &Engine<K, AutomergeRowCodec>,
    sql: &str,
) -> EngineResult<Vec<QueryResult>> {
    engine
        .execute(
            SqlTranslator
                .translate(sql)
                .await
                .expect("valid fixture SQL"),
        )
        .await
}

fn observed<K: Kernel>(
    inner: K,
) -> (
    Engine<ObservedKernel<K>, AutomergeRowCodec>,
    Arc<Completions>,
) {
    let completions = Arc::new(Completions::default());
    let engine = Engine::new(
        ObservedKernel {
            inner,
            completions: Arc::clone(&completions),
        },
        AutomergeRowCodec::new(),
    );
    (engine, completions)
}

fn redb_kernel() -> RedbKernel {
    RedbKernel::new(Arc::new(
        redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .expect("real Redb storage"),
    ))
}

async fn read_only_batches<K: Kernel>(kernel: K) {
    let (engine, completions) = observed(kernel);
    assert!(
        engine
            .execute(vec![])
            .await
            .expect("empty engine batch")
            .is_empty()
    );
    assert_eq!(completions.take(), (0, 1, 0));
    execute(
        &engine,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT); \
         INSERT INTO items VALUES (1, 'before'); \
         CREATE TABLE other (id INTEGER PRIMARY KEY); INSERT INTO other VALUES (1)",
    )
    .await
    .expect("committed fixture");
    let (commits, rollbacks, writes) = completions.take();
    assert_eq!((commits, rollbacks), (1, 0));
    assert!(writes > 0);

    let results = execute(&engine, "SELECT label FROM items; SELECT id FROM items")
        .await
        .expect("standalone SELECT batch");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].rows, vec![Row::from(["before"])]);
    assert_eq!(results[0].columns[0].name, "label");
    assert_eq!(results[1].rows, vec![Row::new(vec![Value::Integer(1)])]);
    assert_eq!(completions.take(), (0, 1, 0));

    for sql in [
        "SELECT label FROM items WHERE id IN (SELECT id FROM other)",
        "SELECT items.label FROM items INNER JOIN other ON items.id = other.id",
        "SELECT items.label FROM items LEFT JOIN other ON items.id = other.id",
    ] {
        let results = execute(&engine, sql).await.expect("read-only nested path");
        assert_eq!(results[0].rows, vec![Row::from(["before"])]);
        assert_eq!(completions.take(), (0, 1, 0));
    }
    let error = execute(&engine, "SELECT label FROM items; SELECT * FROM missing")
        .await
        .expect_err("failed SELECT batch");
    assert!(matches!(
        error,
        EngineError::InvalidQuery("Table not found")
    ));
    assert_eq!(completions.take(), (0, 1, 0));

    assert!(
        engine
            .execute(vec![])
            .await
            .expect("later empty batch")
            .is_empty()
    );
    assert_eq!(completions.take(), (0, 1, 0));
}

async fn write_completion_semantics<K: Kernel>(kernel: K) {
    let (engine, completions) = observed(kernel);
    execute(
        &engine,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT); \
         INSERT INTO items VALUES (1, 'before')",
    )
    .await
    .expect("fixture");
    completions.take();
    let results = execute(
        &engine,
        "SELECT label FROM items; UPDATE items SET label = 'after'; \
         SELECT label FROM items; INSERT INTO items VALUES (2, 'second')",
    )
    .await
    .expect("mixed batch commits all writes");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].rows, vec![Row::from(["before"])]);
    assert_eq!(results[2].rows, vec![Row::from(["after"])]);
    let (commits, rollbacks, writes) = completions.take();
    assert_eq!((commits, rollbacks), (1, 0));
    assert!(writes > 0);
    let expected = vec![Row::from(["after"]), Row::from(["second"])];
    assert_eq!(
        execute(&engine, "SELECT label FROM items ORDER BY id")
            .await
            .expect("fresh committed rows")[0]
            .rows,
        expected
    );
    assert_eq!(completions.take(), (0, 1, 0));

    for sql in [
        "UPDATE items SET label = 'never' WHERE id = 99",
        "DELETE FROM items WHERE id = 99",
        "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, label TEXT)",
    ] {
        execute(&engine, sql)
            .await
            .expect("no-op write still commits");
        let (commits, rollbacks, _) = completions.take();
        assert_eq!((commits, rollbacks), (1, 0));
    }

    let mut transaction = engine.transaction().await.expect("explicit transaction");
    let results = transaction
        .execute(
            SqlTranslator
                .translate("SELECT label FROM items ORDER BY id")
                .await
                .expect("SELECT SQL"),
        )
        .await
        .expect("explicit SELECT");
    assert_eq!(results[0].rows, expected);
    assert_eq!(completions.take(), (0, 0, 0));
    transaction
        .commit()
        .await
        .expect("explicit read-only commit unchanged");
    assert_eq!(completions.take(), (1, 0, 0));

    let error = execute(
        &engine,
        "UPDATE items SET label = 'discard'; INSERT INTO items VALUES (3, 'discard'); \
         SELECT * FROM missing",
    )
    .await
    .expect_err("failed mixed batch rolls back all mutations");
    assert!(matches!(
        error,
        EngineError::InvalidQuery("Table not found")
    ));
    let (commits, rollbacks, writes) = completions.take();
    assert_eq!((commits, rollbacks), (0, 1));
    assert!(writes > 0);
    assert_eq!(
        execute(&engine, "SELECT label FROM items ORDER BY id")
            .await
            .expect("atomic rollback preserves committed rows")[0]
            .rows,
        expected
    );
    assert_eq!(completions.take(), (0, 1, 0));
}

#[test]
fn standalone_selects_finish_without_committing_or_writing() {
    block_on(read_only_batches(InMemoryKernel::new()));
}

#[test]
fn redb_standalone_selects_finish_without_committing_or_writing() {
    block_on(read_only_batches(redb_kernel()));
}

#[test]
fn writes_mixed_batches_explicit_commits_and_failures_keep_their_semantics() {
    block_on(write_completion_semantics(InMemoryKernel::new()));
}

#[test]
fn redb_writes_mixed_batches_explicit_commits_and_failures_keep_their_semantics() {
    block_on(write_completion_semantics(redb_kernel()));
}
