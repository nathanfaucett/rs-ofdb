use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use automerge::{AutoCommit, ROOT, transaction::Transactable};
use btree::BTreeTransaction;
use btree_automerge::{AutomergeBTreeTransaction, AutomergeChangeStore, ThresholdPolicy};
use engine::{
    ENGINE_TABLE_FIELDS_STORAGE, ENGINE_TABLES_STORAGE, Engine, EngineResult, InMemoryKernel,
    Kernel, KernelTransaction, RowCatalog, RowCodec,
};
use engine_automerge::AutomergeRowCodec;
use futures::{FutureExt, Stream, executor::block_on};
use sql_translator::SqlTranslator;
use uuid::Uuid;
use value::{Row, Value};

#[derive(Default)]
struct Reads {
    tables: AtomicUsize,
    fields: AtomicUsize,
    conflicts: AtomicUsize,
    pause_ensure: AtomicBool,
    pause_merge: AtomicBool,
}

impl Reads {
    fn take(&self) -> (usize, usize, usize) {
        (
            self.tables.swap(0, Ordering::Relaxed),
            self.fields.swap(0, Ordering::Relaxed),
            self.conflicts.swap(0, Ordering::Relaxed),
        )
    }
}

struct CountingCodec {
    inner: AutomergeRowCodec,
    reads: Arc<Reads>,
}

macro_rules! delegate {
    ($($name:ident($($argument:ident: $argument_type:ty),*) -> $output:ty;)*) => {$ (
        async fn $name(&self, $($argument: $argument_type),*) -> EngineResult<$output> {
            self.inner.$name($($argument),*).await
        }
    )*};
}

impl<T: KernelTransaction> RowCodec<T> for CountingCodec {
    delegate! {

        drop_table(transaction: &mut T, table: &str) -> ();
        get_row(transaction: &T, table: &str, row: &Uuid) -> Option<Row>;
        put_row(transaction: &mut T, table: &str, row: Uuid, value: Row) -> ();
        remove_row(transaction: &mut T, table: &str, row: &Uuid) -> Option<Row>;
        encode_row(transaction: &T, table: &str, row: &Uuid, value: &Row, columns: &[usize]) -> Vec<u8>;
        conflict_values(transaction: &T, table: &str, row: &Uuid) -> Vec<(usize, Vec<Value>)>;
        encode_resolution(transaction: &T, table: &str, row: &Uuid, value: &Row, columns: &[usize]) -> Vec<u8>;

        delete_row(transaction: &mut T, table: &str, row: &Uuid) -> Option<Row>;
        row_is_deleted(transaction: &T, table: &str, row: &Uuid) -> bool;
        row_has_dropped_definition(transaction: &T, table: &str, row: &Uuid) -> bool;
        table_definition_was_dropped(transaction: &T, row: &Uuid) -> bool;
        drop_table_definition(transaction: &mut T, row: &Uuid, event: Uuid) -> ();
        table_drop_events(transaction: &T, table: &str) -> Vec<Uuid>;
        set_observed_table_drops(transaction: &mut T, row: &Uuid, events: Vec<Uuid>) -> ();
        observed_table_drops(transaction: &T, row: &Uuid) -> Vec<Uuid>;
    }

    async fn ensure_table(&self, transaction: &mut T, table: &str) -> EngineResult<()> {
        self.inner.ensure_table(transaction, table).await?;
        if self.reads.pause_ensure.swap(false, Ordering::Relaxed) {
            core::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn merge_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        value: &[u8],
    ) -> EngineResult<Option<Row>> {
        let result = self.inner.merge_row(transaction, table, row, value).await?;
        if table == "items" && self.reads.pause_merge.swap(false, Ordering::Relaxed) {
            core::future::pending::<()>().await;
        }
        Ok(result)
    }

    async fn conflicted_columns(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
        tables: &[(Uuid, Row)],
        fields: &[Row],
    ) -> EngineResult<Vec<usize>> {
        self.reads.conflicts.fetch_add(1, Ordering::Relaxed);
        self.inner
            .conflicted_columns(transaction, table, row, tables, fields)
            .await
    }

    fn scan_rows<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
        catalog: Option<RowCatalog<'a>>,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row)>> + Send + 'a {
        self.inner.scan_rows(transaction, table, catalog)
    }

    fn scan_row_states<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row, bool)>> + Send + 'a {
        match table {
            ENGINE_TABLES_STORAGE => {
                self.reads.tables.fetch_add(1, Ordering::Relaxed);
            }
            ENGINE_TABLE_FIELDS_STORAGE => {
                self.reads.fields.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        self.inner.scan_row_states(transaction, table)
    }
}

fn id(value: u64) -> Uuid {
    Uuid::parse_str(&format!("018f0f8e-7b6d-7c4a-8f12-{value:012x}"))
        .expect("fixed UUIDv7 identity")
}

async fn fixture() -> (
    Engine<InMemoryKernel, CountingCodec>,
    InMemoryKernel,
    Arc<Reads>,
) {
    let kernel = InMemoryKernel::new();
    let reads = Arc::new(Reads::default());
    let engine = Engine::new(
        kernel.clone(),
        CountingCodec {
            inner: AutomergeRowCodec::new(),
            reads: Arc::clone(&reads),
        },
    );
    engine
        .translate_and_execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, label TEXT); \
         CREATE TABLE other (id UUID PRIMARY KEY, label TEXT); \
         INSERT INTO items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID), 'base'); \
         INSERT INTO other VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-000000000002' AS UUID), 'base')",
            &SqlTranslator,
        )
        .await
        .expect("two real rows");
    let left = AutomergeRowCodec::new();
    let right = AutomergeRowCodec::new();
    let mut transaction = kernel.transaction().await.expect("conflict fixture");
    for (table, key) in [("items", id(1)), ("other", id(2))] {
        let a = left
            .encode_row(
                &transaction,
                table,
                &key,
                &Row::new(vec![Value::Uuid(key), Value::from("left")]),
                &[1],
            )
            .await
            .expect("left branch");
        let b = right
            .encode_row(
                &transaction,
                table,
                &key,
                &Row::new(vec![Value::Uuid(key), Value::from("right")]),
                &[1],
            )
            .await
            .expect("right branch");
        for change in [a, b] {
            left.merge_row(&mut transaction, table, key, &change)
                .await
                .expect("merge real conflict");
        }
    }
    transaction.commit().await.expect("commit conflicts");
    reads.take();
    (engine, kernel, reads)
}

#[test]
fn select_conflict_select_conflict_reuses_one_transaction_catalog_but_inspects_both_rows() {
    block_on(async {
        let (engine, _, reads) = fixture().await;
        let mut transaction = engine.transaction().await.expect("one immutable snapshot");
        for (table, key) in [("items", id(1)), ("other", id(2))] {
            let results = transaction
                .translate_and_execute(&format!("SELECT id FROM {table}"), &SqlTranslator)
                .await
                .expect("SELECT before conflict inspection");
            assert_eq!(results[0].rows, vec![Row::new(vec![Value::Uuid(key)])]);
            assert_eq!(
                transaction
                    .row_conflicts(table, &Row::new(vec![Value::Uuid(key)]))
                    .await
                    .expect("real concurrent conflict"),
                vec!["label"]
            );
        }
        assert_eq!(
            reads.take(),
            (1, 1, 2),
            "one table/field traversal pair, two actual conflict inspections"
        );
        transaction
            .rollback()
            .await
            .expect("finish read transaction");
    });
}

fn key(value: u64) -> Row {
    Row::new(vec![Value::Uuid(id(value))])
}

#[test]
fn conflict_first_initializes_catalog_and_every_call_inspects_current_snapshot() {
    block_on(async {
        let (engine, _, reads) = fixture().await;
        for _ in 0..2 {
            let mut transaction = engine.transaction().await.expect("fresh transaction");
            for _ in 0..2 {
                assert_eq!(
                    transaction
                        .row_conflicts("items", &key(1))
                        .await
                        .expect("real conflict"),
                    vec!["label"]
                );
                let results = transaction
                    .translate_and_execute("SELECT id FROM other", &SqlTranslator)
                    .await
                    .expect("other schema from complete catalog");
                assert_eq!(results[0].rows, vec![key(2)]);
            }
            assert_eq!(
                reads.take(),
                (1, 1, 2),
                "each new transaction loads once, never caches conflict results"
            );
            transaction.rollback().await.expect("finish snapshot");
        }
    });
}

#[test]
fn cached_catalog_stays_with_its_kernel_snapshot_and_next_transaction_is_fresh() {
    block_on(async {
        let (engine, _, reads) = fixture().await;
        let mut old = engine.transaction().await.expect("earlier snapshot");
        old.translate_and_execute("SELECT * FROM items", &SqlTranslator)
            .await
            .expect("warm catalog");
        engine
            .resolve_row(
                "items",
                &key(1),
                vec![("label".into(), Value::from("current"))],
            )
            .await
            .expect("external explicit resolution");
        engine
            .translate_and_execute(
                "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'fresh'",
                &SqlTranslator,
            )
            .await
            .expect("external catalog commit");
        reads.take();
        let previous = old
            .translate_and_execute("SELECT * FROM items", &SqlTranslator)
            .await
            .expect("old coherent snapshot");
        assert_eq!(previous[0].columns.len(), 2);
        assert_eq!(
            old.row_conflicts("items", &key(1))
                .await
                .expect("old row still conflicted"),
            vec!["label"]
        );
        assert_eq!(reads.take(), (0, 0, 1));
        old.rollback().await.expect("finish old snapshot");
        let mut fresh = engine.transaction().await.expect("new snapshot");
        assert!(
            fresh
                .row_conflicts("items", &key(1))
                .await
                .expect("fresh resolution")
                .is_empty()
        );
        let current = fresh
            .translate_and_execute("SELECT label, extra FROM items", &SqlTranslator)
            .await
            .expect("current schema and defaults");
        assert_eq!(current[0].rows, vec![Row::from(["current", "fresh"])]);
        assert_eq!(reads.take(), (1, 1, 1));
        fresh.rollback().await.expect("finish fresh snapshot");
    });
}

#[test]
fn dml_ddl_and_mixed_batches_invalidate_before_reusing_catalogs() {
    block_on(async {
        let (engine, _, reads) = fixture().await;
        engine
            .resolve_row(
                "items",
                &key(1),
                vec![("label".into(), Value::from("settled"))],
            )
            .await
            .expect("resolve before ordinary DML");
        let mut transaction = engine.transaction().await.expect("mutable snapshot");
        transaction
            .row_conflicts("items", &key(1))
            .await
            .expect("warm conflicts");
        transaction
            .translate_and_execute("UPDATE items SET label = 'written'", &SqlTranslator)
            .await
            .expect("DML");
        reads.take();
        let results = transaction
            .translate_and_execute("SELECT label FROM items", &SqlTranslator)
            .await
            .expect("read own write");
        assert_eq!(results[0].rows, vec![Row::from(["written"])]);
        assert!(
            transaction
                .row_conflicts("items", &key(1))
                .await
                .expect("DML conflict inspection")
                .is_empty()
        );
        assert_eq!(
            reads.take(),
            (1, 1, 1),
            "DML conservatively discards even unchanged schema"
        );
        transaction
            .translate_and_execute(
                "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'default'",
                &SqlTranslator,
            )
            .await
            .expect("DDL");
        reads.take();
        assert!(
            transaction
                .row_conflicts("items", &key(1))
                .await
                .expect("conflict-first after DDL")
                .is_empty()
        );
        let results = transaction
            .translate_and_execute("SELECT label, extra FROM items", &SqlTranslator)
            .await
            .expect("DDL default");
        assert_eq!(results[0].rows, vec![Row::from(["written", "default"])]);
        assert_eq!(reads.take(), (1, 1, 1));
        let results = transaction.translate_and_execute("SELECT * FROM items; ALTER TABLE items ADD COLUMN amount INTEGER DEFAULT 7; SELECT * FROM items; UPDATE items SET label = 'mixed'", &SqlTranslator).await.expect("mixed batch");
        assert_eq!(results[0].columns.len(), 3);
        assert_eq!(results[2].columns.len(), 4);
        assert_eq!(results[2].rows[0].values[3], Value::Integer(7));
        reads.take();
        assert!(
            transaction
                .row_conflicts("items", &key(1))
                .await
                .expect("after mixed final write")
                .is_empty()
        );
        assert_eq!(reads.take(), (1, 1, 1));
        transaction
            .rollback()
            .await
            .expect("rollback local changes");
    });
}

#[test]
fn failed_write_cannot_use_cached_catalog_or_commit_partial_changes() {
    block_on(async {
        let (engine, _, reads) = fixture().await;
        let mut transaction = engine.transaction().await.expect("transaction");
        transaction
            .translate_and_execute("SELECT * FROM items", &SqlTranslator)
            .await
            .expect("warm catalog");
        transaction
            .translate_and_execute(
                "ALTER TABLE items ADD COLUMN extra TEXT; UPDATE missing SET label = 'bad'",
                &SqlTranslator,
            )
            .await
            .expect_err("failed batch");
        reads.take();
        transaction
            .row_conflicts("items", &key(1))
            .await
            .expect_err("aborted conflict inspection");
        transaction
            .translate_and_execute("SELECT * FROM items", &SqlTranslator)
            .await
            .expect_err("aborted SELECT");
        assert_eq!(reads.take(), (0, 0, 0));
        transaction
            .commit()
            .await
            .expect_err("aborted writes roll back");
        let results = engine
            .translate_and_execute("SELECT * FROM items", &SqlTranslator)
            .await
            .expect("no partial DDL");
        assert_eq!(results[0].columns.len(), 2);
        assert_eq!(
            engine
                .row_conflicts("items", &key(1))
                .await
                .expect("original conflict"),
            vec!["label"]
        );
    });
}

#[test]
fn cancelled_writes_discard_catalog_before_schema_ensure_and_after_actual_mutation() {
    block_on(async {
        let (engine, _, reads) = fixture().await;
        engine
            .resolve_row(
                "items",
                &key(1),
                vec![("label".into(), Value::from("settled"))],
            )
            .await
            .expect("resolve before cancellable DML");
        let mut transaction = engine.transaction().await.expect("transaction");
        transaction
            .translate_and_execute("SELECT * FROM items", &SqlTranslator)
            .await
            .expect("warm catalog");
        reads.pause_ensure.store(true, Ordering::Relaxed);
        assert!(
            transaction
                .translate_and_execute("UPDATE items SET label = 'not reached'", &SqlTranslator)
                .now_or_never()
                .is_none()
        );
        reads.take();
        assert!(
            transaction
                .row_conflicts("items", &key(1))
                .await
                .expect("inspect after cancelled ensure")
                .is_empty()
        );
        assert_eq!(
            reads.take(),
            (1, 1, 1),
            "invalidation precedes ensure_schema await"
        );
        reads.pause_merge.store(true, Ordering::Relaxed);
        assert!(
            transaction
                .translate_and_execute("UPDATE items SET label = 'cancelled'", &SqlTranslator)
                .now_or_never()
                .is_none()
        );
        reads.take();
        let results = transaction
            .translate_and_execute("SELECT label FROM items", &SqlTranslator)
            .await
            .expect("partially staged actual write is read afresh");
        assert_eq!(results[0].rows, vec![Row::from(["cancelled"])]);
        assert!(
            transaction
                .row_conflicts("items", &key(1))
                .await
                .expect("inspect actual changed row")
                .is_empty()
        );
        assert_eq!(reads.take(), (1, 1, 1));
        transaction
            .rollback()
            .await
            .expect("discard cancelled mutation");
        let results = engine
            .translate_and_execute("SELECT label FROM items", &SqlTranslator)
            .await
            .expect("rollback retains committed value");
        assert_eq!(results[0].rows, vec![Row::from(["settled"])]);
    });
}

#[test]
fn malformed_unrelated_live_catalogs_are_rejected_by_select_and_conflict_initializers() {
    block_on(async {
        for (storage, message) in [
            (ENGINE_TABLES_STORAGE, "Invalid table name"),
            (ENGINE_TABLE_FIELDS_STORAGE, "Invalid column table identity"),
        ] {
            let (engine, kernel, _) = fixture().await;
            let codec = AutomergeRowCodec::new();
            let mut raw = kernel
                .transaction()
                .await
                .expect("malformed catalog fixture");
            codec
                .put_row(
                    &mut raw,
                    storage,
                    id(90),
                    Row::new(vec![Value::Integer(42)]),
                )
                .await
                .expect("unrelated malformed row");
            raw.commit().await.expect("commit malformed catalog");
            let mut transaction = engine.transaction().await.expect("conflict-first snapshot");
            for _ in 0..2 {
                let error = transaction
                    .row_conflicts("items", &Row::new(vec![Value::Bool(true)]))
                    .await
                    .expect_err("complete catalog validation precedes key validation");
                assert_eq!(error.to_string(), format!("Error: {message}"));
            }
            transaction
                .rollback()
                .await
                .expect("finish rejected inspection");
            let mut transaction = engine.transaction().await.expect("SELECT-first snapshot");
            let error = transaction
                .translate_and_execute("SELECT * FROM items LIMIT 0", &SqlTranslator)
                .await
                .expect_err("SELECT validates unrelated live catalog");
            assert_eq!(error.to_string(), format!("Error: {message}"));
            transaction.rollback().await.expect("finish failed SELECT");
        }
    });
}

#[test]
fn cached_reads_preserve_defaults_row_tombstones_and_deleted_catalog_filtering() {
    block_on(async {
        let (engine, kernel, reads) = fixture().await;
        engine
            .translate_and_execute(
                "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'default'; DELETE FROM other",
                &SqlTranslator,
            )
            .await
            .expect("default and row tombstone");
        let codec = AutomergeRowCodec::new();
        let mut raw = kernel.transaction().await.expect("deleted catalog fixture");
        for (offset, storage) in [
            (90, ENGINE_TABLES_STORAGE),
            (91, ENGINE_TABLE_FIELDS_STORAGE),
        ] {
            codec
                .put_row(
                    &mut raw,
                    storage,
                    id(offset),
                    Row::new(vec![Value::Integer(42)]),
                )
                .await
                .expect("malformed catalog row");
            codec
                .delete_row(&mut raw, storage, &id(offset))
                .await
                .expect("deleted catalog row is not live schema");
        }
        raw.commit().await.expect("catalog tombstones");
        reads.take();
        let mut transaction = engine.transaction().await.expect("snapshot");
        assert_eq!(
            transaction
                .row_conflicts("items", &key(1))
                .await
                .expect("live conflict"),
            vec!["label"]
        );
        let results = transaction
            .translate_and_execute(
                "SELECT extra FROM items; SELECT * FROM other",
                &SqlTranslator,
            )
            .await
            .expect("defaults and deleted rows");
        assert_eq!(results[0].rows, vec![Row::from(["default"])]);
        assert!(results[1].rows.is_empty());
        assert_eq!(
            transaction
                .row_conflicts("other", &key(2))
                .await
                .expect("tombstone retains conflict history"),
            vec!["label"]
        );
        assert_eq!(reads.take(), (1, 1, 2));
        transaction.rollback().await.expect("finish snapshot");
    });
}

#[test]
fn cached_schema_does_not_skip_malformed_nonmatching_row_decoding() {
    block_on(async {
        let (engine, kernel, reads) = fixture().await;
        let mut raw = kernel.transaction().await.expect("malformed row fixture");
        let mut malformed = AutoCommit::new();
        malformed.put(ROOT, "label", 42_i64).expect("non-byte cell");
        {
            let changes =
                AutomergeChangeStore::new(engine::BytesTableTransaction::new(&mut raw, "items"));
            let mut documents = AutomergeBTreeTransaction::new(changes, ThresholdPolicy::default());
            documents
                .insert(id(99).as_bytes().to_vec(), malformed)
                .await
                .expect("raw malformed user row");
        }
        raw.commit().await.expect("commit malformed row");
        reads.take();
        let mut transaction = engine.transaction().await.expect("snapshot");
        transaction
            .translate_and_execute("SELECT * FROM items LIMIT 0", &SqlTranslator)
            .await
            .expect("metadata-only read warms catalog");
        assert_eq!(
            transaction
                .row_conflicts("items", &key(1))
                .await
                .expect("good row still inspected"),
            vec!["label"]
        );
        transaction.translate_and_execute("SELECT * FROM items WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID)", &SqlTranslator).await.expect_err("nonmatching malformed row is still decoded");
        assert_eq!(reads.take(), (1, 1, 1));
        transaction.rollback().await.expect("finish rejected read");
    });
}
