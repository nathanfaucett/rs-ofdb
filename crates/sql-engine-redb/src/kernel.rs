use std::{ops::Bound, sync::Arc};

use async_stream::stream;
use btree::{BTreeRead, BTreeTransaction};
use btree_redb::{Bytes, RedbDatabase, RedbDatabaseTransaction};
use engine::{EngineError, EngineResult, Kernel, KernelTransaction};

use futures::Stream;

#[derive(Clone)]
pub struct RedbKernel {
    database: RedbDatabase,
}

impl RedbKernel {
    pub fn new(database: Arc<redb::Database>) -> Self {
        Self {
            database: RedbDatabase::new(database),
        }
    }

    pub fn from_database(database: RedbDatabase) -> Self {
        Self { database }
    }
}

pub struct RedbKernelTransaction {
    pub(crate) database: RedbDatabaseTransaction,
    decoded_budget: Option<std::sync::Mutex<(usize, usize)>>,
}

impl Kernel for RedbKernel {
    type Transaction = RedbKernelTransaction;

    fn subscribe_changes(&self) -> Option<btree::ChangeSubscription> {
        Some(self.database.subscribe_changes())
    }

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        self.database
            .transaction()
            .await
            .map(|database| RedbKernelTransaction {
                database,
                decoded_budget: None,
            })
            .map_err(EngineError::custom)
    }
}

impl RedbKernelTransaction {
    pub(crate) fn entries(
        &self,
        table: &str,
    ) -> btree_redb::RedbBTreeScopedTransaction<'_, Bytes, Bytes> {
        self.database.table(table)
    }
}

impl KernelTransaction for RedbKernelTransaction {
    fn set_read_limits(&mut self, entries: usize, bytes: usize) -> EngineResult<()> {
        self.database
            .set_read_limits(entries, bytes)
            .map_err(EngineError::custom)?;
        self.decoded_budget = Some(std::sync::Mutex::new((entries, bytes)));
        Ok(())
    }

    fn check_read_row(&self, row: &value::Row) -> EngineResult<()> {
        let Some(budget) = self.decoded_budget.as_ref() else {
            return Ok(());
        };
        let mut budget = budget.lock().expect("decoded read budget lock");
        let (entries, remaining) = &mut *budget;
        let mut bytes = core::mem::size_of::<value::Row>();
        for value in &row.values {
            let size = match value {
                value::Value::Text(text) => text.len(),
                value::Value::Blob(blob) => blob.len(),
                value::Value::Json(_) | value::Value::Type(_) => {
                    return Err(EngineError::Unsupported("bounded decoded value"));
                }
                _ => 0,
            };
            bytes = bytes
                .saturating_add(core::mem::size_of::<value::Value>())
                .saturating_add(size);
        }
        if *entries == 0 || bytes > *remaining {
            return Err(EngineError::LimitExceeded);
        }
        *entries -= 1;
        *remaining -= bytes;
        Ok(())
    }

    fn read_limit_exceeded(&self) -> bool {
        self.database.read_limit_exceeded()
    }

    async fn ensure_table(&mut self, table: &str) -> EngineResult<()> {
        self.database
            .create_table::<Bytes, Bytes>(table)
            .map_err(EngineError::custom)
    }

    async fn drop_table(&mut self, table: &str) -> EngineResult<()> {
        self.database
            .drop_table::<Bytes, Bytes>(table)
            .map(|_| ())
            .map_err(EngineError::custom)
    }

    async fn get_bytes(&self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.entries(table)
            .get(&Bytes(key.to_vec()))
            .await
            .map(|value| value.map(|value| value.0))
            .map_err(EngineError::custom)
    }

    fn scan_bytes<'a>(
        &'a self,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + Send + 'a {
        self.scan_bytes_range(table, (Bound::Unbounded, Bound::Unbounded))
    }

    fn scan_bytes_range<'a>(
        &'a self,
        table: &'a str,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + Send + 'a {
        stream! {
            let name = table.to_string();
            let entries = self.entries(&name);
            let map_bound = |bound| match bound {
                Bound::Included(key) => Bound::Included(Bytes(key)),
                Bound::Excluded(key) => Bound::Excluded(Bytes(key)),
                Bound::Unbounded => Bound::Unbounded,
            };
            let range = (map_bound(range.0), map_bound(range.1));
            for await entry in entries.range(range) {
                yield entry.map(|(key, value)| (key.0, value.0)).map_err(EngineError::custom);
            }
        }
    }

    async fn put_bytes(&mut self, table: &str, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        self.entries(table)
            .insert(Bytes(key), Bytes(value))
            .await
            .map_err(EngineError::custom)
    }

    async fn remove_bytes(&mut self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.entries(table)
            .remove(&Bytes(key.to_vec()))
            .await
            .map(|value| value.map(|value| value.0))
            .map_err(EngineError::custom)
    }

    async fn commit(self) -> EngineResult<()> {
        self.database.commit().map_err(EngineError::custom)
    }

    async fn rollback(self) -> EngineResult<()> {
        self.database.rollback().map_err(EngineError::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use futures::executor::block_on;
    use uuid::Uuid;

    use super::RedbKernel;
    use engine::{Kernel, KernelTransaction};

    #[test]
    fn missing_table_reads_do_not_create_physical_tables() {
        use std::ops::Bound;

        use futures::StreamExt;
        use redb::ReadableDatabase;

        let database = Arc::new(
            redb::Database::builder()
                .create_with_backend(redb::backends::InMemoryBackend::new())
                .expect("in-memory Redb backend"),
        );
        let kernel = RedbKernel::new(database.clone());
        block_on(async {
            let mut changes = kernel.subscribe_changes().expect("subscription");
            let initial = changes.select().expect("initial work");
            assert!(changes.acknowledge(initial));
            let reader = kernel.transaction().await.expect("read transaction");
            assert_eq!(
                reader
                    .get_bytes("missing", b"key")
                    .await
                    .expect("missing key"),
                None
            );
            assert!(
                reader
                    .scan_bytes("missing")
                    .collect::<Vec<_>>()
                    .await
                    .is_empty()
            );
            assert!(
                reader
                    .scan_bytes_range(
                        "missing",
                        (
                            Bound::Included(b"a".to_vec()),
                            Bound::Excluded(b"z".to_vec())
                        )
                    )
                    .collect::<Vec<_>>()
                    .await
                    .is_empty()
            );
            reader.commit().await.expect("commit read-only transaction");
            assert!(
                changes.select().is_none(),
                "reads do not publish table creation"
            );
        });
        assert_eq!(
            database
                .begin_read()
                .expect("physical snapshot")
                .list_tables()
                .expect("physical tables")
                .count(),
            0
        );
    }

    #[test]
    fn logical_names_are_isolated_and_persisted() {
        let path = PathBuf::from(format!(
            "{}-engine-redb-names.redb",
            std::env::temp_dir()
                .join(Uuid::now_v7().to_string())
                .display()
        ));
        let database = Arc::new(redb::Database::create(&path).unwrap());
        let kernel = RedbKernel::new(database.clone());
        block_on(async {
            let mut transaction = kernel.transaction().await.unwrap();
            transaction.ensure_table("alpha").await.unwrap();
            transaction.ensure_table("beta").await.unwrap();
            transaction
                .put_bytes("alpha", b"key".to_vec(), b"alpha".to_vec())
                .await
                .unwrap();
            transaction
                .put_bytes("beta", b"key".to_vec(), b"beta".to_vec())
                .await
                .unwrap();
            transaction.commit().await.unwrap();

            let transaction = kernel.transaction().await.unwrap();
            assert_eq!(
                transaction.get_bytes("alpha", b"key").await.unwrap(),
                Some(b"alpha".to_vec())
            );
            assert_eq!(
                transaction.get_bytes("beta", b"key").await.unwrap(),
                Some(b"beta".to_vec())
            );
            transaction.rollback().await.unwrap();
        });
        drop(kernel);
        drop(database);
        std::fs::remove_file(path).unwrap();
    }
}
