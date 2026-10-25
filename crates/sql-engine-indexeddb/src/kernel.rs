use std::ops::Bound;

use btree::ChangeSubscription;
use btree_indexeddb::{Database, Snapshot};
use engine::{EngineError, EngineResult, Kernel, KernelTransaction};
use futures::{Stream, stream};

#[derive(Clone)]
pub struct IndexedDbKernel {
    database: Database,
}

impl IndexedDbKernel {
    pub async fn open(name: &str) -> EngineResult<Self> {
        Database::open(name, "sql")
            .await
            .map(|database| Self { database })
            .map_err(EngineError::custom)
    }
}

pub struct IndexedDbTransaction {
    snapshot: Snapshot,
}

impl Kernel for IndexedDbKernel {
    type Transaction = IndexedDbTransaction;

    fn subscribe_changes(&self) -> Option<ChangeSubscription> {
        Some(self.database.subscribe_changes())
    }

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        self.database
            .snapshot()
            .await
            .map(|snapshot| IndexedDbTransaction { snapshot })
            .map_err(EngineError::custom)
    }
}

impl KernelTransaction for IndexedDbTransaction {
    async fn ensure_table(&mut self, table: &str) -> EngineResult<()> {
        self.snapshot.create_table(table);
        Ok(())
    }

    async fn drop_table(&mut self, table: &str) -> EngineResult<()> {
        self.snapshot.drop_table(table);
        Ok(())
    }

    async fn get_bytes(&self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        Ok(self
            .snapshot
            .table(table)
            .and_then(|table| table.get(key))
            .cloned())
    }

    fn scan_bytes<'a>(
        &'a self,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + 'a {
        self.scan_bytes_range(table, (Bound::Unbounded, Bound::Unbounded))
    }

    fn scan_bytes_range<'a>(
        &'a self,
        table: &'a str,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + 'a {
        stream::iter(
            self.snapshot
                .table(table)
                .map(move |table| table.range(range))
                .into_iter()
                .flatten()
                .map(|(key, value)| Ok((key.clone(), value.clone()))),
        )
    }

    async fn put_bytes(&mut self, table: &str, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        self.snapshot.create_table(table);
        self.snapshot
            .table_mut(table)
            .expect("created logical table")
            .insert(key, value);
        Ok(())
    }

    async fn remove_bytes(&mut self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        Ok(self
            .snapshot
            .table_mut(table)
            .and_then(|table| table.remove(key)))
    }

    async fn commit(self) -> EngineResult<()> {
        self.snapshot.commit().await.map_err(EngineError::custom)
    }

    async fn rollback(self) -> EngineResult<()> {
        self.snapshot.rollback();
        Ok(())
    }
}
