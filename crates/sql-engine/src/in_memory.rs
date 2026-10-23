use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    sync::Arc,
    vec,
    vec::Vec,
};

use async_lock::RwLock;

use futures::{Stream, stream};

use crate::{
    EngineError, EngineResult,
    kernel::{Kernel, KernelTransaction},
};

type Tables = BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>;

struct State {
    revision: u64,
    tables: Tables,
}

#[derive(Clone)]
pub struct InMemoryKernel {
    state: Arc<RwLock<State>>,
    #[cfg(feature = "std")]
    changes: btree::ChangeSignal,
}

impl InMemoryKernel {
    #[cfg(feature = "std")]
    pub fn subscribe_changes(&self) -> btree::ChangeSubscription {
        self.changes.subscribe()
    }

    pub fn new() -> Self {
        Self {
            #[cfg(feature = "std")]
            changes: btree::ChangeSignal::default(),
            state: Arc::new(RwLock::new(State {
                revision: 0,
                tables: Tables::new(),
            })),
        }
    }
}

impl Default for InMemoryKernel {
    fn default() -> Self {
        Self::new()
    }
}

pub struct InMemoryKernelTransaction {
    state: Arc<RwLock<State>>,
    revision: u64,
    tables: Tables,
    touched_tables: BTreeSet<String>,
    touched_keys: BTreeSet<(String, Vec<u8>)>,
    #[cfg(feature = "std")]
    changes: btree::ChangeSignal,
}

impl Kernel for InMemoryKernel {
    type Transaction = InMemoryKernelTransaction;

    #[cfg(feature = "std")]
    fn subscribe_changes(&self) -> Option<btree::ChangeSubscription> {
        Some(InMemoryKernel::subscribe_changes(self))
    }

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        let state = self.state.read().await;
        Ok(InMemoryKernelTransaction {
            state: self.state.clone(),
            revision: state.revision,
            tables: state.tables.clone(),
            touched_tables: BTreeSet::new(),
            touched_keys: BTreeSet::new(),
            #[cfg(feature = "std")]
            changes: self.changes.clone(),
        })
    }
}

impl InMemoryKernelTransaction {
    fn table(&self, table: &str) -> EngineResult<&BTreeMap<Vec<u8>, Vec<u8>>> {
        self.tables
            .get(table)
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    fn table_mut(&mut self, table: &str) -> EngineResult<&mut BTreeMap<Vec<u8>, Vec<u8>>> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    fn scan(&self, table: &str) -> Vec<EngineResult<(Vec<u8>, Vec<u8>)>> {
        match self.table(table) {
            Ok(table) => table
                .iter()
                .map(|(key, value)| Ok((key.clone(), value.clone())))
                .collect(),
            Err(error) => vec![Err(error)],
        }
    }
}

impl KernelTransaction for InMemoryKernelTransaction {
    async fn ensure_table(&mut self, table: &str) -> EngineResult<()> {
        if !self.tables.contains_key(table) {
            self.touched_tables.insert(table.to_string());
        }
        self.tables.entry(table.to_string()).or_default();
        Ok(())
    }

    async fn drop_table(&mut self, table: &str) -> EngineResult<()> {
        self.touched_tables.insert(table.to_string());
        self.tables
            .remove(table)
            .map(|_| ())
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    async fn get_bytes(&self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        Ok(self.table(table)?.get(key).cloned())
    }

    fn scan_bytes<'a>(
        &'a self,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + Send + 'a {
        stream::iter(self.scan(table))
    }

    fn scan_bytes_range<'a>(
        &'a self,
        table: &'a str,
        range: (core::ops::Bound<Vec<u8>>, core::ops::Bound<Vec<u8>>),
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + Send + 'a {
        async_stream::stream! {
            match self.table(table) {
                Ok(rows) => {
                    for (key, value) in rows.range(range) {
                        yield Ok((key.clone(), value.clone()));
                    }
                }
                Err(error) => yield Err(error),
            }
        }
    }

    async fn put_bytes(&mut self, table: &str, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        self.touched_keys.insert((table.to_string(), key.clone()));
        self.table_mut(table)?.insert(key, value);
        Ok(())
    }

    async fn remove_bytes(&mut self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.touched_keys.insert((table.to_string(), key.to_vec()));
        Ok(self.table_mut(table)?.remove(key))
    }

    async fn commit(self) -> EngineResult<()> {
        let InMemoryKernelTransaction {
            state,
            revision,
            tables,
            touched_tables,
            touched_keys,
            #[cfg(feature = "std")]
            changes,
        } = self;
        let mut state = state.write().await;
        if state.revision != revision {
            return Err(EngineError::custom("Transaction conflict"));
        }
        #[cfg(feature = "std")]
        let invalidations = touched_tables
            .iter()
            .filter(|table| state.tables.get(*table) != tables.get(*table))
            .map(|table| btree::StorageInvalidation::Table(table.clone()))
            .chain(
                touched_keys
                    .iter()
                    .filter(|(table, key)| {
                        state.tables.get(table).and_then(|rows| rows.get(key))
                            != tables.get(table).and_then(|rows| rows.get(key))
                    })
                    .map(|(table, key)| btree::StorageInvalidation::Key {
                        table: table.clone(),
                        key: key.clone(),
                    }),
            )
            .collect::<Vec<_>>();
        #[cfg(not(feature = "std"))]
        let _ = (touched_tables, touched_keys);
        state.tables = tables;
        state.revision += 1;
        #[cfg(feature = "std")]
        changes.committed_with(invalidations);
        Ok(())
    }

    async fn rollback(self) -> EngineResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;
    use core::ops::Bound;

    use futures::{StreamExt, executor::block_on};

    use super::InMemoryKernel;
    use crate::{Kernel, KernelTransaction};

    #[test]
    fn logical_names_are_isolated() {
        block_on(async {
            let kernel = InMemoryKernel::new();
            let first = "first";
            let second = "second";
            let key = b"key";

            let mut transaction = kernel.transaction().await.unwrap();
            transaction.ensure_table(first).await.unwrap();
            transaction.ensure_table(second).await.unwrap();
            transaction
                .put_bytes(first, key.to_vec(), b"first".to_vec())
                .await
                .unwrap();
            transaction.commit().await.unwrap();

            let transaction = kernel.transaction().await.unwrap();
            assert_eq!(
                transaction.get_bytes(first, key).await.unwrap(),
                Some(b"first".to_vec())
            );
            assert_eq!(transaction.get_bytes(second, key).await.unwrap(), None);
            transaction.rollback().await.unwrap();
        });
    }

    #[test]
    fn byte_range_scan_returns_only_ordered_matching_keys() {
        block_on(async {
            let kernel = InMemoryKernel::new();
            let mut transaction = kernel.transaction().await.unwrap();
            transaction.ensure_table("range").await.unwrap();
            for key in [b"a".as_slice(), b"b", b"c", b"d"] {
                transaction
                    .put_bytes("range", key.to_vec(), key.to_vec())
                    .await
                    .unwrap();
            }
            let rows = transaction
                .scan_bytes_range(
                    "range",
                    (
                        Bound::Included(b"b".to_vec()),
                        Bound::Excluded(b"d".to_vec()),
                    ),
                )
                .map(|row| row.unwrap().0)
                .collect::<Vec<_>>()
                .await;
            assert_eq!(rows, [b"b".to_vec(), b"c".to_vec()]);
        });
    }
}
