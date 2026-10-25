use std::ops::RangeBounds;

use btree::{BTree, BTreeRead, BTreeResult, BTreeTransaction};
use futures::{Stream, TryStreamExt, stream};

use crate::{Database, Snapshot, Table};

#[derive(Clone)]
pub struct ByteBTree {
    database: Database,
    table: String,
}

impl ByteBTree {
    /// A table handle; table creation is staged with its first mutation.
    pub fn new(database: Database, table: impl Into<String>) -> Self {
        Self {
            database,
            table: table.into(),
        }
    }
}

impl BTreeRead<Vec<u8>, Vec<u8>> for ByteBTree {
    async fn get(&self, key: &Vec<u8>) -> BTreeResult<Option<Vec<u8>>> {
        self.transaction().await?.get(key).await
    }
    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>> + Clone,
    {
        stream::once(async move {
            Ok::<_, btree::BTreeError>(
                self.transaction()
                    .await?
                    .data()
                    .range(range)
                    .map(|(k, v)| Ok((k.clone(), v.clone())))
                    .collect::<Vec<_>>(),
            )
        })
        .map_ok(stream::iter)
        .try_flatten()
    }
}

impl BTree<Vec<u8>, Vec<u8>> for ByteBTree {
    type Transaction = Transaction;
    async fn transaction(&self) -> BTreeResult<Transaction> {
        Ok(Transaction {
            snapshot: self.database.snapshot().await?,
            table: self.table.clone(),
            empty: Table::new(),
        })
    }
}

pub struct Transaction {
    snapshot: Snapshot,
    table: String,
    empty: Table,
}

impl Transaction {
    fn data(&self) -> &Table {
        self.snapshot.table(&self.table).unwrap_or(&self.empty)
    }
    fn data_mut(&mut self) -> &mut Table {
        self.snapshot.create_table(&self.table);
        self.snapshot
            .table_mut(&self.table)
            .expect("table was created")
    }
}

impl BTreeRead<Vec<u8>, Vec<u8>> for Transaction {
    async fn get(&self, key: &Vec<u8>) -> BTreeResult<Option<Vec<u8>>> {
        Ok(self.data().get(key).cloned())
    }
    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>> + Clone,
    {
        stream::iter(
            self.data()
                .range(range)
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect::<Vec<_>>(),
        )
    }
}

impl BTreeTransaction<Vec<u8>, Vec<u8>> for Transaction {
    async fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> BTreeResult<()> {
        self.data_mut().insert(key, value);
        Ok(())
    }
    async fn update<F>(&mut self, key: Vec<u8>, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut Vec<u8>) -> BTreeResult<()>,
    {
        let Some(mut value) = self.data().get(&key).cloned() else {
            return Ok(None);
        };
        update_fn(&mut value)?;
        self.data_mut().insert(key, value);
        Ok(Some(()))
    }
    async fn remove(&mut self, key: &Vec<u8>) -> BTreeResult<Option<Vec<u8>>> {
        if !self.data().contains_key(key) {
            return Ok(None);
        }
        Ok(self.data_mut().remove(key))
    }
    fn remove_range<R>(&mut self, range: R) -> impl Stream<Item = BTreeResult<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<Vec<u8>> + Clone,
    {
        let removed = self
            .data()
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        for (key, _) in &removed {
            self.data_mut().remove(key);
        }
        stream::iter(removed.into_iter().map(Ok))
    }
    async fn commit(mut self) -> BTreeResult<()> {
        if !self.snapshot.original_table_exists(&self.table) && self.data().is_empty() {
            self.snapshot.drop_table(&self.table);
        }
        self.snapshot.commit().await
    }
    async fn rollback(self) -> BTreeResult<()> {
        Ok(())
    }
}
