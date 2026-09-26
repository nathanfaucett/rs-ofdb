use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use async_stream::stream;
use btree::{BTree, BTreeRead, BTreeResult, BTreeTransaction};
use futures::Stream;
use redb::Database;

use crate::{Bytes, RedbBTree, RedbBTreeTransaction};

#[derive(Clone)]
pub struct RedbByteBTree(RedbBTree<Bytes, Vec<u8>>);

impl RedbByteBTree {
    pub fn new(db: Arc<Database>, name: impl Into<String>) -> Self {
        Self(RedbBTree::new(db, name))
    }
}

fn byte_bounds<R: RangeBounds<Vec<u8>>>(range: R) -> (Bound<Bytes>, Bound<Bytes>) {
    let convert = |bound: Bound<&Vec<u8>>| match bound {
        Bound::Included(key) => Bound::Included(Bytes(key.clone())),
        Bound::Excluded(key) => Bound::Excluded(Bytes(key.clone())),
        Bound::Unbounded => Bound::Unbounded,
    };
    (convert(range.start_bound()), convert(range.end_bound()))
}

impl BTreeRead<Vec<u8>, Vec<u8>> for RedbByteBTree {
    async fn get(&self, key: &Vec<u8>) -> BTreeResult<Option<Vec<u8>>> {
        self.0.get(&Bytes(key.clone())).await
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(Vec<u8>, Vec<u8>)>> + Send
    where
        R: RangeBounds<Vec<u8>> + Send,
    {
        let entries = self.0.range(byte_bounds(range));
        stream! {
            futures::pin_mut!(entries);
            while let Some(entry) = futures::StreamExt::next(&mut entries).await {
                yield entry.map(|(key, value)| (key.0, value));
            }
        }
    }
}

impl BTree<Vec<u8>, Vec<u8>> for RedbByteBTree {
    type Transaction = RedbByteBTreeTransaction;

    async fn transaction(&self) -> BTreeResult<Self::Transaction> {
        Ok(RedbByteBTreeTransaction(self.0.transaction().await?))
    }
}

pub struct RedbByteBTreeTransaction(RedbBTreeTransaction<Bytes, Vec<u8>>);

impl BTreeRead<Vec<u8>, Vec<u8>> for RedbByteBTreeTransaction {
    async fn get(&self, key: &Vec<u8>) -> BTreeResult<Option<Vec<u8>>> {
        self.0.get(&Bytes(key.clone())).await
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(Vec<u8>, Vec<u8>)>> + Send
    where
        R: RangeBounds<Vec<u8>> + Send,
    {
        let entries = self.0.range(byte_bounds(range));
        stream! {
            futures::pin_mut!(entries);
            while let Some(entry) = futures::StreamExt::next(&mut entries).await {
                yield entry.map(|(key, value)| (key.0, value));
            }
        }
    }
}

impl BTreeTransaction<Vec<u8>, Vec<u8>> for RedbByteBTreeTransaction {
    async fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> BTreeResult<()> {
        self.0.insert(Bytes(key), value).await
    }

    async fn update<F>(&mut self, key: Vec<u8>, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut Vec<u8>) -> BTreeResult<()> + Send,
    {
        self.0.update(Bytes(key), update_fn).await
    }

    async fn remove(&mut self, key: &Vec<u8>) -> BTreeResult<Option<Vec<u8>>> {
        self.0.remove(&Bytes(key.clone())).await
    }

    fn remove_range<R>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = BTreeResult<(Vec<u8>, Vec<u8>)>> + Send
    where
        R: RangeBounds<Vec<u8>> + Send,
    {
        let entries = self.0.remove_range(byte_bounds(range));
        stream! {
            futures::pin_mut!(entries);
            while let Some(entry) = futures::StreamExt::next(&mut entries).await {
                yield entry.map(|(key, value)| (key.0, value));
            }
        }
    }

    async fn commit(self) -> BTreeResult<()> {
        self.0.commit().await
    }

    async fn rollback(self) -> BTreeResult<()> {
        self.0.rollback().await
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use futures::{StreamExt, executor::block_on};

    use super::*;

    #[test]
    fn byte_keys_use_lexicographic_ranges() {
        block_on(async {
            let path: PathBuf =
                std::env::temp_dir().join(format!("byte-tree-{}.db", uuid::Uuid::now_v7()));
            let db = Arc::new(Database::create(&path).expect("create database"));
            let tree = RedbByteBTree::new(db, "bytes");
            let mut tx = tree.transaction().await.expect("begin transaction");
            for key in [vec![0], vec![0, 1], vec![1]] {
                tx.insert(key.clone(), key).await.expect("insert key");
            }
            tx.commit().await.expect("commit");

            let entries = tree.range(vec![0]..vec![1]).collect::<Vec<_>>().await;
            let keys = entries
                .into_iter()
                .map(|entry| entry.expect("range").0)
                .collect::<Vec<_>>();
            assert_eq!(keys, vec![vec![0], vec![0, 1]]);
            drop(tree);
            std::fs::remove_file(path).expect("remove database");
        });
    }
}
