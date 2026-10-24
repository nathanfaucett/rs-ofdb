#[cfg(not(feature = "std"))]
use alloc::sync::Arc;
#[cfg(feature = "std")]
use std::sync::Arc;

use dashmap::{DashMap, Entry, mapref::one::RefMut};
use std::collections::btree_map::BTreeMap;

use core::{hash::Hash, marker::PhantomData, ops::RangeBounds};

use async_stream::stream;
use futures::{Stream, StreamExt, pin_mut};

use crate::{BTree, BTreeKey, BTreeRead, BTreeResult, BTreeTransaction, BTreeValue};

/// A trait for extracting a stable routing token from a key.
pub trait Sharder<K>: Sync + Send {
    type Key: Eq + Hash + Sync + Send + Clone;

    /// Extracts the stable routing token from the key.
    fn shard(&self, key: &K) -> Self::Key;
}

/// Creates a concrete BTree for a given routing token.
pub trait BTreeShardFactory<K, V, Key>: Send + Sync
where
    K: BTreeKey + Send + Sync,
    V: BTreeValue + Send + Sync,
    Key: Clone + Eq + Hash + Send + Sync,
{
    type BTree: BTree<K, V>;

    /// Build (or open) a shard for the given token.
    /// May be async if opening involves I/O.
    fn create(&self, token: &Key) -> impl Future<Output = BTreeResult<Self::BTree>> + Send;
    /// Delete a shard for the given token.
    /// May be async if deletion involves I/O.
    fn delete(&self, token: &Key) -> impl Future<Output = BTreeResult<()>> + Send;
}

/// A Sharded BTree that supports a dynamic number of shards.
pub struct ShardedBTree<K, V, F, S>
where
    K: BTreeKey + Send + Sync,
    V: BTreeValue + Send + Sync,
    F: BTreeShardFactory<K, V, S::Key>,
    S: Sharder<K>,
{
    factory: Arc<F>,
    shards: Arc<DashMap<S::Key, F::BTree>>,
    sharder: Arc<S>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V, F, S> ShardedBTree<K, V, F, S>
where
    K: BTreeKey + Send + Sync,
    V: BTreeValue + Send + Sync,
    F: BTreeShardFactory<K, V, S::Key>,
    S: Sharder<K>,
{
    pub fn new(factory: F, sharder: S) -> Self {
        Self {
            factory: Arc::new(factory),
            shards: Arc::new(DashMap::new()),
            sharder: Arc::new(sharder),
            _marker: PhantomData,
        }
    }

    /// Dynamically add a shard to the keyspace routing table
    pub fn add_shard(&mut self, token_boundary: S::Key, shard: F::BTree) {
        self.shards.insert(token_boundary, shard);
    }

    /// Dynamically remove a shard from the routing table
    pub fn remove_shard(&mut self, token_boundary: &S::Key) -> Option<F::BTree> {
        self.shards.remove(token_boundary).map(|(_, shard)| shard)
    }
}

impl<K, V, F, S> BTreeRead<K, V> for ShardedBTree<K, V, F, S>
where
    K: BTreeKey + Send + Sync + 'static,
    V: BTreeValue + Send + Sync + 'static,
    F: BTreeShardFactory<K, V, S::Key> + 'static,
    S: Sharder<K> + 'static,
{
    async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
        let token = self.sharder.shard(key);
        let shard = match self.shards.entry(token) {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => {
                let shard = self.factory.create(entry.key()).await?;
                entry.insert(shard)
            }
        };
        shard.get(key).await
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Clone + Send,
    {
        let shards = self.shards.clone();

        stream!({
            let mut items = BTreeMap::new();

            for entry in shards.iter() {
                let s = entry.value().range(range.clone());
                pin_mut!(s);

                while let Some(item) = s.next().await {
                    let (k, v) = item?;
                    items.insert(k, v);
                }
            }

            for item in items {
                yield Ok(item);
            }
        })
    }
}

impl<K, V, F, S> BTree<K, V> for ShardedBTree<K, V, F, S>
where
    K: BTreeKey + Send + Sync + 'static,
    V: BTreeValue + Send + Sync + 'static,
    F: BTreeShardFactory<K, V, S::Key> + 'static,
    S: Sharder<K> + 'static,
{
    type Transaction = ShardedBTreeTransaction<K, V, F, S>;

    async fn transaction(&self) -> BTreeResult<Self::Transaction> {
        let txs = DashMap::new();
        for entry in self.shards.iter() {
            let token = entry.key().clone();
            let shard = entry.value();
            txs.insert(token, shard.transaction().await?);
        }

        Ok(ShardedBTreeTransaction {
            transactions: txs,
            shards: self.shards.clone(),
            factory: self.factory.clone(),
            sharder: self.sharder.clone(),
            _marker: PhantomData,
        })
    }
}

pub struct ShardedBTreeTransaction<K, V, F, S>
where
    K: BTreeKey + Send + Sync,
    V: BTreeValue + Send + Sync,
    F: BTreeShardFactory<K, V, S::Key>,
    S: Sharder<K>,
{
    transactions: DashMap<S::Key, <F::BTree as BTree<K, V>>::Transaction>,
    shards: Arc<DashMap<S::Key, F::BTree>>,
    factory: Arc<F>,
    sharder: Arc<S>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V, F, S> ShardedBTreeTransaction<K, V, F, S>
where
    K: BTreeKey + Send + Sync + 'static,
    V: BTreeValue + Send + Sync + 'static,
    F: BTreeShardFactory<K, V, S::Key> + 'static,
    S: Sharder<K> + 'static,
{
    /// Resolves an existing transaction or lazily initializes a new BTree shard using `B::default()`
    async fn get_or_create_transaction(
        &self,
        token: S::Key,
    ) -> BTreeResult<RefMut<'_, S::Key, <F::BTree as BTree<K, V>>::Transaction>> {
        if let Some(tx) = self.transactions.get_mut(&token) {
            return Ok(tx);
        }

        match self.transactions.entry(token.clone()) {
            Entry::Occupied(entry) => Ok(entry.into_ref()),
            Entry::Vacant(entry) => {
                let shard = match self.shards.entry(token) {
                    Entry::Occupied(entry) => entry.into_ref(),
                    Entry::Vacant(entry) => {
                        let shard = self.factory.create(entry.key()).await?;
                        entry.insert(shard)
                    }
                };
                Ok(entry.insert(shard.transaction().await?))
            }
        }
    }
}

impl<K, V, F, S> BTreeRead<K, V> for ShardedBTreeTransaction<K, V, F, S>
where
    K: BTreeKey + Send + Sync + 'static,
    V: BTreeValue + Send + Sync + 'static,
    F: BTreeShardFactory<K, V, S::Key> + 'static,
    S: Sharder<K> + 'static,
{
    async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
        let token = self.sharder.shard(key);
        let tx = self.get_or_create_transaction(token).await?;
        tx.get(key).await
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Clone + Send,
    {
        stream!({
            for entry in self.shards.iter() {
                let _ = self.get_or_create_transaction(entry.key().clone()).await?;
            }

            let mut items = BTreeMap::new();
            for entry in self.transactions.iter() {
                let s = entry.value().range(range.clone());
                pin_mut!(s);

                while let Some(item) = s.next().await {
                    let (k, v) = item?;
                    items.insert(k, v);
                }
            }

            for item in items {
                yield Ok(item);
            }
        })
    }
}

impl<K, V, F, S> BTreeTransaction<K, V> for ShardedBTreeTransaction<K, V, F, S>
where
    K: BTreeKey + Send + Sync + 'static,
    V: BTreeValue + Send + Sync + 'static,
    F: BTreeShardFactory<K, V, S::Key> + 'static,
    S: Sharder<K> + 'static,
{
    async fn insert(&mut self, key: K, value: V) -> BTreeResult<()> {
        let token = self.sharder.shard(&key);
        let mut tx = self.get_or_create_transaction(token).await?;
        tx.insert(key, value).await
    }

    async fn update<U>(&mut self, key: K, update_fn: U) -> BTreeResult<Option<()>>
    where
        U: FnOnce(&mut V) -> BTreeResult<()> + Send,
    {
        let token = self.sharder.shard(&key);
        let mut tx = self.get_or_create_transaction(token).await?;
        tx.update(key, update_fn).await
    }

    async fn remove(&mut self, key: &K) -> BTreeResult<Option<V>> {
        let token = self.sharder.shard(key);
        let mut tx = self.get_or_create_transaction(token).await?;
        tx.remove(key).await
    }

    fn remove_range<R>(&mut self, _range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Clone + Send,
    {
        stream!({
            for entry in self.shards.iter() {
                let _ = self.get_or_create_transaction(entry.key().clone()).await?;
            }

            let mut items = BTreeMap::new();
            for mut entry in self.transactions.iter_mut() {
                let s = entry.value_mut().remove_range(_range.clone());
                pin_mut!(s);

                while let Some(item) = s.next().await {
                    let (k, v) = item?;
                    items.insert(k, v);
                }
            }

            for item in items {
                yield Ok(item);
            }
        })
    }

    async fn commit(self) -> BTreeResult<()> {
        for (_, tx) in self.transactions.into_iter() {
            tx.commit().await?;
        }
        Ok(())
    }

    async fn rollback(self) -> BTreeResult<()> {
        for (_, tx) in self.transactions.into_iter() {
            tx.rollback().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core::hash::{Hash, Hasher};
    use std::{hash::DefaultHasher, sync::Mutex};

    use futures::executor::block_on;

    use crate::InMemoryBTreeShardFactory;

    use super::*;

    struct BooleanSharder<T, H> {
        hasher: Mutex<H>,
        _marker: PhantomData<T>,
    }

    impl Default for BooleanSharder<i32, DefaultHasher> {
        fn default() -> Self {
            Self {
                hasher: Mutex::new(DefaultHasher::new()),
                _marker: PhantomData,
            }
        }
    }

    impl<T, H> Sharder<T> for BooleanSharder<T, H>
    where
        T: Hash + Send + Sync,
        H: Hasher + Send + Sync,
    {
        type Key = bool;

        fn shard(&self, key: &T) -> Self::Key {
            let mut hasher = self.hasher.lock().expect("failed to lock hasher");
            key.hash(&mut *hasher);
            hasher.finish().is_multiple_of(2)
        }
    }

    #[test]
    fn smoke_test() {
        block_on(async {
            let sharder = BooleanSharder::default();
            let factory = InMemoryBTreeShardFactory::default();
            let btree = ShardedBTree::new(factory, sharder);

            let mut tx = btree
                .transaction()
                .await
                .expect("failed to start transaction");
            tx.insert(1, "one".to_string())
                .await
                .expect("failed to insert one");
            tx.insert(2, "two".to_string())
                .await
                .expect("failed to insert two");
            tx.commit().await.expect("failed to commit transaction");

            let value = btree.get(&1).await.expect("failed to get one");
            assert_eq!(value, Some("one".to_string()));
            let value = btree.get(&2).await.expect("failed to get two");
            assert_eq!(value, Some("two".to_string()));
        });
    }
}
