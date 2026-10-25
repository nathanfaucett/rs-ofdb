#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};

use futures::{Stream, StreamExt, pin_mut};

use core::ops::RangeBounds;

use platform::{MaybeSend, MaybeSync};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BTreeError {
    #[error("Invalid document")]
    InvalidDocument,

    #[error("Type mismatch")]
    TypeMismatch,

    #[error("Conflict")]
    Conflict,

    #[error("Commit failed")]
    CommitFailed,

    #[error("Rollback failed")]
    RollbackFailed,

    #[error("Unsupported operation")]
    UnsupportedOperation,

    #[error("Error: {0}")]
    Custom(String),
}

impl BTreeError {
    pub fn custom<T>(error: T) -> Self
    where
        T: ToString,
    {
        Self::Custom(error.to_string())
    }
}

pub type BTreeResult<T> = Result<T, BTreeError>;

pub trait BTreeValue {}

impl<T> BTreeValue for T where T: {}

pub trait BTreeKey: BTreeValue + Ord {}

impl<T> BTreeKey for T where T: BTreeValue + Ord {}

pub trait BTreeRead<K, V>: MaybeSend + MaybeSync
where
    K: BTreeKey + MaybeSend + MaybeSync,
    V: BTreeValue + MaybeSend + MaybeSync,
{
    fn get(&self, key: &K) -> impl Future<Output = BTreeResult<Option<V>>> + MaybeSend;
    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + MaybeSend
    where
        R: RangeBounds<K> + Clone + MaybeSend;
    fn is_empty(&self) -> impl Future<Output = BTreeResult<bool>> + MaybeSend {
        async move {
            let s = self.range(..);
            pin_mut!(s);
            Ok(s.next().await.is_none())
        }
    }
}

pub trait BTreeTransaction<K, V>: BTreeRead<K, V> + MaybeSend + MaybeSync
where
    K: BTreeKey + MaybeSend + MaybeSync,
    V: BTreeValue + MaybeSend + MaybeSync,
{
    fn insert(&mut self, key: K, value: V) -> impl Future<Output = BTreeResult<()>> + MaybeSend;
    fn update<F>(
        &mut self,
        key: K,
        update_fn: F,
    ) -> impl Future<Output = BTreeResult<Option<()>>> + MaybeSend
    where
        F: FnOnce(&mut V) -> BTreeResult<()> + MaybeSend;

    fn remove(&mut self, key: &K) -> impl Future<Output = BTreeResult<Option<V>>> + MaybeSend;
    fn remove_range<R>(&mut self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + MaybeSend
    where
        R: RangeBounds<K> + Clone + MaybeSend;

    fn commit(self) -> impl Future<Output = BTreeResult<()>> + MaybeSend;
    fn rollback(self) -> impl Future<Output = BTreeResult<()>> + MaybeSend;
}

pub trait BTree<K, V>: BTreeRead<K, V>
where
    K: BTreeKey + MaybeSend + MaybeSync,
    V: BTreeValue + MaybeSend + MaybeSync,
{
    type Transaction: BTreeTransaction<K, V>;

    fn transaction(&self) -> impl Future<Output = BTreeResult<Self::Transaction>> + MaybeSend;
}
