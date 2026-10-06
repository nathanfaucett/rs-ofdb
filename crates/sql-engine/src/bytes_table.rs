use alloc::string::String;
use alloc::vec::Vec;
use core::ops::{Bound, RangeBounds};

use async_stream::stream;
use btree::{BTreeError, BTreeRead, BTreeResult, BTreeTransaction};
use futures::{Stream, StreamExt, pin_mut};

use crate::KernelTransaction;

pub struct BytesTable<'a, T> {
    transaction: &'a T,
    table: String,
}

pub struct BytesTableTransaction<'a, T> {
    transaction: &'a mut T,
    table: String,
}

impl<'a, T> BytesTable<'a, T> {
    pub fn new(transaction: &'a T, table: &str) -> Self {
        Self {
            transaction,
            table: table.into(),
        }
    }
}

impl<'a, T> BytesTableTransaction<'a, T> {
    pub fn new(transaction: &'a mut T, table: &str) -> Self {
        Self {
            transaction,
            table: table.into(),
        }
    }
}

fn owned_bounds<R>(range: R) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
where
    R: RangeBounds<Vec<u8>>,
{
    let own = |bound: Bound<&Vec<u8>>| match bound {
        Bound::Included(key) => Bound::Included(key.clone()),
        Bound::Excluded(key) => Bound::Excluded(key.clone()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (own(range.start_bound()), own(range.end_bound()))
}

macro_rules! read {
    ($type:ty) => {
        impl<T: KernelTransaction + Send + Sync> BTreeRead<Vec<u8>, Vec<u8>> for $type {
            fn get(
                &self,
                key: &Vec<u8>,
            ) -> impl Future<Output = BTreeResult<Option<Vec<u8>>>> + Send {
                async move {
                    self.transaction
                        .get_bytes(&self.table, key)
                        .await
                        .map_err(BTreeError::custom)
                }
            }

            fn range<R>(
                &self,
                range: R,
            ) -> impl Stream<Item = BTreeResult<(Vec<u8>, Vec<u8>)>> + Send
            where
                R: RangeBounds<Vec<u8>> + Send,
            {
                let entries = self
                    .transaction
                    .scan_bytes_range(&self.table, owned_bounds(range));
                stream! {
                    pin_mut!(entries);
                    while let Some(entry) = entries.next().await {
                        yield entry.map_err(BTreeError::custom);
                    }
                }
            }
        }
    };
}

read!(BytesTable<'_, T>);
read!(BytesTableTransaction<'_, T>);

impl<T: KernelTransaction + Send + Sync> BTreeTransaction<Vec<u8>, Vec<u8>>
    for BytesTableTransaction<'_, T>
{
    async fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> BTreeResult<()> {
        self.transaction
            .put_bytes(&self.table, key, value)
            .await
            .map_err(BTreeError::custom)
    }

    async fn update<F>(&mut self, key: Vec<u8>, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut Vec<u8>) -> BTreeResult<()> + Send,
    {
        let Some(mut value) = self.get(&key).await? else {
            return Ok(None);
        };
        update_fn(&mut value)?;
        self.insert(key, value).await?;
        Ok(Some(()))
    }

    async fn remove(&mut self, key: &Vec<u8>) -> BTreeResult<Option<Vec<u8>>> {
        self.transaction
            .remove_bytes(&self.table, key)
            .await
            .map_err(BTreeError::custom)
    }

    fn remove_range<R>(
        &mut self,
        _: R,
    ) -> impl Stream<Item = BTreeResult<(Vec<u8>, Vec<u8>)>> + Send
    where
        R: RangeBounds<Vec<u8>> + Send,
    {
        stream! { yield Err(BTreeError::UnsupportedOperation); }
    }

    async fn commit(self) -> BTreeResult<()> {
        Ok(())
    }

    async fn rollback(self) -> BTreeResult<()> {
        Ok(())
    }
}
