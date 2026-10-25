use std::{marker::PhantomData, ops::RangeBounds};

use async_lock::MutexGuardArc;
use async_stream::stream;
use futures::Stream;
use redb::{ReadableTable, Table, TableHandle, WriteTransaction};

use btree::{BTreeError, BTreeRead, BTreeResult, BTreeTransaction, ChangeSignal};

use crate::{
    changes::Mutations,
    key::Key,
    redb::{RedbKey, RedbValue, table_definition},
    value::Value,
};

trait WriteTransactionHandle {
    fn native(&self) -> &NativeWriteTransaction;
    fn commit(self) -> BTreeResult<()>;
    fn rollback(self) -> BTreeResult<()>;
}

pub struct NativeWriteTransaction {
    pub(crate) inner: WriteTransaction,
    mutations: Mutations,
    changes: ChangeSignal,
    _admission: MutexGuardArc<()>,
}

impl NativeWriteTransaction {
    pub(crate) fn new(
        inner: WriteTransaction,
        admission: MutexGuardArc<()>,
        changes: ChangeSignal,
    ) -> Self {
        Self {
            inner,
            mutations: Mutations::default(),
            changes,
            _admission: admission,
        }
    }

    pub(crate) fn open_table<K: RedbKey, V: RedbValue>(
        &self,
        name: &str,
    ) -> BTreeResult<Table<'_, Key<K>, Value<V>>> {
        let exists = match self.mutations.table_exists(name) {
            Some(exists) => exists,
            None => self
                .inner
                .list_tables()
                .map_err(BTreeError::custom)?
                .any(|table| table.name() == name),
        };
        let table = self
            .inner
            .open_table(table_definition::<K, V>(name))
            .map_err(BTreeError::custom)?;
        self.record_table(name, exists, true);
        Ok(table)
    }

    fn open_existing_table<K: RedbKey, V: RedbValue>(
        &self,
        name: &str,
    ) -> BTreeResult<Option<Table<'_, Key<K>, Value<V>>>> {
        if !self
            .inner
            .list_tables()
            .map_err(BTreeError::custom)?
            .any(|table| table.name() == name)
        {
            return Ok(None);
        }
        self.inner
            .open_table(table_definition::<K, V>(name))
            .map(Some)
            .map_err(BTreeError::custom)
    }

    pub(crate) fn record(
        &self,
        table: &str,
        key: Vec<u8>,
        old: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    ) {
        self.mutations.record(table, key, old, new);
    }

    pub(crate) fn record_table(&self, name: &str, old: bool, new: bool) {
        self.mutations.table(name, old, new);
    }

    pub(crate) fn commit(self) -> BTreeResult<()> {
        let invalidations = self.mutations.invalidations();
        self.inner.commit().map_err(BTreeError::custom)?;
        self.changes.committed_with(invalidations);
        Ok(())
    }

    pub(crate) fn rollback(self) -> BTreeResult<()> {
        self.inner.abort().map_err(BTreeError::custom)
    }
}

impl WriteTransactionHandle for NativeWriteTransaction {
    fn native(&self) -> &NativeWriteTransaction {
        self
    }
    fn commit(self) -> BTreeResult<()> {
        self.commit()
    }
    fn rollback(self) -> BTreeResult<()> {
        self.rollback()
    }
}

impl WriteTransactionHandle for &NativeWriteTransaction {
    fn native(&self) -> &NativeWriteTransaction {
        self
    }
    fn commit(self) -> BTreeResult<()> {
        Ok(())
    }
    fn rollback(self) -> BTreeResult<()> {
        Ok(())
    }
}

pub struct RedbBTreeTransactionCore<T, K, V> {
    tx: T,
    name: String,
    _phantom_marker: PhantomData<fn(K, V)>,
}

pub type RedbBTreeTransaction<K, V> = RedbBTreeTransactionCore<NativeWriteTransaction, K, V>;
pub type RedbBTreeScopedTransaction<'a, K, V> =
    RedbBTreeTransactionCore<&'a NativeWriteTransaction, K, V>;

impl<T, K, V> RedbBTreeTransactionCore<T, K, V> {
    pub fn new(tx: T, name: impl Into<String>) -> Self {
        Self {
            tx,
            name: name.into(),
            _phantom_marker: PhantomData,
        }
    }
}

impl<T, K, V> BTreeRead<K, V> for RedbBTreeTransactionCore<T, K, V>
where
    T: WriteTransactionHandle + Send + Sync,
    K: RedbKey,
    V: RedbValue,
{
    async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
        let Some(table) = self.tx.native().open_existing_table::<K, V>(&self.name)? else {
            return Ok(None);
        };
        Ok(table
            .get(Key::new(key.clone()))
            .map_err(BTreeError::custom)?
            .map(|value| value.value().into_inner()))
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream! {
            let entries = {
                let Some(table) = self.tx.native().open_existing_table::<K, V>(&self.name)? else {
                    return;
                };
                let results = table.range(Key::range(range)).map_err(BTreeError::custom)?;
                let mut entries = Vec::new();
                for result in results {
                    let (key, value) = result.map_err(BTreeError::custom)?;
                    entries.push((key.value().into_inner(), value.value().into_inner()));
                }
                entries
            };
            for entry in entries { yield Ok(entry); }
        }
    }
}

impl<T, K, V> BTreeTransaction<K, V> for RedbBTreeTransactionCore<T, K, V>
where
    T: WriteTransactionHandle + Send + Sync,
    K: RedbKey,
    V: RedbValue,
{
    async fn insert(&mut self, key: K, value: V) -> BTreeResult<()> {
        let table_name = &self.name;
        let tx = self.tx.native();
        let mut table = tx.open_table::<K, V>(table_name)?;
        let old = table
            .get(Key::new(key.clone()))
            .map_err(BTreeError::custom)?
            .map(|entry| V::as_bytes(&entry.value().into_inner()).as_ref().to_vec());
        let new = V::as_bytes(&value).as_ref().to_vec();
        if old.as_ref() == Some(&new) {
            return Ok(());
        }
        table
            .insert(Key::new(key.clone()), Value::new(value))
            .map_err(BTreeError::custom)?;
        tx.record(
            table_name,
            K::as_bytes(&key).as_ref().to_vec(),
            old,
            Some(new),
        );
        Ok(())
    }

    async fn update<F>(&mut self, key: K, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut V) -> BTreeResult<()> + Send,
    {
        let Some(mut value) = self.get(&key).await? else {
            return Ok(None);
        };
        update_fn(&mut value)?;
        self.insert(key, value).await?;
        Ok(Some(()))
    }

    async fn remove(&mut self, key: &K) -> BTreeResult<Option<V>> {
        let tx = self.tx.native();
        let mut table = tx.open_table::<K, V>(&self.name)?;
        let value = table
            .remove(Key::new(key.clone()))
            .map_err(BTreeError::custom)?
            .map(|entry| entry.value().into_inner());
        if let Some(value) = &value {
            tx.record(
                &self.name,
                K::as_bytes(key).as_ref().to_vec(),
                Some(V::as_bytes(value).as_ref().to_vec()),
                None,
            );
        }
        Ok(value)
    }

    fn remove_range<R>(&mut self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream! {
            let tx = self.tx.native();
            let mut table = tx.open_table::<K, V>(&self.name)?;
            let results = table.range(Key::range(range)).map_err(BTreeError::custom)?;
            let mut keys = Vec::new();
            for result in results {
                let (key, value) = result.map_err(BTreeError::custom)?;
                let key: K = key.value().into_inner();
                let value: V = value.value().into_inner();
                keys.push((Key::new(key.clone()), V::as_bytes(&value).as_ref().to_vec()));
                yield Ok((key, value));
            }
            for (key, old) in keys {
                table.remove(&key).map_err(BTreeError::custom)?;
                tx.record(&self.name, <Key<K> as redb::Value>::as_bytes(&key).as_ref().to_vec(), Some(old), None);
            }
        }
    }

    async fn commit(self) -> BTreeResult<()> {
        self.tx.commit()
    }
    async fn rollback(self) -> BTreeResult<()> {
        self.tx.rollback()
    }
}
