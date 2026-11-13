use std::{marker::PhantomData, ops::RangeBounds, sync::Arc};

use async_lock::MutexGuardArc;
use async_stream::stream;
use futures::Stream;
use redb::{ReadableTable, Table, TableHandle, WriteTransaction};

use btree::{
    BTreeError, BTreeRead, BTreeResult, BTreeTransaction, ChangeEvent, ChangeSignal, StorageKey,
};

use crate::{
    changes::Mutations,
    key::Key,
    read_budget::{Borrowed, ReadBudget},
    redb::{RedbKey, RedbValue, table_definition},
    value::Value,
};

type BorrowedTable<'a, K, V> = Table<'a, Borrowed<Key<K>>, Borrowed<Value<V>>>;

trait WriteTransactionHandle {
    fn native(&self) -> &NativeWriteTransaction;
    fn commit(self) -> BTreeResult<()>;
    fn rollback(self) -> BTreeResult<()>;
}

pub struct NativeWriteTransaction {
    backend: WriteBackend,
    mutations: Mutations,
    pub read_budget: ReadBudget,
    changes: ChangeSignal,
}

enum WriteBackend {
    Native {
        inner: Box<WriteTransaction>,
        _admission: MutexGuardArc<()>,
    },
    Shared(crate::shared_transaction::SharedWriteTransaction),
}

impl NativeWriteTransaction {
    pub fn new_shared(database: Arc<redb::Database>, changes: ChangeSignal) -> BTreeResult<Self> {
        Ok(Self {
            backend: WriteBackend::Shared(crate::shared_transaction::SharedWriteTransaction::new(
                database,
            )?),
            mutations: Mutations::default(),
            read_budget: ReadBudget::default(),
            changes,
        })
    }

    pub fn ensure_table<K: RedbKey, V: RedbValue>(&self, name: &str) -> BTreeResult<()> {
        match &self.backend {
            WriteBackend::Shared(transaction) => transaction.ensure_table::<K, V>(name),
            WriteBackend::Native { .. } => self.open_table::<K, V>(name).map(|_| ()),
        }
    }

    pub fn drop_table<K: RedbKey, V: RedbValue>(&self, name: &str) -> BTreeResult<bool> {
        let existed = match &self.backend {
            WriteBackend::Shared(transaction) => transaction.drop_table::<K, V>(name)?,
            WriteBackend::Native { inner, .. } => inner
                .delete_table(table_definition::<K, V>(name))
                .map_err(BTreeError::custom)?,
        };
        self.record_table(name, existed, false);
        Ok(existed)
    }

    fn native_inner(&self) -> &WriteTransaction {
        match &self.backend {
            WriteBackend::Native { inner, .. } => inner,
            WriteBackend::Shared(_) => unreachable!("shared writes use their transaction overlay"),
        }
    }

    fn write_value<K: RedbKey, V: RedbValue>(
        &self,
        name: &str,
        key: K,
        value: Option<V>,
    ) -> BTreeResult<()> {
        let old = self
            .read_value::<K, V>(name, &key)?
            .map(|value| V::as_bytes(&value).as_ref().to_vec());
        self.write_known::<K, V>(name, key, old, value)
    }

    fn write_known<K: RedbKey, V: RedbValue>(
        &self,
        name: &str,
        key: K,
        old: Option<Vec<u8>>,
        value: Option<V>,
    ) -> BTreeResult<()> {
        let new = value
            .as_ref()
            .map(|value| V::as_bytes(value).as_ref().to_vec());
        if old == new {
            return Ok(());
        }
        match &self.backend {
            WriteBackend::Shared(transaction) => {
                transaction.put::<K, V>(name, &key, value.as_ref())?
            }
            WriteBackend::Native { .. } => {
                let mut table = self.open_table::<K, V>(name)?;
                if let Some(value) = value {
                    table
                        .insert(Key::new(key.clone()), Value::new(value))
                        .map_err(BTreeError::custom)?;
                } else {
                    table
                        .remove(Key::new(key.clone()))
                        .map_err(BTreeError::custom)?;
                }
            }
        }
        self.record(name, K::as_bytes(&key).as_ref().to_vec(), old, new);
        Ok(())
    }

    pub fn new(
        inner: WriteTransaction,
        admission: MutexGuardArc<()>,
        changes: ChangeSignal,
    ) -> Self {
        Self {
            backend: WriteBackend::Native {
                inner: Box::new(inner),
                _admission: admission,
            },
            mutations: Mutations::default(),
            read_budget: ReadBudget::default(),
            changes,
        }
    }

    pub fn open_table<K: RedbKey, V: RedbValue>(
        &self,
        name: &str,
    ) -> BTreeResult<Table<'_, Key<K>, Value<V>>> {
        let exists = match self.mutations.table_exists(name) {
            Some(exists) => exists,
            None => self
                .native_inner()
                .list_tables()
                .map_err(BTreeError::custom)?
                .any(|table| table.name() == name),
        };
        let table = self
            .native_inner()
            .open_table(table_definition::<K, V>(name))
            .map_err(BTreeError::custom)?;
        self.record_table(name, exists, true);
        Ok(table)
    }

    fn open_existing_table<K: RedbKey, V: RedbValue>(
        &self,
        name: &str,
    ) -> BTreeResult<Option<BorrowedTable<'_, K, V>>> {
        if !self
            .native_inner()
            .list_tables()
            .map_err(BTreeError::custom)?
            .any(|table| table.name() == name)
        {
            return Ok(None);
        }
        self.native_inner()
            .open_table(redb::TableDefinition::<Borrowed<Key<K>>, Borrowed<Value<V>>>::new(name))
            .map(Some)
            .map_err(BTreeError::custom)
    }

    #[inline(never)]
    fn read_value<K: RedbKey, V: RedbValue>(&self, name: &str, key: &K) -> BTreeResult<Option<V>> {
        if let WriteBackend::Shared(transaction) = &self.backend {
            let value = transaction.get::<K, V>(name, key)?;
            if let Some(value) = &value {
                self.read_budget.charge(
                    K::as_bytes(key)
                        .as_ref()
                        .len()
                        .saturating_add(V::as_bytes(value).as_ref().len()),
                )?;
            }
            return Ok(value);
        }
        let Some(table) = self.open_existing_table::<K, V>(name)? else {
            return Ok(None);
        };
        let key_bytes = K::as_bytes(key);
        table
            .get(key_bytes.as_ref())
            .map_err(BTreeError::custom)?
            .map(|value| {
                let bytes = value.value();
                self.read_budget
                    .charge(key_bytes.as_ref().len().saturating_add(bytes.len()))?;
                Ok(V::from_bytes(bytes))
            })
            .transpose()
    }

    #[inline(never)]
    fn read_range<K: RedbKey, V: RedbValue, R: RangeBounds<K>>(
        &self,
        name: &str,
        range: R,
    ) -> BTreeResult<Vec<(K, V)>> {
        if let WriteBackend::Shared(transaction) = &self.backend {
            return transaction.range::<K, V, R>(name, range, &self.read_budget);
        }
        let Some(table) = self.open_existing_table::<K, V>(name)? else {
            return Ok(Vec::new());
        };
        let bound = |bound| match bound {
            std::ops::Bound::Included(key) => {
                std::ops::Bound::Included(K::as_bytes(key).as_ref().to_vec())
            }
            std::ops::Bound::Excluded(key) => {
                std::ops::Bound::Excluded(K::as_bytes(key).as_ref().to_vec())
            }
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };
        let bounds = (bound(range.start_bound()), bound(range.end_bound()));
        let results = table
            .range::<&[u8]>((
                bounds.0.as_ref().map(Vec::as_slice),
                bounds.1.as_ref().map(Vec::as_slice),
            ))
            .map_err(BTreeError::custom)?;
        let mut entries = Vec::new();
        for result in results {
            let (key, value) = result.map_err(BTreeError::custom)?;
            let key = key.value();
            let value = value.value();
            self.read_budget
                .charge(key.len().saturating_add(value.len()))?;
            entries.push((K::from_bytes(key), V::from_bytes(value)));
        }
        Ok(entries)
    }

    pub fn record(&self, table: &str, key: Vec<u8>, old: Option<Vec<u8>>, new: Option<Vec<u8>>) {
        self.mutations.record(table, key, old, new);
    }

    pub fn record_table(&self, name: &str, old: bool, new: bool) {
        self.mutations.table(name, old, new);
    }

    pub fn commit(self) -> BTreeResult<()> {
        let _invalidations = self.mutations.invalidations();
        match self.backend {
            WriteBackend::Native { inner, .. } => (*inner).commit().map_err(BTreeError::custom)?,
            WriteBackend::Shared(transaction) => transaction.commit()?,
        };
        // Build events from mutations for push-based sync
        let events = self.mutations.take_events();
        let change_events: Vec<ChangeEvent> = events
            .into_iter()
            .map(|(table, key, blob)| ChangeEvent {
                key: StorageKey { table, key },
                blob,
                generation: 0,
            })
            .collect();
        self.changes.committed_with_events(change_events);
        Ok(())
    }

    pub fn rollback(self) -> BTreeResult<()> {
        match self.backend {
            WriteBackend::Native { inner, .. } => (*inner).abort().map_err(BTreeError::custom),
            WriteBackend::Shared(_) => Ok(()),
        }
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
        self.tx.native().read_value::<K, V>(&self.name, key)
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream! {
            let entries = self.tx.native().read_range::<K, V, R>(&self.name, range)?;
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
        self.tx
            .native()
            .write_value::<K, V>(&self.name, key, Some(value))
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
        let value = tx.read_value::<K, V>(&self.name, key)?;
        tx.write_known::<K, V>(
            &self.name,
            key.clone(),
            value
                .as_ref()
                .map(|value| V::as_bytes(value).as_ref().to_vec()),
            None,
        )?;
        Ok(value)
    }

    fn remove_range<R>(&mut self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream! {
            let tx = self.tx.native();
            let mut keys = Vec::new();
            for (key, value) in tx.read_range::<K,V,R>(&self.name, range)? {
                keys.push((key.clone(), V::as_bytes(&value).as_ref().to_vec()));
                yield Ok((key, value));
            }
            for (key, old) in keys { tx.write_known::<K,V>(&self.name, key, Some(old), None)?; }
        }
    }

    async fn commit(self) -> BTreeResult<()> {
        self.tx.commit()
    }
    async fn rollback(self) -> BTreeResult<()> {
        self.tx.rollback()
    }
}
