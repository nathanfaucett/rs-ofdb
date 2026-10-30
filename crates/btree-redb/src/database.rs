use std::sync::Arc;

use async_lock::Mutex;
use redb::Database;

use btree::{BTreeError, BTreeResult, ChangeSignal, ChangeSubscription};

use crate::{
    RedbBTreeScopedTransaction,
    redb::{RedbKey, RedbValue, table_definition},
    transaction::NativeWriteTransaction,
};

#[derive(Clone)]
pub struct RedbDatabase {
    inner: Arc<Database>,
    admission: Arc<Mutex<()>>,
    changes: ChangeSignal,
}

impl RedbDatabase {
    pub fn new(inner: Arc<Database>) -> Self {
        Self {
            inner,
            admission: Arc::new(Mutex::new(())),
            changes: ChangeSignal::default(),
        }
    }

    /// Subscribes to successfully committed storage changes.
    pub fn subscribe_changes(&self) -> ChangeSubscription {
        self.changes.subscribe()
    }

    pub async fn transaction(&self) -> BTreeResult<RedbDatabaseTransaction> {
        let admission = self.admission.lock_arc().await;
        Ok(RedbDatabaseTransaction {
            inner: NativeWriteTransaction::new(
                self.inner.begin_write().map_err(BTreeError::custom)?,
                admission,
                self.changes.clone(),
            ),
        })
    }
}

pub struct RedbDatabaseTransaction {
    inner: NativeWriteTransaction,
}

impl RedbDatabaseTransaction {
    /// Sets one cumulative native read budget; repeated configuration is rejected.
    pub fn set_read_limits(&mut self, entries: usize, bytes: usize) -> BTreeResult<()> {
        self.inner.read_budget.set(entries, bytes)
    }

    pub fn read_limit_exceeded(&self) -> bool {
        self.inner.read_budget.exhausted()
    }

    pub fn table<K, V>(&self, name: impl Into<String>) -> RedbBTreeScopedTransaction<'_, K, V> {
        RedbBTreeScopedTransaction::new(&self.inner, name)
    }

    pub fn create_table<K: RedbKey, V: RedbValue>(&self, name: &str) -> BTreeResult<()> {
        self.inner.open_table::<K, V>(name)?;
        Ok(())
    }

    pub fn drop_table<K: RedbKey, V: RedbValue>(&self, name: &str) -> BTreeResult<bool> {
        let existed = self
            .inner
            .inner
            .delete_table(table_definition::<K, V>(name))
            .map_err(BTreeError::custom)?;
        self.inner.record_table(name, existed, false);
        Ok(existed)
    }

    pub fn commit(self) -> BTreeResult<()> {
        self.inner.commit()
    }
    pub fn rollback(self) -> BTreeResult<()> {
        self.inner.rollback()
    }
}
