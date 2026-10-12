use std::{sync::Arc, time::Instant};

use async_lock::{Mutex, MutexGuardArc};
use redb::{Database, WriteTransaction};

use btree::BTreeResult;

use crate::{
    RedbBTreeScopedTransaction,
    redb::{RedbKey, RedbValue, table_definition},
};

#[derive(Clone)]
pub struct RedbDatabase {
    inner: Arc<Database>,
    admission: Arc<Mutex<()>>,
}

impl RedbDatabase {
    pub fn new(inner: Arc<Database>) -> Self {
        Self {
            inner,
            admission: Arc::new(Mutex::new(())),
        }
    }

    pub async fn transaction(&self) -> BTreeResult<RedbDatabaseTransaction> {
        let started =
            log::log_enabled!(target: "of::idp_stage_timing", log::Level::Info).then(Instant::now);
        let admission = self.admission.lock_arc().await;
        if let Some(started) = started {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.native.admission elapsed_us={}", started.elapsed().as_micros());
        }
        let started = started.map(|_| Instant::now());
        let inner = self.inner.begin_write().map_err(btree::BTreeError::custom);
        if let Some(started) = started {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.native.begin_write elapsed_us={}", started.elapsed().as_micros());
        }
        Ok(RedbDatabaseTransaction {
            _admission: admission,
            inner: inner?,
            body_started: started.map(|_| Instant::now()),
        })
    }
}

pub struct RedbDatabaseTransaction {
    inner: WriteTransaction,
    _admission: MutexGuardArc<()>,
    body_started: Option<Instant>,
}

impl RedbDatabaseTransaction {
    pub fn table<K, V>(&self, name: impl Into<String>) -> RedbBTreeScopedTransaction<'_, K, V> {
        RedbBTreeScopedTransaction::new(&self.inner, name)
    }

    pub fn create_table<K, V>(&self, name: &str) -> BTreeResult<()>
    where
        K: RedbKey,
        V: RedbValue,
    {
        self.inner
            .open_table(table_definition::<K, V>(name))
            .map_err(btree::BTreeError::custom)?;
        Ok(())
    }

    pub fn drop_table<K, V>(&self, name: &str) -> BTreeResult<bool>
    where
        K: RedbKey,
        V: RedbValue,
    {
        self.inner
            .delete_table(table_definition::<K, V>(name))
            .map_err(btree::BTreeError::custom)
    }

    pub fn commit(self) -> BTreeResult<()> {
        if let Some(started) = self.body_started {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.native.body_commit elapsed_us={}", started.elapsed().as_micros());
        }
        let started = self.body_started.map(|_| Instant::now());
        let result = self.inner.commit().map_err(btree::BTreeError::custom);
        if let Some(started) = started {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.native.commit elapsed_us={}", started.elapsed().as_micros());
        }
        result
    }

    pub fn rollback(self) -> BTreeResult<()> {
        if let Some(started) = self.body_started {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.native.body_rollback elapsed_us={}", started.elapsed().as_micros());
        }
        let started = self.body_started.map(|_| Instant::now());
        let result = self.inner.abort().map_err(btree::BTreeError::custom);
        if let Some(started) = started {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.native.rollback elapsed_us={}", started.elapsed().as_micros());
        }
        result
    }
}
