use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use btree::{BTreeError, BTreeResult, ChangeSignal, ChangeSubscription, StorageInvalidation};
use futures::channel::oneshot;
use js_sys::{Array, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, spawn_local};

use crate::platform;

pub type Table = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone)]
pub struct Database {
    handle: Rc<Connection>,
    changes: ChangeSignal,
}

struct Connection(JsValue);

impl Drop for Connection {
    fn drop(&mut self) {
        platform::close(&self.0);
    }
}

impl Database {
    /// Store kinds have disjoint physical namespaces. No fallback or implicit migration.
    pub async fn open(name: &str, kind: &str) -> BTreeResult<Self> {
        let physical = format!("ofdb:{}:{kind}:{name}", kind.len());
        let handle = JsFuture::from(platform::open(&physical).map_err(error)?)
            .await
            .map_err(error)?;
        let database = Self {
            handle: Rc::new(Connection(handle)),
            changes: ChangeSignal::default(),
        };
        database.snapshot().await?;
        Ok(database)
    }

    pub fn subscribe_changes(&self) -> ChangeSubscription {
        self.changes.subscribe()
    }

    /// Reads and copies the whole database, using O(database size) memory.
    /// BTree reconstruction additionally costs O(record count * log(record count)).
    /// The revision, logical table names and records share one native read transaction.
    pub async fn snapshot(&self) -> BTreeResult<Snapshot> {
        let value = JsFuture::from(platform::snapshot(&self.handle.0).map_err(error)?)
            .await
            .map_err(error)?;
        let result = Array::from(&value);
        let revision = result
            .get(0)
            .as_string()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| BTreeError::custom("Invalid IndexedDB revision"))?;
        let mut tables = BTreeMap::new();
        for name in Array::from(&result.get(1)) {
            let name = name
                .as_string()
                .ok_or_else(|| BTreeError::custom("Invalid table name"))?;
            tables.insert(name, Table::new());
        }
        let keys = Array::from(&result.get(2));
        let values = Array::from(&result.get(3));
        for (i, key) in keys.iter().enumerate() {
            let key = Array::from(&key);
            let name = key
                .get(0)
                .as_string()
                .ok_or_else(|| BTreeError::custom("Invalid record table"))?;
            let table = tables
                .get_mut(&name)
                .ok_or_else(|| BTreeError::custom("Record without logical table"))?;
            table.insert(bytes(key.get(1))?, bytes(values.get(i as u32))?);
        }
        Ok(Snapshot {
            database: self.clone(),
            revision,
            original: tables.clone(),
            tables,
            dropped: BTreeSet::new(),
        })
    }
}

/// Rust-owned staging: no native transaction lives across caller awaits.
/// A single snapshot enlists every logical table and commits them atomically.
pub struct Snapshot {
    database: Database,
    revision: u64,
    original: BTreeMap<String, Table>,
    tables: BTreeMap<String, Table>,
    dropped: BTreeSet<String>,
}

impl Snapshot {
    pub(crate) fn original_table_exists(&self, name: &str) -> bool {
        self.original.contains_key(name)
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
    pub fn table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }
    pub fn create_table(&mut self, name: &str) -> bool {
        if self.tables.contains_key(name) {
            return false;
        }
        self.tables.insert(name.to_owned(), Table::new());
        true
    }
    pub fn drop_table(&mut self, name: &str) -> bool {
        if self.tables.remove(name).is_none() {
            return false;
        }
        self.dropped.insert(name.to_owned());
        true
    }
    pub fn rollback(self) {}

    pub async fn commit(self) -> BTreeResult<()> {
        if self.original == self.tables {
            return Ok(());
        }
        let tables = Array::new();
        let records = Array::new();
        let mut invalidations = self
            .dropped
            .iter()
            .cloned()
            .map(StorageInvalidation::Table)
            .collect::<Vec<_>>();
        for name in self
            .original
            .keys()
            .chain(self.tables.keys())
            .collect::<BTreeSet<_>>()
        {
            let old = self.original.get(name);
            let new = self.tables.get(name);
            if old.is_some() != new.is_some() {
                let entry = Array::new();
                entry.push(&name.into());
                entry.push(&JsValue::from_bool(new.is_some()));
                tables.push(&entry);
                invalidations.push(StorageInvalidation::Table(name.clone()));
            }
            let empty = Table::new();
            let old = old.unwrap_or(&empty);
            let new = new.unwrap_or(&empty);
            for key in old.keys().chain(new.keys()).collect::<BTreeSet<_>>() {
                if old.get(key) == new.get(key) {
                    continue;
                }
                let entry = Array::new();
                entry.push(&name.into());
                entry.push(&Uint8Array::from(key.as_slice()));
                entry.push(
                    &new.get(key)
                        .map_or(JsValue::NULL, |v| Uint8Array::from(v.as_slice()).into()),
                );
                records.push(&entry);
                invalidations.push(StorageInvalidation::Key {
                    table: name.clone(),
                    key: key.clone(),
                });
            }
        }
        let promise = platform::commit(
            &self.database.handle.0,
            &self.revision.to_string(),
            &tables,
            &records,
        )
        .map_err(error)?;
        drop(self.original);
        drop(self.tables);
        let (sender, receiver) = oneshot::channel();
        let signal = self.database.changes;
        let connection = self.database.handle;
        // Completion owns only the issued persistence and its invalidation, not
        // the caller's snapshot. Cancellation cannot discard the commit outcome.
        spawn_local(async move {
            let result = JsFuture::from(promise).await.map_err(error);
            drop(connection);
            if result.is_ok() {
                signal.committed_with(invalidations);
            }
            let _ = sender.send(result.map(|_| ()));
        });
        receiver.await.map_err(|_| BTreeError::CommitFailed)?
    }
}

fn bytes(value: JsValue) -> BTreeResult<Vec<u8>> {
    if value.is_instance_of::<Uint8Array>() {
        return Ok(Uint8Array::new(&value).to_vec());
    }
    if value.is_instance_of::<js_sys::ArrayBuffer>() {
        return Ok(Uint8Array::new(&value).to_vec());
    }
    Err(BTreeError::custom("Invalid IndexedDB binary record"))
}

fn error(value: JsValue) -> BTreeError {
    if value.as_string().as_deref() == Some("ofdb-revision-conflict") {
        return BTreeError::Conflict;
    }
    BTreeError::custom(format!("IndexedDB: {value:?}"))
}
