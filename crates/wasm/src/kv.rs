use std::time::Duration;

use ofdb_kv::{Client, Database, Transaction};
use serde_json::Value as Json;
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::value::{parse_json, value_from_json, value_to_json, write_json};

#[wasm_bindgen]
pub struct KvDatabase {
    database: Database,
}

#[wasm_bindgen]
impl KvDatabase {
    /// Open a key-value database backed by IndexedDB.
    pub async fn open(name: String) -> Result<KvDatabase, JsError> {
        let database = Database::open_indexeddb(&name).await?;
        Ok(Self { database })
    }

    /// Create a query handle sharing this database's storage.
    pub fn client(&self) -> KvClient {
        KvClient {
            client: self.database.client(),
        }
    }
}

#[wasm_bindgen]
pub struct KvClient {
    client: Client,
}

#[wasm_bindgen]
impl KvClient {
    /// Read a committed value as JSON; a missing key becomes JavaScript undefined.
    pub async fn get(&self, key: String) -> Result<Option<String>, JsError> {
        match self.client.get(&key).await? {
            Some(value) => Ok(Some(write_json(&value_to_json(&value))?)),
            None => Ok(None),
        }
    }

    /// Store a JSON value as the committed value, with optional expiry (Unix epoch millis).
    pub async fn set(
        &self,
        key: String,
        value: String,
        expires_at: Option<i64>,
    ) -> Result<(), JsError> {
        let value = value_from_json(&parse_json(&value)?);
        self.client.set(&key, value, expires_at).await?;
        Ok(())
    }

    /// Remove a committed value.
    pub async fn delete(&self, key: String) -> Result<(), JsError> {
        self.client.delete(&key).await?;
        Ok(())
    }

    /// Scan entries with keys in [start, end) as a JSON array of {key, value} objects.
    pub async fn scan(&self, start: String, end: String) -> Result<String, JsError> {
        let entries = self.client.scan(&start, &end).await?;
        write_json(&entries_to_json(&entries)?)
    }

    /// Scan entries whose keys start with `prefix`.
    pub async fn scan_prefix(&self, prefix: String) -> Result<String, JsError> {
        let entries = self.client.scan_prefix(&prefix).await?;
        write_json(&entries_to_json(&entries)?)
    }

    /// Scan all entries.
    pub async fn scan_all(&self) -> Result<String, JsError> {
        let entries = self.client.scan_all().await?;
        write_json(&entries_to_json(&entries)?)
    }

    /// Start a transaction that holds operations until commit or rollback.
    pub async fn transaction(&self) -> Result<KvTransaction, JsError> {
        let tx = self.client.transaction().await?;
        Ok(KvTransaction { tx })
    }

    /// Return a clone of this client with a request deadline in milliseconds.
    pub fn with_deadline(&self, ms: u64) -> KvClient {
        KvClient {
            client: self.client.clone().with_deadline(Duration::from_millis(ms)),
        }
    }

    /// Return an independent clone sharing the same storage.
    pub fn clone_client(&self) -> KvClient {
        KvClient {
            client: self.client.clone(),
        }
    }
}

#[wasm_bindgen]
pub struct KvTransaction {
    tx: Transaction,
}

#[wasm_bindgen]
impl KvTransaction {
    pub async fn get(&mut self, key: String) -> Result<Option<String>, JsError> {
        match self.tx.get(&key).await? {
            Some(value) => Ok(Some(write_json(&value_to_json(&value))?)),
            None => Ok(None),
        }
    }

    pub async fn set(
        &mut self,
        key: String,
        value: String,
        expires_at: Option<i64>,
    ) -> Result<(), JsError> {
        let value = value_from_json(&parse_json(&value)?);
        self.tx.set(&key, value, expires_at).await?;
        Ok(())
    }

    pub async fn delete(&mut self, key: String) -> Result<(), JsError> {
        self.tx.delete(&key).await?;
        Ok(())
    }

    pub async fn scan(&mut self, start: String, end: String) -> Result<String, JsError> {
        let entries = self.tx.scan(&start, &end).await?;
        write_json(&entries_to_json(&entries)?)
    }

    pub async fn scan_prefix(&mut self, prefix: String) -> Result<String, JsError> {
        let entries = self.tx.scan_prefix(&prefix).await?;
        write_json(&entries_to_json(&entries)?)
    }

    pub async fn scan_all(&mut self) -> Result<String, JsError> {
        let entries = self.tx.scan_all().await?;
        write_json(&entries_to_json(&entries)?)
    }

    pub async fn commit(self) -> Result<(), JsError> {
        self.tx.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), JsError> {
        self.tx.rollback().await?;
        Ok(())
    }
}

fn entries_to_json(entries: &[(String, ofdb_value::Value)]) -> Result<Json, JsError> {
    Ok(Json::Array(
        entries
            .iter()
            .map(|(key, value)| {
                Json::Object(
                    [
                        ("key".to_owned(), Json::String(key.clone())),
                        ("value".to_owned(), value_to_json(value)),
                    ]
                    .into_iter()
                    .collect(),
                )
            })
            .collect(),
    ))
}
