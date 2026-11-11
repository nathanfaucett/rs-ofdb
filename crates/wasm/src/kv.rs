use futures_util::future::FutureExt;
use ofdb_btree::{ChangeEvent, StorageKey};
use ofdb_kv::{Database, KvClient, KvTransaction, encode_value};
use ofdb_sync::{
    PushConfig as OfdbPushConfig, PushResult as OfdbPushResult,
    PushSyncTransport as OfdbPushSyncTransport, PushSynchronizeConfig as OfdbPushSynchronizeConfig,
    PushSynchronizeError as OfdbPushSynchronizeError,
};
use serde_json::Value as Json;
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::value::{parse_json, write_json};

#[wasm_bindgen]
pub struct KvDatabase {
    database: Database,
}

#[wasm_bindgen]
impl KvDatabase {
    /// Open a KV database backed by IndexedDB.
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

    /// Get the change subscription for push-based synchronization.
    /// The subscription provides a stream of committed changes via `events()`.
    #[cfg(feature = "sync")]
    pub fn sync_changes(&self) -> Result<crate::sync::KvChangeSubscription, JsError> {
        let subscription = self.database.sync_changes()?;
        Ok(crate::sync::KvChangeSubscription { inner: subscription })
    }

    /// Push-based synchronization with a remote peer.
    /// Streams local changes to peer and applies remote changes.
    #[cfg(feature = "sync")]
    pub async fn synchronize(
        &self,
        transport: crate::sync::JsPushSyncTransport,
        config: crate::sync::PushSynchronizeConfig,
    ) -> Result<crate::sync::PushSynchronizeResult, crate::sync::JsPushSyncError> {
        self.database
            .synchronize(transport, config.into())
            .await
            .map_err(|e| crate::sync::JsPushSyncError::from(e))
            .map(crate::sync::PushSynchronizeResult::from)
    }
}

#[wasm_bindgen]
pub struct KvClient {
    client: ofdb_kv::KvClient,
}

#[wasm_bindgen]
impl KvClient {
    pub async fn get(&self, key: String) -> Result<Option<String>, JsError> {
        Ok(self.client.get(&key).await?.map(|v| v.to_string()))
    }

    pub async fn set(&self, key: String, value: String) -> Result<(), JsError> {
        self.client.set(&key, &value).await?;
        Ok(())
    }

    pub async fn delete(&self, key: String) -> Result<(), JsError> {
        self.client.delete(&key).await?;
        Ok(())
    }

    pub async fn scan(&self, prefix: String) -> Result<String, JsError> {
        let results = self.client.scan(&prefix).await?;
        write_json(&results_to_json(&results))
    }

    pub async fn scan_prefix(&self, prefix: String) -> Result<String, JsError> {
        let results = self.client.scan_prefix(&prefix).await?;
        write_json(&results_to_json(&results))
    }

    pub async fn scan_all(&self) -> Result<String, JsError> {
        let results = self.client.scan_all().await?;
        write_json(&results_to_json(&results))
    }

    pub async fn transaction(&self) -> Result<KvTransaction, JsError> {
        let tx = self.client.transaction().await?;
        Ok(KvTransaction { tx })
    }

    pub fn with_deadline(&self, ms: u64) -> KvClient {
        KvClient {
            client: self.client.clone().with_deadline(std::time::Duration::from_millis(ms)),
        }
    }

    pub fn clone_client(&self) -> KvClient {
        KvClient {
            client: self.client.clone(),
        }
    }
}

#[wasm_bindgen]
pub struct KvTransaction {
    tx: ofdb_kv::KvTransaction,
}

#[wasm_bindgen]
impl KvTransaction {
    pub async fn get(&mut self, key: String) -> Result<Option<String>, JsError> {
        Ok(self.tx.get(&key).await?.map(|v| v.to_string()))
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<(), JsError> {
        self.tx.set(&key, &value).await?;
        Ok(())
    }

    pub async fn delete(&mut self, key: String) -> Result<(), JsError> {
        self.tx.delete(&key).await?;
        Ok(())
    }

    pub async fn scan(&mut self, prefix: String) -> Result<String, JsError> {
        let results = self.tx.scan(&prefix).await?;
        write_json(&results_to_json(&results))
    }

    pub async fn scan_prefix(&mut self, prefix: String) -> Result<String, JsError> {
        let results = self.tx.scan_prefix(&prefix).await?;
        write_json(&results_to_json(&results))
    }

    pub async fn scan_all(&mut self) -> Result<String, JsError> {
        let results = self.tx.scan_all().await?;
        write_json(&results_to_json(&results))
    }

    pub async fn commit(&mut self) -> Result<(), JsError> {
        self.tx.commit().await?;
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), JsError> {
        self.tx.rollback().await?;
        Ok(())
    }
}

fn results_to_json(results: &[ofdb_kv::KvEntry]) -> Json {
    Json::Array(
        results
            .iter()
            .map(|entry| {
                Json::Object(
                    [
                        ("key".to_owned(), Json::String(entry.key.clone())),
                        ("value".to_owned(), encode_value(&entry.value)),
                    ]
                    .into_iter()
                    .collect(),
                )
            })
            .collect(),
    )
}
