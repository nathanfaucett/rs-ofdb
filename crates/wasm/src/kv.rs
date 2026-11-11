use futures_util::future::FutureExt;
use ofdb_kv::{Database, KvClient as InnerKvClient, KvTransaction as InnerKvTransaction, encode_value, KvEntry};
use ofdb_sync::{
    PushConfig as OfdbPushConfig, PushResult as OfdbPushResult,
    PushSyncTransport as OfdbPushSyncTransport, PushSynchronizeConfig as OfdbPushSynchronizeConfig,
    PushSynchronizeError as OfdbPushSynchronizeError,
};
use serde_json::Value as Json;
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen_futures::JsFuture;

use crate::value::{parse_json, value_to_json, write_json};

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
        Ok(crate::sync::KvChangeSubscription::new(subscription))
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
    client: InnerKvClient,
}

#[wasm_bindgen]
impl KvClient {
    pub async fn get(&self, key: String) -> Result<Option<String>, JsError> {
        let value = self.client.get(&key).await?;
        Ok(value.map(|v| write_json(&value_to_json(&v)).unwrap_or_default()))
    }

    pub async fn set(&self, key: String, value: String) -> Result<(), JsError> {
        let json_val: Json = parse_json(&value)?;
        let kv_value = crate::value::value_from_json(&json_val);
        self.client.set(&key, kv_value, None).await?;
        Ok(())
    }

    pub async fn set_with_expiry(&self, key: String, value: String, expiry_ms: u64) -> Result<(), JsError> {
        let json_val: Json = parse_json(&value)?;
        let kv_value = crate::value::value_from_json(&json_val);
        self.client.set(&key, kv_value, Some(expiry_ms as i64)).await?;
        Ok(())
    }

    pub async fn delete(&self, key: String) -> Result<(), JsError> {
        self.client.delete(&key).await?;
        Ok(())
    }

    pub async fn scan(&self, start: String, end: String) -> Result<String, JsError> {
        let results = self.client.scan(&start, &end).await?;
        let json_results: Vec<Json> = results.iter().map(|(k, v)| {
            Json::Object(
                [
                    ("key".to_owned(), Json::String(k.clone())),
                    ("value".to_owned(), value_to_json(v)),
                ]
                .into_iter()
                .collect(),
            )
        }).collect();
        write_json(&Json::Array(json_results))
    }

    pub async fn scan_prefix(&self, prefix: String) -> Result<String, JsError> {
        let results = self.client.scan_prefix(&prefix).await?;
        let json_results: Vec<Json> = results.iter().map(|(k, v)| {
            Json::Object(
                [
                    ("key".to_owned(), Json::String(k.clone())),
                    ("value".to_owned(), value_to_json(v)),
                ]
                .into_iter()
                .collect(),
            )
        }).collect();
        write_json(&Json::Array(json_results))
    }

    pub async fn scan_all(&self) -> Result<String, JsError> {
        let results = self.client.scan_all().await?;
        let json_results: Vec<Json> = results.iter().map(|(k, v)| {
            Json::Object(
                [
                    ("key".to_owned(), Json::String(k.clone())),
                    ("value".to_owned(), value_to_json(v)),
                ]
                .into_iter()
                .collect(),
            )
        }).collect();
        write_json(&Json::Array(json_results))
    }

    pub async fn transaction(&self) -> Result<KvTransaction, JsError> {
        let tx = self.client.transaction().await?;
        Ok(KvTransaction { tx: Some(tx) })
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
    tx: Option<InnerKvTransaction>,
}

#[wasm_bindgen]
impl KvTransaction {
    pub async fn get(&mut self, key: String) -> Result<Option<String>, JsError> {
        let value = self.tx.as_mut().unwrap().get(&key).await?;
        Ok(value.map(|v| write_json(&value_to_json(&v)).unwrap_or_default()))
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<(), JsError> {
        let json_val: Json = parse_json(&value)?;
        let kv_value = crate::value::value_from_json(&json_val);
        self.tx.as_mut().unwrap().set(&key, kv_value, None).await?;
        Ok(())
    }

    pub async fn set_with_expiry(&mut self, key: String, value: String, expiry_ms: u64) -> Result<(), JsError> {
        let json_val: Json = parse_json(&value)?;
        let kv_value = crate::value::value_from_json(&json_val);
        self.tx.as_mut().unwrap().set(&key, kv_value, Some(expiry_ms as i64)).await?;
        Ok(())
    }

    pub async fn delete(&mut self, key: String) -> Result<(), JsError> {
        self.tx.as_mut().unwrap().delete(&key).await?;
        Ok(())
    }

    pub async fn scan(&mut self, start: String, end: String) -> Result<String, JsError> {
        let results = self.tx.as_mut().unwrap().scan(&start, &end).await?;
        let json_results: Vec<Json> = results.iter().map(|(k, v)| {
            Json::Object(
                [
                    ("key".to_owned(), Json::String(k.clone())),
                    ("value".to_owned(), value_to_json(v)),
                ]
                .into_iter()
                .collect(),
            )
        }).collect();
        write_json(&Json::Array(json_results))
    }

    pub async fn scan_prefix(&mut self, prefix: String) -> Result<String, JsError> {
        let results = self.tx.as_mut().unwrap().scan_prefix(&prefix).await?;
        let json_results: Vec<Json> = results.iter().map(|(k, v)| {
            Json::Object(
                [
                    ("key".to_owned(), Json::String(k.clone())),
                    ("value".to_owned(), value_to_json(v)),
                ]
                .into_iter()
                .collect(),
            )
        }).collect();
        write_json(&Json::Array(json_results))
    }

    pub async fn scan_all(&mut self) -> Result<String, JsError> {
        let results = self.tx.as_mut().unwrap().scan_all().await?;
        let json_results: Vec<Json> = results.iter().map(|(k, v)| {
            Json::Object(
                [
                    ("key".to_owned(), Json::String(k.clone())),
                    ("value".to_owned(), value_to_json(v)),
                ]
                .into_iter()
                .collect(),
            )
        }).collect();
        write_json(&Json::Array(json_results))
    }

    pub async fn commit(&mut self) -> Result<(), JsError> {
        if let Some(tx) = self.tx.take() {
            tx.commit().await?;
        }
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), JsError> {
        if let Some(tx) = self.tx.take() {
            tx.rollback().await?;
        }
        Ok(())
    }
}
