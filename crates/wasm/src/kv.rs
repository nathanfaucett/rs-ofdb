use std::time::Duration;

use futures_util::future::FutureExt;
#[cfg(feature = "sync")]
use ofdb_kv::{
    ChangeSubscription as OfdbKvChangeSubscription, SyncConfig as OfdbKvSyncConfig,
    SyncError as OfdbKvSyncError, SyncRole as OfdbKvSyncRole, SyncTransport as OfdbKvSyncTransport,
};
use ofdb_kv::{Client, Database, Transaction};
use serde_json::Value as Json;
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

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

    /// Synchronize with a remote peer using the given transport.
    #[cfg(feature = "sync")]
    pub async fn synchronize(
        &self,
        transport: &mut JsKvSyncTransport,
        role: KvSyncRole,
        config: KvSyncConfig,
    ) -> Result<(), JsKvSyncError> {
        self.database
            .synchronize(transport, role.into(), config.into())
            .await
            .map_err(JsKvSyncError::from)
    }

    /// Synchronize with a remote peer, checking caller authorization before each batch.
    #[cfg(feature = "sync")]
    pub async fn synchronize_checked(
        &self,
        transport: &mut JsKvSyncTransport,
        role: KvSyncRole,
        config: KvSyncConfig,
        check: js_sys::Function,
    ) -> Result<(), JsKvSyncError> {
        let check_fn = move || {
            let promise = check.call0(&JsValue::NULL).unwrap();
            let promise: js_sys::Promise = promise.dyn_into().unwrap();
            JsFuture::from(promise).then(|result| async move {
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(format!("{:?}", e)),
                }
            })
        };
        self.database
            .synchronize_checked(transport, role.into(), config.into(), check_fn)
            .await
            .map_err(JsKvSyncError::from)
    }

    /// Synchronize selected changes with a remote peer, checking caller authorization.
    #[cfg(feature = "sync")]
    pub async fn synchronize_selected_checked(
        &self,
        subscription: &mut KvChangeSubscription,
        transport: &mut JsKvSyncTransport,
        role: KvSyncRole,
        config: KvSyncConfig,
        check: js_sys::Function,
    ) -> Result<(), JsKvSyncError> {
        let check_fn = move || {
            let promise = check.call0(&JsValue::NULL).unwrap();
            let promise: js_sys::Promise = promise.dyn_into().unwrap();
            JsFuture::from(promise).then(|result| async move {
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(format!("{:?}", e)),
                }
            })
        };
        self.database
            .synchronize_selected_checked(
                &mut subscription.inner,
                transport,
                role.into(),
                config.into(),
                check_fn,
            )
            .await
            .map_err(JsKvSyncError::from)
    }

    /// Get the change subscription for continuous synchronization.
    #[cfg(feature = "sync")]
    pub fn sync_changes(&self) -> Result<KvChangeSubscription, JsError> {
        let inner = self.database.sync_changes()?;
        Ok(KvChangeSubscription { inner })
    }
}

/// Sync role: Initiator or Responder.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvSyncRole {
    Initiator = 0,
    Responder = 1,
}

impl From<KvSyncRole> for OfdbKvSyncRole {
    fn from(role: KvSyncRole) -> Self {
        match role {
            KvSyncRole::Initiator => OfdbKvSyncRole::Initiator,
            KvSyncRole::Responder => OfdbKvSyncRole::Responder,
        }
    }
}

impl From<OfdbKvSyncRole> for KvSyncRole {
    fn from(role: OfdbKvSyncRole) -> Self {
        match role {
            OfdbKvSyncRole::Initiator => KvSyncRole::Initiator,
            OfdbKvSyncRole::Responder => KvSyncRole::Responder,
        }
    }
}

/// Sync configuration.
#[wasm_bindgen]
pub struct KvSyncConfig {
    inner: OfdbKvSyncConfig,
}

#[wasm_bindgen]
impl KvSyncConfig {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: OfdbKvSyncConfig::default(),
        }
    }

    #[wasm_bindgen(getter)]
    pub fn max_frame_bytes(&self) -> usize {
        self.inner.max_frame_bytes
    }

    #[wasm_bindgen(setter)]
    pub fn set_max_frame_bytes(&mut self, value: usize) {
        self.inner.max_frame_bytes = value;
    }

    #[wasm_bindgen(getter)]
    pub fn max_snapshots_per_batch(&self) -> usize {
        self.inner.max_snapshots_per_batch
    }

    #[wasm_bindgen(setter)]
    pub fn set_max_snapshots_per_batch(&mut self, value: usize) {
        self.inner.max_snapshots_per_batch = value;
    }

    #[wasm_bindgen(getter)]
    pub fn max_inventory_bytes(&self) -> usize {
        self.inner.max_inventory_bytes
    }

    #[wasm_bindgen(setter)]
    pub fn set_max_inventory_bytes(&mut self, value: usize) {
        self.inner.max_inventory_bytes = value;
    }

    #[wasm_bindgen(getter)]
    pub fn max_inventory_entries(&self) -> usize {
        self.inner.max_inventory_entries
    }

    #[wasm_bindgen(setter)]
    pub fn set_max_inventory_entries(&mut self, value: usize) {
        self.inner.max_inventory_entries = value;
    }
}

impl Default for KvSyncConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl From<KvSyncConfig> for OfdbKvSyncConfig {
    fn from(config: KvSyncConfig) -> Self {
        config.inner
    }
}

impl From<OfdbKvSyncConfig> for KvSyncConfig {
    fn from(config: OfdbKvSyncConfig) -> Self {
        Self { inner: config }
    }
}

/// Sync error.
#[wasm_bindgen]
pub struct JsKvSyncError {
    inner: OfdbKvSyncError<JsKvSyncTransportError>,
}

#[wasm_bindgen]
impl JsKvSyncError {
    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.inner.to_string()
    }

    #[wasm_bindgen(getter)]
    pub fn kind(&self) -> JsKvSyncErrorKind {
        match &self.inner {
            OfdbKvSyncError::PreApply(_) => JsKvSyncErrorKind::PreApply,
            OfdbKvSyncError::Transport(_) => JsKvSyncErrorKind::Transport,
            OfdbKvSyncError::Codec(_) => JsKvSyncErrorKind::Codec,
            OfdbKvSyncError::InvalidFrame => JsKvSyncErrorKind::InvalidFrame,
            OfdbKvSyncError::FrameTooLarge => JsKvSyncErrorKind::FrameTooLarge,
            OfdbKvSyncError::UnsupportedVersion(_) => JsKvSyncErrorKind::UnsupportedVersion,
            OfdbKvSyncError::UnexpectedMessage => JsKvSyncErrorKind::UnexpectedMessage,
            OfdbKvSyncError::Storage(_) => JsKvSyncErrorKind::Storage,
        }
    }
}

impl From<OfdbKvSyncError<JsKvSyncTransportError>> for JsKvSyncError {
    fn from(inner: OfdbKvSyncError<JsKvSyncTransportError>) -> Self {
        Self { inner }
    }
}

/// Sync error kind.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsKvSyncErrorKind {
    PreApply = 0,
    Transport = 1,
    Codec = 2,
    InvalidFrame = 3,
    FrameTooLarge = 4,
    UnsupportedVersion = 5,
    UnexpectedMessage = 6,
    Storage = 7,
}

/// Transport error for JS-implemented transports.
#[wasm_bindgen]
pub struct JsKvSyncTransportError {
    message: String,
}

#[wasm_bindgen]
impl JsKvSyncTransportError {
    #[wasm_bindgen(constructor)]
    pub fn new(message: String) -> Self {
        Self { message }
    }

    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }
}

impl std::fmt::Display for JsKvSyncTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::fmt::Debug for JsKvSyncTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsKvSyncTransportError({})", self.message)
    }
}

impl std::error::Error for JsKvSyncTransportError {}

/// Sync transport trait that can be implemented in JavaScript.
#[wasm_bindgen]
pub struct JsKvSyncTransport {
    inner: JsValue,
}

#[wasm_bindgen]
impl JsKvSyncTransport {
    #[wasm_bindgen(constructor)]
    pub fn new(transport: JsValue) -> Self {
        Self { inner: transport }
    }
}

impl OfdbKvSyncTransport for JsKvSyncTransport {
    type Error = JsKvSyncTransportError;

    fn receive(&mut self) -> impl Future<Output = Result<Vec<u8>, Self::Error>> {
        let transport = self.inner.clone();
        async move {
            let recv_fn = js_sys::Reflect::get(&transport, &"receive".into()).map_err(|_| {
                JsKvSyncTransportError::new("transport has no receive method".into())
            })?;
            let recv_fn: js_sys::Function = recv_fn
                .dyn_into()
                .map_err(|_| JsKvSyncTransportError::new("receive is not a function".into()))?;

            let promise = recv_fn.call0(&transport).map_err(|e| {
                JsKvSyncTransportError::new(format!("receive call failed: {:?}", e))
            })?;
            let promise: js_sys::Promise = promise.dyn_into().map_err(|_| {
                JsKvSyncTransportError::new("receive did not return a Promise".into())
            })?;

            let result = JsFuture::from(promise)
                .await
                .map_err(|e| JsKvSyncTransportError::new(format!("receive failed: {:?}", e)))?;

            let array = js_sys::Uint8Array::from(result);
            let mut vec = vec![0u8; array.length() as usize];
            array.copy_to(&mut vec);
            Ok(vec)
        }
    }

    fn send(&mut self, frame: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>> {
        let transport = self.inner.clone();
        async move {
            let send_fn = js_sys::Reflect::get(&transport, &"send".into())
                .map_err(|_| JsKvSyncTransportError::new("transport has no send method".into()))?;
            let send_fn: js_sys::Function = send_fn
                .dyn_into()
                .map_err(|_| JsKvSyncTransportError::new("send is not a function".into()))?;

            let array = js_sys::Uint8Array::from(frame.as_slice());
            let promise = send_fn
                .call1(&transport, &array)
                .map_err(|e| JsKvSyncTransportError::new(format!("send call failed: {:?}", e)))?;
            let promise: js_sys::Promise = promise
                .dyn_into()
                .map_err(|_| JsKvSyncTransportError::new("send did not return a Promise".into()))?;

            JsFuture::from(promise)
                .await
                .map_err(|e| JsKvSyncTransportError::new(format!("send failed: {:?}", e)))?;

            Ok(())
        }
    }
}

/// Change subscription for continuous synchronization.
#[cfg(feature = "sync")]
#[wasm_bindgen]
pub struct KvChangeSubscription {
    inner: OfdbKvChangeSubscription,
}

#[cfg(feature = "sync")]
#[wasm_bindgen]
impl KvChangeSubscription {
    /// Select the next unit of work to synchronize.
    pub fn select(&mut self) -> Option<JsKvChangeWork> {
        self.inner.select().map(JsKvChangeWork)
    }

    /// Acknowledge completed work.
    pub fn acknowledge(&mut self, work: JsKvChangeWork) -> bool {
        self.inner.acknowledge(work.0)
    }

    /// Merge with another subscription.
    pub fn merge(&mut self, other: KvChangeSubscription) {
        self.inner = self.inner.clone().merge(other.inner);
    }

    /// Wait for changes.
    pub async fn changed(&mut self) -> Result<(), JsKvSyncTransportError> {
        self.inner
            .changed()
            .await
            .map_err(|_| JsKvSyncTransportError::new("subscription closed".into()))
    }
}

/// Opaque change work handle.
#[cfg(feature = "sync")]
#[wasm_bindgen]
pub struct JsKvChangeWork(ofdb_btree::ChangeWork);

#[cfg(feature = "sync")]
#[wasm_bindgen]
impl JsKvChangeWork {
    // Opaque - no public methods
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
