// Shared sync types for both KV and SQL WASM bindings.

use ofdb_btree::ChangeEvent;
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

/// Push sync configuration.
#[wasm_bindgen]
#[derive(Clone, Debug)]
pub struct PushSynchronizeConfig {
    inner: OfdbPushSynchronizeConfig,
}

#[wasm_bindgen]
impl PushSynchronizeConfig {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: OfdbPushSynchronizeConfig::default(),
        }
    }

    #[wasm_bindgen(getter)]
    pub fn protocol(&self) -> PushConfig {
        PushConfig {
            inner: self.inner.protocol.clone(),
        }
    }

    #[wasm_bindgen(setter)]
    pub fn set_protocol(&mut self, value: PushConfig) {
        self.inner.protocol = value.inner;
    }

    #[wasm_bindgen(getter, js_name = "handshakeTimeoutMs")]
    pub fn handshake_timeout_ms(&self) -> u64 {
        self.inner.handshake_timeout.as_millis() as u64
    }

    #[wasm_bindgen(setter, js_name = "handshakeTimeoutMs")]
    pub fn set_handshake_timeout_ms(&mut self, value: u64) {
        self.inner.handshake_timeout = std::time::Duration::from_millis(value);
    }

    #[wasm_bindgen(getter, js_name = "messageTimeoutMs")]
    pub fn message_timeout_ms(&self) -> u64 {
        self.inner.message_timeout.as_millis() as u64
    }

    #[wasm_bindgen(setter, js_name = "messageTimeoutMs")]
    pub fn set_message_timeout_ms(&mut self, value: u64) {
        self.inner.message_timeout = std::time::Duration::from_millis(value);
    }

    #[wasm_bindgen(getter, js_name = "sendDoneOnExhaust")]
    pub fn send_done_on_exhaust(&self) -> bool {
        self.inner.send_done_on_exhaust
    }

    #[wasm_bindgen(setter, js_name = "sendDoneOnExhaust")]
    pub fn set_send_done_on_exhaust(&mut self, value: bool) {
        self.inner.send_done_on_exhaust = value;
    }
}

impl Default for PushSynchronizeConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl From<PushSynchronizeConfig> for OfdbPushSynchronizeConfig {
    fn from(config: PushSynchronizeConfig) -> Self {
        config.inner
    }
}

/// Protocol configuration.
#[wasm_bindgen]
#[derive(Clone, Debug)]
pub struct PushConfig {
    inner: OfdbPushConfig,
}

#[wasm_bindgen]
impl PushConfig {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: OfdbPushConfig::default(),
        }
    }

    #[wasm_bindgen(getter, js_name = "maxFrameBytes")]
    pub fn max_frame_bytes(&self) -> usize {
        self.inner.max_frame_bytes
    }

    #[wasm_bindgen(setter, js_name = "maxFrameBytes")]
    pub fn set_max_frame_bytes(&mut self, value: usize) {
        self.inner.max_frame_bytes = value;
    }

    #[wasm_bindgen(getter, js_name = "maxPendingPerKey")]
    pub fn max_pending_per_key(&self) -> usize {
        self.inner.max_pending_per_key
    }

    #[wasm_bindgen(setter, js_name = "maxPendingPerKey")]
    pub fn set_max_pending_per_key(&mut self, value: usize) {
        self.inner.max_pending_per_key = value;
    }

    #[wasm_bindgen(getter, js_name = "maxTotalPending")]
    pub fn max_total_pending(&self) -> usize {
        self.inner.max_total_pending
    }

    #[wasm_bindgen(setter, js_name = "maxTotalPending")]
    pub fn set_max_total_pending(&mut self, value: usize) {
        self.inner.max_total_pending = value;
    }

    #[wasm_bindgen(getter, js_name = "sessionTimeoutMs")]
    pub fn session_timeout_ms(&self) -> u64 {
        self.inner.session_timeout_ms
    }

    #[wasm_bindgen(setter, js_name = "sessionTimeoutMs")]
    pub fn set_session_timeout_ms(&mut self, value: u64) {
        self.inner.session_timeout_ms = value;
    }

    #[wasm_bindgen(getter, js_name = "immediateAck")]
    pub fn immediate_ack(&self) -> bool {
        self.inner.immediate_ack
    }

    #[wasm_bindgen(setter, js_name = "immediateAck")]
    pub fn set_immediate_ack(&mut self, value: bool) {
        self.inner.immediate_ack = value;
    }
}

impl Default for PushConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl From<PushConfig> for OfdbPushConfig {
    fn from(config: PushConfig) -> Self {
        config.inner
    }
}

/// Push synchronization result.
#[wasm_bindgen]
pub struct PushSynchronizeResult {
    inner: OfdbPushResult,
}

#[wasm_bindgen]
impl PushSynchronizeResult {
    #[wasm_bindgen(getter, js_name = "bytesSent")]
    pub fn bytes_sent(&self) -> usize {
        self.inner.bytes_sent as usize
    }

    #[wasm_bindgen(getter, js_name = "bytesReceived")]
    pub fn bytes_received(&self) -> usize {
        self.inner.bytes_received as usize
    }

    #[wasm_bindgen(getter, js_name = "eventsSent")]
    pub fn events_sent(&self) -> usize {
        self.inner.events_sent as usize
    }

    #[wasm_bindgen(getter, js_name = "eventsReceived")]
    pub fn events_received(&self) -> usize {
        self.inner.events_received as usize
    }
}

impl From<OfdbPushResult> for PushSynchronizeResult {
    fn from(inner: OfdbPushResult) -> Self {
        Self { inner }
    }
}

/// Push synchronization error.
#[wasm_bindgen]
pub struct JsPushSyncError {
    inner: OfdbPushSynchronizeError<JsPushSyncTransportError>,
}

#[wasm_bindgen]
impl JsPushSyncError {
    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.inner.to_string()
    }

    #[wasm_bindgen(getter)]
    pub fn kind(&self) -> JsPushSyncErrorKind {
        match &self.inner {
            OfdbPushSynchronizeError::Transport(_) => JsPushSyncErrorKind::Transport,
            OfdbPushSynchronizeError::Protocol(_) => JsPushSyncErrorKind::Protocol,
            OfdbPushSynchronizeError::Apply(_) => JsPushSyncErrorKind::Apply,
            OfdbPushSynchronizeError::SessionTimeout => JsPushSyncErrorKind::SessionTimeout,
            OfdbPushSynchronizeError::PeerClosed => JsPushSyncErrorKind::PeerClosed,
            OfdbPushSynchronizeError::HandshakeFailed(_) => JsPushSyncErrorKind::HandshakeFailed,
            OfdbPushSynchronizeError::GenerationGap { .. } => JsPushSyncErrorKind::GenerationGap,
        }
    }
}

impl From<OfdbPushSynchronizeError<JsPushSyncTransportError>> for JsPushSyncError {
    fn from(inner: OfdbPushSynchronizeError<JsPushSyncTransportError>) -> Self {
        Self { inner }
    }
}

/// Push sync error kind.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsPushSyncErrorKind {
    Transport = 0,
    Protocol = 1,
    Apply = 2,
    SessionTimeout = 3,
    PeerClosed = 4,
    HandshakeFailed = 5,
    GenerationGap = 6,
}

/// Transport error for JS-implemented transports.
#[wasm_bindgen]
pub struct JsPushSyncTransportError {
    message: String,
}

#[wasm_bindgen]
impl JsPushSyncTransportError {
    #[wasm_bindgen(constructor)]
    pub fn new(message: String) -> Self {
        Self { message }
    }

    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }
}

impl std::fmt::Display for JsPushSyncTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::fmt::Debug for JsPushSyncTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsPushSyncTransportError({})", self.message)
    }
}

impl std::error::Error for JsPushSyncTransportError {}

/// Push sync transport trait that can be implemented in JavaScript.
#[wasm_bindgen]
pub struct JsPushSyncTransport {
    inner: JsValue,
}

#[wasm_bindgen]
impl JsPushSyncTransport {
    #[wasm_bindgen(constructor)]
    pub fn new(transport: JsValue) -> Self {
        Self { inner: transport }
    }
}

impl OfdbPushSyncTransport for JsPushSyncTransport {
    type Error = JsPushSyncTransportError;

    fn send(&mut self, msg: ofdb_sync::PushMessage) -> impl Future<Output = Result<(), Self::Error>> {
        let transport = self.inner.clone();
        async move {
            let send_fn = js_sys::Reflect::get(&transport, &"send".into()).map_err(|_| {
                JsPushSyncTransportError::new("transport has no send method".into())
            })?;
            let send_fn: js_sys::Function = send_fn
                .dyn_into()
                .map_err(|_| JsPushSyncTransportError::new("send is not a function".into()))?;

            // Serialize message to bytes
            let bytes = ofdb_sync::encode_message(&msg, &OfdbPushConfig::default())
                .map_err(|e| JsPushSyncTransportError::new(format!("encode failed: {:?}", e)))?;
            let array = js_sys::Uint8Array::from(bytes.as_slice());
            let promise = send_fn
                .call1(&transport, &array)
                .map_err(|e| JsPushSyncTransportError::new(format!("send call failed: {:?}", e)))?;
            let promise: js_sys::Promise = promise.dyn_into().map_err(|_| {
                JsPushSyncTransportError::new("send did not return a Promise".into())
            })?;

            JsFuture::from(promise)
                .await
                .map_err(|e| JsPushSyncTransportError::new(format!("send failed: {:?}", e)))?;

            Ok(())
        }
    }

    fn receive(&mut self) -> impl Future<Output = Result<ofdb_sync::PushMessage, Self::Error>> {
        let transport = self.inner.clone();
        async move {
            let recv_fn = js_sys::Reflect::get(&transport, &"receive".into()).map_err(|_| {
                JsPushSyncTransportError::new("transport has no receive method".into())
            })?;
            let recv_fn: js_sys::Function = recv_fn
                .dyn_into()
                .map_err(|_| JsPushSyncTransportError::new("receive is not a function".into()))?;

            let promise = recv_fn.call0(&transport).map_err(|e| {
                JsPushSyncTransportError::new(format!("receive call failed: {:?}", e))
            })?;
            let promise: js_sys::Promise = promise.dyn_into().map_err(|_| {
                JsPushSyncTransportError::new("receive did not return a Promise".into())
            })?;

            let result = JsFuture::from(promise)
                .await
                .map_err(|e| JsPushSyncTransportError::new(format!("receive failed: {:?}", e)))?;

            let array = js_sys::Uint8Array::from(result);
            let mut vec = vec![0u8; array.length() as usize];
            array.copy_to(&mut vec);

            ofdb_sync::decode_message(&vec, &OfdbPushConfig::default())
                .map_err(|e: ofdb_sync::Error<JsPushSyncTransportError>| JsPushSyncTransportError::new(format!("decode failed: {:?}", e)))
        }
    }
}

impl OfdbPushSyncTransport for &mut JsPushSyncTransport {
    type Error = JsPushSyncTransportError;

    fn send(&mut self, msg: ofdb_sync::PushMessage) -> impl Future<Output = Result<(), Self::Error>> {
        (*self).send(msg)
    }

    fn receive(&mut self) -> impl Future<Output = Result<ofdb_sync::PushMessage, Self::Error>> {
        (*self).receive()
    }
}

/// A committed change event with its incremental Automerge blob.
#[wasm_bindgen]
#[derive(Clone)]
pub struct JsChangeEvent {
    inner: ChangeEvent,
}

#[wasm_bindgen]
impl JsChangeEvent {
    #[wasm_bindgen(getter)]
    pub fn table(&self) -> String {
        self.inner.key.table.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn key(&self) -> Vec<u8> {
        self.inner.key.key.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn blob(&self) -> Vec<u8> {
        self.inner.blob.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn generation(&self) -> u64 {
        self.inner.generation
    }
}

impl From<ChangeEvent> for JsChangeEvent {
    fn from(inner: ChangeEvent) -> Self {
        Self { inner }
    }
}

/// Change subscription for push-based sync (KV).
/// Provides a stream of committed changes via `events()`.
#[wasm_bindgen]
pub struct KvChangeSubscription {
    inner: ofdb_btree::ChangeSubscription,
}

impl KvChangeSubscription {
    pub(crate) fn new(inner: ofdb_btree::ChangeSubscription) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl KvChangeSubscription {
    /// Get the next change event from the stream.
    /// Returns `null` when no more events are currently available.
    /// Call again after awaiting `wait_for_changes()` to get new events.
    #[cfg(feature = "sync")]
    pub async fn next_event(&mut self) -> Option<JsChangeEvent> {
        let mut stream = self.inner.events();
        futures_util::StreamExt::next(&mut stream)
            .await
            .map(JsChangeEvent::from)
    }

    /// Wait for new changes to become available.
    /// Returns `true` if new changes are available, `false` if the subscription is closed.
    #[cfg(feature = "sync")]
    pub async fn wait_for_changes(&mut self) -> bool {
        self.inner.changed().await.is_ok()
    }

    /// Merge another subscription into this one.
    #[cfg(feature = "sync")]
    pub fn merge(&mut self, other: KvChangeSubscription) {
        let other_inner = other.inner;
        let self_inner = std::mem::replace(&mut self.inner, other_inner.clone());
        self.inner = self_inner.merge(other_inner);
    }
}

/// Change subscription for push-based sync (SQL).
/// Provides a stream of committed changes via `events()`.
#[wasm_bindgen]
pub struct SqlChangeSubscription {
    inner: ofdb_btree::ChangeSubscription,
}

impl SqlChangeSubscription {
    pub(crate) fn new(inner: ofdb_btree::ChangeSubscription) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl SqlChangeSubscription {
    /// Get the next change event from the stream.
    /// Returns `null` when no more events are currently available.
    /// Call again after awaiting `wait_for_changes()` to get new events.
    #[cfg(feature = "sync")]
    pub async fn next_event(&mut self) -> Option<JsChangeEvent> {
        let mut stream = self.inner.events();
        futures_util::StreamExt::next(&mut stream)
            .await
            .map(JsChangeEvent::from)
    }

    /// Wait for new changes to become available.
    /// Returns `true` if new changes are available, `false` if the subscription is closed.
    #[cfg(feature = "sync")]
    pub async fn wait_for_changes(&mut self) -> bool {
        self.inner.changed().await.is_ok()
    }

    /// Merge another subscription into this one.
    #[cfg(feature = "sync")]
    pub fn merge(&mut self, other: SqlChangeSubscription) {
        let other_inner = other.inner;
        let self_inner = std::mem::replace(&mut self.inner, other_inner.clone());
        self.inner = self_inner.merge(other_inner);
    }
}
