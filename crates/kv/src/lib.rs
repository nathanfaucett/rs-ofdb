#![forbid(unsafe_code)]

mod error;

#[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
/// A key-value entry for WASM bindings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvEntry {
    pub key: String,
    pub value: Value,
}

#[cfg(any(
    feature = "remote",
    feature = "in-memory",
    feature = "redb",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
mod client;
#[cfg(any(
    feature = "in-memory",
    feature = "redb",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
mod database;
#[cfg(any(
    feature = "remote",
    feature = "in-memory",
    feature = "redb",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
mod transaction;

#[cfg(feature = "sync")]
pub use btree::ChangeWork;
#[cfg(any(
    feature = "remote",
    feature = "in-memory",
    feature = "redb",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
pub use client::Client;
#[cfg(any(
    feature = "in-memory",
    feature = "redb",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
pub use database::Database;
pub use error::{Error, ErrorKind};
#[cfg(feature = "sync")]
pub use sync::{
    ChangeSubscription, Config as SyncConfig, SyncResult, SyncTransport,
    SynchronizeError as SyncError,
};
#[cfg(any(
    feature = "remote",
    feature = "in-memory",
    feature = "redb",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
pub use transaction::Transaction;

#[cfg(all(test, feature = "remote"))]
mod error_tests;
#[cfg(feature = "server")]
pub use kv_server::Server;

pub use value::{JsonNumber, JsonValue, Value, ValueType};
mod value_codec;
pub use value_codec::{encode_value, parse_value};

#[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
pub use automerge;
#[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
pub use client::Client as KvClient;
#[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
pub use database::Storage;
#[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
pub use kv_store::NativeChange;
#[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
pub use transaction::Transaction as KvTransaction;

#[cfg(feature = "sync")]
/// Snapshot of a key-value pair for synchronization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvSnapshot {
    pub key: String,
    pub payload: Vec<u8>,
}

#[cfg(feature = "sync")]
/// Role in a synchronization session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncRole {
    Initiator,
    Responder,
}
