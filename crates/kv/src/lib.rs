#![forbid(unsafe_code)]

mod error;

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
pub use kv_sync::{
    ChangeSubscription, Config as SyncConfig, Error as SyncError, KvInvalidation, KvSnapshot,
    SyncRole, SyncTransport,
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
