#![forbid(unsafe_code)]

mod error;

#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb"))]
mod client;
#[cfg(any(feature = "in-memory", feature = "redb"))]
mod database;
#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb"))]
mod transaction;

#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb"))]
pub use client::Client;
#[cfg(any(feature = "in-memory", feature = "redb"))]
pub use database::Database;
pub use error::{Error, ErrorKind};
#[cfg(feature = "sync")]
pub use kv_sync::{Config as SyncConfig, KvSnapshot, SyncRole, SyncTransport};
#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb"))]
pub use transaction::Transaction;

#[cfg(all(test, feature = "remote"))]
mod error_tests;
#[cfg(feature = "server")]
pub use kv_server::Server;
