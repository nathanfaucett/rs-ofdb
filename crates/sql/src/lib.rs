#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod api;
#[cfg(any(
    feature = "remote",
    feature = "redb",
    feature = "in-memory",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
mod client;
#[cfg(any(
    feature = "redb",
    feature = "in-memory",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
mod database;
mod error;
#[cfg(all(
    feature = "sql",
    any(
        feature = "remote",
        feature = "redb",
        feature = "in-memory",
        all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
    )
))]
mod migration;
#[cfg(all(
    feature = "sql",
    any(
        feature = "remote",
        feature = "redb",
        feature = "in-memory",
        all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
    )
))]
pub use migration::{Migration, MigrationError, MigrationPhase, MigrationResult};

pub use api::*;
#[cfg(any(
    feature = "remote",
    feature = "redb",
    feature = "in-memory",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
pub use client::{Client, Transaction};
#[cfg(any(
    feature = "redb",
    feature = "in-memory",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
pub use database::Database;
pub use error::{Error, ErrorKind};
