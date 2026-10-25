#![cfg(target_arch = "wasm32")]

mod deadlines;
mod local_storage;
mod smoke;

mod indexeddb;

#[cfg(feature = "sync")]
mod kv_sync;
mod sql_indexeddb;
mod sql_semantics;
mod storage_lifecycle;

#[cfg(feature = "sync")]
mod sql_sync;

#[cfg(feature = "sync")]
mod sql_sync_support;
