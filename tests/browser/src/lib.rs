#![cfg(target_arch = "wasm32")]

mod local_storage;
mod smoke;

mod indexeddb;

#[cfg(feature = "sync")]
mod kv_sync;
mod sql_indexeddb;
mod sql_semantics;
