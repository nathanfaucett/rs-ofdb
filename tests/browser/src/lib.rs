#![cfg(target_arch = "wasm32")]

mod deadlines;
mod local_storage;
mod smoke;

mod indexeddb;

#[cfg(feature = "sync")]
mod kv_sync;
mod sql_indexeddb;
