#![forbid(unsafe_code)]

mod kv;
mod sql;
mod sql_migration;
mod value;

use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen(start)]
pub async fn init() {
    console_error_panic_hook::set_once();
}

#[cfg(feature = "sync")]
pub use kv::{JsKvChangeWork, KvChangeSubscription};
pub use kv::{
    JsKvSyncError, JsKvSyncErrorKind, JsKvSyncTransport, JsKvSyncTransportError, KvSyncConfig,
    KvSyncRole,
};
pub use kv::{KvClient, KvDatabase, KvTransaction};
pub use sql::{
    JsSqlSyncError, JsSqlSyncErrorKind, JsSqlSyncTransport, JsSqlSyncTransportError,
    SqlSessionConfig, SqlSyncDirection, SqlSyncResult, SqlSyncRole,
};
pub use sql::{SqlClient, SqlDatabase, SqlTransaction};
