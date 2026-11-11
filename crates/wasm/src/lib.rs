#![forbid(unsafe_code)]

mod kv;
mod sql;
mod sql_migration;
mod sync;
mod value;

use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen(start)]
pub async fn init() {
    console_error_panic_hook::set_once();
}

#[cfg(feature = "sync")]
pub use sync::{
    JsChangeEvent, JsPushSyncError, JsPushSyncErrorKind, JsPushSyncTransport,
    JsPushSyncTransportError, PushConfig, PushSynchronizeConfig, PushSynchronizeResult,
    KvChangeSubscription, SqlChangeSubscription,
};
pub use kv::{KvClient, KvDatabase, KvTransaction};
pub use sql::{SqlClient, SqlDatabase, SqlTransaction};
