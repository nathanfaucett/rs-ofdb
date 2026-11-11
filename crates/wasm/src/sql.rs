use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::future::FutureExt;
use ofdb_kv::{encode_value, parse_value};
use ofdb_sql::{Client, Database, QueryParams, QueryResult, Transaction};
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

#[wasm_bindgen]
pub struct SqlDatabase {
    database: Database,
}

#[wasm_bindgen]
impl SqlDatabase {
    /// Open a SQL database backed by IndexedDB.
    pub async fn open(name: String) -> Result<SqlDatabase, JsError> {
        let database = Database::open_indexeddb(&name).await?;
        Ok(Self { database })
    }

    /// Create a query handle sharing this database's storage.
    pub fn client(&self) -> SqlClient {
        SqlClient {
            client: self.database.client(),
        }
    }

    /// Get the change subscription for push-based synchronization.
    /// The subscription provides a stream of committed changes via `events()`.
    #[cfg(feature = "sync")]
    pub fn sync_changes(&self) -> Result<crate::sync::SqlChangeSubscription, JsError> {
        let subscription = self.database.sync_changes()?;
        Ok(crate::sync::SqlChangeSubscription { inner: subscription })
    }

    /// Push-based synchronization with a remote peer.
    /// Streams local changes to peer and applies remote changes.
    #[cfg(feature = "sync")]
    pub async fn synchronize(
        &self,
        transport: &mut crate::sync::JsPushSyncTransport,
        config: crate::sync::PushSynchronizeConfig,
    ) -> Result<crate::sync::PushSynchronizeResult, crate::sync::JsPushSyncError> {
        self.database
            .synchronize(transport, config.into())
            .await
            .map_err(|e| crate::sync::JsPushSyncError::from(e))
            .map(crate::sync::PushSynchronizeResult::from)
    }
}

#[wasm_bindgen]
pub struct SqlClient {
    client: Client,
}

#[wasm_bindgen]
impl SqlClient {
    /// Apply the complete application migration history through the Rust runner.
    pub async fn migrate(&self, migrations: String) -> Result<String, JsValue> {
        crate::sql_migration::migrate(&self.client, &migrations).await
    }

    /// Execute SQL text with optional bound parameters and return results as JSON.
    ///
    /// Parameters are a JSON array for positional placeholders or a JSON object for
    /// named placeholders. Every value uses the owner's lossless tagged codec.
    /// The result is a JSON array of `{ columns, rows }` objects with tagged cells.
    pub async fn execute(&self, sql: String, params: Option<String>) -> Result<String, JsError> {
        let params = match params {
            Some(params) => Some(parse_params(&params)?),
            None => None,
        };
        let results = self.client.execute_sql(&sql, params.as_ref()).await?;
        write_json(&results_to_json(&results))
    }

    /// Start a transaction that holds statements until commit or rollback.
    pub async fn transaction(&self) -> Result<SqlTransaction, JsError> {
        let tx = self.client.transaction().await?;
        Ok(SqlTransaction { tx })
    }

    /// Return a clone of this client with a request deadline in milliseconds.
    pub fn with_deadline(&self, ms: u64) -> SqlClient {
        SqlClient {
            client: self.client.clone().with_deadline(Duration::from_millis(ms)),
        }
    }

    /// Return an independent clone sharing the same storage.
    pub fn clone_client(&self) -> SqlClient {
        SqlClient {
            client: self.client.clone(),
        }
    }
}

#[wasm_bindgen]
pub struct SqlTransaction {
    tx: Transaction,
}

#[wasm_bindgen]
impl SqlTransaction {
    /// Execute SQL text within the transaction.
    pub async fn execute(
        &mut self,
        sql: String,
        params: Option<String>,
    ) -> Result<String, JsError> {
        let params = match params {
            Some(params) => Some(parse_params(&params)?),
            None => None,
        };
        let results = self.tx.execute_sql(&sql, params.as_ref()).await?;
        write_json(&results_to_json(&results))
    }

    pub async fn commit(self) -> Result<(), JsError> {
        self.tx.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), JsError> {
        self.tx.rollback().await?;
        Ok(())
    }
}

fn results_to_json(results: &[QueryResult]) -> Json {
    Json::Array(
        results
            .iter()
            .map(|result| {
                let columns = result
                    .columns
                    .iter()
                    .map(|column| Json::String(column.name.clone()))
                    .collect::<Vec<_>>();
                let rows = result
                    .rows
                    .iter()
                    .map(|row| Json::Array(row.values.iter().cloned().map(encode_value).collect()))
                    .collect::<Vec<_>>();
                Json::Object(
                    [
                        ("columns".to_owned(), Json::Array(columns)),
                        ("rows".to_owned(), Json::Array(rows)),
                    ]
                    .into_iter()
                    .collect(),
                )
            })
            .collect(),
    )
}

/// Parse bound parameters supplied as JSON by the TypeScript layer.
fn parse_params(text: &str) -> Result<QueryParams, JsError> {
    match parse_json(text)? {
        Json::Array(values) => Ok(QueryParams::Positional(
            values
                .iter()
                .map(parse_value)
                .collect::<Result<_, _>>()
                .map_err(|error| JsError::new(&error))?,
        )),
        Json::Object(values) => Ok(QueryParams::Named(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), parse_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, String>>()
                .map_err(|error| JsError::new(&error))?,
        )),
        other => Err(JsError::new(&format!(
            "query parameters must be a JSON array or object, got {other}"
        ))),
    }
}
