use std::collections::BTreeMap;
use std::time::Duration;

use ofdb_sql::{Client, Database, QueryParams, QueryResult, Transaction};
use serde_json::Value as Json;
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::value::{parse_json, value_from_json, value_to_json, write_json};

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
}

#[wasm_bindgen]
pub struct SqlClient {
    client: Client,
}

#[wasm_bindgen]
impl SqlClient {
    /// Execute SQL text with optional bound parameters and return results as JSON.
    ///
    /// Parameters are a JSON array for positional placeholders or a JSON object for
    /// named placeholders. The result is a JSON array of `{ columns, rows }` objects.
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
                    .map(|row| Json::Array(row.values.iter().map(value_to_json).collect()))
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
            values.iter().map(value_from_json).collect(),
        )),
        Json::Object(values) => Ok(QueryParams::Named(
            values
                .iter()
                .map(|(name, value)| (name.clone(), value_from_json(value)))
                .collect::<BTreeMap<_, _>>(),
        )),
        other => Err(JsError::new(&format!(
            "query parameters must be a JSON array or object, got {other}"
        ))),
    }
}
