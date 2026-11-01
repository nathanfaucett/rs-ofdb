use std::collections::BTreeMap;

use ofdb_sql::{Client, Database, QueryParams};
use serde_json::Value as Json;
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::value::{parse_json, value_from_json, value_to_json, write_json};

#[wasm_bindgen]
pub struct Sql {
    client: Client,
}

#[wasm_bindgen]
impl Sql {
    /// Open a SQL store backed by IndexedDB.
    /// `name` is the database name (used as the IndexedDB database name).
    pub async fn open(name: String) -> Result<Sql, JsError> {
        let client = Database::open_indexeddb(&name).await?.client();
        Ok(Self { client })
    }

    /// Execute SQL text with optional bound parameters and return rows as JSON.
    ///
    /// Parameters are a JSON array for positional placeholders or a JSON object for
    /// named placeholders. The result is a JSON array of `{ columns, rows }` objects.
    pub async fn execute(&self, sql: String, params: Option<String>) -> Result<String, JsError> {
        let params = match params {
            Some(params) => Some(parse_params(&params)?),
            None => None,
        };
        let results = self.client.execute_sql(&sql, params.as_ref()).await?;
        let results = results
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
            .collect::<Vec<_>>();
        write_json(&Json::Array(results))
    }
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
