use ofdb_kv::{Client, Database};
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::value::{parse_json, value_from_json, value_to_json, write_json};

#[wasm_bindgen]
pub struct Kv {
    client: Client,
}

#[wasm_bindgen]
impl Kv {
    /// Open a key-value store backed by IndexedDB.
    /// `name` is the database name (used as the IndexedDB database name).
    pub async fn open(name: String) -> Result<Kv, JsError> {
        let database = Database::open_indexeddb(&name).await?;
        Ok(Self {
            client: database.client(),
        })
    }

    /// Read a committed value as JSON; a missing key becomes JavaScript undefined.
    pub async fn get(&self, key: String) -> Result<Option<String>, JsError> {
        match self.client.get(&key).await? {
            Some(value) => Ok(Some(write_json(&value_to_json(&value))?)),
            None => Ok(None),
        }
    }

    /// Store a JSON value as the committed value.
    pub async fn set(&self, key: String, value: String) -> Result<(), JsError> {
        let value = value_from_json(&parse_json(&value)?);
        self.client.set(&key, value, None).await?;
        Ok(())
    }

    /// Remove a committed value.
    pub async fn delete(&self, key: String) -> Result<(), JsError> {
        self.client.delete(&key).await?;
        Ok(())
    }
}
