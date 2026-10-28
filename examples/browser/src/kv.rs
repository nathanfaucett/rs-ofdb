use ofdb_kv::{Client, Database, Value};
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
pub struct Kv {
    client: Client,
}

#[wasm_bindgen]
impl Kv {
    /// Open the stable example store without resetting existing data.
    pub async fn open() -> Result<Kv, JsError> {
        let database = Database::open_indexeddb("ofdb-browser-example-kv").await?;
        Ok(Self {
            client: database.client(),
        })
    }

    /// Resolve only after the standalone write commits.
    pub async fn save(&self, key: String, value: String) -> Result<(), JsError> {
        self.client.set(&key, value, None).await?;
        Ok(())
    }

    /// Read committed text; a missing key becomes JavaScript undefined.
    pub async fn retrieve(&self, key: String) -> Result<Option<String>, JsError> {
        match self.client.get(&key).await? {
            Some(Value::Text(text)) => Ok(Some(text)),
            None => Ok(None),
            Some(_) => Err(JsError::new("Stored value is not text")),
        }
    }
}
