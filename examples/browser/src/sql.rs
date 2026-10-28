use ofdb_sql::{Client, Database, QueryParams, Value};
use wasm_bindgen::{JsError, prelude::wasm_bindgen};

#[wasm_bindgen]
pub struct Sql {
    client: Client,
}

#[wasm_bindgen]
impl Sql {
    /// Open persistent SQL storage and initialize the schema without seeding rows.
    pub async fn open() -> Result<Sql, JsError> {
        let client = Database::open_indexeddb("ofdb-browser-example-sql")
            .await?
            .client();
        client
            .execute_sql(
                "CREATE TABLE IF NOT EXISTS records (id UUID PRIMARY KEY, text TEXT)",
                None,
            )
            .await?;
        Ok(Self { client })
    }

    /// Bind literal user text; resolve only after the standalone insert commits.
    pub async fn add(&self, text: String) -> Result<(), JsError> {
        let params = QueryParams::Positional(vec![Value::Text(text)]);
        self.client
            .execute_sql("INSERT INTO records (text) VALUES ($1)", Some(&params))
            .await?;
        Ok(())
    }

    /// Read committed records in deterministic row-identity order.
    pub async fn list(&self) -> Result<Vec<String>, JsError> {
        let results = self
            .client
            .execute_sql("SELECT text FROM records ORDER BY id", None)
            .await?;
        results
            .into_iter()
            .flat_map(|result| result.rows)
            .map(|row| match row.values.into_iter().next() {
                Some(Value::Text(text)) => Ok(text),
                _ => Err(JsError::new("Stored SQL record is not text")),
            })
            .collect()
    }
}
