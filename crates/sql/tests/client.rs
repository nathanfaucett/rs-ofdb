#![cfg(all(any(feature = "in-memory", feature = "redb"), feature = "sql"))]

use futures::executor::block_on;
use ofdb_sql::{
    Database, FromRow, FromRowError, QueryParams, Row, SqlTranslator, Statement, TranslateError,
    Translator, Value,
};

#[cfg(feature = "in-memory")]
#[test]
fn embedded_client_owns_shared_storage_without_tokio() {
    block_on(async {
        let database = Database::in_memory();
        let client = database.client();
        drop(database);

        client
            .execute_sql("CREATE TABLE items (id UUID PRIMARY KEY)", None)
            .await
            .expect("execute without Tokio runtime");
        let rows = client
            .translate_and_execute("SELECT id FROM items", &SqlTranslator)
            .await
            .expect("translate and execute through client");
        assert_eq!(rows.len(), 1);

        let clone = client.clone();
        assert!(clone.execute(Vec::new()).await.is_err());
    });
}

#[cfg(all(feature = "in-memory", feature = "sql"))]
#[derive(Debug, PartialEq, Eq)]
struct NamedRow(String);

#[cfg(all(feature = "in-memory", feature = "sql"))]
impl FromRow for NamedRow {
    fn from_row(row: &Row, columns: &[&str]) -> Result<Self, FromRowError> {
        let value = ofdb_sql::decode::<String>(ofdb_sql::value(row, columns, "name")?, "name")?;
        Ok(Self(value))
    }
}

#[cfg(all(feature = "in-memory", feature = "sql"))]
#[test]
fn embedded_client_exposes_generic_translator_helpers() {
    block_on(async {
        let database = Database::in_memory();
        let client = database.client();
        let shared = database.client();
        client
            .translate_and_execute(
                "CREATE TABLE names (id UUID PRIMARY KEY, name TEXT)",
                &SqlTranslator,
            )
            .await
            .expect("create table through generic helper");
        let params = QueryParams::Positional(vec![
            Value::Uuid(
                ofdb_sql::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc")
                    .expect("valid test UUID"),
            ),
            Value::from("Ada"),
        ]);
        client
            .translate_and_execute_with_params(
                "INSERT INTO names VALUES ($1, $2)",
                Some(&params),
                &SqlTranslator,
            )
            .await
            .expect("execute parameterized statement through client");
        let names = shared
            .translate_and_select::<_, NamedRow>("SELECT name FROM names", &SqlTranslator)
            .await
            .expect("second client sees first client's write");
        assert_eq!(names, vec![NamedRow("Ada".to_owned())]);
    });
}

#[cfg(feature = "redb")]
#[test]
fn redb_client_survives_database_drop_and_persists_data() {
    block_on(async {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let path = directory.path().join("embedded.redb");
        let client = {
            let database = Database::open(&path).expect("open embedded database");
            let client = database.client();
            client
                .execute_sql("CREATE TABLE names (id UUID PRIMARY KEY, name TEXT)", None)
                .await
                .expect("create table through embedded client");
            client
                .execute_sql(
                    "INSERT INTO names VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
                    None,
                )
                .await
                .expect("insert through embedded client");
            client
        };
        let results = client
            .execute_sql("SELECT name FROM names", None)
            .await
            .expect("query after dropping database handle");
        assert_eq!(results[0].rows, vec![Row::from(["Ada"])]);
        drop(client);

        let reopened = Database::open(&path).expect("reopen persistent database");
        let results = reopened
            .client()
            .execute_sql("SELECT name FROM names", None)
            .await
            .expect("query persisted row");
        assert_eq!(results[0].rows, vec![Row::from(["Ada"])]);
    });
}

#[cfg(all(feature = "in-memory", feature = "sql"))]
struct PendingTranslator;

#[cfg(all(feature = "in-memory", feature = "sql"))]
impl Translator for PendingTranslator {
    fn translate_with_params(
        &self,
        _query: &str,
        _params: Option<&QueryParams>,
    ) -> impl core::future::Future<Output = Result<Vec<Statement>, TranslateError>> {
        core::future::pending()
    }
}

#[cfg(all(feature = "in-memory", feature = "sql"))]
#[tokio::test]
async fn deadline_covers_translation_and_is_independent_per_clone() {
    let database = Database::in_memory();
    let base = database.client();
    let timed = base
        .clone()
        .with_deadline(std::time::Duration::from_millis(1));
    let error = timed
        .translate_and_execute("pending", &PendingTranslator)
        .await
        .expect_err("pending translation must time out");
    assert_eq!(error.kind, ofdb_sql::ErrorKind::Timeout);
    base.execute_sql("CREATE TABLE independent (id UUID PRIMARY KEY)", None)
        .await
        .expect("untimed clone keeps its independent deadline");
}

#[cfg(feature = "in-memory")]
#[test]
fn embedded_client_debug_does_not_show_database_data() {
    let client = Database::in_memory().client();
    assert!(format!("{client:?}").contains("Embedded"));
}
