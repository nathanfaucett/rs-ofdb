#![cfg(all(feature = "redb", feature = "sql"))]

use ofdb_sql::{Database, Value};

#[tokio::test]
async fn existing_only_open_preserves_data_and_never_initializes_missing_or_corrupt_files() {
    let directory = tempfile::tempdir().expect("isolated database directory");
    let path = directory.path().join("existing.redb");
    assert!(Database::open_existing(&path).is_err());
    assert!(!path.exists());
    let database = Database::open(&path).expect("initialize database");
    database
        .client()
        .execute_sql(
            "CREATE TABLE records (id TEXT PRIMARY KEY); INSERT INTO records (id) VALUES ('saved')",
            None,
        )
        .await
        .expect("commit record");
    drop(database);
    let database = Database::open_existing(&path).expect("validate existing database");
    let results = database
        .client()
        .execute_sql("SELECT id FROM records", None)
        .await
        .expect("saved record");
    assert_eq!(results[0].rows[0].values, vec![Value::Text("saved".into())]);
    drop(database);
    for bytes in [b"".as_slice(), b"corrupt database".as_slice()] {
        std::fs::write(&path, bytes).expect("simulate damaged acknowledged content");
        assert!(Database::open_existing(&path).is_err());
        assert_eq!(std::fs::read(&path).expect("unchanged corrupt file"), bytes);
    }
}
