#![cfg(feature = "redb")]

use ofdb_kv::{Database, SharedDatabase, Value};

#[tokio::test]
async fn shared_kv_is_durable_isolated_and_retirement_preserves_other_namespaces() {
    let root = tempfile::tempdir().expect("host root");
    let path = root.path().join("unified.redb");
    let host = SharedDatabase::create(&path).expect("host");
    let first = Database::open_shared(&host, "first").expect("first store");
    let second = Database::open_shared(&host, "second").expect("second store");
    first
        .client()
        .set("same", Value::Text("first".into()), None)
        .await
        .expect("first value");
    second
        .client()
        .set("same", Value::Text("second".into()), None)
        .await
        .expect("second value");
    drop(first);
    drop(second);
    drop(host);
    let host = SharedDatabase::create(&path).expect("reopen host");
    let first = Database::open_shared(&host, "first").expect("retained store");
    let issued = Database::open_shared(&host, "first")
        .expect("shared lifecycle")
        .client();
    assert_eq!(
        issued.get("same").await.expect("retained value"),
        Some(Value::Text("first".into()))
    );
    let mut transaction = issued.transaction().await.expect("idle transaction");
    transaction
        .set("not-committed", Value::Integer(1), None)
        .await
        .expect("pending value");
    first
        .retire_and_remove()
        .await
        .expect("retire one namespace");
    assert!(issued.get("same").await.is_err());
    assert!(transaction.commit().await.is_err());
    assert!(Database::open_shared(&host, "first").is_err());
    assert_eq!(
        Database::open_shared(&host, "second")
            .expect("other store")
            .client()
            .get("same")
            .await
            .expect("other value"),
        Some(Value::Text("second".into()))
    );
    assert!(path.is_file());
    assert_eq!(std::fs::read_dir(root.path()).expect("layout").count(), 1);
}
