use futures::executor::block_on;
use ofdb_kv::Database;
use ofdb_kv::Value;

#[test]
fn existing_named_table_transactions_reopen_without_conversion() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("named.redb");
    block_on(async {
        let database = Database::open_with_table(&path, "kv").expect("create consumer table");
        let client = database.client();
        let mut tx = client.transaction().await.expect("begin");
        tx.set("a", Value::Integer(42), None)
            .await
            .expect("typed value");
        tx.set("ab", Value::Blob(vec![0, 255]), None)
            .await
            .expect("blob");
        tx.set("expired", Value::Bool(true), Some(0))
            .await
            .expect("absolute expiry");
        tx.commit().await.expect("commit");
        let mut tx = client.transaction().await.expect("begin rollback");
        tx.delete("a").await.expect("delete");
        tx.set("ab", Value::Null, None).await.expect("overwrite");
        tx.rollback().await.expect("rollback");
        assert_eq!(
            client.scan_prefix("a").await.expect("prefix"),
            vec![
                ("a".into(), Value::Integer(42)),
                ("ab".into(), Value::Blob(vec![0, 255]))
            ]
        );
        assert_eq!(
            client.scan("a", "ab").await.expect("range"),
            vec![("a".into(), Value::Integer(42))]
        );
        assert_eq!(client.get("expired").await.expect("expiry"), None);
        client.delete("ab").await.expect("tombstone");
    });
    block_on(async {
        let database = Database::open_with_table(&path, "kv").expect("open existing table");
        assert_eq!(
            database.client().get("a").await.expect("persisted"),
            Some(Value::Integer(42))
        );
        assert_eq!(database.client().get("ab").await.expect("deleted"), None);
    });
    assert!(
        Database::open(&path).is_err(),
        "missing default table must not replace kv"
    );
    assert!(Database::open_with_table(&path, "missing").is_err());
}

#[test]
fn unsupported_named_table_is_not_replaced() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("unsupported.redb");
    {
        let db = redb::Database::create(&path).expect("fixture database");
        let tx = db.begin_write().expect("fixture transaction");
        tx.open_table(redb::TableDefinition::<u64, u64>::new("kv"))
            .expect("incompatible table");
        tx.commit().expect("fixture commit");
    }
    assert!(Database::open_with_table(&path, "kv").is_err());
}

#[test]
fn redb_client_shares_storage_and_survives_database_drop() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("embedded.redb");
    let client = {
        let database = Database::open(&path).expect("open embedded database");
        let client = database.client();
        let shared = database.client();
        block_on(async {
            client
                .set("binary", Value::Blob(vec![0, 255]), None)
                .await
                .expect("write through client");
            assert_eq!(
                shared
                    .get("binary")
                    .await
                    .expect("database reads client write"),
                Some(Value::Blob(vec![0, 255]))
            );
        });
        client
    };
    block_on(async {
        assert_eq!(
            client
                .get("binary")
                .await
                .expect("client works after database drop"),
            Some(Value::Blob(vec![0, 255]))
        );
    });
    drop(client);

    block_on(async {
        let reopened = Database::open(&path).expect("reopen persisted database");
        assert_eq!(
            reopened
                .client()
                .get("binary")
                .await
                .expect("read persisted value"),
            Some(Value::Blob(vec![0, 255]))
        );
    });
}
