#![cfg(all(feature = "redb", feature = "sql"))]

use ofdb_sql::{Database, SharedDatabase};

#[tokio::test]
async fn shared_transactions_allow_control_reads_and_reject_stale_writes() {
    let host = SharedDatabase::in_memory().expect("host");
    let control = Database::open_shared(&host, "control").expect("control");
    let resource = Database::open_shared(&host, "resource").expect("resource");
    control
        .client()
        .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY)", None)
        .await
        .expect("control schema");
    resource.client().execute_sql("CREATE TABLE notes (id UUID PRIMARY KEY, value TEXT); INSERT INTO notes (value) VALUES ('first')", None).await.expect("resource schema");
    let client = resource.client();
    let mut transaction = client.transaction().await.expect("resource transaction");
    transaction
        .execute_sql("SELECT * FROM notes", None)
        .await
        .expect("read snapshot");
    tokio::time::timeout(
        std::time::Duration::from_millis(200),
        control.client().execute_sql("SELECT * FROM users", None),
    )
    .await
    .expect("control recheck must not wait for resource transaction")
    .expect("control read");
    resource
        .client()
        .execute_sql("UPDATE notes SET value = 'second'", None)
        .await
        .expect("concurrent write");
    transaction
        .execute_sql("UPDATE notes SET value = 'stale'", None)
        .await
        .expect("write old snapshot");
    assert!(transaction.commit().await.is_err());
    let rows = resource
        .client()
        .execute_sql("SELECT value FROM notes", None)
        .await
        .expect("winner");
    assert_eq!(
        rows[0].rows[0].values[0],
        ofdb_sql::Value::Text("second".into())
    );
}

#[tokio::test]
async fn shared_schema_indexes_and_rollbacks_use_one_atomic_snapshot() {
    let host = SharedDatabase::in_memory().expect("host");
    let database = Database::open_shared(&host, "resource").expect("resource");
    let client = database.client();
    let mut transaction = client.transaction().await.expect("schema transaction");
    transaction
        .execute_sql("CREATE TABLE abandoned (id UUID PRIMARY KEY)", None)
        .await
        .expect("uncommitted schema");
    transaction.rollback().await.expect("schema rollback");
    client
            .execute_sql("SELECT * FROM abandoned", None)
            .await.unwrap_err();
    client.execute_sql("CREATE TABLE notes (name TEXT PRIMARY KEY, value TEXT NOT NULL); INSERT INTO notes VALUES ('one', 'saved')", None).await.expect("natural primary-key index");
    let mut transaction = client.transaction().await.expect("data transaction");
    transaction
        .execute_sql(
            "UPDATE notes SET value = 'temporary'; INSERT INTO notes VALUES ('two', 'temporary')",
            None,
        )
        .await
        .expect("uncommitted changes");
    assert_eq!(
        transaction
            .execute_sql("SELECT value FROM notes WHERE name = 'one'", None)
            .await
            .expect("read own writes")[0]
            .rows[0]
            .values[0],
        ofdb_sql::Value::Text("temporary".into())
    );
    transaction.rollback().await.expect("data rollback");
    assert_eq!(
        client
            .execute_sql("SELECT value FROM notes", None)
            .await
            .expect("retained row")[0]
            .rows[0]
            .values[0],
        ofdb_sql::Value::Text("saved".into())
    );
    client
        .execute_sql(
            "DROP TABLE notes; CREATE TABLE notes (id UUID PRIMARY KEY)",
            None,
        )
        .await
        .expect("drop and recreate");
    assert_eq!(
        client
            .execute_sql("SELECT * FROM notes", None)
            .await
            .expect("fresh table")[0]
            .rows, [] as [ofdb_sql::Row; 0]
    );
}

#[tokio::test]
async fn shared_namespaces_isolate_restart_and_retire_without_removing_the_host() {
    let root = tempfile::tempdir().expect("host directory");
    let path = root.path().join("unified.redb");
    let host = SharedDatabase::create(&path).expect("shared host");
    let control = Database::open_shared(&host, "control").expect("control namespace");
    let resource = Database::open_shared(&host, "resource").expect("resource namespace");
    control
        .client()
        .execute_sql("CREATE TABLE private_keys (id UUID PRIMARY KEY)", None)
        .await
        .expect("private schema");
    resource.client().execute_sql("CREATE TABLE notes (id UUID PRIMARY KEY, value TEXT); INSERT INTO notes (value) VALUES ('saved')", None).await.expect("resource data");
    resource
            .client()
            .execute_sql("SELECT * FROM private_keys", None)
            .await.unwrap_err();
    let issued = Database::open_shared(&host, "resource")
        .expect("second resource owner")
        .client();
    drop(resource);
    drop(control);
    drop(issued);
    drop(host);
    let host = SharedDatabase::create(&path).expect("reopen host");
    let resource = Database::open_shared(&host, "resource").expect("reopen resource");
    let issued = Database::open_shared(&host, "resource")
        .expect("shared lifecycle")
        .client();
    assert_eq!(
        issued
            .execute_sql("SELECT value FROM notes", None)
            .await
            .expect("durable data")[0]
            .rows
            .len(),
        1
    );
    let mut idle = issued.transaction().await.expect("idle transaction");
    idle.execute_sql("SELECT * FROM notes", None)
        .await
        .expect("transaction snapshot");
    resource
        .retire_and_remove()
        .await
        .expect("retire namespace");
    issued
            .execute_sql("SELECT * FROM notes", None)
            .await.unwrap_err();
    assert!(idle.commit().await.is_err());
    assert!(Database::open_shared(&host, "resource").is_err());
    let control = Database::open_shared(&host, "control").expect("control survives");
    control
        .client()
        .execute_sql("SELECT * FROM private_keys", None)
        .await
        .expect("host data survives");
    assert!(path.is_file());
    assert_eq!(std::fs::read_dir(root.path()).expect("one file").count(), 1);
    drop(control);
    drop(resource);
    drop(issued);
    drop(host);
    let host = SharedDatabase::create(&path).expect("restart after retirement");
    assert!(Database::open_shared(&host, "resource").is_err());
    Database::open_shared(&host, "control").expect("control remains usable");
}
