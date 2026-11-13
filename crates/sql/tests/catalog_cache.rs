#![cfg(all(feature = "redb", feature = "sql"))]

use std::time::{Duration, Instant};

use ofdb_sql::{Database, Row, Value};

#[tokio::test]
async fn repeated_native_catalog_reads_fit_startup_budget() {
    let directory = tempfile::tempdir().expect("catalog database directory");
    let database = Database::open(directory.path().join("catalog.redb")).expect("catalog database");
    let client = database.client();
    let fields = (0..200)
        .map(|index| format!("field_{index} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .execute_sql(
            &format!("CREATE TABLE inventory (id UUID PRIMARY KEY, {fields}); CREATE TABLE items (id UUID PRIMARY KEY, value INTEGER); INSERT INTO items (value) VALUES (7)"),
            None,
        )
        .await
        .expect("representative startup catalog");
    let started = Instant::now();
    for _ in 0..128 {
        assert_eq!(
            client
                .execute_sql("SELECT value FROM items", None)
                .await
                .expect("read current catalog")[0]
                .rows,
            vec![Row::from([7])]
        );
    }
    let elapsed = started.elapsed();
    eprintln!("128 native catalog reads: {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(1),
        "catalog reads exceed startup budget: {elapsed:?}"
    );
}

#[tokio::test]
async fn cached_catalog_changes_rollback_without_leaking_to_later_transactions() {
    let directory = tempfile::tempdir().expect("rollback database directory");
    let database =
        Database::open(directory.path().join("rollback.redb")).expect("rollback database");
    let client = database.client();
    client.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value INTEGER); INSERT INTO items (value) VALUES (7)", None).await.expect("initial layout");
    let mut transaction = client.transaction().await.expect("uncommitted layout");
    transaction
        .execute_sql(
            "SELECT * FROM items; ALTER TABLE items ADD COLUMN role TEXT DEFAULT 'member'",
            None,
        )
        .await
        .expect("read then change catalog");
    assert_eq!(
        transaction
            .execute_sql("SELECT value, role FROM items", None)
            .await
            .expect("read own schema change")[0]
            .rows,
        vec![Row::new(vec![Value::Integer(7), Value::from("member")])]
    );
    transaction
        .rollback()
        .await
        .expect("discard catalog change");
    assert_eq!(
        client
            .execute_sql("SELECT * FROM items", None)
            .await
            .expect("read original layout")[0]
            .columns
            .len(),
        2
    );
    client
        .execute_sql(
            "ALTER TABLE items ADD COLUMN enabled BOOLEAN DEFAULT TRUE",
            None,
        )
        .await
        .expect("different committed layout");
    assert_eq!(
        client
            .execute_sql("SELECT value, enabled FROM items", None)
            .await
            .expect("no uncommitted layout leak")[0]
            .rows,
        vec![Row::new(vec![Value::Integer(7), Value::Bool(true)])]
    );
}

#[cfg(feature = "in-memory")]
#[tokio::test]
async fn catalog_cache_preserves_concurrent_snapshots_and_read_your_writes() {
    let database = Database::in_memory();
    let client = database.client();
    client.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value INTEGER); INSERT INTO items (value) VALUES (7)", None).await.expect("initial layout");
    let mut old = client
        .transaction()
        .await
        .expect("old snapshot without a schema read");
    let concurrent = client.clone();
    concurrent
        .execute_sql(
            "ALTER TABLE items ADD COLUMN role TEXT DEFAULT 'member'",
            None,
        )
        .await
        .expect("concurrent catalog commit");
    assert_eq!(
        concurrent
            .execute_sql("SELECT role FROM items", None)
            .await
            .expect("warm latest schema")[0]
            .rows,
        vec![Row::from(["member"])]
    );
    assert_eq!(
        old.execute_sql("SELECT * FROM items", None)
            .await
            .expect("old transaction reconstructs its own catalog")[0]
            .columns
            .len(),
        2
    );
    old.execute_sql(
        "ALTER TABLE items ADD COLUMN enabled BOOLEAN DEFAULT TRUE",
        None,
    )
    .await
    .expect("independent uncommitted schema");
    assert_eq!(
        old.execute_sql("SELECT enabled FROM items", None)
            .await
            .expect("own writes in old snapshot")[0]
            .rows,
        vec![Row::new(vec![Value::Bool(true)])]
    );
    assert_eq!(
        concurrent
            .execute_sql("SELECT role FROM items", None)
            .await
            .expect("uncommitted changes stay private")[0]
            .rows,
        vec![Row::from(["member"])]
    );
    old.rollback().await.expect("discard independent catalog");
    assert_eq!(
        client
            .execute_sql("SELECT role FROM items", None)
            .await
            .expect("latest committed catalog remains visible")[0]
            .rows,
        vec![Row::from(["member"])]
    );
}
