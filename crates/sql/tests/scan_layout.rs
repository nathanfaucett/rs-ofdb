#![cfg(all(feature = "redb", feature = "sql"))]

use std::time::{Duration, Instant};

use ofdb_sql::{Database, Row, Uuid, Value};

#[tokio::test]
async fn native_select_reuses_current_catalog_within_fixed_budget() {
    let directory = tempfile::tempdir().expect("native SELECT catalog store");
    let database = Database::open(directory.path().join("select.redb")).expect("native database");
    let client = database.client();
    client.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value INTEGER); INSERT INTO items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 1)", None).await.expect("SELECT target");
    let fields = (0..200)
        .map(|index| format!("field_{index} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .execute_sql(
            &format!("CREATE TABLE catalog_inventory (id UUID PRIMARY KEY, {fields})"),
            None,
        )
        .await
        .expect("representative current catalog");
    let started = Instant::now();
    for _ in 0..512 {
        let result = client.execute_sql("SELECT value FROM items WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)", None).await.expect("public predicate SELECT");
        assert_eq!(result[0].rows, vec![Row::from([1])]);
    }
    let elapsed = started.elapsed();
    eprintln!("native SELECT catalog: calls=512 elapsed={elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(12),
        "public SELECTs exceeded fixed 12s machine-local catalog budget: {elapsed:?}"
    );
}

#[tokio::test]
async fn native_conflict_checks_reuse_raw_table_catalog_within_budget() {
    let directory = tempfile::tempdir().expect("native raw catalog store");
    let database = Database::open(directory.path().join("raw.redb")).expect("native database");
    let client = database.client();
    client.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY); INSERT INTO items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))", None).await.expect("catalog target row");
    for index in 0..256 {
        client
            .execute_sql(
                &format!("CREATE TABLE inventory_{index} (id UUID PRIMARY KEY)"),
                None,
            )
            .await
            .expect("table catalog entry");
    }
    let key = Row::new(vec![Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").expect("target key"),
    )]);
    let started = Instant::now();
    for _ in 0..512 {
        assert!(
            client
                .row_conflicts("items", &key)
                .await
                .expect("public conflict inspection")
                .is_empty()
        );
    }
    let elapsed = started.elapsed();
    eprintln!("native raw table catalog: calls=512 elapsed={elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(18),
        "public conflict inspections exceeded fixed 18s machine-local raw-catalog budget: {elapsed:?}"
    );
}

#[tokio::test]
async fn native_conflict_checks_with_many_tables_keep_fixed_budget() {
    let directory = tempfile::tempdir().expect("native table catalog store");
    let database = Database::open(directory.path().join("tables.redb")).expect("native database");
    let client = database.client();
    client.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY); INSERT INTO items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))", None).await.expect("catalog target row");
    for index in 0..128 {
        client
            .execute_sql(
                &format!("CREATE TABLE inventory_{index} (id UUID PRIMARY KEY)"),
                None,
            )
            .await
            .expect("independent table catalog entry");
    }
    let key = Row::new(vec![Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").expect("target key"),
    )]);
    let started = Instant::now();
    for _ in 0..512 {
        assert!(
            client
                .row_conflicts("items", &key)
                .await
                .expect("public conflict inspection")
                .is_empty()
        );
    }
    let elapsed = started.elapsed();
    eprintln!("native table catalog: calls=512 elapsed={elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(15),
        "public conflict inspections exceeded fixed 15s machine-local table-catalog budget: {elapsed:?}"
    );
}

#[tokio::test]
async fn native_conflict_checks_with_large_catalog_keep_fixed_budget() {
    let directory = tempfile::tempdir().expect("native conflict store");
    let database = Database::open(directory.path().join("conflict.redb")).expect("native database");
    let client = database.client();
    client.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value INTEGER); INSERT INTO items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 1)", None).await.expect("conflict-free row");
    let fields = (0..200)
        .map(|index| format!("field_{index} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .execute_sql(
            &format!("CREATE TABLE catalog_inventory (id UUID PRIMARY KEY, {fields})"),
            None,
        )
        .await
        .expect("large current catalog");
    let key = Row::new(vec![Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").expect("row key"),
    )]);
    let started = Instant::now();
    for _ in 0..512 {
        assert!(
            client
                .row_conflicts("items", &key)
                .await
                .expect("public conflict inspection")
                .is_empty()
        );
    }
    let elapsed = started.elapsed();
    eprintln!("native conflict catalog: calls=512 elapsed={elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(12),
        "public conflict inspections exceeded fixed 12s machine-local regression budget: {elapsed:?}"
    );
}

#[tokio::test]
async fn native_select_with_large_catalog_keeps_scan_within_budget() {
    let directory = tempfile::tempdir().expect("native scan store");
    let database = Database::open(directory.path().join("scan.redb")).expect("native database");
    let client = database.client();
    client
        .execute_sql(
            "CREATE TABLE items (id UUID PRIMARY KEY, value INTEGER)",
            None,
        )
        .await
        .expect("item catalog");
    let values = (0..512)
        .map(|index| format!("(CAST('018f0f8e-7b6d-7c4a-8f12-{index:012x}' AS UUID), {index})"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .execute_sql(&format!("INSERT INTO items VALUES {values}"), None)
        .await
        .expect("scan inventory");
    let fields = (0..200)
        .map(|index| format!("field_{index} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .execute_sql(
            &format!("CREATE TABLE catalog_inventory (id UUID PRIMARY KEY, {fields})"),
            None,
        )
        .await
        .expect("representative large catalog");

    let started = Instant::now();
    let result = client
        .execute_sql("SELECT value FROM items ORDER BY value", None)
        .await
        .expect("public native SELECT");
    let elapsed = started.elapsed();
    assert_eq!(result[0].rows.len(), 512);
    assert_eq!(
        result[0].rows.first(),
        Some(&Row::new(vec![Value::Integer(0)]))
    );
    assert_eq!(
        result[0].rows.last(),
        Some(&Row::new(vec![Value::Integer(511)]))
    );
    eprintln!("native catalog scan: rows=512 elapsed={elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "native SELECT scan exceeded fixed 2s machine-local regression budget: {elapsed:?}"
    );
}

#[tokio::test]
async fn native_scans_observe_catalog_changes_defaults_and_tombstones_per_operation() {
    let directory = tempfile::tempdir().expect("native evolving catalog store");
    let path = directory.path().join("catalog.redb");
    let database = Database::open(&path).expect("native database");
    let client = database.client();
    client
        .execute_sql(
            "CREATE TABLE items (id UUID PRIMARY KEY, value INTEGER); \
             INSERT INTO items VALUES \
             (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 1), \
             (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 2), \
             (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 3)",
            None,
        )
        .await
        .expect("initial rows");
    let mut transaction = client.transaction().await.expect("catalog snapshot");
    assert_eq!(
        transaction
            .execute_sql("SELECT value FROM items ORDER BY value", None)
            .await
            .expect("first scan")[0]
            .rows,
        vec![Row::from([1]), Row::from([2]), Row::from([3])]
    );
    transaction
        .execute_sql("ALTER TABLE items ADD COLUMN role TEXT DEFAULT 'member'; DELETE FROM items WHERE value = 2", None)
        .await
        .expect("change catalog and tombstone inside same transaction");
    let expected = vec![
        Row::new(vec![Value::Integer(1), Value::Text("member".into())]),
        Row::new(vec![Value::Integer(3), Value::Text("member".into())]),
    ];
    assert_eq!(
        transaction
            .execute_sql("SELECT value, role FROM items ORDER BY value", None)
            .await
            .expect("new scan uses current catalog/defaults")[0]
            .rows,
        expected
    );
    let key = Row::new(vec![Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").expect("retained row"),
    )]);
    assert!(
        transaction
            .row_conflicts("items", &key)
            .await
            .expect("current snapshot conflicts")
            .is_empty()
    );
    transaction.commit().await.expect("persist current catalog");
    drop(client);
    drop(database);

    let database = Database::open(&path).expect("reopen catalog/defaults/tombstone");
    let client = database.client();
    assert_eq!(
        client
            .execute_sql("SELECT value, role FROM items ORDER BY value", None)
            .await
            .expect("reopened scan")[0]
            .rows,
        expected
    );
    client.execute_sql("DROP TABLE items; CREATE TABLE items (id UUID PRIMARY KEY, enabled BOOLEAN DEFAULT TRUE)", None).await.expect("replace catalog");
    assert!(
        client
            .execute_sql("SELECT enabled FROM items", None)
            .await
            .expect("empty recreated table")[0]
            .rows
            .is_empty()
    );
    client
        .execute_sql(
            "INSERT INTO items (id) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abf' AS UUID))",
            None,
        )
        .await
        .expect("new layout row");
    assert_eq!(
        client
            .execute_sql("SELECT enabled FROM items", None)
            .await
            .expect("new scan layout")[0]
            .rows,
        vec![Row::new(vec![Value::Bool(true)])]
    );
}
