use ofdb_sql::{Database, Row};
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn sql_persistent_reopen_and_client_ownership() {
    let name = format!("sql-reopen-{}", uuid::Uuid::now_v7());
    let database = Database::open_indexeddb(&name).await.expect("open SQL");
    let client = database.client();
    drop(database);
    client
        .execute_sql(
            "CREATE TABLE people (name TEXT PRIMARY KEY); INSERT INTO people VALUES ('Ada')",
            None,
        )
        .await
        .expect("persist catalog and row");
    drop(client);
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen SQL")
        .client();
    assert_eq!(
        client
            .execute_sql("SELECT name FROM people", None)
            .await
            .expect("read persisted row")[0]
            .rows,
        vec![Row::from(["Ada"])]
    );
}

#[wasm_bindgen_test]
async fn typed_sql_statements_persist_without_translation() {
    use ofdb_sql::{Query, QueryInsert, Statement, Value, ValueType};
    use query::DataDefinition;
    use schema::{ColumnSchema, TableSchema};
    let name = format!("sql-typed-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client
        .execute(vec![
            Statement::DataDefinition(DataDefinition::CreateTable {
                schema: TableSchema {
                    name: "typed".into(),
                    columns: vec![ColumnSchema {
                        name: "name".into(),
                        r#type: ValueType::Text,
                        default: Value::Null,
                        primary_key: true,
                    }],
                },
                if_not_exists: false,
            }),
            Statement::Query(Query::Insert(QueryInsert {
                table: "typed".into(),
                row: Row::from(["typed Ada"]),
                returning: Some(vec!["name".into()]),
            })),
        ])
        .await
        .expect("typed catalog and insert");
    drop(client);
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    let query = Statement::Query(Query::Select(ofdb_sql::QuerySelect {
        from: ofdb_sql::QueryFrom {
            table: "typed".into(),
            joins: vec![],
        },
        projection: vec![ofdb_sql::QueryColumn::new("typed".into(), "name".into())],
        text_concats: vec![],
        distinct: false,
        predicate: None,
        aggregates: vec![],
        group_by: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
        having: None,
    }));
    assert_eq!(
        client
            .execute(vec![query])
            .await
            .expect("typed persisted select")[0]
            .rows,
        vec![Row::from(["typed Ada"])]
    );
}

#[wasm_bindgen_test]
async fn sql_failed_batches_roll_back_catalog_rows_and_indexes() {
    let name = format!("sql-atomic-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    assert!(client.execute_sql("CREATE TABLE transient (id TEXT PRIMARY KEY, name TEXT UNIQUE); CREATE INDEX transient_name ON transient (name); INSERT INTO transient VALUES ('id1', 'Ada'); INSERT INTO transient VALUES ('id1', 'Ada')", None).await.is_err());
    drop(client);
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    assert!(
        client
            .execute_sql("SELECT name FROM transient", None)
            .await
            .is_err()
    );
    client.execute_sql("CREATE TABLE transient (id TEXT PRIMARY KEY, name TEXT UNIQUE); CREATE INDEX transient_name ON transient (name); INSERT INTO transient VALUES ('id1', 'Ada')", None).await.expect("no partial catalog or index");
    assert!(
        client
            .execute_sql(
                "UPDATE transient SET name = 'Lin'; INSERT INTO transient VALUES ('id2', 'Lin')",
                None
            )
            .await
            .is_err()
    );
    assert_eq!(
        client
            .execute_sql("SELECT name FROM transient WHERE name = 'Ada'", None)
            .await
            .expect("old index and row retained")[0]
            .rows,
        vec![Row::from(["Ada"])]
    );
    assert!(
        client
            .execute_sql("SELECT name FROM transient WHERE name = 'Lin'", None)
            .await
            .expect("no partial updated index")[0]
            .rows
            .is_empty()
    );
}

#[wasm_bindgen_test]
async fn sql_indexed_transactions_survive_await_and_reject_stale_commits() {
    let name = format!("sql-tx-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client.execute_sql("CREATE TABLE people (id TEXT PRIMARY KEY, name TEXT UNIQUE); CREATE INDEX people_name ON people (name); INSERT INTO people VALUES ('id1', 'Ada')", None).await.expect("initialize");
    let other = Database::open_indexeddb(&name)
        .await
        .expect("independent open")
        .client();
    let mut tx = client.transaction().await.expect("snapshot");
    tx.execute_sql("UPDATE people SET name = 'Lin' WHERE name = 'Ada'", None)
        .await
        .expect("staged index change");
    platform::sleep(std::time::Duration::from_millis(5)).await;
    other
        .execute_sql("INSERT INTO people VALUES ('id2', 'Grace')", None)
        .await
        .expect("independent commit");
    assert_eq!(
        tx.execute_sql("SELECT name FROM people", None)
            .await
            .expect("snapshot plus pending writes")[0]
            .rows,
        vec![Row::from(["Lin"])]
    );
    assert!(
        tx.commit().await.is_err(),
        "stale mutating transaction must fail"
    );
    let mut tx = client.transaction().await.expect("rollback");
    tx.execute_sql("DELETE FROM people", None)
        .await
        .expect("stage delete");
    tx.rollback().await.expect("rollback");
    let mut tx = client.transaction().await.expect("aborted transaction");
    tx.execute_sql("INSERT INTO people VALUES ('id3', 'discarded')", None)
        .await
        .expect("stage insert");
    assert!(
        tx.execute_sql("INSERT INTO people VALUES ('id1', 'Ada')", None)
            .await
            .is_err()
    );
    assert!(tx.commit().await.is_err());
    let mut tx = client.transaction().await.expect("dropped transaction");
    tx.execute_sql("DROP TABLE people", None)
        .await
        .expect("staged drop");
    drop(tx);
    drop(other);
    drop(client);
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    assert_eq!(
        client
            .execute_sql("SELECT name FROM people ORDER BY name", None)
            .await
            .expect("no partial mutation")[0]
            .rows,
        vec![Row::from(["Ada"]), Row::from(["Grace"])]
    );
}

#[wasm_bindgen(module = "/src/sql_indexeddb_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = restartSql)]
    fn restart_sql(name: &str, write: bool, page: bool) -> js_sys::Promise;
}

#[wasm_bindgen(module = "/src/indexeddb_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = failNextCommit)]
    fn fail_next_commit(mode: &str);
}

#[wasm_bindgen]
pub async fn sql_restart_probe(name: &str, write: bool) -> Result<String, JsValue> {
    let error = |error: ofdb_sql::Error| JsValue::from_str(&error.to_string());
    let client = Database::open_indexeddb(name)
        .await
        .map_err(error)?
        .client();
    if write {
        client.execute_sql("CREATE TABLE people (id UUID PRIMARY KEY, name TEXT UNIQUE); CREATE INDEX people_name ON people (name); INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada'); INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'deleted'); UPDATE people SET name = 'Lin' WHERE name = 'Ada'; DELETE FROM people WHERE name = 'deleted'", None).await.map_err(error)?;
    }
    let rows = client
        .execute_sql("SELECT name FROM people WHERE name = 'Lin'", None)
        .await
        .map_err(error)?;
    if rows[0].rows != vec![Row::from(["Lin"])] {
        return Err("persisted indexed row differs".into());
    }
    if !client
        .execute_sql("SELECT name FROM people WHERE name = 'deleted'", None)
        .await
        .map_err(error)?[0]
        .rows
        .is_empty()
    {
        return Err("deleted row survived".into());
    }
    if !write {
        if client.execute_sql("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'revived')", None).await.is_ok() { return Err("tombstoned UUID revived".into()); }
        if client.execute_sql("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin')", None).await.is_ok() { return Err("persisted unique constraint lost".into()); }
    }
    Ok("Lin;deleted-tombstone;unique-index".into())
}

#[wasm_bindgen_test]
async fn sql_worker_restart_retains_catalog_indexes_and_tombstones() {
    let name = format!("sql-worker-{}", uuid::Uuid::now_v7());
    let before = JsFuture::from(restart_sql(&name, true, false))
        .await
        .expect("writer worker");
    let after = JsFuture::from(restart_sql(&name, false, false))
        .await
        .expect("fresh reader worker");
    assert_eq!(
        before.as_string(),
        Some("Lin;deleted-tombstone;unique-index".into())
    );
    assert_eq!(before, after);
}

#[cfg(not(feature = "worker"))]
#[wasm_bindgen_test]
async fn sql_page_restart_retains_catalog_indexes_and_tombstones() {
    let name = format!("sql-page-{}", uuid::Uuid::now_v7());
    let before = JsFuture::from(restart_sql(&name, true, true))
        .await
        .expect("writer page");
    let after = JsFuture::from(restart_sql(&name, false, true))
        .await
        .expect("fresh reader page");
    assert_eq!(before, after);
}

#[wasm_bindgen_test]
async fn sql_native_commit_failures_do_not_persist_partial_ddl_or_rows() {
    let name = format!("sql-abort-{}", uuid::Uuid::now_v7());
    for mode in ["abort", "throw"] {
        let client = Database::open_indexeddb(&name)
            .await
            .expect("open")
            .client();
        let mut tx = client.transaction().await.expect("begin");
        tx.execute_sql("CREATE TABLE atomic (name TEXT PRIMARY KEY); CREATE INDEX atomic_name ON atomic (name); INSERT INTO atomic VALUES ('Ada')", None).await.expect("stage catalog and rows");
        fail_next_commit(mode);
        let error = tx.commit().await.expect_err("native commit fails");
        assert_eq!(error.kind, ofdb_sql::ErrorKind::Storage);
        drop(client);
        let client = Database::open_indexeddb(&name)
            .await
            .expect("reopen")
            .client();
        assert!(
            client
                .execute_sql("SELECT name FROM atomic", None)
                .await
                .is_err()
        );
    }
    Database::open_indexeddb(&name).await.expect("reopen").client().execute_sql("CREATE TABLE atomic (name TEXT PRIMARY KEY); CREATE INDEX atomic_name ON atomic (name)", None).await.expect("no partial names left behind");
}

#[wasm_bindgen_test]
async fn sql_namespaces_and_transactional_drop_recreate_are_isolated() {
    let name = format!("sql-kind-{}", uuid::Uuid::now_v7());
    let sql = Database::open_indexeddb(&name)
        .await
        .expect("SQL open")
        .client();
    let kv = ofdb_kv::Database::open_indexeddb(&name)
        .await
        .expect("same name KV open")
        .client();
    kv.set("people", ofdb_kv::Value::Text("KV".into()), None)
        .await
        .expect("KV insert");
    sql.execute_sql("CREATE TABLE people (name TEXT PRIMARY KEY); CREATE INDEX people_name ON people (name); INSERT INTO people VALUES ('Ada'); DROP TABLE people; CREATE TABLE people (name TEXT PRIMARY KEY); CREATE INDEX people_name ON people (name)", None).await.expect("transactional drop and recreate");
    drop(sql);
    let sql = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    assert!(
        sql.execute_sql("SELECT name FROM people", None)
            .await
            .expect("empty recreated table")[0]
            .rows
            .is_empty()
    );
    assert_eq!(
        kv.get("people").await.expect("KV unaffected"),
        Some(ofdb_kv::Value::Text("KV".into()))
    );
    let other = Database::open_indexeddb(&format!("{name}-other"))
        .await
        .expect("other name")
        .client();
    assert!(
        other
            .execute_sql("SELECT name FROM people", None)
            .await
            .is_err()
    );
}

#[wasm_bindgen_test]
async fn sql_kernel_binary_ranges_atomic_tables_and_readonly_commits() {
    use engine::{Kernel, KernelTransaction};
    use futures::TryStreamExt;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let name = format!("sql-kernel-{}", uuid::Uuid::now_v7());
    let kernel = engine_indexeddb::IndexedDbKernel::open(&name)
        .await
        .expect("open kernel");
    let mut tx = kernel.transaction().await.expect("snapshot");
    tx.ensure_table("empty").await.expect("empty table");
    tx.ensure_table("bytes").await.expect("byte table");
    for key in [vec![], vec![0], vec![0, 0], vec![255]] {
        tx.put_bytes("bytes", key.clone(), key)
            .await
            .expect("put binary key");
    }
    assert_eq!(
        tx.get_bytes("absent", b"key").await.expect("missing table"),
        None
    );
    assert_eq!(
        tx.remove_bytes("absent", b"key")
            .await
            .expect("remove missing"),
        None
    );
    assert_eq!(
        tx.scan_bytes_range("bytes", (Excluded(vec![]), Included(vec![0, 0])))
            .try_collect::<Vec<_>>()
            .await
            .expect("bounded scan"),
        vec![(vec![0], vec![0]), (vec![0, 0], vec![0, 0])]
    );
    tx.commit().await.expect("atomic tables");
    let old_read = kernel.transaction().await.expect("read snapshot");
    let mut tx = kernel.transaction().await.expect("drop snapshot");
    assert_eq!(
        tx.remove_bytes("bytes", &[0])
            .await
            .expect("remove existing"),
        Some(vec![0])
    );
    tx.drop_table("bytes").await.expect("drop");
    tx.commit().await.expect("commit drop");
    assert_eq!(
        old_read
            .scan_bytes("bytes")
            .try_collect::<Vec<_>>()
            .await
            .expect("stable read")
            .len(),
        4
    );
    old_read
        .commit()
        .await
        .expect("stale readonly commit does not conflict");
    drop(kernel);
    let kernel = engine_indexeddb::IndexedDbKernel::open(&name)
        .await
        .expect("reopen kernel");
    let mut tx = kernel.transaction().await.expect("new snapshot");
    assert!(
        tx.scan_bytes_range("bytes", (Unbounded, Unbounded))
            .try_collect::<Vec<_>>()
            .await
            .expect("dropped records")
            .is_empty()
    );
    tx.put_bytes("bytes", vec![1], vec![2])
        .await
        .expect("staged put");
    tx.rollback().await.expect("discard");
    assert_eq!(
        kernel
            .transaction()
            .await
            .expect("snapshot")
            .get_bytes("bytes", &[1])
            .await
            .expect("rolled back"),
        None
    );
}
