use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

#[cfg(not(feature = "worker"))]
wasm_bindgen_test_configure!(run_in_browser);
#[cfg(feature = "worker")]
wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[wasm_bindgen_test]
async fn kv_queries_transactions_expiry_and_ownership() {
    use ofdb_kv::Database;
    use value::Value;
    let database = Database::in_memory();
    let client = database.client();
    let clone = client.clone();
    drop(database);
    let mut transaction = client.transaction().await.expect("begin transaction");
    transaction
        .set("hello", Value::Blob(vec![0, 255]), None)
        .await
        .expect("stage value");
    assert_eq!(
        transaction.get("hello").await.expect("read own write"),
        Some(Value::Blob(vec![0, 255]))
    );
    transaction.rollback().await.expect("rollback");
    assert_eq!(client.get("hello").await.expect("rolled back"), None);
    let mut transaction = client.transaction().await.expect("begin commit");
    transaction
        .set("hello", Value::Blob(vec![0, 255]), None)
        .await
        .expect("stage commit");
    transaction.commit().await.expect("commit");
    assert_eq!(
        clone.scan_all().await.expect("shared scan"),
        vec![("hello".into(), Value::Blob(vec![0, 255]))]
    );
    let now = js_sys::Date::now() as i64;
    client
        .set("expired", Value::Bool(true), Some(now - 1))
        .await
        .expect("expired write");
    client
        .set("future", Value::Bool(true), Some(now + 60_000))
        .await
        .expect("future write");
    assert_eq!(
        client
            .get("expired")
            .await
            .expect("expiry uses browser clock"),
        None
    );
    assert_eq!(
        client.get("future").await.expect("unexpired value"),
        Some(Value::Bool(true))
    );
    client.delete("hello").await.expect("delete");
    assert_eq!(clone.get("hello").await.expect("shared deletion"), None);
}

#[wasm_bindgen_test]
async fn sql_queries_transactions_and_generated_identities() {
    use ofdb_sql::{Database, Row, SqlTranslator};
    let database = Database::in_memory();
    let client = database.client();
    client
        .translate_and_execute(
            "CREATE TABLE people (name TEXT PRIMARY KEY)",
            &SqlTranslator,
        )
        .await
        .expect("create catalog with random actors and UUIDs");
    let mut transaction = client.transaction().await.expect("begin SQL transaction");
    transaction
        .translate_and_execute("INSERT INTO people VALUES ('Ada')", &SqlTranslator)
        .await
        .expect("generate row identity");
    let rows = transaction
        .translate_and_execute("SELECT name FROM people", &SqlTranslator)
        .await
        .expect("read own write");
    assert_eq!(rows[0].rows, vec![Row::from(["Ada"])]);
    transaction.rollback().await.expect("rollback");
    assert!(
        client
            .translate_and_execute("SELECT name FROM people", &SqlTranslator)
            .await
            .expect("rolled back")[0]
            .rows
            .is_empty()
    );
    let mut transaction = client.transaction().await.expect("begin commit");
    transaction
        .translate_and_execute("INSERT INTO people VALUES ('Lin')", &SqlTranslator)
        .await
        .expect("insert");
    transaction.commit().await.expect("commit");
    let clone = client.clone();
    drop(database);
    assert_eq!(
        clone
            .translate_and_execute("SELECT name FROM people", &SqlTranslator)
            .await
            .expect("owned handle")[0]
            .rows,
        vec![Row::from(["Lin"])]
    );
}

#[wasm_bindgen_test]
fn uuid_and_actor_randomness_and_wall_clock() {
    let has_document =
        js_sys::Reflect::has(&js_sys::global(), &"document".into()).expect("global context");
    assert_eq!(has_document, !cfg!(feature = "worker"));
    let first_actor = automerge::AutoCommit::new();
    let second_actor = automerge::AutoCommit::new();
    assert_ne!(first_actor.get_actor(), second_actor.get_actor());
    assert!((platform::unix_millis() as f64 - js_sys::Date::now()).abs() < 1000.0);
    let first = uuid::Uuid::now_v7();
    let second = uuid::Uuid::now_v7();
    assert_ne!(first, second);
    assert_eq!(first.get_version_num(), 7);
    let (seconds, nanos) = first.get_timestamp().expect("v7 timestamp").to_unix();
    let millis = seconds as f64 * 1000.0 + f64::from(nanos) / 1_000_000.0;
    assert!((js_sys::Date::now() - millis).abs() < 10_000.0);
}

#[wasm_bindgen_test]
async fn browser_timers_and_deadlines_without_tokio() {
    use core::time::Duration;
    assert!(
        platform::timeout(Duration::from_millis(5), core::future::pending::<()>())
            .await
            .is_err()
    );
    assert_eq!(
        platform::timeout(Duration::from_secs(1), async { 42 }).await,
        Ok(42)
    );
    let client = ofdb_kv::Database::in_memory()
        .client()
        .with_deadline(Duration::from_secs(5));
    client
        .set("timed", value::Value::Bool(true), None)
        .await
        .expect("browser KV deadline");
    let mut transaction = client.transaction().await.expect("timed KV transaction");
    platform::sleep(Duration::from_millis(1)).await;
    assert_eq!(
        transaction
            .get("timed")
            .await
            .expect("timed transaction read"),
        Some(value::Value::Bool(true))
    );
    transaction.rollback().await.expect("timed rollback");
    let client = ofdb_sql::Database::in_memory()
        .client()
        .with_deadline(Duration::from_secs(5));
    client
        .translate_and_execute(
            "CREATE TABLE timed (id TEXT PRIMARY KEY)",
            &ofdb_sql::SqlTranslator,
        )
        .await
        .expect("browser SQL deadline");
    let mut transaction = client.transaction().await.expect("timed SQL transaction");
    platform::sleep(Duration::from_millis(1)).await;
    transaction
        .translate_and_execute(
            "INSERT INTO timed VALUES ('works')",
            &ofdb_sql::SqlTranslator,
        )
        .await
        .expect("timed SQL write after await");
    transaction.commit().await.expect("timed SQL commit");
}
