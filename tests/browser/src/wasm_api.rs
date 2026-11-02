use ofdb_wasm::{KvDatabase, KvTransaction, SqlDatabase};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn kv_wasm_database_client_transaction_commit() {
    let name = format!("wasm-kv-commit-{}", uuid::Uuid::now_v7());
    let db = KvDatabase::open(name.clone())
        .await
        .expect("open KV database");
    let client = db.client();

    client
        .set("a".into(), "1".into(), None)
        .await
        .expect("set a");
    client
        .set("b".into(), "2".into(), None)
        .await
        .expect("set b");

    let mut tx = client.transaction().await.expect("begin tx");
    tx.set("c".into(), "3".into(), None).await.expect("tx set");
    assert_eq!(tx.get("c".into()).await.expect("tx get"), Some("3".into()));
    tx.commit().await.expect("commit");

    let all = client.scan_all().await.expect("scan all");
    assert!(all.contains("\"key\":\"a\""));
    assert!(all.contains("\"key\":\"c\""));

    let reopened = KvDatabase::open(name).await.expect("reopen").client();
    assert_eq!(
        reopened.get("c".into()).await.expect("get persisted"),
        Some("3".into())
    );
}

#[wasm_bindgen_test]
async fn kv_wasm_transaction_rollback() {
    let name = format!("wasm-kv-rollback-{}", uuid::Uuid::now_v7());
    let client = KvDatabase::open(name.clone()).await.expect("open").client();
    client
        .set("x".into(), "original".into(), None)
        .await
        .expect("set");

    let mut tx: KvTransaction = client.transaction().await.expect("begin tx");
    tx.set("x".into(), "changed".into(), None)
        .await
        .expect("tx set");
    tx.rollback().await.expect("rollback");

    assert_eq!(
        client.get("x".into()).await.expect("get"),
        Some("original".into())
    );
}

#[wasm_bindgen_test]
async fn kv_wasm_scan_prefix_and_deadline_clone() {
    let name = format!("wasm-kv-scan-{}", uuid::Uuid::now_v7());
    let client = KvDatabase::open(name).await.expect("open").client();
    client
        .set("user:1".into(), "alice".into(), None)
        .await
        .expect("set");
    client
        .set("user:2".into(), "bob".into(), None)
        .await
        .expect("set");
    client
        .set("other".into(), "x".into(), None)
        .await
        .expect("set");

    let prefix_scan = client
        .scan_prefix("user:".into())
        .await
        .expect("scan prefix");
    assert!(prefix_scan.contains("user:1"));
    assert!(prefix_scan.contains("user:2"));
    assert!(!prefix_scan.contains("\"key\":\"other\""));

    let range = client
        .scan("user:".into(), "user;".into())
        .await
        .expect("scan range");
    assert!(range.contains("user:1"));

    let deadline_client = client.with_deadline(5000);
    let cloned = deadline_client.clone_client();
    assert_eq!(
        cloned.get("user:1".into()).await.expect("cloned get"),
        Some("alice".into())
    );
}

#[wasm_bindgen_test]
async fn sql_wasm_database_client_transaction_commit() {
    let name = format!("wasm-sql-commit-{}", uuid::Uuid::now_v7());
    let db = SqlDatabase::open(name.clone())
        .await
        .expect("open SQL database");
    let client = db.client();

    client
        .execute("CREATE TABLE items (name TEXT PRIMARY KEY)".into(), None)
        .await
        .expect("create table");

    let mut tx = client.transaction().await.expect("begin tx");
    tx.execute("INSERT INTO items VALUES ('one')".into(), None)
        .await
        .expect("tx insert");
    tx.execute("INSERT INTO items VALUES ('two')".into(), None)
        .await
        .expect("tx insert");
    tx.commit().await.expect("commit");

    let result = client
        .execute("SELECT name FROM items ORDER BY name".into(), None)
        .await
        .expect("select");
    assert!(result.contains("\"value\":\"one\""));
    assert!(result.contains("\"value\":\"two\""));

    let reopened = SqlDatabase::open(name).await.expect("reopen").client();
    let persisted = reopened
        .execute("SELECT COUNT(*) FROM items".into(), None)
        .await
        .expect("count");
    assert!(persisted.contains('2'));
}

#[wasm_bindgen_test]
async fn sql_wasm_transaction_rollback() {
    let name = format!("wasm-sql-rollback-{}", uuid::Uuid::now_v7());
    let client = SqlDatabase::open(name.clone())
        .await
        .expect("open")
        .client();
    client
        .execute("CREATE TABLE t (v TEXT PRIMARY KEY)".into(), None)
        .await
        .expect("create");
    client
        .execute("INSERT INTO t VALUES ('kept')".into(), None)
        .await
        .expect("insert kept");

    let mut tx = client.transaction().await.expect("begin tx");
    tx.execute("INSERT INTO t VALUES ('discarded')".into(), None)
        .await
        .expect("tx insert");
    tx.rollback().await.expect("rollback");

    let result = client
        .execute("SELECT v FROM t".into(), None)
        .await
        .expect("select");
    assert!(result.contains("kept"));
    assert!(!result.contains("discarded"));
}

#[wasm_bindgen_test]
async fn sql_wasm_execute_with_params_and_deadline() {
    let name = format!("wasm-sql-params-{}", uuid::Uuid::now_v7());
    let client = SqlDatabase::open(name).await.expect("open").client();
    client
        .execute(
            "CREATE TABLE p (id INTEGER PRIMARY KEY, label TEXT)".into(),
            None,
        )
        .await
        .expect("create");

    client
        .execute(
            "INSERT INTO p (id, label) VALUES ($1, $2)".into(),
            Some(r#"[{"type":"integer","value":"42"},{"type":"text","value":"hello"}]"#.into()),
        )
        .await
        .expect("insert with positional params");

    let deadline_client = client.with_deadline(5000);
    let result = deadline_client
        .execute(
            "SELECT label FROM p WHERE id = $1".into(),
            Some(r#"[{"type":"integer","value":"42"}]"#.into()),
        )
        .await
        .expect("select with params");
    assert!(result.contains("hello"));
}
