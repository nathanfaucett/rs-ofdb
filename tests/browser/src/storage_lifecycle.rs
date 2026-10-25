use wasm_bindgen::{JsValue, prelude::wasm_bindgen};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(module = "/src/storage_lifecycle_helpers.js")]
extern "C" {
    fn malformed(name: &str, kind: &str, mode: &str) -> js_sys::Promise;
}

#[wasm_bindgen_test]
async fn lifecycle_rejects_wrong_store_kind_without_reset_or_fallback() {
    for kind in ["sql", "kv"] {
        let name = format!("lifecycle-kind-{}", uuid::Uuid::now_v7());
        JsFuture::from(malformed(&name, kind, "kind"))
            .await
            .expect("prepare wrong store kind");
        let error = if kind == "sql" {
            ofdb_sql::Database::open_indexeddb(&name)
                .await
                .err()
                .expect("reject SQL kind")
                .to_string()
        } else {
            ofdb_kv::Database::open_indexeddb(&name)
                .await
                .err()
                .expect("reject KV kind")
                .to_string()
        };
        assert!(error.contains("store kind"), "{error}");
    }
}

#[wasm_bindgen(module = "/src/storage_lifecycle_helpers.js")]
extern "C" {
    fn sentinel(name: &str, kind: &str) -> js_sys::Promise;
    fn context(name: &str, kind: &str, action: &str) -> js_sys::Promise;
    fn release(worker: &JsValue) -> js_sys::Promise;
    fn absent(name: &str, kind: &str) -> js_sys::Promise;
}

#[wasm_bindgen_test]
async fn lifecycle_rejects_incompatible_schema_without_modifying_metadata() {
    for kind in ["sql", "kv"] {
        for mode in [
            "missing",
            "keypath",
            "autoincrement",
            "index",
            "extra",
            "missing-kind",
            "format",
            "revision",
            "version",
        ] {
            let name = format!("lifecycle-{mode}-{}", uuid::Uuid::now_v7());
            JsFuture::from(malformed(&name, kind, mode))
                .await
                .expect("prepare incompatible schema");
            let error = if kind == "sql" {
                ofdb_sql::Database::open_indexeddb(&name)
                    .await
                    .err()
                    .expect("reject SQL schema")
                    .to_string()
            } else {
                ofdb_kv::Database::open_indexeddb(&name)
                    .await
                    .err()
                    .expect("reject KV schema")
                    .to_string()
            };
            assert!(error.contains("IndexedDB"), "{error}");
            if mode == "keypath" || mode == "missing" {
                assert!(error.contains("schema"), "actionable schema error: {error}");
            }
            assert_eq!(
                JsFuture::from(sentinel(&name, kind))
                    .await
                    .expect("read sentinel")
                    .as_string()
                    .as_deref(),
                Some("preserve-me")
            );
        }
    }
}

#[wasm_bindgen_test]
async fn lifecycle_queued_open_is_bounded_and_late_upgrade_does_not_create_storage() {
    for kind in ["sql", "kv"] {
        let name = format!("lifecycle-blocked-{}", uuid::Uuid::now_v7());
        let worker = JsFuture::from(context(&name, kind, "block"))
            .await
            .expect("other context blocks deletion");
        let open = async {
            if kind == "sql" {
                ofdb_sql::Database::open_indexeddb(&name)
                    .await
                    .err()
                    .map(|e| e.to_string())
            } else {
                ofdb_kv::Database::open_indexeddb(&name)
                    .await
                    .err()
                    .map(|e| e.to_string())
            }
        };
        let outcome = platform::timeout(std::time::Duration::from_millis(6500), open).await;
        JsFuture::from(release(&worker))
            .await
            .expect("release uncooperative peer");
        let error = outcome
            .expect("open must settle within its five-second policy")
            .expect("blocked open must fail");
        assert!(error.contains("close other"), "{error}");
        assert_eq!(
            JsFuture::from(absent(&name, kind))
                .await
                .expect("check late open cleanup")
                .as_bool(),
            Some(true)
        );
    }
}

#[wasm_bindgen_test]
async fn lifecycle_versionchange_closes_clones_and_rejects_staged_commits() {
    let name = format!("lifecycle-ownership-{}", uuid::Uuid::now_v7());
    let sql_db = ofdb_sql::Database::open_indexeddb(&name)
        .await
        .expect("SQL open");
    let kv_db = ofdb_kv::Database::open_indexeddb(&name)
        .await
        .expect("KV same name, isolated kind");
    let sql = sql_db.client().clone();
    let kv = kv_db.client().clone();
    drop(sql_db);
    drop(kv_db);
    sql.execute_sql(
        "CREATE TABLE entries (id TEXT PRIMARY KEY); INSERT INTO entries VALUES ('retained')",
        None,
    )
    .await
    .expect("SQL client outlives Database");
    kv.set("retained", true, None)
        .await
        .expect("KV client outlives Database");
    let mut sql_pending = sql.transaction().await.expect("SQL staging");
    sql_pending
        .execute_sql("INSERT INTO entries VALUES ('pending')", None)
        .await
        .expect("stage SQL");
    let sql_readonly = sql.transaction().await.expect("SQL readonly staging");
    let mut kv_pending = kv.transaction().await.expect("KV staging");
    kv_pending
        .set("pending", true, None)
        .await
        .expect("stage KV");
    let kv_readonly = kv.transaction().await.expect("KV readonly staging");
    let independent = ofdb_kv::Database::open_indexeddb(&format!("{name}-other"))
        .await
        .expect("isolated database")
        .client();
    JsFuture::from(context(&name, "sql", "upgrade"))
        .await
        .expect("SQL versionchange closes owners");
    kv.get("retained")
        .await
        .expect("SQL upgrade does not close KV");
    let error = sql_pending
        .commit()
        .await
        .expect_err("closed SQL cannot commit staged writes")
        .to_string();
    assert!(error.contains("version change"), "{error}");
    assert!(
        sql_readonly.commit().await.is_err(),
        "closed SQL readonly commit must not report success"
    );
    assert!(
        sql.clone()
            .execute_sql("SELECT id FROM entries", None)
            .await
            .is_err()
    );
    JsFuture::from(context(&name, "kv", "upgrade"))
        .await
        .expect("KV versionchange closes owners");
    let error = kv_pending
        .commit()
        .await
        .expect_err("closed KV cannot commit staged writes")
        .to_string();
    assert!(error.contains("version change"), "{error}");
    assert!(
        kv_readonly.commit().await.is_err(),
        "closed KV readonly commit must not report success"
    );
    assert!(kv.clone().get("retained").await.is_err());
    independent
        .set("unaffected", true, None)
        .await
        .expect("independent name stays usable");
    assert!(ofdb_sql::Database::open_indexeddb(&name).await.is_err());
    assert!(ofdb_kv::Database::open_indexeddb(&name).await.is_err());
}

#[wasm_bindgen(module = "/src/storage_lifecycle_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = denyOpen)]
    fn deny_open();
    fn exhaust(name: &str) -> js_sys::Promise;
}

#[wasm_bindgen(module = "/src/indexeddb_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = failNextCommit)]
    fn fail_next_commit(mode: &str);
}

#[wasm_bindgen_test]
async fn lifecycle_revision_exhaustion_is_actionable_and_atomic() {
    let name = format!("lifecycle-exhausted-{}", uuid::Uuid::now_v7());
    let db = btree_indexeddb::Database::open(&name, "test")
        .await
        .expect("byte storage");
    JsFuture::from(exhaust(&name))
        .await
        .expect("set final revision");
    let mut tx = db.snapshot().await.expect("last revision snapshot");
    tx.create_table("must-not-persist");
    let error = tx
        .commit()
        .await
        .expect_err("cannot advance revision")
        .to_string();
    assert!(error.contains("revision exhausted"), "{error}");
    let snapshot = db.snapshot().await.expect("read after failed commit");
    assert_eq!(snapshot.revision(), u64::MAX);
    assert!(snapshot.table("must-not-persist").is_none());
}

#[wasm_bindgen_test]
async fn lifecycle_access_errors_are_structured_actionable_and_value_free() {
    let name = format!("lifecycle-denied-{}", uuid::Uuid::now_v7());
    deny_open();
    let error = ofdb_sql::Database::open_indexeddb(&name)
        .await
        .err()
        .expect("SQL must not fall back");
    assert_eq!(error.kind, ofdb_sql::ErrorKind::Storage);
    assert!(error.to_string().contains("SecurityError"));
    assert!(error.to_string().contains("allow storage access"));
    assert!(!error.to_string().contains("private-key-and-value"));
    deny_open();
    let error = ofdb_kv::Database::open_indexeddb(&name)
        .await
        .err()
        .expect("KV must not fall back");
    assert!(matches!(
        &error,
        ofdb_kv::Error::Storage {
            kind: ofdb_kv::ErrorKind::Storage,
            ..
        }
    ));
    assert!(error.to_string().contains("SecurityError"));
    assert!(!error.to_string().contains("private-key-and-value"));
}

#[wasm_bindgen_test]
async fn lifecycle_failed_facade_commits_leave_no_partial_writes() {
    for mode in ["abort", "throw"] {
        let name = format!("lifecycle-failure-{mode}-{}", uuid::Uuid::now_v7());
        let sql = ofdb_sql::Database::open_indexeddb(&name)
            .await
            .expect("SQL open")
            .client();
        sql.execute_sql("CREATE TABLE entries (id TEXT PRIMARY KEY, value TEXT UNIQUE); CREATE INDEX entry_value ON entries (value); INSERT INTO entries VALUES ('old', 'retained')", None).await.expect("SQL baseline");
        let mut tx = sql.transaction().await.expect("SQL transaction");
        tx.execute_sql(
            "UPDATE entries SET value = 'changed'; INSERT INTO entries VALUES ('new', 'pending')",
            None,
        )
        .await
        .expect("stage SQL writes");
        fail_next_commit(mode);
        let error = tx.commit().await.expect_err("injected SQL commit fails");
        assert_eq!(error.kind, ofdb_sql::ErrorKind::Storage);
        assert!(error.to_string().contains(if mode == "throw" {
            "QuotaExceededError"
        } else {
            "aborted"
        }));
        drop(sql);
        let sql = ofdb_sql::Database::open_indexeddb(&name)
            .await
            .expect("SQL reopen")
            .client();
        assert_eq!(
            sql.execute_sql("SELECT value FROM entries WHERE value = 'retained'", None)
                .await
                .expect("index and row retained")[0]
                .rows,
            vec![ofdb_sql::Row::from(["retained"])]
        );
        assert!(
            sql.execute_sql(
                "SELECT value FROM entries WHERE value = 'pending' OR value = 'changed'",
                None
            )
            .await
            .expect("no new index entries")[0]
                .rows
                .is_empty()
        );
        let kv = ofdb_kv::Database::open_indexeddb(&name)
            .await
            .expect("KV open")
            .client();
        kv.set("old", "retained", None).await.expect("KV baseline");
        let mut tx = kv.transaction().await.expect("KV transaction");
        tx.set("old", "changed", None).await.expect("stage update");
        tx.set("new", "pending", None).await.expect("stage insert");
        fail_next_commit(mode);
        let error = tx.commit().await.expect_err("injected KV commit fails");
        assert!(matches!(
            &error,
            ofdb_kv::Error::Storage {
                kind: ofdb_kv::ErrorKind::Storage,
                ..
            }
        ));
        assert!(error.to_string().contains(if mode == "throw" {
            "QuotaExceededError"
        } else {
            "aborted"
        }));
        drop(kv);
        let kv = ofdb_kv::Database::open_indexeddb(&name)
            .await
            .expect("KV reopen")
            .client();
        assert_eq!(
            kv.get("old").await.expect("old value"),
            Some(ofdb_kv::Value::from("retained"))
        );
        assert_eq!(kv.get("new").await.expect("no partial insert"), None);
    }
}
