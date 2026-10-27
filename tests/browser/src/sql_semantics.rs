use ofdb_sql::{Database, ErrorKind, Row, Value};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn sql_semantics_defaults_natural_composite_keys_and_indexes() {
    let name = format!("sql-semantics-keys-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client.execute_sql("CREATE TABLE accounts (name TEXT PRIMARY KEY, quota INTEGER DEFAULT 7, label TEXT DEFAULT 'hello'); CREATE TABLE memberships (tenant TEXT, code INTEGER, email TEXT UNIQUE, PRIMARY KEY (tenant, code)); CREATE INDEX memberships_email ON memberships (email); INSERT INTO accounts (name) VALUES ('Ada'); INSERT INTO memberships VALUES ('a', 1, 'old'); INSERT INTO memberships VALUES ('b', 1, 'other')", None).await.expect("defaults and composite keys");
    assert_eq!(
        client
            .execute_sql("SELECT name, quota, label FROM accounts", None)
            .await
            .expect("default values")[0]
            .rows,
        vec![Row::new(vec![
            Value::from("Ada"),
            Value::Integer(7),
            Value::from("hello")
        ])]
    );
    for sql in [
        "INSERT INTO accounts VALUES ('Ada', 9, 'duplicate')",
        "INSERT INTO memberships VALUES ('a', 1, 'new')",
        "INSERT INTO memberships VALUES ('c', 2, 'old')",
        "INSERT INTO memberships VALUES (NULL, 2, 'null')",
    ] {
        assert_eq!(
            client
                .execute_sql(sql, None)
                .await
                .expect_err("constraint violation")
                .kind,
            ErrorKind::Rejected
        );
    }
    client.execute_sql("UPDATE memberships SET email = 'updated' WHERE tenant = 'a'; DELETE FROM memberships WHERE tenant = 'b'", None).await.expect("maintain derived indexes");
    drop(client);
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    assert_eq!(
        client
            .execute_sql(
                "SELECT tenant, code FROM memberships WHERE email = 'updated'",
                None
            )
            .await
            .expect("persisted index")[0]
            .rows,
        vec![Row::new(vec![Value::from("a"), Value::Integer(1)])]
    );
    assert!(
        client
            .execute_sql(
                "SELECT tenant FROM memberships WHERE email = 'old' OR email = 'other'",
                None
            )
            .await
            .expect("obsolete index entries removed")[0]
            .rows
            .is_empty()
    );
    client
        .execute_sql("INSERT INTO memberships VALUES ('c', 2, 'old')", None)
        .await
        .expect("updated unique value reusable");
}

#[wasm_bindgen_test]
async fn sql_semantics_multicall_snapshots_rollback_drop_and_aborted_state() {
    let name = format!("sql-semantics-tx-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client.execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT UNIQUE); CREATE INDEX items_label ON items (label); INSERT INTO items VALUES ('one', 'original')", None).await.expect("seed");
    let other = Database::open_indexeddb(&name)
        .await
        .expect("independent open")
        .client();
    let mut tx = client.transaction().await.expect("snapshot");
    tx.execute_sql("UPDATE items SET label = 'pending' WHERE id = 'one'; INSERT INTO items VALUES ('two', 'local')", None).await.expect("pending writes");
    platform::sleep(std::time::Duration::from_millis(5)).await;
    other
        .execute_sql("INSERT INTO items VALUES ('three', 'external')", None)
        .await
        .expect("outside commit");
    assert_eq!(
        tx.execute_sql("SELECT label FROM items ORDER BY id", None)
            .await
            .expect("snapshot and read your writes")[0]
            .rows,
        vec![Row::from(["pending"]), Row::from(["local"])]
    );
    assert_eq!(
        tx.execute_sql("SELECT id FROM items WHERE label = 'pending'", None)
            .await
            .expect("pending derived index")[0]
            .rows,
        vec![Row::from(["one"])]
    );
    assert_eq!(
        tx.commit().await.expect_err("stale writer").kind,
        ErrorKind::Storage
    );
    let mut snapshot = client.transaction().await.expect("read only snapshot");
    other
        .execute_sql(
            "UPDATE items SET label = 'committed' WHERE id = 'one'",
            None,
        )
        .await
        .expect("later commit");
    assert!(
        snapshot
            .row_conflicts("items", &Row::from(["one"]))
            .await
            .expect("snapshot conflict inspection")
            .is_empty()
    );
    snapshot.commit().await.expect("stale read only commit");
    for mode in ["rollback", "drop", "abort"] {
        let mut tx = client.transaction().await.expect("begin discard case");
        tx.execute_sql("CREATE TABLE pending (id TEXT PRIMARY KEY); CREATE INDEX pending_id ON pending (id); INSERT INTO pending VALUES ('discard'); UPDATE items SET label = 'discarded' WHERE id = 'one'; DELETE FROM items WHERE id = 'three'", None).await.expect("stage catalog rows indexes and tombstone");
        match mode {
            "rollback" => tx.rollback().await.expect("rollback"),
            "drop" => drop(tx),
            _ => {
                let sql = "INSERT INTO items VALUES ('one', 'duplicate')";
                let error = tx
                    .execute_sql(sql, None)
                    .await
                    .expect_err("abort execution");
                assert_eq!(error.kind, ErrorKind::Rejected);
                assert!(tx.execute_sql("SELECT * FROM items", None).await.is_err());
                assert!(
                    tx.row_conflicts("items", &Row::from(["one"]))
                        .await
                        .is_err()
                );
                assert!(
                    tx.row_conflict_values("items", &Row::from(["one"]))
                        .await
                        .is_err()
                );
                tx.commit()
                    .await
                    .expect_err("aborted commit cannot persist earlier calls");
            }
        }
        let reopened = Database::open_indexeddb(&name)
            .await
            .expect("reopen")
            .client();
        assert_eq!(
            reopened
                .execute_sql("SELECT label FROM items ORDER BY id", None)
                .await
                .expect("all staged effects discarded")[0]
                .rows,
            vec![Row::from(["committed"]), Row::from(["external"])]
        );
        assert!(
            reopened
                .execute_sql("SELECT * FROM pending", None)
                .await
                .is_err()
        );
        assert!(
            reopened
                .execute_sql("SELECT name FROM indices WHERE name = 'pending_id'", None)
                .await
                .expect("no staged index")[0]
                .rows
                .is_empty()
        );
    }
    let mut tx = client.transaction().await.expect("successful multi call");
    tx.execute_sql("UPDATE items SET label = 'final' WHERE id = 'one'", None)
        .await
        .expect("update");
    platform::sleep(std::time::Duration::from_millis(5)).await;
    tx.execute_sql("INSERT INTO items VALUES ('two', 'local')", None)
        .await
        .expect("second call");
    tx.commit().await.expect("commit multi call");
    drop(other);
    drop(client);
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen committed")
        .client();
    assert_eq!(
        client
            .execute_sql("SELECT label FROM items ORDER BY id", None)
            .await
            .expect("committed values")[0]
            .rows,
        vec![
            Row::from(["final"]),
            Row::from(["external"]),
            Row::from(["local"])
        ]
    );
}

#[wasm_bindgen::prelude::wasm_bindgen(module = "/src/indexeddb_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = failNextCommit)]
    fn fail_semantics_commit(mode: &str);
}

#[wasm_bindgen_test]
async fn sql_semantics_multitable_failures_preserve_catalog_rows_and_indexes() {
    let name = format!("sql-semantics-atomic-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client.execute_sql("CREATE TABLE left_items (id TEXT PRIMARY KEY, label TEXT UNIQUE); CREATE TABLE right_items (id TEXT PRIMARY KEY, label TEXT UNIQUE); CREATE INDEX left_label ON left_items (label); CREATE INDEX right_label ON right_items (label); INSERT INTO left_items VALUES ('one', 'left'); INSERT INTO right_items VALUES ('one', 'right')", None).await.expect("seed two tables");
    let catalog = client
        .execute_sql("SELECT name FROM indices ORDER BY name", None)
        .await
        .expect("initial catalog")[0]
        .rows
        .clone();
    for mode in ["constraint", "abort", "throw"] {
        let mut tx = client.transaction().await.expect("begin atomic mutation");
        tx.execute_sql("UPDATE left_items SET label = 'changed'; DELETE FROM right_items; CREATE TABLE pending (id TEXT PRIMARY KEY); CREATE INDEX pending_id ON pending (id); INSERT INTO pending VALUES ('pending'); DROP INDEX right_label; CREATE INDEX replacement ON right_items (label)", None).await.expect("stage multiple logical tables");
        if mode == "constraint" {
            assert_eq!(
                tx.execute_sql("INSERT INTO left_items VALUES ('two', 'changed')", None)
                    .await
                    .expect_err("late constraint failure")
                    .kind,
                ErrorKind::Rejected
            );
            tx.commit().await.expect_err("aborted commit");
        } else {
            fail_semantics_commit(mode);
            assert_eq!(
                tx.commit()
                    .await
                    .expect_err("native atomic commit failure")
                    .kind,
                ErrorKind::Storage
            );
        }
        let reopened = Database::open_indexeddb(&name)
            .await
            .expect("reopen")
            .client();
        for (table, label) in [("left_items", "left"), ("right_items", "right")] {
            assert_eq!(
                reopened
                    .execute_sql(
                        &format!("SELECT id FROM {table} WHERE label = '{label}'"),
                        None
                    )
                    .await
                    .expect("old row and derived index intact")[0]
                    .rows,
                vec![Row::from(["one"])]
            );
            assert!(
                reopened
                    .execute_sql(
                        &format!("SELECT id FROM {table} WHERE label = 'changed'"),
                        None
                    )
                    .await
                    .expect("no partial index update")[0]
                    .rows
                    .is_empty()
            );
        }
        assert_eq!(
            reopened
                .execute_sql("SELECT name FROM indices ORDER BY name", None)
                .await
                .expect("catalog unchanged")[0]
                .rows,
            catalog
        );
        assert!(
            reopened
                .execute_sql("SELECT * FROM pending", None)
                .await
                .is_err()
        );
    }
    client.execute_sql("CREATE TABLE pending (id TEXT PRIMARY KEY); CREATE INDEX pending_id ON pending (id); INSERT INTO right_items VALUES ('two', 'new')", None).await.expect("failed names and tombstones did not leak");
}

#[wasm_bindgen_test]
async fn sql_semantics_drop_recreate_namespace_and_stale_transaction() {
    let name = format!("sql-semantics-ddl-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client.execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT UNIQUE); CREATE INDEX shared_name ON items (label); INSERT INTO items VALUES ('old', 'old')", None).await.expect("old schema");
    let other = Database::open_indexeddb(&name)
        .await
        .expect("other open")
        .client();
    let mut stale = other.transaction().await.expect("pre drop snapshot");
    stale
        .execute_sql("UPDATE items SET label = 'stale'", None)
        .await
        .expect("unobserved update");
    for sql in [
        "CREATE TABLE shared_name (id TEXT PRIMARY KEY)",
        "CREATE INDEX items ON items (label)",
    ] {
        assert_eq!(
            client
                .execute_sql(sql, None)
                .await
                .expect_err("shared namespace")
                .kind,
            ErrorKind::Rejected
        );
    }
    client.execute_sql("DROP TABLE items; CREATE TABLE items (id TEXT PRIMARY KEY, count INTEGER DEFAULT 9); CREATE INDEX shared_name ON items (count); INSERT INTO items (id) VALUES ('new')", None).await.expect("drop dependent index and recreate with different schema");
    assert_eq!(
        stale
            .commit()
            .await
            .expect_err("stale writer cannot restore old schema or row")
            .kind,
        ErrorKind::Storage
    );
    drop(other);
    drop(client);
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    assert_eq!(
        client
            .execute_sql("SELECT id, count FROM items WHERE count = 9", None)
            .await
            .expect("new schema and index")[0]
            .rows,
        vec![Row::new(vec![Value::from("new"), Value::Integer(9)])]
    );
    assert_eq!(
        client
            .execute_sql(
                "SELECT name FROM table_fields WHERE table = 'items' ORDER BY position",
                None
            )
            .await
            .expect("no old columns")[0]
            .rows,
        vec![Row::from(["id"]), Row::from(["count"])]
    );
    client
        .execute_sql("DROP INDEX shared_name", None)
        .await
        .expect("drop index");
    assert_eq!(
        client
            .execute_sql("CREATE TABLE shared_name (id TEXT PRIMARY KEY)", None)
            .await
            .expect_err("historical index claim cannot become user row storage")
            .kind,
        ErrorKind::Rejected
    );
    client
        .execute_sql("CREATE INDEX shared_name ON items (count)", None)
        .await
        .expect("recreate same kind");
    assert_eq!(
        client
            .execute_sql("SELECT id FROM items WHERE count = 9", None)
            .await
            .expect("recreated derived index")[0]
            .rows,
        vec![Row::from(["new"])]
    );
}

#[cfg(feature = "sync")]
const LIVE_ID: &str = "018f0f8e-7b6d-7c4a-8f12-123456789abc";
#[cfg(feature = "sync")]
const DELETED_ID: &str = "018f0f8e-7b6d-7c4a-8f12-123456789abd";

#[cfg(feature = "sync")]
async fn persist_concurrent_history(name: &str) {
    let seed = Database::in_memory();
    seed.client().execute_sql(&format!("CREATE TABLE records (id UUID PRIMARY KEY, label TEXT, allowed BOOLEAN); CREATE INDEX records_label ON records (label); INSERT INTO records VALUES (CAST('{LIVE_ID}' AS UUID), 'seed', NULL); INSERT INTO records VALUES (CAST('{DELETED_ID}' AS UUID), 'deleted', false); CREATE TABLE recreated (id UUID PRIMARY KEY, label TEXT); CREATE INDEX recreated_value ON recreated (label); INSERT INTO recreated VALUES (CAST('{LIVE_ID}' AS UUID), 'old schema')"), None).await.expect("shared causal history");
    let left = Database::in_memory();
    let right = Database::in_memory();
    for replica in [&left, &right] {
        exchange_history(&seed, replica).await;
    }
    left.client().execute_sql(&format!("UPDATE records SET label = 'left', allowed = true WHERE id = CAST('{LIVE_ID}' AS UUID); DELETE FROM records WHERE id = CAST('{DELETED_ID}' AS UUID); DROP TABLE recreated; CREATE TABLE recreated (id UUID PRIMARY KEY, count INTEGER DEFAULT 9); CREATE INDEX recreated_value ON recreated (count); INSERT INTO recreated (id) VALUES (CAST('{DELETED_ID}' AS UUID))"), None).await.expect("offline left edit and tombstone");
    right.client().execute_sql(&format!("UPDATE records SET label = 'right', allowed = false WHERE id = CAST('{LIVE_ID}' AS UUID); UPDATE records SET label = 'stale' WHERE id = CAST('{DELETED_ID}' AS UUID); UPDATE recreated SET label = 'stale schema'"), None).await.expect("offline right edits");
    let stale: Vec<_> = right.export_sync_state().await.expect("retained stale history").into_iter().filter(|unit| matches!(&unit.key, ofdb_sql::SyncKey::Row { table, .. } if table == "records" || table == "recreated")).collect();
    let database = Database::open_indexeddb(name)
        .await
        .expect("persistent destination");
    for replica in [&left, &right] {
        exchange_history(replica, &database).await;
    }
    let archive = serde_json::to_string(&stale).expect("serialize existing public history format");
    database
        .client()
        .execute_sql(
            "CREATE TABLE fixture (id TEXT PRIMARY KEY, history TEXT)",
            None,
        )
        .await
        .expect("fixture archive table");
    database
        .client()
        .execute(vec![ofdb_sql::Statement::Query(ofdb_sql::Query::Insert(
            ofdb_sql::QueryInsert {
                table: "fixture".into(),
                row: Row::from(["stale", archive.as_str()]),
                returning: None,
            },
        ))])
        .await
        .expect("persist stale fixture through query handle");
}

#[cfg(feature = "sync")]
async fn inspect_history(database: &Database, resolved: bool) {
    let client = database.client();
    let key = Row::new(vec![Value::Uuid(
        uuid::Uuid::parse_str(LIVE_ID).expect("live UUID"),
    )]);
    let before = database
        .export_sync_state()
        .await
        .expect("history before inspection");
    assert_eq!(
        client
            .execute_sql("SELECT count FROM recreated WHERE count = 9", None)
            .await
            .expect("recreated schema and derived index survive stale history")[0]
            .rows,
        vec![Row::new(vec![Value::Integer(9)])]
    );
    assert_eq!(
        client
            .execute_sql(
                "SELECT name FROM table_fields WHERE table = 'recreated' ORDER BY position",
                None
            )
            .await
            .expect("recreated columns")[0]
            .rows,
        vec![Row::from(["id"]), Row::from(["count"])]
    );
    let conflicts = client
        .row_conflict_values("records", &key)
        .await
        .expect("persisted losing values");
    if resolved {
        assert!(conflicts.is_empty());
        assert_eq!(
            client
                .execute_sql(
                    "SELECT label, allowed FROM records WHERE label = 'resolved'",
                    None
                )
                .await
                .expect("resolved derived index")[0]
                .rows,
            vec![Row::new(vec![Value::from("resolved"), Value::Bool(false)])]
        );
    } else {
        assert_eq!(conflicts.len(), 2);
        let labels = &conflicts
            .iter()
            .find(|(column, _)| column == "label")
            .expect("label conflict")
            .1;
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&Value::from("left")));
        assert!(labels.contains(&Value::from("right")));
        let allowed = &conflicts
            .iter()
            .find(|(column, _)| column == "allowed")
            .expect("permission conflict")
            .1;
        assert_eq!(allowed.len(), 2);
        assert!(allowed.contains(&Value::Bool(true)) && allowed.contains(&Value::Bool(false)));
        let mut tx = client
            .transaction()
            .await
            .expect("transaction history snapshot");
        platform::sleep(std::time::Duration::from_millis(5)).await;
        assert_eq!(
            tx.row_conflict_values("records", &key)
                .await
                .expect("transaction losing values"),
            conflicts
        );
        let names = client
            .row_conflicts("records", &key)
            .await
            .expect("conflict columns");
        assert_eq!(
            tx.row_conflicts("records", &key)
                .await
                .expect("snapshot columns"),
            names
        );
        assert_eq!(
            client
                .row_conflicts_batch("records", &[key.clone(), key.clone()])
                .await
                .expect("same snapshot batch"),
            vec![names.clone(), names]
        );
        tx.commit().await.expect("read only conflict inspection");
        let visible = client
            .execute_sql("SELECT label FROM records", None)
            .await
            .expect("canonical projection")[0]
            .rows
            .clone();
        assert!(visible == vec![Row::from(["left"])] || visible == vec![Row::from(["right"])]);
        let label = if visible == vec![Row::from(["left"])] {
            "left"
        } else {
            "right"
        };
        assert_eq!(
            client
                .execute_sql(
                    &format!("SELECT label FROM records WHERE label = '{label}'"),
                    None
                )
                .await
                .expect("canonical derived index")[0]
                .rows,
            visible
        );
    }
    assert!(
        client
            .execute_sql(
                &format!("SELECT label FROM records WHERE id = CAST('{DELETED_ID}' AS UUID)"),
                None
            )
            .await
            .expect("persistent tombstone")[0]
            .rows
            .is_empty()
    );
    assert_eq!(
        database
            .export_sync_state()
            .await
            .expect("inspection is history preserving"),
        before
    );
    assert_eq!(
        client
            .execute_sql(
                &format!(
                    "INSERT INTO records VALUES (CAST('{DELETED_ID}' AS UUID), 'revived', true)"
                ),
                None
            )
            .await
            .expect_err("cannot revive same row identity")
            .kind,
        ErrorKind::Rejected
    );
    assert_eq!(
        database
            .export_sync_state()
            .await
            .expect("rejected resurrection preserves history"),
        before
    );
}

#[cfg(feature = "sync")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub async fn sql_semantics_history_probe(
    name: &str,
    action: &str,
) -> Result<String, wasm_bindgen::JsValue> {
    if action == "write" {
        persist_concurrent_history(name).await;
    }
    let database = Database::open_indexeddb(name)
        .await
        .map_err(|error| wasm_bindgen::JsValue::from_str(&error.to_string()))?;
    if action == "resolve" {
        inspect_history(&database, false).await;
        let client = database.client();
        let key = Row::new(vec![Value::Uuid(
            uuid::Uuid::parse_str(LIVE_ID).expect("UUID"),
        )]);
        let before = database
            .export_sync_state()
            .await
            .expect("before failed resolution");
        for assignments in [
            vec![("absent".into(), Value::Bool(false))],
            vec![("allowed".into(), Value::from("wrong type"))],
            vec![
                ("label".into(), Value::from("partial")),
                ("allowed".into(), Value::from("wrong type")),
            ],
        ] {
            assert_eq!(
                client
                    .resolve_row("records", &key, assignments)
                    .await
                    .expect_err("invalid resolution is atomic")
                    .kind,
                ErrorKind::Rejected
            );
            assert_eq!(
                database
                    .export_sync_state()
                    .await
                    .expect("no partial resolution history"),
                before
            );
        }
        let snapshot = client.transaction().await.expect("pre resolution snapshot");
        let old = snapshot
            .row_conflict_values("records", &key)
            .await
            .expect("old conflicts");
        client
            .resolve_row(
                "records",
                &key,
                vec![("label".into(), Value::from("resolved"))],
            )
            .await
            .expect("partial explicit Resolution");
        assert_eq!(
            client
                .row_conflicts("records", &key)
                .await
                .expect("only requested column settled"),
            vec!["allowed"]
        );
        assert_eq!(
            snapshot
                .row_conflict_values("records", &key)
                .await
                .expect("stable conflict snapshot across external resolution"),
            old
        );
        snapshot
            .commit()
            .await
            .expect("stale read only history snapshot");
        drop(client);
        drop(database);
        let database = Database::open_indexeddb(name)
            .await
            .expect("reopen partial Resolution");
        assert_eq!(
            database
                .client()
                .row_conflicts("records", &key)
                .await
                .expect("partial Resolution persisted"),
            vec!["allowed"]
        );
        database
            .client()
            .resolve_row(
                "records",
                &key,
                vec![("allowed".into(), Value::Bool(false))],
            )
            .await
            .expect("finish Resolution");
        let archive = database
            .client()
            .execute_sql("SELECT history FROM fixture", None)
            .await
            .expect("persisted stale fixture")[0]
            .rows[0]
            .values[0]
            .clone();
        let Value::Text(archive) = archive else {
            panic!("fixture history must be text")
        };
        let stale: Vec<ofdb_sql::SyncStateUnit> =
            serde_json::from_str(&archive).expect("existing history format");
        for unit in stale {
            database
                .apply_sync_state(unit)
                .await
                .expect("replay stale history");
        }
        inspect_history(&database, true).await;
        return Ok(serde_json::to_string(
            &database
                .export_sync_state()
                .await
                .expect("resolved history"),
        )
        .expect("serialize history"));
    }
    inspect_history(&database, action == "resolved").await;
    Ok(serde_json::to_string(
        &database
            .export_sync_state()
            .await
            .expect("persisted history"),
    )
    .expect("serialize history"))
}

#[cfg(feature = "sync")]
#[wasm_bindgen::prelude::wasm_bindgen(inline_js = r#"
export function semanticsContext(name, action, page) {
    const root = new URL(import.meta.url).origin;
    return new Promise((resolve, reject) => {
        let target, url;
        const timer = setTimeout(() => finish(new Error('SQL semantics context timeout')), 60000);
        function finish(error, value) {
            clearTimeout(timer);
            if (page) { window.removeEventListener('message', receive); target.remove(); }
            else { target.terminate(); URL.revokeObjectURL(url); }
            if (error) reject(error); else resolve(value);
        }
        function receive(event) {
            if (page && event.source !== target.contentWindow) return;
            if ('error' in event.data) finish(new Error(event.data.error));
            else finish(null, event.data.ok);
        }
        const source = `try {
            const {default: init, sql_semantics_history_probe} = await import('${root}/wasm-bindgen-test');
            await init({module_or_path: '${root}/wasm-bindgen-test_bg.wasm'});
            const ok = await sql_semantics_history_probe(${JSON.stringify(name)}, ${JSON.stringify(action)});
            ${page ? `parent.postMessage({ok}, '${root}')` : 'self.postMessage({ok})'};
        } catch (error) { ${page ? `parent.postMessage({error: String(error)}, '${root}')` : 'self.postMessage({error: String(error)})'}; }`;
        if (page) {
            target = document.createElement('iframe');
            window.addEventListener('message', receive);
            target.srcdoc = '<script type="module">' + source + '</script>';
            document.body.append(target);
        } else {
            url = URL.createObjectURL(new Blob([source], {type: 'text/javascript'}));
            target = new Worker(url, {type: 'module'});
            target.onmessage = receive;
            target.onerror = event => finish(new Error(event.message));
        }
    });
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = semanticsContext)]
    fn semantics_context(name: &str, action: &str, page: bool) -> js_sys::Promise;
}

#[cfg(feature = "sync")]
async fn history_context_restart(page: bool) {
    use wasm_bindgen_futures::JsFuture;
    let name = format!("sql-semantics-history-{}", uuid::Uuid::now_v7());
    let before = JsFuture::from(semantics_context(&name, "write", page))
        .await
        .expect("writer context persists concurrent history");
    let after = JsFuture::from(semantics_context(&name, "read", page))
        .await
        .expect("fresh context conflict inspection");
    assert_eq!(
        before, after,
        "native document history and catalog survive destruction exactly"
    );
    let resolved = JsFuture::from(semantics_context(&name, "resolve", page))
        .await
        .expect("fresh context explicit Resolution");
    assert_ne!(resolved, before, "Resolution changes persisted history");
    let reopened = JsFuture::from(semantics_context(&name, "resolved", page))
        .await
        .expect("fresh context retained Resolution and tombstone");
    assert_eq!(
        resolved, reopened,
        "resolved history survives another context restart exactly"
    );
}

#[cfg(feature = "sync")]
#[wasm_bindgen_test]
async fn sql_semantics_worker_restart_retains_conflicts_resolution_history_and_tombstones() {
    history_context_restart(false).await;
}

#[cfg(all(feature = "sync", not(feature = "worker")))]
#[wasm_bindgen_test]
async fn sql_semantics_page_restart_retains_conflicts_resolution_history_and_tombstones() {
    history_context_restart(true).await;
}

#[cfg(feature = "sync")]
#[wasm_bindgen_test]
async fn sql_semantics_reopen_retains_conflicts_resolution_history_and_tombstones() {
    let name = format!("sql-semantics-reopen-history-{}", uuid::Uuid::now_v7());
    let before = sql_semantics_history_probe(&name, "write")
        .await
        .expect("persist fixture");
    assert_eq!(
        sql_semantics_history_probe(&name, "read")
            .await
            .expect("reopen fixture"),
        before
    );
    let resolved = sql_semantics_history_probe(&name, "resolve")
        .await
        .expect("resolve fixture");
    assert_eq!(
        sql_semantics_history_probe(&name, "resolved")
            .await
            .expect("reopen resolved fixture"),
        resolved
    );
}

#[cfg(feature = "sync")]
struct HistoryChannel {
    sender: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
    receiver: futures::channel::mpsc::UnboundedReceiver<Vec<u8>>,
}

#[cfg(feature = "sync")]
impl ofdb_sql::SyncTransport for HistoryChannel {
    type Error = &'static str;
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sender
            .unbounded_send(frame)
            .map_err(|_| "closed fixture channel")
    }
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        use futures::StreamExt;
        self.receiver.next().await.ok_or("closed fixture channel")
    }
}

#[cfg(feature = "sync")]
async fn exchange_history(left: &Database, right: &Database) {
    let (a, ar) = futures::channel::mpsc::unbounded();
    let (b, br) = futures::channel::mpsc::unbounded();
    let mut lt = HistoryChannel {
        sender: a,
        receiver: br,
    };
    let mut rt = HistoryChannel {
        sender: b,
        receiver: ar,
    };
    let config = ofdb_sql::SessionConfig::default();
    let (l, r) = futures::join!(
        left.synchronize(&mut lt, &config, ofdb_sql::SyncRole::Initiator),
        right.synchronize(&mut rt, &config, ofdb_sql::SyncRole::Responder)
    );
    l.expect("export fixture through public sync");
    r.expect("persist fixture through public sync");
}

#[wasm_bindgen::prelude::wasm_bindgen(inline_js = r#"
export function semanticsCommitGate() {
    return new Promise(resolve => {
        self.onmessage = () => resolve();
        self.postMessage({ready: true});
    });
}
export function semanticsRace(name) {
    const root = new URL(import.meta.url).origin;
    return new Promise((resolve, reject) => {
        const workers = [], urls = [], results = [];
        let ready = 0;
        const timer = setTimeout(() => finish(new Error('SQL worker race timeout')), 60000);
        function finish(error) {
            clearTimeout(timer);
            workers.forEach(worker => worker.terminate());
            urls.forEach(url => URL.revokeObjectURL(url));
            if (error) reject(error); else resolve(results);
        }
        for (const label of ['a', 'b']) {
            const source = `try {
                const {default: init, sql_semantics_race_probe} = await import('${root}/wasm-bindgen-test');
                await init({module_or_path: '${root}/wasm-bindgen-test_bg.wasm'});
                self.postMessage({ok: await sql_semantics_race_probe(${JSON.stringify(name)}, '${label}')});
            } catch (error) { self.postMessage({error: String(error)}); }`;
            const url = URL.createObjectURL(new Blob([source], {type: 'text/javascript'}));
            urls.push(url);
            const worker = new Worker(url, {type: 'module'});
            workers.push(worker);
            worker.onerror = event => finish(new Error(event.message));
            worker.onmessage = event => {
                if (event.data.error) finish(new Error(event.data.error));
                else if (event.data.ready) {
                    if (++ready === 2) workers.forEach(worker => worker.postMessage('commit'));
                } else {
                    results.push(event.data.ok);
                    if (results.length === 2) finish();
                }
            };
        }
    });
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = semanticsCommitGate)]
    fn semantics_commit_gate() -> js_sys::Promise;
    #[wasm_bindgen(js_name = semanticsRace)]
    fn semantics_race(name: &str) -> js_sys::Promise;
}

#[wasm_bindgen::prelude::wasm_bindgen]
pub async fn sql_semantics_race_probe(
    name: &str,
    label: &str,
) -> Result<String, wasm_bindgen::JsValue> {
    let error = |error: ofdb_sql::Error| wasm_bindgen::JsValue::from_str(&error.to_string());
    let client = Database::open_indexeddb(name)
        .await
        .map_err(error)?
        .client();
    let mut tx = client.transaction().await.map_err(error)?;
    tx.execute_sql(&format!("UPDATE left_items SET label = '{label}'; UPDATE right_items SET label = '{label}'; CREATE TABLE contender_{label} (id TEXT PRIMARY KEY); CREATE INDEX contender_index_{label} ON contender_{label} (id); INSERT INTO contender_{label} VALUES ('{label}')"), None).await.map_err(error)?;
    wasm_bindgen_futures::JsFuture::from(semantics_commit_gate()).await?;
    match tx.commit().await {
        Ok(()) => Ok(label.into()),
        Err(error) if error.kind == ErrorKind::Storage => Ok("stale".into()),
        Err(failure) => Err(error(failure)),
    }
}

#[wasm_bindgen_test]
async fn sql_semantics_separate_worker_commits_are_atomic_across_tables_and_catalog() {
    let name = format!("sql-semantics-race-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client.execute_sql("CREATE TABLE left_items (id TEXT PRIMARY KEY, label TEXT UNIQUE); CREATE TABLE right_items (id TEXT PRIMARY KEY, label TEXT UNIQUE); CREATE INDEX left_label ON left_items (label); CREATE INDEX right_label ON right_items (label); INSERT INTO left_items VALUES ('one', 'old'); INSERT INTO right_items VALUES ('one', 'old')", None).await.expect("seed multi table race");
    drop(client);
    let results = wasm_bindgen_futures::JsFuture::from(semantics_race(&name))
        .await
        .expect("simultaneously staged independent Wasm instances");
    let results: Vec<_> = js_sys::Array::from(&results)
        .iter()
        .map(|result| result.as_string().expect("commit outcome"))
        .collect();
    assert_eq!(
        results
            .iter()
            .filter(|result| result.as_str() == "stale")
            .count(),
        1
    );
    let winner = results
        .iter()
        .find(|result| result.as_str() != "stale")
        .expect("one winner");
    assert!(winner == "a" || winner == "b");
    let loser = if winner == "a" { "b" } else { "a" };
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen after terminating both writers")
        .client();
    for table in ["left_items", "right_items"] {
        assert_eq!(
            client
                .execute_sql(
                    &format!("SELECT id FROM {table} WHERE label = '{winner}'"),
                    None
                )
                .await
                .expect("winner row and index")[0]
                .rows,
            vec![Row::from(["one"])]
        );
        assert!(
            client
                .execute_sql(
                    &format!("SELECT id FROM {table} WHERE label = '{loser}' OR label = 'old'"),
                    None
                )
                .await
                .expect("no losing or old index entries")[0]
                .rows
                .is_empty()
        );
    }
    assert_eq!(
        client
            .execute_sql(&format!("SELECT id FROM contender_{winner}"), None)
            .await
            .expect("winner catalog and row")[0]
            .rows,
        vec![Row::from([winner.as_str()])]
    );
    assert!(
        client
            .execute_sql(&format!("SELECT * FROM contender_{loser}"), None)
            .await
            .is_err()
    );
    assert!(
        client
            .execute_sql(
                &format!("SELECT name FROM indices WHERE name = 'contender_index_{loser}'"),
                None
            )
            .await
            .expect("no partial losing catalog")[0]
            .rows
            .is_empty()
    );
    client.execute_sql(&format!("CREATE TABLE contender_{loser} (id TEXT PRIMARY KEY); CREATE INDEX contender_index_{loser} ON contender_{loser} (id)"), None).await.expect("no failed namespace claims");
}

#[wasm_bindgen_test]
async fn sql_semantics_composite_unique_updates_and_immutable_primary_keys() {
    let name = format!("sql-semantics-composite-{}", uuid::Uuid::now_v7());
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client.execute_sql("CREATE TABLE entries (tenant TEXT, code INTEGER, first TEXT, last TEXT, PRIMARY KEY (tenant, code)); CREATE UNIQUE INDEX full_name ON entries (first, last); INSERT INTO entries VALUES ('a', 1, 'Ada', 'Lovelace'); INSERT INTO entries VALUES ('a', 2, 'Ada', 'Byron'); INSERT INTO entries VALUES ('b', 1, 'Grace', 'Lovelace')", None).await.expect("composite natural keys and unique tuples");
    for sql in [
        "INSERT INTO entries VALUES ('c', 3, 'Ada', 'Lovelace')",
        "UPDATE entries SET last = 'Lovelace' WHERE tenant = 'a' AND code = 2",
        "UPDATE entries SET tenant = 'changed' WHERE tenant = 'b'",
        "UPDATE entries SET code = 4 WHERE tenant = 'b'",
        "INSERT INTO entries VALUES ('z', NULL, 'Null', 'Key')",
    ] {
        assert_eq!(
            client
                .execute_sql(sql, None)
                .await
                .expect_err("reject without partial rows or indexes")
                .kind,
            ErrorKind::Rejected
        );
    }
    drop(client);
    let client = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    assert_eq!(
        client
            .execute_sql(
                "SELECT tenant, code FROM entries WHERE first = 'Ada' AND last = 'Byron'",
                None
            )
            .await
            .expect("failed update preserves composite index")[0]
            .rows,
        vec![Row::new(vec![Value::from("a"), Value::Integer(2)])]
    );
    assert_eq!(
        client
            .execute_sql(
                "SELECT tenant, code FROM entries ORDER BY tenant, code",
                None
            )
            .await
            .expect("immutable composite identity")[0]
            .rows,
        vec![
            Row::new(vec![Value::from("a"), Value::Integer(1)]),
            Row::new(vec![Value::from("a"), Value::Integer(2)]),
            Row::new(vec![Value::from("b"), Value::Integer(1)])
        ]
    );
}

#[cfg(feature = "sync")]
#[wasm_bindgen_test]
async fn sql_semantics_unique_key_losers_are_not_promoted_after_persisted_winner_deletion() {
    let name = format!("sql-semantics-unique-conflict-{}", uuid::Uuid::now_v7());
    let left = Database::in_memory();
    let right = Database::in_memory();
    left.client()
        .execute_sql(
            "CREATE TABLE claims (id UUID PRIMARY KEY, name TEXT UNIQUE)",
            None,
        )
        .await
        .expect("unique schema");
    exchange_history(&left, &right).await;
    left.client()
        .execute_sql(
            &format!("INSERT INTO claims VALUES (CAST('{LIVE_ID}' AS UUID), 'same')"),
            None,
        )
        .await
        .expect("smaller independent claimant");
    right
        .client()
        .execute_sql(
            &format!("INSERT INTO claims VALUES (CAST('{DELETED_ID}' AS UUID), 'same')"),
            None,
        )
        .await
        .expect("larger independent claimant");
    let stale = left.export_sync_state().await.expect("losing history");
    let database = Database::open_indexeddb(&name)
        .await
        .expect("persistent destination");
    exchange_history(&left, &database).await;
    exchange_history(&right, &database).await;
    let client = database.client();
    assert_eq!(
        client
            .execute_sql("SELECT id FROM claims WHERE name = 'same'", None)
            .await
            .expect("largest row UUID wins")[0]
            .rows,
        vec![Row::new(vec![Value::Uuid(
            uuid::Uuid::parse_str(DELETED_ID).expect("winner UUID")
        )])]
    );
    assert!(
        client
            .row_conflicts(
                "claims",
                &Row::new(vec![Value::Uuid(
                    uuid::Uuid::parse_str(DELETED_ID).expect("UUID")
                )])
            )
            .await
            .expect("unique key collision is not a same row Conflict")
            .is_empty()
    );
    client
        .execute_sql("DELETE FROM claims", None)
        .await
        .expect("delete winning claimant");
    drop(client);
    drop(database);
    let database = Database::open_indexeddb(&name)
        .await
        .expect("reopen tombstoned winner");
    for unit in stale.into_iter().filter(
        |unit| matches!(&unit.key, ofdb_sql::SyncKey::Row { table, .. } if table == "claims"),
    ) {
        database
            .apply_sync_state(unit)
            .await
            .expect("replay losing claimant");
    }
    let client = database.client();
    assert!(
        client
            .execute_sql("SELECT * FROM claims", None)
            .await
            .expect("no promotion or resurrection")[0]
            .rows
            .is_empty()
    );
    for id in [LIVE_ID, DELETED_ID] {
        assert_eq!(
            client
                .execute_sql(
                    &format!("INSERT INTO claims VALUES (CAST('{id}' AS UUID), 'same')"),
                    None
                )
                .await
                .expect_err("permanently deleted claimants")
                .kind,
            ErrorKind::Rejected
        );
    }
    client.execute_sql("INSERT INTO claims VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'same')", None).await.expect("larger fresh identity supersedes deleted claim");
}
