#![cfg(all(not(target_arch = "wasm32"), feature = "sync"))]

#[path = "../src/sql_sync_support.rs"]
mod support;

use ofdb_sql::Database;

#[tokio::test]
async fn native_sql_current_protocol_fixture() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sql_native_fixture.json");
    if std::env::var_os("OFDB_REGENERATE_SQL_FIXTURE").is_some() {
        let source = Database::in_memory();
        let target = Database::in_memory();
        source.client().execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT UNIQUE, count INTEGER); CREATE INDEX items_label ON items (label); INSERT INTO items VALUES ('live', 'native', 7); INSERT INTO items VALUES ('deleted', 'old', 7)", None).await.expect("native seed");
        let (a, b) = support::broad(&source, &target).await;
        let mut fixture = serde_json::json!({ "initial_initiator": a, "initial_responder": b, "initial_state": source.export_sync_state().await.expect("native history") });
        source.client().execute_sql("UPDATE items SET label = 'updated', count = 9223372036854775807 WHERE id = 'live'; DELETE FROM items WHERE id = 'deleted'; CREATE TABLE extra (id TEXT PRIMARY KEY); INSERT INTO extra VALUES ('catalog')", None).await.expect("native changes and deletion");
        let (a, b) = support::broad(&source, &target).await;
        fixture["delta_initiator"] = serde_json::to_value(a).expect("frames");
        fixture["delta_responder"] = serde_json::to_value(b).expect("frames");
        fixture["delta_state"] =
            serde_json::to_value(source.export_sync_state().await.expect("delta history"))
                .expect("state");
        std::fs::write(&path, serde_json::to_string(&fixture).expect("JSON") + "\n")
            .expect("write fixture");
    }
    let fixture = serde_json::from_slice(&std::fs::read(path).expect("committed native fixture"))
        .expect("JSON");
    support::verify_fixture(&Database::in_memory(), &fixture).await;
}
