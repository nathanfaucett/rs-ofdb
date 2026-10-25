use ofdb_kv::{Database, Value};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn kv_persistent_reopen() {
    let name = format!("kv-reopen-{}", uuid::Uuid::now_v7());
    let database = Database::open_indexeddb(&name)
        .await
        .expect("persistent open");
    let client = database.client();
    drop(database);
    client
        .set("hello", Value::Blob(vec![0, 255]), None)
        .await
        .expect("persist");
    drop(client);
    let reopened = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    assert_eq!(
        reopened.get("hello").await.expect("read persisted"),
        Some(Value::Blob(vec![0, 255]))
    );
}

#[wasm_bindgen_test]
async fn byte_net_noop_does_not_create_a_table_or_advance_revision() {
    use btree::{BTree, BTreeTransaction};
    let db = btree_indexeddb::Database::open(&format!("noop-{}", uuid::Uuid::now_v7()), "test")
        .await
        .expect("open");
    let tree = btree_indexeddb::ByteBTree::new(db.clone(), "bytes");
    let mut tx = tree.transaction().await.expect("snapshot");
    tx.insert(vec![], vec![1]).await.expect("insert");
    tx.remove(&vec![]).await.expect("remove");
    tx.commit().await.expect("net noop");
    let snapshot = db.snapshot().await.expect("read revision");
    assert_eq!(snapshot.revision(), 0);
    assert_eq!(snapshot.table_names().count(), 0);
}

use btree::{BTree, BTreeError, BTreeRead, BTreeTransaction};
use btree_indexeddb::{ByteBTree, Database as ByteDatabase};
use futures::{FutureExt, TryStreamExt};
use std::time::Duration;
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(module = "/src/indexeddb_helpers.js")]
extern "C" {
    fn compare(a: &js_sys::Uint8Array, b: &js_sys::Uint8Array) -> i32;
    #[wasm_bindgen(js_name = failNextCommit)]
    fn fail_next_commit(mode: &str);
    #[wasm_bindgen(js_name = restartWorker)]
    fn restart_worker(name: &str, write: bool) -> js_sys::Promise;
}

fn name(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

#[wasm_bindgen_test]
async fn byte_snapshots_multi_table_staging_conflicts_and_order() {
    let name = name("snapshot");
    let db = ByteDatabase::open(&name, "test").await.expect("open");
    let other = ByteDatabase::open(&name, "test")
        .await
        .expect("independent open");
    let keys = [
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 1],
        vec![1],
        vec![127],
        vec![128],
        vec![255],
        vec![255, 0],
    ];
    for pair in keys.windows(2) {
        assert_eq!(
            compare(
                &js_sys::Uint8Array::from(pair[0].as_slice()),
                &js_sys::Uint8Array::from(pair[1].as_slice())
            ),
            -1
        );
    }
    let mut initial = db.snapshot().await.expect("initial");
    assert!(initial.create_table("a"));
    assert!(initial.create_table("b"));
    for key in &keys {
        initial
            .table_mut("a")
            .expect("a")
            .insert(key.clone(), vec![42]);
    }
    initial.table_mut("b").expect("b").insert(vec![1], vec![2]);
    initial.commit().await.expect("atomic initial");
    let mut old = db.snapshot().await.expect("old snapshot");
    let mut newer = other.snapshot().await.expect("same revision");
    newer.drop_table("a");
    newer.table_mut("b").expect("b").insert(vec![2], vec![3]);
    newer.commit().await.expect("drop and write atomically");
    platform::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        old.table("a")
            .expect("stable table")
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        keys
    );
    old.table_mut("b").expect("b").insert(vec![3], vec![4]);
    assert!(matches!(old.commit().await, Err(BTreeError::Conflict)));
    let persisted = db.snapshot().await.expect("persisted");
    assert!(persisted.table("a").is_none());
    assert_eq!(persisted.table("b").expect("b").len(), 2);
    assert_eq!(persisted.revision(), 2);
    let isolated = ByteDatabase::open(&name, "sql")
        .await
        .expect("different kind");
    assert_eq!(
        isolated
            .snapshot()
            .await
            .expect("isolated")
            .table_names()
            .count(),
        0
    );
}

#[wasm_bindgen_test]
async fn byte_btree_read_write_range_update_rollback_and_noops() {
    let db = ByteDatabase::open(&name("btree"), "test")
        .await
        .expect("open");
    let tree = ByteBTree::new(db.clone(), "bytes");
    let mut tx = tree.transaction().await.expect("begin");
    tx.insert(vec![], vec![0]).await.expect("empty key");
    tx.insert(vec![0], vec![1]).await.expect("zero key");
    tx.insert(vec![0, 0], vec![2]).await.expect("double zero");
    assert_eq!(
        tx.update(vec![0], |v| {
            v.push(2);
            Ok(())
        })
        .await
        .expect("update"),
        Some(())
    );
    assert!(
        tx.update(vec![], |_| Err(BTreeError::InvalidDocument))
            .await
            .is_err()
    );
    assert_eq!(
        tx.get(&vec![])
            .await
            .expect("unchanged after failed update"),
        Some(vec![0])
    );
    assert_eq!(
        tx.update(vec![9], |_| panic!("missing key must not invoke callback"))
            .await
            .expect("missing"),
        None
    );
    platform::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        tx.range(vec![]..vec![0, 0])
            .try_collect::<Vec<_>>()
            .await
            .expect("range"),
        vec![(vec![], vec![0]), (vec![0], vec![1, 2])]
    );
    tx.commit().await.expect("commit");
    let revision = db.snapshot().await.expect("revision").revision();
    let mut noop = tree.transaction().await.expect("noop");
    noop.insert(vec![], vec![7]).await.expect("change");
    noop.insert(vec![], vec![0]).await.expect("restore");
    noop.commit().await.expect("noop commit");
    assert_eq!(db.snapshot().await.expect("revision").revision(), revision);
    let mut rolled = tree.transaction().await.expect("rollback");
    assert_eq!(
        rolled
            .remove_range(..)
            .try_collect::<Vec<_>>()
            .await
            .expect("remove range")
            .len(),
        3
    );
    assert!(rolled.is_empty().await.expect("empty staging"));
    rolled.rollback().await.expect("rollback");
    let mut dropped = tree.transaction().await.expect("drop");
    dropped.remove(&vec![]).await.expect("stage removal");
    drop(dropped);
    assert_eq!(
        tree.range(..)
            .try_collect::<Vec<_>>()
            .await
            .expect("persisted")
            .len(),
        3
    );
    let readonly = tree.transaction().await.expect("readonly");
    let mut writer = tree.transaction().await.expect("writer");
    writer.insert(vec![9], vec![9]).await.expect("write");
    writer.commit().await.expect("writer commit");
    readonly.commit().await.expect("stale readonly is harmless");
    assert_eq!(
        db.snapshot().await.expect("revision").revision(),
        revision + 1
    );
}

#[wasm_bindgen_test]
async fn issued_commit_completion_survives_cancellation_and_failures_are_atomic() {
    let db = ByteDatabase::open(&name("completion"), "test")
        .await
        .expect("open");
    let mut changes = db.subscribe_changes();
    let initial = changes.select().expect("initial work");
    assert!(changes.acknowledge(initial));
    let mut staged = db.snapshot().await.expect("snapshot");
    staged.create_table("a");
    staged.create_table("b");
    staged.table_mut("a").expect("a").insert(vec![0], vec![1]);
    staged.table_mut("b").expect("b").insert(vec![0], vec![2]);
    // Poll once to issue the native transaction, then abandon caller polling.
    assert!(staged.commit().now_or_never().is_none());
    platform::timeout(Duration::from_secs(5), changes.changed())
        .await
        .expect("completion observed without caller")
        .expect("notification");
    let work = changes.select().expect("committed work");
    assert!(changes.acknowledge(work));
    let persisted = db.snapshot().await.expect("persisted");
    assert_eq!(persisted.revision(), 1);
    assert_eq!(persisted.table_names().count(), 2);
    for mode in ["abort", "throw"] {
        let mut staged = db.snapshot().await.expect("failure snapshot");
        staged.drop_table("a");
        staged.create_table("c");
        staged.table_mut("b").expect("b").insert(vec![0], vec![7]);
        staged.table_mut("c").expect("c").insert(vec![0], vec![8]);
        fail_next_commit(mode);
        assert!(
            staged.commit().await.is_err(),
            "{mode} must not report success"
        );
        let current = db.snapshot().await.expect("after failure");
        assert_eq!(current.revision(), 1);
        assert_eq!(current.table_names().collect::<Vec<_>>(), vec!["a", "b"]);
        assert_eq!(current.table("b").expect("b").get(&vec![0]), Some(&vec![2]));
        assert!(changes.select().is_none(), "failed commit must be silent");
    }
    db.snapshot()
        .await
        .expect("readonly")
        .commit()
        .await
        .expect("readonly commit");
    assert!(changes.select().is_none());
}

#[wasm_bindgen_test]
async fn kv_snapshot_across_await_and_independent_handle_race() {
    let name = name("kv-race");
    let left = Database::open_indexeddb(&name)
        .await
        .expect("left")
        .client();
    let right = Database::open_indexeddb(&name)
        .await
        .expect("right")
        .client();
    left.set("key", Value::Integer(1), None)
        .await
        .expect("seed");
    let mut a = left.transaction().await.expect("a");
    let mut b = right.transaction().await.expect("b");
    a.set("key", Value::Integer(2), None)
        .await
        .expect("a write");
    a.commit().await.expect("a commit");
    platform::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        b.get("key").await.expect("stable old snapshot"),
        Some(Value::Integer(1))
    );
    b.set("other", Value::Integer(3), None)
        .await
        .expect("b write");
    assert_eq!(
        b.get("other").await.expect("read own write"),
        Some(Value::Integer(3))
    );
    assert!(matches!(
        b.commit().await,
        Err(ofdb_kv::Error::Storage {
            kind: ofdb_kv::ErrorKind::Conflict,
            ..
        })
    ));
    assert_eq!(
        right.get("key").await.expect("winner"),
        Some(Value::Integer(2))
    );
    assert_eq!(right.get("other").await.expect("all or nothing"), None);
    let mut a = left.transaction().await.expect("race a");
    let mut b = right.transaction().await.expect("race b");
    a.set("a", true, None).await.expect("a");
    b.set("b", true, None).await.expect("b");
    let (a, b) = futures::join!(a.commit(), b.commit());
    assert_ne!(a.is_ok(), b.is_ok(), "exactly one independent writer wins");
}

#[wasm_bindgen_test]
async fn kv_typed_values_literal_prefixes_order_expiry_and_generations() {
    let name = name("kv-types");
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    let nested = Value::from(
        serde_json::json!({"nested": [true, null, {"i": 9223372036854775807_i64, "text": "🦀"}]}),
    );
    let values = [
        Value::Null,
        Value::Bool(true),
        Value::Integer(i64::MIN),
        Value::Integer(i64::MAX),
        Value::Float(f64::from_bits(0x8000000000000000)),
        Value::Float(f64::MAX),
        Value::Text("\0é🦀".into()),
        Value::Blob(vec![0, 255]),
        nested,
        Value::Uuid(uuid::Uuid::now_v7()),
    ];
    for (i, value) in values.iter().enumerate() {
        client
            .set(&format!("value/{i}"), value.clone(), None)
            .await
            .expect("typed write");
    }
    let keys = ["", "\0", "a", "a\0", "a%", "a_", "ab", "é", "🦀"];
    for key in keys {
        client.set(key, Value::Bool(true), None).await.expect("key");
    }
    client
        .set("expired", true, Some(platform::unix_millis() - 1))
        .await
        .expect("expiry");
    client
        .set("future", true, Some(platform::unix_millis() + 60_000))
        .await
        .expect("future");
    client
        .set("deleted", true, None)
        .await
        .expect("live generation");
    client.delete("deleted").await.expect("tombstone");
    let clone = client.clone();
    drop(client);
    let reopened = Database::open_indexeddb(&name)
        .await
        .expect("reopen")
        .client();
    for (i, value) in values.iter().enumerate() {
        let found = reopened
            .get(&format!("value/{i}"))
            .await
            .expect("persisted type")
            .expect("present");
        assert_eq!(found, *value);
        if let (Value::Float(a), Value::Float(b)) = (&found, value) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }
    assert_eq!(
        reopened
            .scan("", "b")
            .await
            .expect("ordered range")
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
        keys[..7]
    );
    assert_eq!(
        reopened
            .scan_prefix("a%")
            .await
            .expect("literal percent")
            .len(),
        1
    );
    assert_eq!(
        reopened
            .scan_prefix("a_")
            .await
            .expect("literal underscore")
            .len(),
        1
    );
    assert_eq!(
        reopened.scan_prefix("a\0").await.expect("nul prefix").len(),
        1
    );
    assert_eq!(
        reopened
            .scan_prefix("é")
            .await
            .expect("unicode prefix")
            .len(),
        1
    );
    assert_eq!(reopened.get("expired").await.expect("expired"), None);
    assert_eq!(
        reopened.get("future").await.expect("future"),
        Some(Value::Bool(true))
    );
    assert_eq!(reopened.get("deleted").await.expect("tombstone"), None);
    reopened
        .set("deleted", Value::Text("new generation".into()), None)
        .await
        .expect("recreate");
    assert_eq!(
        clone
            .get("deleted")
            .await
            .expect("shared persisted new generation"),
        Some(Value::Text("new generation".into()))
    );
}

#[wasm_bindgen]
pub async fn restart_probe(name: String, write: bool) -> Result<String, JsValue> {
    async fn probe(name: &str, write: bool) -> Result<String, String> {
        let client = Database::open_indexeddb(name)
            .await
            .map_err(|e| e.to_string())?
            .client();
        if write {
            client
                .set("history", Value::Integer(i64::MAX), None)
                .await
                .map_err(|e| e.to_string())?;
            client
                .set("history", Value::Integer(i64::MIN), None)
                .await
                .map_err(|e| e.to_string())?;
            client
                .set("tombstone", true, None)
                .await
                .map_err(|e| e.to_string())?;
            client
                .delete("tombstone")
                .await
                .map_err(|e| e.to_string())?;
        }
        if client.get("history").await.map_err(|e| e.to_string())? != Some(Value::Integer(i64::MIN))
        {
            return Err("lost persisted value".into());
        }
        if client
            .get("tombstone")
            .await
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Err("lost tombstone".into());
        }
        let db = ByteDatabase::open(name, "kv")
            .await
            .map_err(|e| e.to_string())?;
        let store = kv_store::KvStore::new(ByteBTree::new(db, "ofdb-kv"), || {
            uuid::Uuid::now_v7().get_timestamp().expect("v7 timestamp")
        });
        let tx = store.transaction().await.map_err(|e| e.to_string())?;
        let history = tx
            .history("history")
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing native history")?;
        let tombstone = tx
            .history("tombstone")
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing tombstone history")?;
        if history.changes.len() < 2 || tombstone.changes.len() < 2 {
            return Err("lost native changes".into());
        }
        Ok(format!("{history:?}/{tombstone:?}"))
    }
    probe(&name, write).await.map_err(|e| JsValue::from_str(&e))
}

#[wasm_bindgen_test]
async fn actual_worker_destruction_and_restart_retains_values_tombstones_and_history() {
    let name = name("restart");
    let written = JsFuture::from(restart_worker(&name, true))
        .await
        .expect("writer worker")
        .as_string()
        .expect("history signature");
    let read = JsFuture::from(restart_worker(&name, false))
        .await
        .expect("new worker")
        .as_string()
        .expect("history signature");
    assert_eq!(
        written, read,
        "new Wasm instance must retain generation IDs and native history"
    );
}

#[wasm_bindgen(module = "/src/indexeddb_helpers.js")]
extern "C" {
    fn malformed(name: &str, mode: &str) -> js_sys::Promise;
    #[wasm_bindgen(js_name = denyOpenOnce)]
    fn deny_open_once();
    #[cfg(not(feature = "worker"))]
    #[wasm_bindgen(js_name = restartPage)]
    fn restart_page(name: &str, write: bool) -> js_sys::Promise;
    #[wasm_bindgen(js_name = raceWorkers)]
    fn race_workers(name: &str) -> js_sys::Promise;
}

#[wasm_bindgen_test]
async fn failed_persistent_opens_never_fall_back_to_memory() {
    for mode in ["version", "format"] {
        let name = name(mode);
        JsFuture::from(malformed(&name, mode))
            .await
            .expect("prepare incompatible store");
        assert!(
            Database::open_indexeddb(&name).await.is_err(),
            "{mode} must be rejected"
        );
    }
    deny_open_once();
    assert!(Database::open_indexeddb(&name("denied")).await.is_err());
}

#[cfg(not(feature = "worker"))]
#[wasm_bindgen_test]
async fn actual_page_destruction_and_restart_retains_values_tombstones_and_history() {
    let name = name("page-restart");
    let written = JsFuture::from(restart_page(&name, true))
        .await
        .expect("writer page")
        .as_string()
        .expect("history signature");
    let read = JsFuture::from(restart_page(&name, false))
        .await
        .expect("new page")
        .as_string()
        .expect("history signature");
    assert_eq!(written, read);
}

#[wasm_bindgen]
pub struct RaceTransaction {
    tx: ofdb_kv::Transaction,
}

#[wasm_bindgen]
impl RaceTransaction {
    pub async fn commit(self) -> Result<String, JsValue> {
        match self.tx.commit().await {
            Ok(()) => Ok("committed".into()),
            Err(ofdb_kv::Error::Storage {
                kind: ofdb_kv::ErrorKind::Conflict,
                ..
            }) => Ok("conflict".into()),
            Err(error) => Err(JsValue::from_str(&error.to_string())),
        }
    }
}

#[wasm_bindgen]
pub async fn race_begin(name: String, key: String) -> Result<RaceTransaction, JsValue> {
    let client = Database::open_indexeddb(&name)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .client();
    let mut tx = client
        .transaction()
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    tx.set(&key, true, None)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    Ok(RaceTransaction { tx })
}

#[wasm_bindgen_test]
async fn independent_worker_writers_reject_stale_revision_atomically() {
    let name = name("worker-race");
    let result = JsFuture::from(race_workers(&name))
        .await
        .expect("cross-context race");
    let mut results = js_sys::Array::from(&result)
        .iter()
        .map(|v| v.as_string().expect("race result"))
        .collect::<Vec<_>>();
    results.sort();
    assert_eq!(results, ["committed", "conflict"]);
    assert_eq!(
        Database::open_indexeddb(&name)
            .await
            .expect("open winner")
            .client()
            .scan_all()
            .await
            .expect("only one write persisted")
            .len(),
        1
    );
}

#[wasm_bindgen_test]
async fn snapshot_materialization_measurement() {
    for count in [100_u32, 1000] {
        let db = ByteDatabase::open(&name("measure"), "test")
            .await
            .expect("open");
        let mut tx = db.snapshot().await.expect("seed");
        tx.create_table("data");
        for key in 0..count {
            tx.table_mut("data")
                .expect("data")
                .insert(key.to_be_bytes().to_vec(), vec![7; 1024]);
        }
        tx.commit().await.expect("seed commit");
        let started = js_sys::Date::now();
        let snapshot = db.snapshot().await.expect("measured snapshot");
        let elapsed = js_sys::Date::now() - started;
        let data = snapshot.table("data").expect("data");
        assert_eq!(data.len(), count as usize);
        assert_eq!(
            data.values().map(Vec::len).sum::<usize>(),
            count as usize * 1024
        );
        wasm_bindgen_test::console_log!(
            "snapshot: {} records, {} key+value bytes, {} ms, two Rust-owned copies plus IDB/JS materialization",
            count,
            count * 1028,
            elapsed
        );
    }
}

#[wasm_bindgen_test]
async fn persisted_tombstone_rejects_stale_history_and_recreation_has_a_new_generation() {
    fn timestamp() -> uuid::Timestamp {
        uuid::Uuid::now_v7().get_timestamp().expect("v7 timestamp")
    }
    let name = name("stale-history");
    let db = ByteDatabase::open(&name, "test").await.expect("open");
    let store = kv_store::KvStore::new(ByteBTree::new(db, "kv"), timestamp);
    let mut tx = store.transaction().await.expect("seed");
    tx.set("key", Value::Bool(true), None).await.expect("set");
    tx.commit().await.expect("seed commit");
    let tx = store.transaction().await.expect("export");
    let stale = tx
        .export_snapshot("key")
        .await
        .expect("export stale")
        .expect("snapshot");
    let old_id = tx
        .history("key")
        .await
        .expect("history")
        .expect("history exists")
        .id;
    tx.rollback().await.expect("rollback read");
    let mut tx = store.transaction().await.expect("delete");
    tx.delete("key").await.expect("tombstone");
    tx.commit().await.expect("delete commit");
    drop(store);
    let db = ByteDatabase::open(&name, "test").await.expect("reopen");
    let store = kv_store::KvStore::new(ByteBTree::new(db, "kv"), timestamp);
    let mut tx = store.transaction().await.expect("import stale");
    tx.import_snapshot(stale)
        .await
        .expect("merge stale history");
    tx.commit().await.expect("commit merge");
    let mut tx = store.transaction().await.expect("read tombstone");
    assert_eq!(
        tx.get("key", platform::unix_millis())
            .await
            .expect("tombstone wins"),
        None
    );
    tx.set("key", Value::Bool(false), None)
        .await
        .expect("recreate");
    let new_id = tx
        .history("key")
        .await
        .expect("new history")
        .expect("new generation")
        .id;
    assert_ne!(old_id, new_id);
    tx.commit().await.expect("new generation commit");
}

#[wasm_bindgen(module = "/src/indexeddb_helpers.js")]
extern "C" {
    fn upgrade(name: &str) -> js_sys::Promise;
}

#[wasm_bindgen_test]
async fn versionchange_closes_live_connections_and_closed_handles_return_errors() {
    let name = name("upgrade");
    let client = Database::open_indexeddb(&name)
        .await
        .expect("open")
        .client();
    client.set("key", true, None).await.expect("write");
    JsFuture::from(upgrade(&name))
        .await
        .expect("versionchange releases connection");
    assert!(
        client.get("key").await.is_err(),
        "closed connection must not become memory"
    );
    assert!(
        Database::open_indexeddb(&name).await.is_err(),
        "newer format is incompatible"
    );
}
