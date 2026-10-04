use automerge::{AutoCommit, ROOT, transaction::Transactable};
use btree::{BTree, BTreeError, BTreeTransaction, InMemoryBTree};
use btree_automerge::{DocumentChangeKey, hash_heads};
use futures::executor::block_on;
use ofdb_kv_store::{KvStore, encode_document_id};
use value::{JsonValue, Value};

type Store = KvStore<InMemoryBTree<Vec<u8>, Vec<u8>>>;
type Snapshot = (DocumentChangeKey, Vec<u8>);

fn store() -> Store {
    KvStore::new(InMemoryBTree::new(), || {
        uuid::Timestamp::now(uuid::NoContext)
    })
}

async fn snapshot(store: &Store) -> Snapshot {
    let tx = store.transaction().await.expect("start snapshot read");
    let snapshot = tx
        .export_snapshot("key")
        .await
        .expect("export snapshot")
        .expect("key exists");
    tx.rollback().await.expect("finish snapshot read");
    snapshot
}

async fn import(store: &Store, snapshot: Snapshot) {
    let mut tx = store.transaction().await.expect("start import");
    tx.import_snapshot(snapshot).await.expect("import snapshot");
    tx.commit().await.expect("commit import");
}

async fn set(store: &Store, value: Value, expiry: Option<i64>) {
    let mut tx = store.transaction().await.expect("start write");
    tx.set("key", value, expiry).await.expect("set value");
    tx.commit().await.expect("commit value");
}

fn nested(a: bool, b: bool) -> Value {
    Value::Json(JsonValue::Object(
        [(
            "nested".into(),
            JsonValue::Object(
                [
                    ("a".into(), JsonValue::Bool(a)),
                    ("b".into(), JsonValue::Bool(b)),
                ]
                .into(),
            ),
        )]
        .into(),
    ))
}

#[test]
fn loaded_writers_merge_nested_fields_and_lists_in_both_orders() {
    block_on(async {
        for reverse in [false, true] {
            for (seed, left_value, right_value, merged) in [
                (
                    nested(false, false),
                    nested(true, false),
                    nested(false, true),
                    nested(true, true),
                ),
                (
                    Value::Json(JsonValue::Array(vec![
                        JsonValue::Bool(false),
                        JsonValue::Bool(false),
                    ])),
                    Value::Json(JsonValue::Array(vec![
                        JsonValue::Bool(true),
                        JsonValue::Bool(false),
                    ])),
                    Value::Json(JsonValue::Array(vec![
                        JsonValue::Bool(false),
                        JsonValue::Bool(true),
                    ])),
                    Value::Json(JsonValue::Array(vec![
                        JsonValue::Bool(true),
                        JsonValue::Bool(true),
                    ])),
                ),
            ] {
                let left = store();
                let right = store();
                set(&left, seed, Some(50)).await;
                import(&right, snapshot(&left).await).await;
                set(&left, left_value, Some(50)).await;
                set(&right, right_value, Some(50)).await;
                let left_branch = snapshot(&left).await;
                let right_branch = snapshot(&right).await;
                assert_ne!(left_branch.0.change_hash, right_branch.0.change_hash);
                let destination = store();
                for branch in if reverse {
                    [right_branch, left_branch]
                } else {
                    [left_branch, right_branch]
                } {
                    import(&destination, branch).await;
                }
                let tx = destination.transaction().await.expect("read merged value");
                assert_eq!(
                    tx.get("key", 49).await.expect("read before expiry"),
                    Some(merged)
                );
                assert_eq!(tx.get("key", 50).await.expect("read at expiry"), None);
                tx.rollback().await.expect("finish read");
            }
        }
    });
}

#[test]
fn delete_wins_concurrent_update_and_stale_imports_without_resurrection() {
    block_on(async {
        for reverse in [false, true] {
            let deleted = store();
            let updated = store();
            set(&deleted, nested(false, false), None).await;
            let seed = snapshot(&deleted).await;
            import(&updated, seed.clone()).await;
            let mut tx = deleted.transaction().await.expect("start delete");
            tx.delete("key").await.expect("delete generation");
            tx.commit().await.expect("commit delete");
            set(&updated, nested(true, false), Some(100)).await;
            let tombstone = snapshot(&deleted).await;
            let update = snapshot(&updated).await;
            let destination = store();
            for branch in if reverse {
                [update.clone(), tombstone.clone()]
            } else {
                [tombstone.clone(), update.clone()]
            } {
                import(&destination, branch).await;
            }
            for stale in [seed, update, tombstone.clone()] {
                import(&destination, stale).await;
                let tx = destination.transaction().await.expect("read tombstone");
                assert_eq!(tx.get("key", 0).await.expect("read deleted value"), None);
                assert!(tx.scan_all(0).await.expect("scan deleted value").is_empty());
                tx.rollback().await.expect("finish tombstone read");
            }
            set(&destination, Value::Text("new generation".into()), None).await;
            import(&destination, tombstone).await;
            let tx = destination
                .transaction()
                .await
                .expect("read new generation");
            assert_eq!(
                tx.get("key", 0).await.expect("read recreated value"),
                Some(Value::Text("new generation".into()))
            );
            tx.rollback().await.expect("finish recreated read");
        }
    });
}

#[test]
fn old_live_and_tombstone_documents_are_rejected_on_disk_and_import() {
    block_on(async {
        for deleted in [false, true] {
            let mut old = AutoCommit::new();
            old.put(ROOT, "tombstone", deleted)
                .expect("write old marker");
            if !deleted {
                old.put(ROOT, "value", vec![1_u8]).expect("write old bytes");
            }
            let id = encode_document_id("key", uuid::Uuid::now_v7());
            let key = DocumentChangeKey::new_snapshot(id, hash_heads(old.get_heads()));
            let payload = old.save();
            let backend = InMemoryBTree::new();
            let mut tx = backend.transaction().await.expect("start old fixture");
            tx.insert(key.encode_ordered(), payload.clone())
                .await
                .expect("persist old document");
            tx.commit().await.expect("commit old fixture");
            let persisted = KvStore::new(backend, || uuid::Timestamp::now(uuid::NoContext));
            let tx = persisted.transaction().await.expect("start old read");
            assert!(matches!(
                tx.get("key", 0).await,
                Err(BTreeError::InvalidDocument)
            ));
            assert!(matches!(
                tx.scan_all(0).await,
                Err(BTreeError::InvalidDocument)
            ));
            tx.rollback().await.expect("finish old read");
            let destination = store();
            let mut tx = destination.transaction().await.expect("start old import");
            assert!(matches!(
                tx.import_snapshot((key, payload)).await,
                Err(BTreeError::InvalidDocument)
            ));
            tx.rollback().await.expect("finish old import");
        }
    });
}
