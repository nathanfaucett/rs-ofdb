use btree::InMemoryBTree;
use futures::{StreamExt, executor::block_on, join};
use futures_channel::mpsc;
use kv::{KvInvalidation, KvStore};
use ofdb_kv_sync::{
    Config, Message, SyncRole, SyncTransport, decode_frame, synchronize,
    synchronize_selected_checked,
};
use value::{JsonValue, Value};

type Store = KvStore<Measured<InMemoryBTree<Vec<u8>, Vec<u8>>>>;
fn timestamp() -> uuid::Timestamp {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    uuid::Timestamp::from_unix_time(1_700_000_000 + n, 0, 0, 0)
}
fn measured_store() -> (Store, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    (
        KvStore::new(
            Measured {
                inner: InMemoryBTree::new(),
                reads: reads.clone(),
            },
            timestamp,
        ),
        reads,
    )
}
fn store() -> Store {
    measured_store().0
}
struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    sent: Vec<Message>,
    pretend_known: Vec<[u8; 32]>,
}
impl SyncTransport for Channel {
    type Error = ();
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        let mut message = decode_frame::<()>(&frame, Config::default()).expect("valid frame");
        if let Message::Inventory(items) = &mut message {
            for item in items {
                item.hashes.extend_from_slice(&self.pretend_known);
            }
        }
        let frame = ofdb_kv_sync::encode_frame(&message, Config::default()).expect("encode");
        self.sent.push(message);
        self.sender.unbounded_send(frame).map_err(|_| ())
    }
    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.receiver.next().await.ok_or(())
    }
}
fn channels() -> (Channel, Channel) {
    let (a, ar) = mpsc::unbounded();
    let (b, br) = mpsc::unbounded();
    (
        Channel {
            sender: a,
            receiver: br,
            sent: vec![],
            pretend_known: vec![],
        },
        Channel {
            sender: b,
            receiver: ar,
            sent: vec![],
            pretend_known: vec![],
        },
    )
}
async fn broad(a: &Store, b: &Store) {
    let (mut at, mut bt) = channels();
    let (a, b) = join!(
        synchronize(a, &mut at, SyncRole::Initiator, Config::default()),
        synchronize(b, &mut bt, SyncRole::Responder, Config::default())
    );
    a.expect("initiator");
    b.expect("responder");
}
async fn selected(a: &Store, b: &Store, keys: &[KvInvalidation]) -> Vec<Message> {
    let (mut at, mut bt) = channels();
    let (a, b) = join!(
        synchronize_selected_checked(
            a,
            keys,
            &mut at,
            SyncRole::Initiator,
            Config::default(),
            || core::future::ready(Ok(()))
        ),
        synchronize_selected_checked(
            b,
            &[],
            &mut bt,
            SyncRole::Responder,
            Config::default(),
            || core::future::ready(Ok(()))
        )
    );
    a.expect("initiator");
    b.expect("responder");
    at.sent.extend(bt.sent);
    at.sent
}
async fn set(store: &Store, key: &str, value: Value) {
    let mut tx = store.transaction().await.expect("write");
    tx.set(key, value, None).await.expect("set");
    tx.commit().await.expect("commit");
}
async fn key_work(store: &Store, key: &str) -> Vec<KvInvalidation> {
    let tx = store.transaction().await.expect("read");
    let history = tx.history(key).await.expect("history").expect("key");
    tx.rollback().await.expect("rollback");
    let (key, generation) = kv::decode_document_id(&history.id).expect("identity");
    vec![KvInvalidation::Generation { key, generation }]
}
fn map(a: bool, b: bool) -> Value {
    Value::Json(JsonValue::Object(
        [
            ("a".into(), JsonValue::Bool(a)),
            ("b".into(), JsonValue::Bool(b)),
        ]
        .into(),
    ))
}

#[test]
fn selected_native_deltas_merge_nested_edits_and_replay_without_snapshots() {
    block_on(async {
        let (a, a_reads) = measured_store();
        let (b, b_reads) = measured_store();
        for n in 0..100 {
            set(&a, &format!("untouched-{n}"), Value::Integer(n)).await;
        }
        set(&a, "key", map(false, false)).await;
        broad(&a, &b).await;
        set(&a, "key", map(true, false)).await;
        set(&b, "key", map(false, true)).await;
        let work = key_work(&a, "key").await;
        a_reads.store(0, std::sync::atomic::Ordering::SeqCst);
        b_reads.store(0, std::sync::atomic::Ordering::SeqCst);
        let messages = selected(&a, &b, &work).await;
        assert!(
            a_reads.load(std::sync::atomic::Ordering::SeqCst) < 100,
            "no whole-store read/export on source"
        );
        assert!(
            b_reads.load(std::sync::atomic::Ordering::SeqCst) < 100,
            "no whole-store read/export on target"
        );
        assert!(
            messages
                .iter()
                .any(|message| matches!(message, Message::Changes(_)))
        );
        assert!(
            !messages
                .iter()
                .any(|message| matches!(message, Message::Snapshots(_)))
        );
        let advertised = messages
            .iter()
            .filter_map(|message| {
                if let Message::Inventory(items) = message {
                    Some(items.len())
                } else {
                    None
                }
            })
            .sum::<usize>();
        assert_eq!(
            advertised, 4,
            "only one key inspected in each directional comparison"
        );
        for store in [&a, &b] {
            let tx = store.transaction().await.expect("read");
            assert_eq!(
                tx.get("key", 0).await.expect("merged"),
                Some(map(true, true))
            );
            tx.rollback().await.expect("rollback");
        }
        let replay = selected(&a, &b, &work).await;
        assert!(
            !replay
                .iter()
                .any(|message| matches!(message, Message::Changes(_) | Message::Snapshots(_)))
        );
    });
}

#[test]
fn byte_limited_batches_make_progress() {
    block_on(async {
        let a = store();
        let b = store();
        for n in 0..16 {
            set(&a, &format!("key-{n}"), Value::Blob(vec![n; 600])).await;
        }
        let config = Config {
            max_frame_bytes: 1024,
            ..Config::default()
        };
        let (mut at, mut bt) = channels();
        futures::try_join!(
            synchronize(&a, &mut at, SyncRole::Initiator, config),
            synchronize(&b, &mut bt, SyncRole::Responder, config)
        )
        .expect("individually valid documents must fit separate frames");
        assert!(
            at.sent
                .iter()
                .filter(|m| matches!(m, Message::Snapshots(_)))
                .count()
                > 1
        );
        assert!(
            at.sent
                .iter()
                .filter(|m| matches!(m, Message::Inventory(_)))
                .count()
                > 1
        );
        for n in 0..16 {
            set(&a, &format!("key-{n}"), Value::Blob(vec![n + 1; 600])).await;
        }
        let (mut at, mut bt) = channels();
        futures::try_join!(
            synchronize(&a, &mut at, SyncRole::Initiator, config),
            synchronize(&b, &mut bt, SyncRole::Responder, config)
        )
        .expect("native deltas must also fit separate frames");
        assert!(
            at.sent
                .iter()
                .filter(|m| matches!(m, Message::Changes(_)))
                .count()
                > 1
        );
        assert!(!at.sent.iter().any(|m| matches!(m, Message::Snapshots(_))));
        let tx = b.transaction().await.expect("read");
        for n in 0..16 {
            assert_eq!(
                tx.get(&format!("key-{n}"), 0).await.expect("value"),
                Some(Value::Blob(vec![n + 1; 600]))
            );
        }
        tx.rollback().await.expect("rollback");
    });
}

#[test]
fn byte_limited_recovery_makes_progress() {
    fn blob(seed: u8) -> Value {
        let mut state = u32::from(seed) + 1;
        Value::Blob(
            (0..600)
                .map(|_| {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    (state >> 24) as u8
                })
                .collect(),
        )
    }
    block_on(async {
        let a = store();
        let b = store();
        for n in 0..12 {
            set(&a, &format!("key-{n}"), blob(n)).await;
        }
        broad(&a, &b).await;
        let mut parents = Vec::new();
        for n in 0..12 {
            let key = format!("key-{n}");
            set(&a, &key, blob(n + 1)).await;
            let tx = a.transaction().await.expect("read");
            parents.extend(tx.history(&key).await.expect("history").expect("key").heads);
            tx.rollback().await.expect("rollback");
            let mut tx = a.transaction().await.expect("expiry edit");
            tx.set(&key, blob(n + 1), Some(100)).await.expect("set");
            tx.commit().await.expect("commit");
        }
        let config = Config {
            max_frame_bytes: 8192,
            ..Config::default()
        };
        let (mut at, mut bt) = channels();
        bt.pretend_known = parents;
        let result = futures::try_join!(
            synchronize(&a, &mut at, SyncRole::Initiator, config),
            synchronize(&b, &mut bt, SyncRole::Responder, config)
        );
        assert!(bt.sent.contains(&Message::Recover));
        result.expect("recovery snapshots must fit separate frames");
        assert!(
            at.sent
                .iter()
                .filter(|m| matches!(m, Message::Snapshots(_)))
                .count()
                > 1
        );
        let tx = b.transaction().await.expect("read");
        for n in 0..12 {
            assert_eq!(
                tx.get(&format!("key-{n}"), 0).await.expect("value"),
                Some(blob(n + 1))
            );
            assert_eq!(
                tx.get(&format!("key-{n}"), 100).await.expect("expiry"),
                None
            );
        }
        tx.rollback().await.expect("rollback");
    });
}

#[test]
fn unchanged_sync_does_not_read_documents_again_for_transfer() {
    block_on(async {
        let (a, reads) = measured_store();
        let b = store();
        set(&a, "key", Value::Integer(1)).await;
        broad(&a, &b).await;
        reads.store(0, std::sync::atomic::Ordering::SeqCst);
        let tx = a.transaction().await.expect("read");
        tx.histories().await.expect("inventory");
        tx.rollback().await.expect("rollback");
        let inventory_reads = reads.swap(0, std::sync::atomic::Ordering::SeqCst);
        broad(&a, &b).await;
        assert!(
            reads.load(std::sync::atomic::Ordering::SeqCst) <= 2 * inventory_reads,
            "an unchanged sync needs only the two directional inventory reads"
        );
    });
}

#[test]
fn large_valid_inventory_syncs_values_across_many_frames() {
    block_on(async {
        let a = store();
        let b = store();
        for n in 0..257 {
            set(&a, &format!("key-{n}"), Value::Integer(n)).await;
        }
        let config = Config {
            max_snapshots_per_batch: 16,
            max_inventory_entries: 257,
            ..Config::default()
        };
        let (mut at, mut bt) = channels();
        let (left, right) = join!(
            synchronize(&a, &mut at, SyncRole::Initiator, config),
            synchronize(&b, &mut bt, SyncRole::Responder, config)
        );
        left.expect("source");
        right.expect("receiver");
        assert!(
            at.sent
                .iter()
                .filter(|message| matches!(message, Message::Inventory(_)))
                .count()
                > 16
        );
        let tx = b.transaction().await.expect("received values");
        for n in 0..257 {
            assert_eq!(
                tx.get(&format!("key-{n}"), 0).await.expect("value"),
                Some(Value::Integer(n))
            );
        }
        tx.rollback().await.expect("rollback");
    });
}

#[test]
fn dependency_recovery_merges_snapshots_without_discarding_concurrent_work() {
    block_on(async {
        let a = store();
        let b = store();
        set(&a, "key", map(false, false)).await;
        broad(&a, &b).await;
        set(&a, "key", map(true, false)).await;
        let tx = a.transaction().await.expect("read parent");
        let parent = tx
            .history("key")
            .await
            .expect("history")
            .expect("key")
            .heads;
        tx.rollback().await.expect("rollback");
        let mut tx = a.transaction().await.expect("child");
        tx.set("key", map(true, false), Some(100))
            .await
            .expect("expiry edit");
        tx.commit().await.expect("commit");
        set(&b, "key", map(false, true)).await;
        let work = key_work(&a, "key").await;
        let (mut at, mut bt) = channels();
        // A stale receiver inventory claims a parent it no longer has. The child
        // is valid but cannot apply until snapshot recovery supplies that parent.
        bt.pretend_known = parent;
        let (ar, br) = join!(
            synchronize_selected_checked(
                &a,
                &work,
                &mut at,
                SyncRole::Initiator,
                Config::default(),
                || core::future::ready(Ok(()))
            ),
            synchronize_selected_checked(
                &b,
                &[],
                &mut bt,
                SyncRole::Responder,
                Config::default(),
                || core::future::ready(Ok(()))
            )
        );
        ar.expect("initiator recovery");
        br.expect("responder recovery");
        assert!(bt.sent.contains(&Message::Recover));
        assert!(
            at.sent
                .iter()
                .any(|message| matches!(message, Message::Snapshots(_)))
        );
        for store in [&a, &b] {
            let tx = store.transaction().await.expect("read");
            assert_eq!(
                tx.get("key", 0).await.expect("concurrent history"),
                Some(map(true, true))
            );
            assert_eq!(tx.get("key", 100).await.expect("expiry preserved"), None);
            tx.rollback().await.expect("rollback");
        }
    });
}

// Counts actual physical records read through the public backend seam, not timings.
struct Measured<T> {
    inner: T,
    reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl<T: btree::BTreeRead<Vec<u8>, Vec<u8>>> btree::BTreeRead<Vec<u8>, Vec<u8>> for Measured<T> {
    async fn get(&self, key: &Vec<u8>) -> btree::BTreeResult<Option<Vec<u8>>> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get(key).await
    }
    fn range<R>(
        &self,
        range: R,
    ) -> impl futures::Stream<Item = btree::BTreeResult<(Vec<u8>, Vec<u8>)>> + Send
    where
        R: std::ops::RangeBounds<Vec<u8>> + Clone + Send,
    {
        self.inner.range(range).map(|item| {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            item
        })
    }
}
impl<T: btree::BTree<Vec<u8>, Vec<u8>>> btree::BTree<Vec<u8>, Vec<u8>> for Measured<T> {
    type Transaction = Measured<T::Transaction>;
    async fn transaction(&self) -> btree::BTreeResult<Self::Transaction> {
        Ok(Measured {
            inner: self.inner.transaction().await?,
            reads: self.reads.clone(),
        })
    }
}
impl<T: btree::BTreeTransaction<Vec<u8>, Vec<u8>>> btree::BTreeTransaction<Vec<u8>, Vec<u8>>
    for Measured<T>
{
    async fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> btree::BTreeResult<()> {
        self.inner.insert(key, value).await
    }
    async fn update<F>(&mut self, key: Vec<u8>, update: F) -> btree::BTreeResult<Option<()>>
    where
        F: FnOnce(&mut Vec<u8>) -> btree::BTreeResult<()> + Send,
    {
        self.inner.update(key, update).await
    }
    async fn remove(&mut self, key: &Vec<u8>) -> btree::BTreeResult<Option<Vec<u8>>> {
        self.inner.remove(key).await
    }
    fn remove_range<R>(
        &mut self,
        range: R,
    ) -> impl futures::Stream<Item = btree::BTreeResult<(Vec<u8>, Vec<u8>)>> + Send
    where
        R: std::ops::RangeBounds<Vec<u8>> + Clone + Send,
    {
        self.inner.remove_range(range)
    }
    async fn commit(self) -> btree::BTreeResult<()> {
        self.inner.commit().await
    }
    async fn rollback(self) -> btree::BTreeResult<()> {
        self.inner.rollback().await
    }
}

#[test]
fn split_recovery_failure_does_not_commit_the_first_frame() {
    block_on(async {
        for failure in ["disconnect", "invalid", "denied", "budget"] {
            let source = store();
            let target = store();
            let mut changes = Vec::new();
            let mut snapshots = Vec::new();
            for key in ["a", "b"] {
                set(&source, key, Value::Integer(1)).await;
                set(&source, key, Value::Integer(2)).await;
                let tx = source.transaction().await.expect("source read");
                let history = tx.history(key).await.expect("history").expect("key");
                let native = tx.export_changes(&history.id, &[]).await.expect("changes");
                let child = native.last().expect("child");
                changes.push(ofdb_kv_sync::KvChanges {
                    id: history.id,
                    changes: vec![ofdb_kv_sync::KvChange {
                        hash: child.hash,
                        deps: child.deps.clone(),
                        payload: child.payload.clone(),
                    }],
                });
                let (key, payload) = tx
                    .export_snapshot(key)
                    .await
                    .expect("snapshot")
                    .expect("key");
                snapshots.push(ofdb_kv_sync::KvSnapshot { key, payload });
                tx.rollback().await.expect("rollback");
            }
            let first = Message::Snapshots(vec![snapshots.remove(0)]);
            let config = Config {
                max_inventory_bytes: if failure == "budget" {
                    ofdb_kv_sync::encode_frame(&first, Config::default())
                        .expect("first frame")
                        .len()
                } else {
                    Config::default().max_inventory_bytes
                },
                ..Config::default()
            };
            let (mut at, mut bt) = channels();
            for message in [
                Message::Hello { version: 3 },
                Message::Selection(None),
                Message::End,
                Message::Changes(changes),
                first,
            ] {
                at.send(ofdb_kv_sync::encode_frame(&message, config).expect("frame"))
                    .await
                    .expect("queue");
            }
            if failure != "disconnect" {
                if failure == "invalid" {
                    snapshots[0].payload = vec![0];
                }
                at.send(
                    ofdb_kv_sync::encode_frame(&Message::Snapshots(snapshots), config)
                        .expect("frame"),
                )
                .await
                .expect("queue");
            }
            at.sender.close_channel();
            let mut checks = 0;
            let result = ofdb_kv_sync::synchronize_checked(
                &target,
                &mut bt,
                SyncRole::Responder,
                config,
                || {
                    checks += 1;
                    core::future::ready(if failure == "denied" && checks == 3 {
                        Err("denied".into())
                    } else {
                        Ok(())
                    })
                },
            )
            .await;
            match failure {
                "disconnect" => assert!(matches!(result, Err(ofdb_kv_sync::Error::Transport(())))),
                "invalid" => assert!(matches!(result, Err(ofdb_kv_sync::Error::Storage(_)))),
                "denied" => assert!(matches!(result, Err(ofdb_kv_sync::Error::PreApply(_)))),
                "budget" => assert_eq!(result, Err(ofdb_kv_sync::Error::FrameTooLarge)),
                _ => unreachable!("known failure"),
            }
            let tx = target.transaction().await.expect("target read");
            for key in ["a", "b"] {
                assert_eq!(
                    tx.history(key).await.expect("history"),
                    None,
                    "{failure} must leave {key} unchanged"
                );
            }
            tx.rollback().await.expect("rollback");
        }
    });
}

#[test]
fn incomplete_snapshot_recovery_is_rejected_without_committing_partial_history() {
    block_on(async {
        let source = store();
        let target = store();
        set(&source, "key", Value::Integer(1)).await;
        set(&source, "key", Value::Integer(2)).await;
        let tx = source.transaction().await.expect("read");
        let history = tx.history("key").await.expect("history").expect("key");
        let changes = tx.export_changes(&history.id, &[]).await.expect("changes");
        tx.rollback().await.expect("rollback");
        let child = changes.last().expect("child");
        let (mut at, mut bt) = channels();
        for message in [
            Message::Hello { version: 3 },
            Message::Selection(None),
            Message::End,
            Message::Changes(vec![ofdb_kv_sync::KvChanges {
                id: history.id,
                changes: vec![ofdb_kv_sync::KvChange {
                    hash: child.hash,
                    deps: child.deps.clone(),
                    payload: child.payload.clone(),
                }],
            }]),
            Message::Snapshots(vec![]),
            Message::End,
            Message::Abort,
        ] {
            at.send(ofdb_kv_sync::encode_frame(&message, Config::default()).expect("frame"))
                .await
                .expect("queue");
        }
        let result = synchronize(&target, &mut bt, SyncRole::Responder, Config::default()).await;
        assert!(
            matches!(result, Err(ofdb_kv_sync::Error::Storage(_))),
            "incomplete recovery must not report success: {result:?}"
        );
        let tx = target.transaction().await.expect("read");
        assert_eq!(tx.get("key", 0).await.expect("no incomplete state"), None);
        tx.rollback().await.expect("rollback");
    });
}

#[test]
fn malformed_native_change_rolls_back_the_complete_multi_key_batch() {
    block_on(async {
        for forge_deps in [false, true] {
            let source = store();
            let target = store();
            set(&source, "a", Value::Integer(1)).await;
            set(&source, "b", Value::Integer(2)).await;
            let tx = source.transaction().await.expect("read source");
            let mut items = Vec::new();
            for key in ["a", "b"] {
                let history = tx.history(key).await.expect("history").expect("key");
                let changes = tx
                    .export_changes(&history.id, &[])
                    .await
                    .expect("changes")
                    .into_iter()
                    .map(|change| ofdb_kv_sync::KvChange {
                        hash: change.hash,
                        deps: change.deps,
                        payload: change.payload,
                    })
                    .collect();
                items.push(ofdb_kv_sync::KvChanges {
                    id: history.id,
                    changes,
                });
            }
            tx.rollback().await.expect("rollback");
            let forged = items[1].changes.last_mut().expect("change");
            if forge_deps {
                forged.deps.clear();
            } else {
                forged.hash = [42; 32];
            }
            let (mut at, mut bt) = channels();
            for message in [
                Message::Hello { version: 3 },
                Message::Selection(None),
                Message::End,
                Message::Changes(items),
            ] {
                at.send(ofdb_kv_sync::encode_frame(&message, Config::default()).expect("frame"))
                    .await
                    .expect("queue");
            }
            let result =
                synchronize(&target, &mut bt, SyncRole::Responder, Config::default()).await;
            assert!(matches!(result, Err(ofdb_kv_sync::Error::Storage(_))));
            let tx = target.transaction().await.expect("read target");
            assert!(tx.scan_all(0).await.expect("batch rollback").is_empty());
            tx.rollback().await.expect("rollback");
        }
    });
}

#[test]
fn selected_delete_wins_concurrent_update_and_recreation_selects_latest_generation() {
    block_on(async {
        let a = store();
        let b = store();
        set(&a, "key", map(false, false)).await;
        broad(&a, &b).await;
        let stale_work = key_work(&a, "key").await;
        let mut tx = a.transaction().await.expect("delete");
        tx.delete("key").await.expect("tombstone");
        tx.commit().await.expect("commit");
        set(&b, "key", map(false, true)).await;
        let frames = selected(&a, &b, &stale_work).await;
        assert!(
            !frames
                .iter()
                .any(|frame| matches!(frame, Message::Snapshots(_)))
        );
        let tx = b.transaction().await.expect("read");
        assert_eq!(tx.get("key", 0).await.expect("tombstone wins"), None);
        tx.rollback().await.expect("rollback");
        set(&a, "key", Value::Integer(3)).await;
        let frames = selected(&a, &b, &stale_work).await;
        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, Message::Snapshots(_))),
            "new generation bootstrap"
        );
        let tx = b.transaction().await.expect("read");
        assert_eq!(
            tx.get("key", 0)
                .await
                .expect("latest selected by key, not old generation"),
            Some(Value::Integer(3))
        );
        tx.rollback().await.expect("rollback");
    });
}
