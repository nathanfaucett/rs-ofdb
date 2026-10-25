use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
    time::Duration,
};

use futures::{
    FutureExt, StreamExt,
    channel::mpsc,
    future::{Either, join, select},
};
use kv_sync::{Message, decode_frame, encode_frame};
use ofdb_kv::{
    ChangeSubscription, Database, KvInvalidation, KvSnapshot, SyncConfig, SyncError, SyncRole,
    SyncTransport, Value,
};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn committed_work_tracks_shared_clients_and_exact_generations() {
    let database = Database::open_indexeddb(&format!("sync-{}", uuid::Uuid::now_v7()))
        .await
        .expect("open");
    let client = database.client();
    client
        .set("seed", Value::Integer(0), None)
        .await
        .expect("create table");
    let mut changes = database.sync_changes().expect("browser subscription");
    let initial = changes.select().expect("initial broad work");
    assert_eq!(
        database.sync_invalidations(&initial).await.expect("map"),
        vec![KvInvalidation::All]
    );
    assert!(changes.acknowledge(initial));
    let mut other = database.sync_changes().expect("independent subscription");
    let other_initial = other.select().expect("independent initial work");
    assert!(other.acknowledge(other_initial));
    client
        .clone()
        .set("a", Value::Integer(1), None)
        .await
        .expect("commit");
    let work = changes.select().expect("committed work");
    assert!(
        matches!(database.sync_invalidations(&work).await.expect("map").as_slice(),
        [KvInvalidation::Generation { key, .. }] if key == "a")
    );
    client
        .set("b", Value::Integer(2), None)
        .await
        .expect("later commit");
    assert!(changes.acknowledge(work));
    assert!(changes.select().is_some(), "later commit retained");
    let independent = other.select().expect("other peer retains both");
    assert_eq!(
        database
            .sync_invalidations(&independent)
            .await
            .expect("map")
            .len(),
        2
    );
    assert!(other.acknowledge(independent));
    while other.changed().now_or_never().is_some() {}
    client
        .set("a", Value::Integer(1), None)
        .await
        .expect("identical set");
    client.delete("missing").await.expect("missing delete");
    client
        .transaction()
        .await
        .expect("read only")
        .commit()
        .await
        .expect("commit");
    assert!(other.select().is_none());
    assert!(other.changed().now_or_never().is_none());
}

struct Transcript {
    incoming: VecDeque<Vec<u8>>,
    sent: Vec<Vec<u8>>,
}
impl SyncTransport for Transcript {
    type Error = ();
    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.incoming.pop_front().ok_or(())
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        self.sent.push(frame);
        Ok(())
    }
}

async fn database() -> Database {
    Database::open_indexeddb(&format!("sync-{}", uuid::Uuid::now_v7()))
        .await
        .expect("open persistent replica")
}
fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("kv_native_fixture.json")).expect("native fixture")
}
fn transcript(fixture: &serde_json::Value) -> Transcript {
    Transcript {
        incoming: serde_json::from_value(fixture["initiator"].clone()).expect("native frames"),
        sent: vec![],
    }
}
fn acknowledge(changes: &mut ChangeSubscription) {
    if let Some(work) = changes.select() {
        assert!(changes.acknowledge(work));
    }
}

#[wasm_bindgen_test]
async fn native_wire_fixture_preserves_history_types_and_exact_responses() {
    let fixture = fixture();
    let target = database().await;
    let mut changes = target.sync_changes().expect("subscribe");
    acknowledge(&mut changes);
    let mut transport = transcript(&fixture);
    target
        .synchronize_checked(
            &mut transport,
            SyncRole::Responder,
            SyncConfig::default(),
            || async { Ok(()) },
        )
        .await
        .expect("native protocol exchange");
    assert!(transport.incoming.is_empty());
    let replies: Vec<Vec<u8>> =
        serde_json::from_value(fixture["responder"].clone()).expect("native responses");
    assert_eq!(
        transport.sent, replies,
        "wire protocol matches native byte for byte"
    );
    let snapshots: Vec<KvSnapshot> =
        serde_json::from_value(fixture["snapshots"].clone()).expect("native snapshots");
    assert_eq!(
        target.export_snapshots().await.expect("native history"),
        snapshots
    );
    let client = target.client();
    assert_eq!(
        client.get("integer").await.expect("integer"),
        Some(Value::Integer(i64::MIN))
    );
    assert_eq!(
        client.get("blob").await.expect("blob"),
        Some(Value::Blob(vec![0, 255, 0]))
    );
    assert_eq!(
        client.get("replacement").await.expect("latest generation"),
        Some(Value::Integer(2))
    );
    assert_eq!(client.get("expired").await.expect("expiry"), None);
    assert_eq!(client.get("deleted").await.expect("tombstone"), None);
    assert!(changes.select().is_some(), "incoming commit publishes work");
    acknowledge(&mut changes);
    target
        .import_snapshots(snapshots)
        .await
        .expect("known snapshot replay");
    assert!(changes.select().is_none(), "known replay stays silent");
    let mut delta = Transcript {
        incoming: serde_json::from_value(fixture["delta_initiator"].clone())
            .expect("native deltas"),
        sent: vec![],
    };
    target
        .synchronize_checked(
            &mut delta,
            SyncRole::Responder,
            SyncConfig::default(),
            || async { Ok(()) },
        )
        .await
        .expect("native incremental history exchange");
    let replies: Vec<Vec<u8>> =
        serde_json::from_value(fixture["delta_responder"].clone()).expect("native delta responses");
    assert_eq!(delta.sent, replies);
    let expected: Vec<KvSnapshot> = serde_json::from_value(fixture["delta_snapshots"].clone())
        .expect("native incremental history");
    assert_eq!(
        target
            .export_snapshots()
            .await
            .expect("identical incremental history"),
        expected
    );
    assert_eq!(
        client.get("integer").await.expect("native update"),
        Some(Value::Integer(i64::MAX))
    );
    assert!(
        changes.select().is_some(),
        "native delta commit publishes work"
    );
    acknowledge(&mut changes);
    let mut replay = Transcript {
        incoming: serde_json::from_value(fixture["delta_initiator"].clone())
            .expect("known native deltas"),
        sent: vec![],
    };
    target
        .synchronize_checked(
            &mut replay,
            SyncRole::Responder,
            SyncConfig::default(),
            || async { Ok(()) },
        )
        .await
        .expect("known native change replay");
    assert!(
        changes.select().is_none(),
        "known native changes remain silent"
    );
}

struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    sent: Rc<RefCell<Vec<Message>>>,
    missing_dependency: bool,
    corrupt_change: bool,
}
impl SyncTransport for Channel {
    type Error = ();
    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.receiver.next().await.ok_or(())
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        let mut message: Message =
            decode_frame::<()>(&frame, SyncConfig::default()).expect("current protocol frame");
        if let Message::Changes(items) = &mut message {
            if self.missing_dependency {
                assert!(
                    items[0].changes.len() >= 2,
                    "fixture has dependent native changes"
                );
                items[0].changes.remove(0);
                self.missing_dependency = false;
            }
            if self.corrupt_change {
                items[0].changes[0].hash = [0; 32];
                self.corrupt_change = false;
            }
        }
        self.sent.borrow_mut().push(message.clone());
        let frame = encode_frame(&message, SyncConfig::default()).expect("encode frame");
        self.sender.unbounded_send(frame).map_err(|_| ())
    }
}
fn channels() -> (Channel, Channel) {
    let (a, ar) = mpsc::unbounded();
    let (b, br) = mpsc::unbounded();
    (
        Channel {
            sender: a,
            receiver: br,
            sent: Rc::default(),
            missing_dependency: false,
            corrupt_change: false,
        },
        Channel {
            sender: b,
            receiver: ar,
            sent: Rc::default(),
            missing_dependency: false,
            corrupt_change: false,
        },
    )
}
pub(super) async fn exchange(left: &Database, right: &Database) -> (Vec<Message>, Vec<Message>) {
    let (mut a, mut b) = channels();
    let (ar, br) = platform::timeout(
        Duration::from_secs(10),
        join(
            left.synchronize_checked(
                &mut a,
                SyncRole::Initiator,
                SyncConfig::default(),
                || async { Ok(()) },
            ),
            right.synchronize_checked(
                &mut b,
                SyncRole::Responder,
                SyncConfig::default(),
                || async { Ok(()) },
            ),
        ),
    )
    .await
    .expect("bounded exchange");
    ar.expect("initiator");
    br.expect("responder");
    (a.sent.take(), b.sent.take())
}
async fn selected(
    left: &Database,
    lc: &mut ChangeSubscription,
    right: &Database,
    rc: &mut ChangeSubscription,
) -> (Vec<Message>, Vec<Message>) {
    let (mut a, mut b) = channels();
    let (ar, br) = platform::timeout(
        Duration::from_secs(10),
        join(
            left.synchronize_selected_checked(
                lc,
                &mut a,
                SyncRole::Initiator,
                SyncConfig::default(),
                || async { Ok(()) },
            ),
            right.synchronize_selected_checked(
                rc,
                &mut b,
                SyncRole::Responder,
                SyncConfig::default(),
                || async { Ok(()) },
            ),
        ),
    )
    .await
    .expect("bounded selected exchange");
    ar.expect("selected initiator");
    br.expect("selected responder");
    (a.sent.take(), b.sent.take())
}

#[wasm_bindgen_test]
async fn selected_offline_nested_merges_generation_replacement_tombstones_and_replay() {
    let left = database().await;
    let right = database().await;
    let mut lc = left.sync_changes().expect("left peer");
    let mut rc = right.sync_changes().expect("right peer");
    left.client()
        .set(
            "nested",
            Value::from(serde_json::json!({"profile":{"name":"seed","count":1},"tags":[1,2]})),
            None,
        )
        .await
        .expect("seed nested value");
    left.client()
        .set("deleted", Value::Integer(1), None)
        .await
        .expect("seed deleted value");
    let (a, b) = selected(&left, &mut lc, &right, &mut rc).await;
    assert!(
        a.contains(&Message::Selection(None)) && b.contains(&Message::Selection(None)),
        "fresh subscriptions compare broadly"
    );
    let stale = left.export_snapshots().await.expect("stale state");
    acknowledge(&mut lc);
    acknowledge(&mut rc);
    left.client()
        .set(
            "nested",
            Value::from(serde_json::json!({"profile":{"name":"left","count":1},"tags":[1,2]})),
            None,
        )
        .await
        .expect("offline name");
    right
        .client()
        .set(
            "nested",
            Value::from(serde_json::json!({"profile":{"name":"seed","count":2},"tags":[1,2]})),
            None,
        )
        .await
        .expect("offline count");
    left.client()
        .delete("deleted")
        .await
        .expect("offline tombstone");
    right
        .client()
        .set("deleted", Value::Integer(2), None)
        .await
        .expect("offline stale update");
    let (a, b) = selected(&left, &mut lc, &right, &mut rc).await;
    assert!(a.contains(&Message::Selection(Some(vec![
        "deleted".into(),
        "nested".into()
    ]))));
    assert!(
        a.iter().chain(&b).any(|m| matches!(m, Message::Changes(_))),
        "existing generations exchange native changes"
    );
    let expected = Some(Value::from(
        serde_json::json!({"profile":{"name":"left","count":2},"tags":[1,2]}),
    ));
    for db in [&left, &right] {
        assert_eq!(
            db.client()
                .get("nested")
                .await
                .expect("merged nested value"),
            expected
        );
        assert_eq!(
            db.client().get("deleted").await.expect("tombstone wins"),
            None
        );
    }
    acknowledge(&mut lc);
    acknowledge(&mut rc);
    left.import_snapshots(stale)
        .await
        .expect("merge stale import");
    assert_eq!(
        left.client().get("deleted").await.expect("no revival"),
        None
    );
    assert!(lc.select().is_none(), "known stale history is silent");
    right
        .client()
        .set("deleted", Value::Integer(3), None)
        .await
        .expect("replacement generation");
    selected(&left, &mut lc, &right, &mut rc).await;
    assert_eq!(
        left.client()
            .get("deleted")
            .await
            .expect("latest generation"),
        Some(Value::Integer(3))
    );
    selected(&left, &mut lc, &right, &mut rc).await;
    let (a, b) = selected(&left, &mut lc, &right, &mut rc).await;
    assert!(
        a.iter()
            .chain(&b)
            .all(|m| !matches!(m, Message::Snapshots(_) | Message::Changes(_))),
        "known replay needs no payload and produces no echo"
    );
    assert!(lc.select().is_none() && rc.select().is_none());
    assert_eq!(
        left.export_snapshots()
            .await
            .expect("left history")
            .into_iter()
            .map(|s| s.key)
            .collect::<Vec<_>>(),
        right
            .export_snapshots()
            .await
            .expect("right history")
            .into_iter()
            .map(|s| s.key)
            .collect::<Vec<_>>(),
        "generation identities and native history heads converge; snapshot serialization order is not canonical"
    );
}

#[wasm_bindgen_test]
async fn checked_batches_deny_or_cancel_without_partial_apply_and_retry_converges() {
    for target_role in [SyncRole::Initiator, SyncRole::Responder] {
        for cancel in [false, true] {
            let source = database().await;
            let target = database().await;
            for (key, value) in [("a", 1), ("b", 2)] {
                source
                    .client()
                    .set(key, Value::Integer(value), None)
                    .await
                    .expect("source");
            }
            let mut changes = target.sync_changes().expect("subscribe");
            acknowledge(&mut changes);
            let config = SyncConfig {
                max_snapshots_per_batch: 1,
                ..SyncConfig::default()
            };
            let (mut st, mut tt) = channels();
            let source_role = if target_role == SyncRole::Initiator {
                SyncRole::Responder
            } else {
                SyncRole::Initiator
            };
            let (reached, waiting) = futures::channel::oneshot::channel();
            let reached = Rc::new(RefCell::new(Some(reached)));
            let calls = Rc::new(Cell::new(0));
            let check = || {
                calls.set(calls.get() + 1);
                let stop = calls.get() == 2;
                if stop {
                    reached
                        .borrow_mut()
                        .take()
                        .expect("signal once")
                        .send(())
                        .expect("waiter");
                }
                async move {
                    if stop && cancel {
                        core::future::pending().await
                    } else if stop {
                        Err("caller authorization revoked".into())
                    } else {
                        Ok(())
                    }
                }
            };
            let sessions = Box::pin(join(
                source.synchronize_checked(&mut st, source_role, config, || async { Ok(()) }),
                target.synchronize_checked(&mut tt, target_role, config, check),
            ));
            if cancel {
                match platform::timeout(Duration::from_secs(10), select(sessions, waiting))
                    .await
                    .expect("reach second check")
                {
                    Either::Right((result, pending)) => {
                        result.expect("reached");
                        drop(pending);
                    }
                    Either::Left(_) => panic!("pending authorization must not finish"),
                }
            } else {
                let (_, result) = platform::timeout(Duration::from_secs(10), sessions)
                    .await
                    .expect("denial terminates");
                assert!(
                    matches!(result, Err(SyncError::PreApply(detail)) if detail == "caller authorization revoked")
                );
            }
            assert_eq!(calls.get(), 2);
            assert_eq!(
                target
                    .client()
                    .scan_all()
                    .await
                    .expect("earlier valid progress"),
                vec![("a".into(), Value::Integer(1))]
            );
            assert!(
                changes.select().is_some(),
                "only the earlier batch committed"
            );
            acknowledge(&mut changes);
            exchange(&source, &target).await;
            assert_eq!(
                target
                    .client()
                    .scan_all()
                    .await
                    .expect("fresh session converges"),
                vec![
                    ("a".into(), Value::Integer(1)),
                    ("b".into(), Value::Integer(2))
                ]
            );
        }
    }
}

#[wasm_bindgen_test]
async fn malformed_or_denied_native_batch_persists_nothing_and_retains_selected_work() {
    for malformed in [false, true] {
        let target = database().await;
        let mut changes = target.sync_changes().expect("subscribe");
        let fixture = fixture();
        let mut transport = transcript(&fixture);
        if malformed {
            for frame in &mut transport.incoming {
                let mut message =
                    decode_frame::<()>(frame, SyncConfig::default()).expect("native frame");
                if let Message::Snapshots(items) = &mut message {
                    assert!(items.len() > 1, "atomic multi-document fixture");
                    items[1].payload = vec![255];
                    *frame = encode_frame(&message, SyncConfig::default())
                        .expect("malformed payload in valid frame");
                }
            }
        }
        let result = target
            .synchronize_selected_checked(
                &mut changes,
                &mut transport,
                SyncRole::Responder,
                SyncConfig::default(),
                || async {
                    if malformed {
                        Ok(())
                    } else {
                        Err("denied".into())
                    }
                },
            )
            .await;
        if malformed {
            assert!(matches!(result, Err(SyncError::Storage(_))));
        } else {
            assert!(matches!(result, Err(SyncError::PreApply(_))));
        }
        assert!(
            target
                .client()
                .scan_all()
                .await
                .expect("no partial visible writes")
                .is_empty()
        );
        assert!(
            target
                .export_snapshots()
                .await
                .expect("no partial history")
                .is_empty()
        );
        let work = changes
            .select()
            .expect("failed pass cannot acknowledge initial work");
        assert_eq!(
            target.sync_invalidations(&work).await.expect("broad retry"),
            vec![KvInvalidation::All]
        );
        acknowledge(&mut changes);
        let mut retry = transcript(&fixture);
        target
            .synchronize_checked(
                &mut retry,
                SyncRole::Responder,
                SyncConfig::default(),
                || async { Ok(()) },
            )
            .await
            .expect("fresh session");
        assert!(changes.select().is_some(), "successful retry publishes");
    }
}

#[wasm_bindgen_test]
async fn missing_native_dependencies_recover_and_invalid_hashes_are_atomic() {
    let source = database().await;
    let target = database().await;
    source
        .client()
        .set("key", Value::Integer(0), None)
        .await
        .expect("seed");
    exchange(&source, &target).await;
    source
        .client()
        .set("key", Value::Integer(1), None)
        .await
        .expect("dependency");
    source
        .client()
        .set("key", Value::Integer(2), None)
        .await
        .expect("dependent change");
    let (mut st, mut tt) = channels();
    st.missing_dependency = true;
    let (a, b) = platform::timeout(
        Duration::from_secs(10),
        join(
            source.synchronize_checked(
                &mut st,
                SyncRole::Initiator,
                SyncConfig::default(),
                || async { Ok(()) },
            ),
            target.synchronize_checked(
                &mut tt,
                SyncRole::Responder,
                SyncConfig::default(),
                || async { Ok(()) },
            ),
        ),
    )
    .await
    .expect("bounded recovery");
    a.expect("sender");
    b.expect("receiver");
    assert!(tt.sent.borrow().contains(&Message::Recover));
    assert!(
        st.sent
            .borrow()
            .iter()
            .any(|m| matches!(m, Message::Snapshots(_)))
    );
    assert_eq!(
        target.client().get("key").await.expect("recovered"),
        Some(Value::Integer(2))
    );
    let mut changes = target.sync_changes().expect("subscribe");
    acknowledge(&mut changes);
    let before = target
        .export_snapshots()
        .await
        .expect("history before invalid batch");
    source
        .client()
        .set("key", Value::Integer(3), None)
        .await
        .expect("next change");
    let (mut st, mut tt) = channels();
    st.corrupt_change = true;
    let receiver = Box::pin(target.synchronize_checked(
        &mut tt,
        SyncRole::Responder,
        SyncConfig::default(),
        || async { Ok(()) },
    ));
    let sender = Box::pin(source.synchronize_checked(
        &mut st,
        SyncRole::Initiator,
        SyncConfig::default(),
        || async { Ok(()) },
    ));
    match platform::timeout(Duration::from_secs(10), select(receiver, sender))
        .await
        .expect("reject bad native hash")
    {
        Either::Left((result, pending)) => {
            assert!(matches!(result, Err(SyncError::Storage(_))));
            drop(pending);
        }
        Either::Right(_) => panic!("sender cannot finish an unacknowledged bad batch"),
    }
    assert_eq!(
        target.export_snapshots().await.expect("unchanged history"),
        before
    );
    assert!(
        changes.select().is_none(),
        "rejected native history is silent"
    );
    exchange(&source, &target).await;
    assert_eq!(
        target.client().get("key").await.expect("retry"),
        Some(Value::Integer(3))
    );
}

#[wasm_bindgen::prelude::wasm_bindgen(module = "/src/kv_sync_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = afterNextWrite)]
    fn after_next_write() -> js_sys::Promise;
}
#[wasm_bindgen::prelude::wasm_bindgen(module = "/src/indexeddb_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = failNextCommit)]
    fn fail_next_commit(mode: &str);
}

#[wasm_bindgen_test]
async fn canceled_issued_local_and_incoming_commits_publish_after_completion() {
    let name = format!("sync-cancel-{}", uuid::Uuid::now_v7());
    let db = Database::open_indexeddb(&name).await.expect("open");
    let mut changes = db.sync_changes().expect("subscribe");
    let mut other = db.sync_changes().expect("other subscriber");
    acknowledge(&mut changes);
    acknowledge(&mut other);
    let client = db.client();
    let mut tx = client.transaction().await.expect("stage");
    tx.set("local-a", Value::Integer(1), None)
        .await
        .expect("first");
    tx.set("local-b", Value::Integer(2), None)
        .await
        .expect("second");
    assert!(changes.select().is_none(), "staging is silent");
    assert!(
        tx.commit().now_or_never().is_none(),
        "issue commit and abandon caller"
    );
    platform::timeout(Duration::from_secs(10), changes.changed())
        .await
        .expect("independent completion")
        .expect("wake");
    assert!(
        other.select().is_some(),
        "incoming and local commits fan out independently"
    );
    acknowledge(&mut changes);
    acknowledge(&mut other);
    assert_eq!(
        client
            .scan_prefix("local-")
            .await
            .expect("atomic local commit")
            .len(),
        2
    );
    let fixture = fixture();
    let mut transport = transcript(&fixture);
    let issued = wasm_bindgen_futures::JsFuture::from(after_next_write());
    let mut selected_changes = db.sync_changes().expect("selected session subscription");
    let session = Box::pin(db.synchronize_selected_checked(
        &mut selected_changes,
        &mut transport,
        SyncRole::Responder,
        SyncConfig::default(),
        || async { Ok(()) },
    ));
    match select(session, Box::pin(issued)).await {
        Either::Right((result, pending)) => {
            result.expect("native write issued");
            assert!(
                changes.select().is_none(),
                "request success is not committed publication"
            );
            drop(pending);
        }
        Either::Left(_) => panic!("must cancel an issued incoming commit before session finishes"),
    }
    platform::timeout(Duration::from_secs(10), changes.changed())
        .await
        .expect("incoming completion survives cancellation")
        .expect("wake");
    assert!(other.select().is_some());
    let unacknowledged = selected_changes
        .select()
        .expect("canceled session retains selected work");
    assert_eq!(
        db.sync_invalidations(&unacknowledged)
            .await
            .expect("broad retry after cancellation"),
        vec![KvInvalidation::All]
    );
    let reopened = Database::open_indexeddb(&name)
        .await
        .expect("independent reopen");
    let expected: Vec<KvSnapshot> =
        serde_json::from_value(fixture["snapshots"].clone()).expect("native history");
    for snapshot in expected {
        assert!(
            reopened
                .export_snapshots()
                .await
                .expect("persisted incoming history")
                .contains(&snapshot)
        );
    }
    let clone = client.clone();
    drop(reopened);
    drop(db);
    acknowledge(&mut changes);
    clone
        .set("after-drop", Value::Bool(true), None)
        .await
        .expect("owned client after Database drop");
    assert!(changes.select().is_some());
}

#[wasm_bindgen_test]
async fn rolled_back_dropped_failed_conflicting_and_read_only_commits_are_silent() {
    let name = format!("sync-silence-{}", uuid::Uuid::now_v7());
    let db = Database::open_indexeddb(&name).await.expect("open");
    let client = db.client();
    client
        .set("seed", Value::Integer(0), None)
        .await
        .expect("seed");
    let mut changes = db.sync_changes().expect("subscribe");
    acknowledge(&mut changes);
    let mut tx = client.transaction().await.expect("rollback");
    tx.set("discarded", Value::Bool(true), None)
        .await
        .expect("stage");
    tx.rollback().await.expect("rollback");
    let mut tx = client.transaction().await.expect("drop staging");
    tx.set("discarded", Value::Bool(true), None)
        .await
        .expect("stage");
    drop(tx);
    let mut never_polled = client
        .transaction()
        .await
        .expect("cancel before persistence");
    never_polled
        .set("discarded", Value::Bool(true), None)
        .await
        .expect("stage");
    drop(never_polled.commit());
    client.get("seed").await.expect("read");
    client
        .transaction()
        .await
        .expect("read only")
        .commit()
        .await
        .expect("read only commit");
    for mode in ["abort", "throw"] {
        let mut tx = client.transaction().await.expect("fault transaction");
        tx.set("a", Value::Integer(1), None).await.expect("first");
        tx.set("b", Value::Integer(2), None).await.expect("second");
        fail_next_commit(mode);
        assert!(tx.commit().await.is_err(), "injected {mode} failure");
        assert!(
            changes.select().is_none(),
            "failed native commit stays silent"
        );
        assert_eq!(
            client.scan_all().await.expect("no partial writes"),
            vec![("seed".into(), Value::Integer(0))]
        );
    }
    let mut stale = client.transaction().await.expect("stale transaction");
    stale
        .set("discarded", Value::Bool(true), None)
        .await
        .expect("stage");
    let external = Database::open_indexeddb(&name)
        .await
        .expect("independent open");
    external
        .client()
        .set("external", Value::Bool(true), None)
        .await
        .expect("external write");
    assert!(matches!(
        stale.commit().await,
        Err(ofdb_kv::Error::Storage {
            kind: ofdb_kv::ErrorKind::Conflict,
            ..
        })
    ));
    assert!(
        changes.select().is_none(),
        "external writes do not produce local wakeups and conflicts stay silent"
    );
    assert_eq!(client.get("discarded").await.expect("discarded"), None);
    let fresh = db
        .sync_changes()
        .expect("broad recovery after external gap");
    assert_eq!(
        db.sync_invalidations(&fresh.select().expect("initial"))
            .await
            .expect("broad"),
        vec![KvInvalidation::All]
    );
}

#[wasm_bindgen_test]
async fn history_only_commits_notify_but_elapsed_expiry_does_not() {
    let db = database().await;
    let client = db.client();
    client
        .set("expired", Value::Integer(1), Some(0))
        .await
        .expect("expired history");
    let mut changes = db.sync_changes().expect("subscribe");
    acknowledge(&mut changes);
    client
        .set("expired", Value::Integer(2), Some(0))
        .await
        .expect("invisible history change");
    assert_eq!(client.get("expired").await.expect("still hidden"), None);
    assert!(
        changes.select().is_some(),
        "history mutation notifies despite unchanged visibility"
    );
    let expiry = platform::unix_millis() + 100;
    client
        .set("elapsed", Value::Integer(3), Some(expiry))
        .await
        .expect("future expiry");
    acknowledge(&mut changes);
    let before = db.export_snapshots().await.expect("history before expiry");
    platform::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        client
            .get("elapsed")
            .await
            .expect("expired by browser clock"),
        None
    );
    assert!(changes.select().is_none(), "clock passage is not a commit");
    assert_eq!(
        db.export_snapshots().await.expect("expiry retains history"),
        before
    );
    let peer = database().await;
    exchange(&db, &peer).await;
    assert_eq!(
        peer.client()
            .get("expired")
            .await
            .expect("replicated expiry"),
        None
    );
    assert_eq!(
        peer.client()
            .get("elapsed")
            .await
            .expect("replicated elapsed expiry"),
        None
    );
    assert_eq!(
        peer.export_snapshots()
            .await
            .expect("replicated expiry history"),
        before
    );
}

#[wasm_bindgen_test]
async fn selected_sessions_retain_later_commits_and_independent_peer_work() {
    let left = database().await;
    let right = database().await;
    left.client()
        .set("unrelated", Value::Integer(0), None)
        .await
        .expect("seed");
    exchange(&left, &right).await;
    let mut lc = left.sync_changes().expect("left peer");
    let mut rc = right.sync_changes().expect("right peer");
    let mut other = right.sync_changes().expect("independent peer");
    acknowledge(&mut lc);
    acknowledge(&mut rc);
    acknowledge(&mut other);
    left.client()
        .set("a", Value::Integer(1), None)
        .await
        .expect("pending source");
    right
        .client()
        .set("selected-before", Value::Integer(2), None)
        .await
        .expect("selected target");
    let during = right.client();
    let wrote = Rc::new(Cell::new(false));
    let (mut st, mut tt) = channels();
    let (a, b) = platform::timeout(
        Duration::from_secs(10),
        join(
            left.synchronize_selected_checked(
                &mut lc,
                &mut st,
                SyncRole::Initiator,
                SyncConfig::default(),
                || async { Ok(()) },
            ),
            right.synchronize_selected_checked(
                &mut rc,
                &mut tt,
                SyncRole::Responder,
                SyncConfig::default(),
                || {
                    let client = during.clone();
                    let first = !wrote.replace(true);
                    async move {
                        if first {
                            client
                                .set("during", Value::Integer(3), None)
                                .await
                                .map_err(|e| e.to_string())?;
                        }
                        Ok(())
                    }
                },
            ),
        ),
    )
    .await
    .expect("bounded selected race");
    a.expect("left session");
    b.expect("right session");
    assert!(wrote.get());
    for message in st.sent.borrow().iter().chain(tt.sent.borrow().iter()) {
        if let Message::Inventory(items) = message {
            for item in items {
                let (key, _) = kv_store::decode_document_id(&item.id).expect("document identity");
                assert!(
                    key == "a" || key == "selected-before",
                    "selected logical inventory excludes unrelated and later keys"
                );
            }
        }
    }
    assert_eq!(
        left.client()
            .get("during")
            .await
            .expect("outside original selection"),
        None
    );
    let retained = rc.select().expect("later commits survive acknowledgement");
    assert!(
        right
            .sync_invalidations(&retained)
            .await
            .expect("retained")
            .iter()
            .any(|i| matches!(i, KvInvalidation::Generation { key, .. } if key == "during"))
    );
    assert!(
        other.select().is_some(),
        "one peer cannot acknowledge another"
    );
    selected(&left, &mut lc, &right, &mut rc).await;
    assert_eq!(
        left.client()
            .get("during")
            .await
            .expect("next selected pass"),
        Some(Value::Integer(3))
    );
}

#[wasm_bindgen_test]
async fn notification_overflow_and_lag_fall_back_to_broad_recovery() {
    let db = database().await;
    let client = db.client();
    client
        .set("counter", Value::Integer(0), None)
        .await
        .expect("create table");
    let mut lagged = db.sync_changes().expect("lagged consumer");
    acknowledge(&mut lagged);
    for value in 1..=129 {
        client
            .set(&format!("lag-{value:03}"), Value::Integer(value), None)
            .await
            .expect("committed history");
    }
    let work = lagged.select().expect("gap fallback");
    assert_eq!(
        db.sync_invalidations(&work).await.expect("gapped work"),
        vec![KvInvalidation::All]
    );
    assert!(lagged.acknowledge(work));
    let mut tx = client.transaction().await.expect("oversized commit");
    for i in 0..129 {
        tx.set(&format!("batch-{i:03}"), Value::Bool(true), None)
            .await
            .expect("stage many identities");
    }
    tx.commit().await.expect("large atomic commit");
    let work = lagged.select().expect("oversized invalidation fallback");
    assert_eq!(
        db.sync_invalidations(&work).await.expect("oversized work"),
        vec![KvInvalidation::All]
    );
    let peer = database().await;
    let mut remote = peer.sync_changes().expect("new peer");
    let (a, _) = selected(&db, &mut lagged, &peer, &mut remote).await;
    assert!(a.contains(&Message::Selection(None)));
    assert_eq!(
        peer.client()
            .get("lag-129")
            .await
            .expect("broad gap recovery"),
        Some(Value::Integer(129))
    );
    assert_eq!(
        peer.client()
            .scan_prefix("batch-")
            .await
            .expect("complete oversized commit")
            .len(),
        129
    );
}

#[wasm_bindgen_test]
async fn failed_incoming_persistence_is_atomic_silent_and_retryable() {
    for mode in ["abort", "throw"] {
        let target = database().await;
        let mut changes = target.sync_changes().expect("subscribe");
        acknowledge(&mut changes);
        let fixture = fixture();
        let mut transport = transcript(&fixture);
        fail_next_commit(mode);
        let result = target
            .synchronize_checked(
                &mut transport,
                SyncRole::Responder,
                SyncConfig::default(),
                || async { Ok(()) },
            )
            .await;
        assert!(
            matches!(result, Err(SyncError::Storage(_))),
            "injected incoming commit failure"
        );
        assert!(
            target
                .export_snapshots()
                .await
                .expect("no partial incoming history")
                .is_empty()
        );
        assert!(
            target
                .client()
                .scan_all()
                .await
                .expect("no partial incoming values")
                .is_empty()
        );
        assert!(
            changes.select().is_none(),
            "failed incoming commit is silent"
        );
        let mut retry = transcript(&fixture);
        target
            .synchronize_checked(
                &mut retry,
                SyncRole::Responder,
                SyncConfig::default(),
                || async { Ok(()) },
            )
            .await
            .expect("retry failed incoming batch");
        assert!(changes.select().is_some());
        let expected: Vec<KvSnapshot> =
            serde_json::from_value(fixture["snapshots"].clone()).expect("native snapshots");
        assert_eq!(
            target
                .export_snapshots()
                .await
                .expect("complete retry history"),
            expected
        );
    }
}
