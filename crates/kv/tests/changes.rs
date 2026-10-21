#![cfg(all(feature = "redb", feature = "sync"))]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures::FutureExt;
use ofdb_kv::{ChangeSubscription, Database, SyncConfig, SyncRole, SyncTransport, Value};
use tokio::sync::mpsc;

fn pending(changes: &mut ChangeSubscription) {
    assert!(
        changes.changed().now_or_never().is_none(),
        "no committed change"
    );
}
fn changed(changes: &mut ChangeSubscription) {
    assert!(
        changes
            .changed()
            .now_or_never()
            .expect("committed change")
            .is_ok()
    );
}

#[tokio::test]
async fn commits_noops_and_rollbacks_across_client_clones() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = Database::open(directory.path().join("changes.redb")).expect("database");
    let client = database.client();
    let clone = client.clone();
    let mut changes = database.sync_changes().expect("subscribe");
    let mut other = database.sync_changes().expect("second subscriber");
    let mut tx = clone.transaction().await.expect("transaction");
    tx.set("a", Value::Integer(1), None).await.expect("set");
    tx.set("b", Value::Integer(2), None).await.expect("set");
    pending(&mut changes);
    tx.commit().await.expect("commit");
    changed(&mut changes);
    changed(&mut other);
    pending(&mut database.sync_changes().expect("fresh subscription"));
    clone.get("a").await.expect("read");
    clone.scan_all().await.expect("scan");
    clone
        .transaction()
        .await
        .expect("transaction")
        .commit()
        .await
        .expect("empty commit");
    clone.delete("absent").await.expect("absent delete");
    clone
        .set("a", Value::Integer(1), None)
        .await
        .expect("identical set");
    pending(&mut changes);
    let mut tx = clone.transaction().await.expect("transaction");
    tx.set("a", Value::Integer(3), None).await.expect("set");
    tx.rollback().await.expect("rollback");
    let mut tx = clone.transaction().await.expect("transaction");
    tx.set("a", Value::Integer(4), None).await.expect("set");
    drop(tx);
    pending(&mut changes);
    drop(database);
    clone
        .set("a", Value::Integer(5), None)
        .await
        .expect("owned client");
    changed(&mut changes);
    clone.delete("a").await.expect("delete");
    changed(&mut changes);
    drop(client);
    drop(clone);
    assert!(changes.changed().await.is_err());
}

#[tokio::test]
async fn incoming_sync_notifies_but_replay_is_silent() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let source = Database::open(directory.path().join("source.redb")).expect("source");
    let target = Database::open(directory.path().join("target.redb")).expect("target");
    source
        .client()
        .set("key", Value::Integer(1), None)
        .await
        .expect("seed");
    let mut source_changes = source.sync_changes().expect("source subscription");
    let mut changes = target.sync_changes().expect("target subscription");
    exchange(&source, &target).await;
    changed(&mut changes);
    pending(&mut source_changes);
    exchange(&source, &target).await;
    pending(&mut changes);
    source
        .client()
        .set("key", Value::Integer(2), None)
        .await
        .expect("update");
    changed(&mut source_changes);
    exchange(&source, &target).await;
    changed(&mut changes);
    exchange(&source, &target).await;
    pending(&mut changes);
    source.client().delete("key").await.expect("delete");
    exchange(&source, &target).await;
    changed(&mut changes);
    exchange(&source, &target).await;
    pending(&mut changes);
}

#[tokio::test]
async fn continuous_runner_handles_local_remote_and_idle_without_server_scheduling() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let source = Arc::new(Database::open(directory.path().join("source.redb")).expect("source"));
    let target = Arc::new(Database::open(directory.path().join("target.redb")).expect("target"));
    source
        .client()
        .set("key", Value::Integer(1), None)
        .await
        .expect("seed");
    let changes = source
        .sync_changes()
        .expect("subscribe before initial pass");
    let calls = Arc::new(AtomicUsize::new(0));
    let (left, right, count) = (source.clone(), target.clone(), calls.clone());
    let runner = tokio::spawn(changes.run(move || {
        let (left, right, count) = (left.clone(), right.clone(), count.clone());
        async move {
            exchange(&left, &right).await;
            count.fetch_add(1, Ordering::SeqCst);
            Ok::<(), ()>(())
        }
    }));
    wait_passes(&calls, 1).await;
    source
        .client()
        .set("key", Value::Integer(2), None)
        .await
        .expect("local write");
    wait_passes(&calls, 2).await;
    assert_eq!(
        target.client().get("key").await.expect("replicated"),
        Some(Value::Integer(2))
    );
    source.client().get("key").await.expect("read");
    source
        .client()
        .set("key", Value::Integer(2), None)
        .await
        .expect("no-op");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "idle and no-op do not schedule"
    );
    target
        .client()
        .set("key", Value::Integer(3), None)
        .await
        .expect("remote write");
    exchange(&target, &source).await;
    wait_passes(&calls, 3).await;
    exchange(&target, &source).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3, "replay does not schedule");
    runner.abort();
    assert!(runner.await.expect_err("cancelled").is_cancelled());
}

async fn wait_passes(calls: &AtomicUsize, expected: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while calls.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sync pass completed");
}
struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}
impl SyncTransport for Channel {
    type Error = ();
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        self.sender.send(frame).map_err(|_| ())
    }
    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.receiver.recv().await.ok_or(())
    }
}
async fn exchange(source: &Database, target: &Database) {
    let (a, ar) = mpsc::unbounded_channel();
    let (b, br) = mpsc::unbounded_channel();
    let mut a = Channel {
        sender: a,
        receiver: br,
    };
    let mut b = Channel {
        sender: b,
        receiver: ar,
    };
    let (a, b) = tokio::join!(
        source.synchronize(&mut a, SyncRole::Initiator, SyncConfig::default()),
        target.synchronize(&mut b, SyncRole::Responder, SyncConfig::default()),
    );
    a.expect("initiator");
    b.expect("responder");
}

#[cfg(unix)]
#[tokio::test]
async fn retirement_closes_the_signal_without_a_change() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = Database::open(directory.path().join("retired.redb")).expect("database");
    let mut changes = database.sync_changes().expect("subscribe");
    database.retire_and_remove().await.expect("retire");
    assert!(changes.changed().await.is_err());
}

#[cfg(all(feature = "server", feature = "remote"))]
#[tokio::test]
async fn hosted_writes_share_the_backend_signal() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database =
        Arc::new(Database::open(directory.path().join("hosted.redb")).expect("database"));
    let mut changes = database.sync_changes().expect("subscribe");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("address");
    let address = listener.local_addr().expect("local address");
    drop(listener);
    let hosted = database.clone();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        hosted
            .serve_tcp(address, async {
                let _ = stopped.await;
            })
            .await
            .expect("serve");
    });
    let endpoint = format!("http://{address}");
    let client = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(client) = ofdb_kv::Client::connect(&endpoint).await {
                break client;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server starts");
    client.get("key").await.expect("hosted read");
    pending(&mut changes);
    let mut tx = client.transaction().await.expect("remote transaction");
    tx.set("key", Value::Integer(1), None)
        .await
        .expect("remote set");
    pending(&mut changes);
    tx.commit().await.expect("remote commit");
    changed(&mut changes);
    shutdown.send(()).expect("shutdown");
    server.await.expect("server task");
}

#[cfg(feature = "in-memory")]
#[test]
fn memory_subscription_is_explicitly_unsupported() {
    assert_eq!(
        Database::in_memory()
            .sync_changes()
            .expect_err("redb only")
            .kind(),
        std::io::ErrorKind::Unsupported
    );
}
