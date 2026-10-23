#![cfg(all(feature = "redb", feature = "sync", feature = "sql"))]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures::FutureExt;
use ofdb_sql::{ChangeSubscription, Database, SessionConfig, SyncRole, SyncTransport};
use tokio::sync::mpsc;

fn run_test<F: core::future::Future<Output = ()>>(test: impl FnOnce() -> F + Send + 'static) {
    std::thread::Builder::new()
        .name("change-subscription-fixture".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(test());
        })
        .expect("fixture thread")
        .join()
        .expect("fixture passed");
}
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

#[test]
fn commits_noops_and_rollbacks_across_clones() {
    run_test(|| async {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = Database::open(directory.path().join("changes.redb")).expect("database");
        let clone = database.clone();
        let client = clone.client();
        let shared = client.clone();
        let mut changes = database.sync_changes().expect("subscription");
        let mut other = clone.sync_changes().expect("clone subscription");
        client
            .execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value TEXT)", None)
            .await
            .expect("create");
        changed(&mut changes);
        changed(&mut other);
        pending(&mut clone.sync_changes().expect("fresh subscription"));
        for _ in 0..3 {
            shared
                .execute_sql("SELECT * FROM items", None)
                .await
                .expect("idle select");
        }
        shared
            .execute_sql("UPDATE items SET value = 'none'", None)
            .await
            .expect("zero-row update");
        pending(&mut changes);
        let mut tx = client.transaction().await.expect("transaction");
        tx.execute_sql("INSERT INTO items (value) VALUES ('committed')", None)
            .await
            .expect("insert");
        pending(&mut changes);
        tx.commit().await.expect("commit");
        changed(&mut changes);
        shared
            .execute_sql("UPDATE items SET value = 'committed'", None)
            .await
            .expect("identical value update");
        pending(&mut changes);
        let mut tx = shared.transaction().await.expect("transaction");
        tx.execute_sql("INSERT INTO items (value) VALUES ('rolled back')", None)
            .await
            .expect("insert");
        tx.rollback().await.expect("rollback");
        let mut tx = shared.transaction().await.expect("transaction");
        tx.execute_sql("INSERT INTO items (value) VALUES ('dropped')", None)
            .await
            .expect("insert");
        drop(tx);
        pending(&mut changes);
        assert!(
            shared
                .execute_sql(
                    "INSERT INTO items (value) VALUES ('aborted'); INSERT INTO missing VALUES (1)",
                    None
                )
                .await
                .is_err()
        );
        pending(&mut changes);
        drop(clone);
        drop(database);
        shared
            .execute_sql(
                "INSERT INTO items (value) VALUES ('after database drop')",
                None,
            )
            .await
            .expect("owned client");
        changed(&mut changes);
        drop(client);
        drop(shared);
        assert!(changes.changed().await.is_err());
    });
}

#[test]
fn remote_apply_notifies_but_replayed_state_does_not() {
    run_test(|| async {
        let directory = tempfile::tempdir().expect("temporary directory");
        let source = Database::open(directory.path().join("source.redb")).expect("source");
        let target = Database::open(directory.path().join("target.redb")).expect("target");
        source.client().execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); INSERT INTO items (value) VALUES ('remote')", None).await.expect("seed");
        let mut changes = target.sync_changes().expect("subscribe");
        exchange(&source, &target).await;
        changed(&mut changes);
        exchange(&source, &target).await;
        pending(&mut changes);
        source
            .client()
            .execute_sql("UPDATE items SET value = 'new remote'", None)
            .await
            .expect("update");
        exchange(&source, &target).await;
        changed(&mut changes);
        exchange(&source, &target).await;
        for unit in source.export_sync_state().await.expect("export") {
            Box::pin(target.apply_sync_state(unit))
                .await
                .expect("direct replay");
        }
        pending(&mut changes);
    });
}

#[test]
fn continuous_runner_handles_local_remote_and_idle_without_server_scheduling() {
    run_test(|| async {
        let directory = tempfile::tempdir().expect("temporary directory");
        let source =
            Arc::new(Database::open(directory.path().join("source.redb")).expect("source"));
        let target =
            Arc::new(Database::open(directory.path().join("target.redb")).expect("target"));
        source.client().execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); INSERT INTO items (value) VALUES ('seed')", None).await.expect("seed");
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
            .execute_sql("INSERT INTO items (value) VALUES ('local')", None)
            .await
            .expect("local write");
        wait_passes(&calls, 2).await;
        source
            .client()
            .execute_sql(
                "SELECT * FROM items; UPDATE items SET value = 'local' WHERE value = 'local'",
                None,
            )
            .await
            .expect("read and no-op");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "idle and no-op do not schedule"
        );
        target
            .client()
            .execute_sql("UPDATE items SET value = 'remote'", None)
            .await
            .expect("remote write");
        exchange(&target, &source).await;
        wait_passes(&calls, 3).await;
        exchange(&target, &source).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3, "replay does not schedule");
        runner.abort();
        assert!(runner.await.expect_err("cancelled").is_cancelled());
    });
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
    type Error = &'static str;
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sender.send(frame).map_err(|_| "closed")
    }
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.recv().await.ok_or("closed")
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
    let config = SessionConfig::default();
    let (a, b) = tokio::join!(
        Box::pin(source.synchronize(&mut a, &config, SyncRole::Initiator)),
        Box::pin(target.synchronize(&mut b, &config, SyncRole::Responder)),
    );
    a.expect("initiator");
    b.expect("responder");
}

#[cfg(unix)]
#[test]
fn retirement_closes_the_signal_without_a_change() {
    run_test(|| async {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = Database::open(directory.path().join("retired.redb")).expect("database");
        let mut changes = database.sync_changes().expect("subscribe");
        database.retire_and_remove().await.expect("retire");
        assert!(changes.changed().await.is_err());
    });
}

#[cfg(all(feature = "server", feature = "remote"))]
#[test]
fn hosted_writes_share_the_backend_signal() {
    run_test(|| async {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = Database::open(directory.path().join("hosted.redb")).expect("database");
        database
            .client()
            .execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value TEXT)", None)
            .await
            .expect("schema");
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
                if let Ok(client) = ofdb_sql::Client::connect(&endpoint).await {
                    break client;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("server starts");
        client
            .execute_sql("SELECT * FROM items", None)
            .await
            .expect("hosted select");
        pending(&mut changes);
        let mut tx = client.transaction().await.expect("remote transaction");
        tx.execute_sql("INSERT INTO items (value) VALUES ('remote')", None)
            .await
            .expect("remote insert");
        pending(&mut changes);
        tx.commit().await.expect("remote commit");
        changed(&mut changes);
        shutdown.send(()).expect("shutdown");
        server.await.expect("server task");
    });
}

#[cfg(feature = "in-memory")]
#[test]
fn memory_subscription_starts_with_broad_work() {
    assert!(
        Database::in_memory()
            .sync_changes()
            .expect("memory subscription")
            .select()
            .is_some()
    );
}
