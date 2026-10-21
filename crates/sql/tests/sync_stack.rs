#![cfg(all(feature = "redb", feature = "native-stage", feature = "sql"))]

use std::{path::Path, sync::Arc, time::Duration};

use futures::executor::block_on;
use ofdb_sql::{Database, QueryResult, SessionConfig, SyncResult, SyncRole, SyncTransport, Value};
use tokio::sync::mpsc;

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

async fn authorize() -> Result<(), String> {
    let state = [0_u8; 128 * 1024];
    tokio::task::yield_now().await;
    core::hint::black_box(&state);
    Ok(())
}

async fn exchange(
    left: &Database,
    right: &Database,
    stages: &Path,
    left_role: SyncRole,
) -> (SyncResult, SyncResult) {
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
    let config = SessionConfig {
        max_units_per_frame: 1,
        ..SessionConfig::default()
    };
    let right_role = match left_role {
        SyncRole::Initiator => SyncRole::Responder,
        SyncRole::Responder => SyncRole::Initiator,
    };
    let left = left
        .synchronize_with_stage_directory_checked(&mut a, &config, left_role, stages, authorize);
    let right = right
        .synchronize_with_stage_directory_checked(&mut b, &config, right_role, stages, authorize);
    assert!(
        core::mem::size_of_val(&left) < 4096,
        "left sync future must stay indirect"
    );
    assert!(
        core::mem::size_of_val(&right) < 4096,
        "right sync future must stay indirect"
    );
    let (left, right) =
        tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(left, right) })
            .await
            .expect("bounded native staged sync");
    (left.expect("left sync"), right.expect("right sync"))
}

// Fixture SQL execution is separate from the two-MiB sync workers under test.
fn fixture_sql(database: Arc<Database>, sql: &str) -> Vec<QueryResult> {
    let sql = sql.to_owned();
    std::thread::Builder::new()
        .name("sync-sql-fixture".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || block_on(database.client().execute_sql(&sql, None)).expect("fixture SQL"))
        .expect("fixture thread")
        .join()
        .expect("fixture prepared")
}

#[test]
fn native_staged_bootstrap_and_incremental_catchup_on_two_mib_workers() {
    let directory = tempfile::tempdir().expect("database directory");
    let left = Arc::new(Database::open(directory.path().join("left.redb")).expect("left"));
    let right = Arc::new(Database::open(directory.path().join("right.redb")).expect("right"));
    fixture_sql(
        left.clone(),
        "CREATE TABLE documents (id UUID PRIMARY KEY, value TEXT); INSERT INTO documents (value) VALUES ('initial')",
    );
    let stages = directory.path().join("stages");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("two MiB runtime");
    let pass = |role| {
        let (left, right, stages) = (left.clone(), right.clone(), stages.clone());
        runtime
            .block_on(runtime.spawn(async move { exchange(&left, &right, &stages, role).await }))
            .expect("sync completes on a two MiB worker")
    };
    let (_, received) = pass(SyncRole::Initiator);
    assert!(
        received.received_snapshots > 0,
        "bootstrap applies catalog and rows"
    );
    fixture_sql(left.clone(), "UPDATE documents SET value = 'left'");
    let (sent, received) = pass(SyncRole::Responder);
    assert!(sent.sent_changes > 0, "responder sends incremental history");
    assert!(received.received_changes > 0);
    fixture_sql(right.clone(), "UPDATE documents SET value = 'right'");
    let (received, sent) = pass(SyncRole::Initiator);
    assert!(sent.sent_changes > 0);
    assert!(
        received.received_changes > 0,
        "initiator applies incremental history"
    );
    let rows = fixture_sql(left.clone(), "SELECT value FROM documents");
    assert_eq!(rows[0].rows[0].values, vec![Value::Text("right".into())]);
    let (left, right) = pass(SyncRole::Responder);
    assert_eq!(
        left.received_units + left.sent_units + right.received_units + right.sent_units,
        0,
        "replay is idle"
    );
    assert_eq!(
        std::fs::read_dir(&stages).expect("stage directory").count(),
        0,
        "stages cleaned up"
    );
}
