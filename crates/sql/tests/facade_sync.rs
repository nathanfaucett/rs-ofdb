#![cfg(all(feature = "sync", feature = "sql", feature = "in-memory"))]

use ofdb_sql::{Database, SessionConfig, SyncRole, SyncTransport};
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
fn channels() -> (Channel, Channel) {
    let (a, ar) = mpsc::unbounded_channel();
    let (b, br) = mpsc::unbounded_channel();
    (
        Channel {
            sender: a,
            receiver: br,
        },
        Channel {
            sender: b,
            receiver: ar,
        },
    )
}
async fn seed(db: &Database) {
    db.client()
        .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", None)
        .await
        .expect("schema");
    db.client().execute_sql("INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", None).await.expect("row");
}
#[cfg(feature = "native-stage")]
#[tokio::test]
async fn native_facade_replays_catalog_and_rows_and_cleans_files() {
    let a = Database::in_memory();
    let b = Database::in_memory();
    seed(&a).await;
    let dir = tempfile::tempdir().expect("directory");
    let (mut at, mut bt) = channels();
    let config = SessionConfig {
        max_units_per_frame: 1,
        ..SessionConfig::default()
    };
    let (ar, br) = tokio::join!(
        a.synchronize_with_stage_directory(&mut at, &config, SyncRole::Initiator, dir.path()),
        b.synchronize_with_stage_directory(&mut bt, &config, SyncRole::Responder, dir.path())
    );
    ar.expect("initiator");
    br.expect("responder");
    assert_eq!(
        a.export_sync_state().await.expect("source"),
        b.export_sync_state().await.expect("destination")
    );
    assert_eq!(std::fs::read_dir(dir.path()).expect("listing").count(), 0);
}
