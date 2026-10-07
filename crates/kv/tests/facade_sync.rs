#![cfg(all(feature = "sync", feature = "in-memory"))]

use ofdb_kv::{Database, SyncConfig, SyncRole, SyncTransport, Value};
use tokio::sync::mpsc;

struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}
impl SyncTransport for Channel {
    type Error = ();
    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.receiver.recv().await.ok_or(())
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        self.sender.send(frame).map_err(|_| ())
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
async fn exchange(left: &Database, right: &Database) {
    let (mut a, mut b) = channels();
    let (a, b) = tokio::join!(
        left.synchronize(&mut a, SyncRole::Initiator, SyncConfig::default()),
        right.synchronize(&mut b, SyncRole::Responder, SyncConfig::default())
    );
    a.expect("initiator");
    b.expect("responder");
}
#[tokio::test]
async fn facade_only_offline_sync_preserves_generations_tombstones_and_expiry() {
    let left = Database::in_memory();
    let right = Database::in_memory();
    left.client()
        .set("shared", Value::Integer(1), None)
        .await
        .expect("seed");
    exchange(&left, &right).await;
    left.client()
        .delete("shared")
        .await
        .expect("offline delete");
    right
        .client()
        .set("shared", Value::Integer(2), None)
        .await
        .expect("offline update");
    right
        .client()
        .set("expired", Value::Integer(3), Some(0))
        .await
        .expect("expiry");
    exchange(&left, &right).await;
    for db in [&left, &right] {
        assert_eq!(db.client().get("shared").await.expect("tombstone"), None);
        assert_eq!(db.client().get("expired").await.expect("expired"), None);
    }
    right
        .client()
        .set("shared", Value::Integer(4), None)
        .await
        .expect("new generation");
    exchange(&left, &right).await;
    assert_eq!(
        left.client().get("shared").await.expect("recreated"),
        Some(Value::Integer(4))
    );
    assert_eq!(
        left.export_snapshots().await.expect("left snapshot"),
        right.export_snapshots().await.expect("right snapshot")
    );
}
