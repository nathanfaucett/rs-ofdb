use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use futures::{StreamExt, channel::mpsc, future::join};
use ofdb_sql::{Database, SessionConfig, SyncRole, SyncTransport};

pub struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    pub sent: Rc<RefCell<Vec<Vec<u8>>>>,
}
impl SyncTransport for Channel {
    type Error = &'static str;
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.next().await.ok_or("closed")
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sent.borrow_mut().push(frame.clone());
        self.sender.unbounded_send(frame).map_err(|_| "closed")
    }
}
pub fn channels() -> (Channel, Channel) {
    let (a, ar) = mpsc::unbounded();
    let (b, br) = mpsc::unbounded();
    (
        Channel {
            sender: a,
            receiver: br,
            sent: Rc::default(),
        },
        Channel {
            sender: b,
            receiver: ar,
            sent: Rc::default(),
        },
    )
}
pub async fn broad(left: &Database, right: &Database) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let (mut a, mut b) = channels();
    let config = SessionConfig::default();
    let (a_result, b_result) = join(
        left.synchronize_checked(&mut a, &config, SyncRole::Initiator, || async { Ok(()) }),
        right.synchronize_checked(&mut b, &config, SyncRole::Responder, || async { Ok(()) }),
    )
    .await;
    a_result.expect("checked initiator");
    b_result.expect("checked responder");
    (a.sent.take(), b.sent.take())
}
pub struct Transcript {
    pub incoming: VecDeque<Vec<u8>>,
    pub sent: Vec<Vec<u8>>,
}
impl SyncTransport for Transcript {
    type Error = &'static str;
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.incoming.pop_front().ok_or("interrupted")
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sent.push(frame);
        Ok(())
    }
}
pub fn transcript(fixture: &serde_json::Value, key: &str) -> Transcript {
    Transcript {
        incoming: serde_json::from_value(fixture[key].clone()).expect("native frames"),
        sent: vec![],
    }
}
pub async fn verify_fixture(target: &Database, fixture: &serde_json::Value) {
    for prefix in ["initial", "delta"] {
        let mut transport = transcript(fixture, &format!("{prefix}_initiator"));
        target
            .synchronize_checked(
                &mut transport,
                &SessionConfig::default(),
                SyncRole::Responder,
                || async { Ok(()) },
            )
            .await
            .expect("native exchange");
        assert!(transport.incoming.is_empty());
        let expected: Vec<Vec<u8>> =
            serde_json::from_value(fixture[format!("{prefix}_responder")].clone())
                .expect("native replies");
        assert_eq!(transport.sent, expected, "current native wire replies");
        let expected: Vec<ofdb_sql::SyncStateUnit> =
            serde_json::from_value(fixture[format!("{prefix}_state")].clone())
                .expect("native history and deletion metadata");
        assert_eq!(
            target.export_sync_state().await.expect("retained history"),
            expected
        );
    }
}
