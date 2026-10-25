#![cfg(all(not(target_arch = "wasm32"), feature = "sync"))]

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use futures::{StreamExt, channel::mpsc, future::join};
use ofdb_kv::{Database, KvSnapshot, SyncConfig, SyncRole, SyncTransport, Value};

struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    sent: Rc<RefCell<Vec<Vec<u8>>>>,
}
impl SyncTransport for Channel {
    type Error = ();
    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.receiver.next().await.ok_or(())
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        self.sent.borrow_mut().push(frame.clone());
        self.sender.unbounded_send(frame).map_err(|_| ())
    }
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

#[tokio::test]
async fn native_current_protocol_fixture() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/kv_native_fixture.json");
    if std::env::var_os("OFDB_REGENERATE_KV_FIXTURE").is_some() {
        let source = Database::in_memory();
        let target = Database::in_memory();
        let client = source.client();
        client
            .set(
                "nested",
                Value::from(
                    serde_json::json!({"profile":{"name":"native","count":1},"tags":[1,2]}),
                ),
                None,
            )
            .await
            .expect("nested");
        client
            .set("integer", Value::Integer(i64::MIN), None)
            .await
            .expect("integer");
        client
            .set("blob", Value::Blob(vec![0, 255, 0]), None)
            .await
            .expect("blob");
        client
            .set("expired", Value::Bool(true), Some(0))
            .await
            .expect("expired");
        client
            .set("deleted", Value::Integer(1), None)
            .await
            .expect("seed");
        client.delete("deleted").await.expect("tombstone");
        client
            .set("replacement", Value::Integer(1), None)
            .await
            .expect("old generation");
        client.delete("replacement").await.expect("end generation");
        client
            .set("replacement", Value::Integer(2), None)
            .await
            .expect("new generation");
        let (a, ar) = mpsc::unbounded();
        let (b, br) = mpsc::unbounded();
        let sent = Rc::new(RefCell::new(Vec::new()));
        let replies = Rc::new(RefCell::new(Vec::new()));
        let mut st = Channel {
            sender: a,
            receiver: br,
            sent: sent.clone(),
        };
        let mut tt = Channel {
            sender: b,
            receiver: ar,
            sent: replies.clone(),
        };
        let (a, b) = join(
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
        )
        .await;
        a.expect("native initiator");
        b.expect("native responder");
        let mut fixture = serde_json::json!({
            "initiator": *sent.borrow(), "responder": *replies.borrow(),
            "snapshots": source.export_snapshots().await.expect("native snapshots"),
        });
        sent.borrow_mut().clear();
        replies.borrow_mut().clear();
        client
            .set("integer", Value::Integer(i64::MAX), None)
            .await
            .expect("native delta");
        client
            .set(
                "nested",
                Value::from(
                    serde_json::json!({"profile":{"name":"native","count":2},"tags":[1,2]}),
                ),
                None,
            )
            .await
            .expect("nested delta");
        let (a, b) = join(
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
        )
        .await;
        a.expect("native delta initiator");
        b.expect("native delta responder");
        assert!(sent.borrow().iter().any(|f| matches!(
            kv_sync::decode_frame::<()>(f, SyncConfig::default()).expect("native frame"),
            kv_sync::Message::Changes(_)
        )));
        fixture["delta_initiator"] = serde_json::to_value(&*sent.borrow()).expect("delta frames");
        fixture["delta_responder"] =
            serde_json::to_value(&*replies.borrow()).expect("delta responses");
        fixture["delta_snapshots"] =
            serde_json::to_value(source.export_snapshots().await.expect("delta history"))
                .expect("delta snapshots");
        std::fs::write(
            &path,
            serde_json::to_string(&fixture).expect("fixture JSON") + "\n",
        )
        .expect("write fixture");
    }
    let fixture: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("committed fixture")).expect("JSON");
    let target = Database::in_memory();
    for (incoming, replies, snapshots) in [
        ("initiator", "responder", "snapshots"),
        ("delta_initiator", "delta_responder", "delta_snapshots"),
    ] {
        let mut transport = Transcript {
            incoming: serde_json::from_value(fixture[incoming].clone()).expect("initiator frames"),
            sent: vec![],
        };
        target
            .synchronize_checked(
                &mut transport,
                SyncRole::Responder,
                SyncConfig::default(),
                || async { Ok(()) },
            )
            .await
            .expect("native replay");
        assert!(transport.incoming.is_empty());
        let expected: Vec<Vec<u8>> =
            serde_json::from_value(fixture[replies].clone()).expect("response frames");
        assert_eq!(transport.sent, expected);
        let expected: Vec<KvSnapshot> =
            serde_json::from_value(fixture[snapshots].clone()).expect("snapshots");
        assert_eq!(
            target.export_snapshots().await.expect("replayed history"),
            expected
        );
    }
}
