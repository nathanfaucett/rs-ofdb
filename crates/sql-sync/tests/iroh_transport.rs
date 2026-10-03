#![cfg(feature = "iroh")]

use std::{io::ErrorKind, time::Duration};

use iroh::{Endpoint, address_lookup::MemoryLookup, endpoint::presets};
use sync::{IrohTransport, MAX_MESSAGE_BYTES, SyncTransport};

#[tokio::test]
async fn loopback_frames_both_directions_and_rejects_oversize() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let lookup = MemoryLookup::new();
        let a = Endpoint::builder(presets::Minimal)
            .address_lookup(lookup.clone())
            .bind()
            .await
            .expect("bind initiator");
        let b = Endpoint::builder(presets::Minimal)
            .alpns(vec![b"ofdb-sync-test".to_vec()])
            .address_lookup(lookup.clone())
            .bind()
            .await
            .expect("bind responder");
        lookup.add_endpoint_info(b.addr());
        let (connection, peer) = tokio::join!(a.connect(b.id(), b"ofdb-sync-test"), async {
            b.accept()
                .await
                .expect("incoming connection")
                .accept()
                .expect("start accepting")
                .await
        });
        let connection = connection.expect("connect");
        let peer = peer.expect("accept connection");
        let (send, recv) = connection.open_bi().await.expect("open stream");
        let mut initiator = IrohTransport::new(send, recv);
        let payload = vec![42; MAX_MESSAGE_BYTES];
        initiator
            .send(payload.clone())
            .await
            .expect("send boundary frame");

        let (send, recv) = peer.accept_bi().await.expect("accept stream");
        let mut responder = IrohTransport::new(send, recv);
        assert_eq!(responder.receive().await.expect("read frame"), payload);
        responder.send(b"reply".to_vec()).await.expect("send reply");
        assert_eq!(initiator.receive().await.expect("read reply"), b"reply");

        let error = initiator
            .send(vec![0; MAX_MESSAGE_BYTES + 1])
            .await
            .expect_err("reject oversized outbound frame");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        initiator
            .send(b"still framed".to_vec())
            .await
            .expect("send after rejection");
        assert_eq!(
            responder.receive().await.expect("read after rejection"),
            b"still framed"
        );

        let (mut raw_send, _raw_recv) = connection.open_bi().await.expect("open malicious stream");
        raw_send
            .write_all(&((MAX_MESSAGE_BYTES + 1) as u32).to_be_bytes())
            .await
            .expect("send oversized length");
        let (send, recv) = peer.accept_bi().await.expect("accept malicious stream");
        let mut bounded = IrohTransport::new(send, recv);
        let error = bounded
            .receive()
            .await
            .expect_err("reject oversized length without reading payload");
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        a.close().await;
        b.close().await;
    })
    .await
    .expect("loopback test timed out");
}
