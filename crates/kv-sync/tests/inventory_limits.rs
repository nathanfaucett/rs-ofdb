use std::collections::VecDeque;

use btree::InMemoryBTree;
use futures::executor::block_on;
use kv::{KvStore, encode_document_id};
use ofdb_kv_sync::{
    Config, Error, KvInventory, Message, SyncRole, SyncTransport, encode_frame, synchronize,
};

struct Script {
    frames: VecDeque<Vec<u8>>,
    received: usize,
}
impl SyncTransport for Script {
    type Error = ();

    async fn send(&mut self, _: Vec<u8>) -> Result<(), ()> {
        Ok(())
    }

    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.received += 1;
        self.frames.pop_front().ok_or(())
    }
}
fn item(n: usize, hashes: usize) -> KvInventory {
    KvInventory {
        id: encode_document_id(
            &format!("key-{n}"),
            uuid::Uuid::from_u128((1 << 80) | (7 << 76) | (2 << 62)),
        ),
        hashes: vec![[7; 32]; hashes],
    }
}
fn script(messages: Vec<Message>) -> Script {
    Script {
        frames: [Message::Hello { version: 3 }, Message::Selection(None)]
            .into_iter()
            .chain(messages)
            .map(|message| encode_frame(&message, Config::default()).expect("valid frame"))
            .collect(),
        received: 0,
    }
}
fn timestamp() -> uuid::Timestamp {
    uuid::Timestamp::from_unix_time(1_700_000_000, 0, 0, 0)
}

#[test]
fn inventory_rejects_aggregate_bytes_and_entries_before_end() {
    block_on(async {
        for hashes in [0, 2] {
            let store = KvStore::new(InMemoryBTree::<Vec<u8>, Vec<u8>>::new(), timestamp);
            let mut transport = script(
                (0..10)
                    .map(|n| Message::Inventory(vec![item(n, hashes)]))
                    .chain([Message::End])
                    .collect(),
            );
            let config = Config {
                max_inventory_bytes: if hashes == 0 { 4096 } else { 200 },
                max_inventory_entries: if hashes == 0 { 3 } else { 100 },
                ..Config::default()
            };
            assert_eq!(
                synchronize(&store, &mut transport, SyncRole::Responder, config).await,
                Err(Error::FrameTooLarge)
            );
            assert!(
                transport.received <= 6,
                "reject before retaining excess or reading End"
            );
            assert!(
                matches!(transport.frames.back(), Some(frame) if frame == &encode_frame(&Message::End, config).expect("End"))
            );
        }
    });
}

#[test]
fn inventory_rejects_duplicate_document_ids_before_end() {
    block_on(async {
        for same_frame in [false, true] {
            let store = KvStore::new(InMemoryBTree::<Vec<u8>, Vec<u8>>::new(), timestamp);
            let messages = if same_frame {
                vec![
                    Message::Inventory(vec![item(0, 0), item(0, 1)]),
                    Message::End,
                ]
            } else {
                vec![
                    Message::Inventory(vec![item(0, 0)]),
                    Message::Inventory(vec![item(0, 1)]),
                    Message::End,
                ]
            };
            let mut transport = script(messages);
            assert_eq!(
                synchronize(
                    &store,
                    &mut transport,
                    SyncRole::Responder,
                    Config::default()
                )
                .await,
                Err(Error::InvalidFrame)
            );
            assert_eq!(transport.frames.len(), 1, "End was not consumed");
        }
    });
}

#[test]
fn large_valid_multiframe_inventory_completes_at_budget() {
    block_on(async {
        let store = KvStore::new(InMemoryBTree::<Vec<u8>, Vec<u8>>::new(), timestamp);
        let inventory: Vec<_> = (0..1024)
            .map(|n| Message::Inventory(vec![item(n, 8)]))
            .collect();
        let bytes = inventory
            .iter()
            .map(|message| {
                encode_frame(message, Config::default())
                    .expect("frame")
                    .len()
            })
            .sum();
        let mut transport = script(
            inventory
                .into_iter()
                .chain([Message::End, Message::End, Message::End, Message::Finished])
                .collect(),
        );
        synchronize(
            &store,
            &mut transport,
            SyncRole::Responder,
            Config {
                max_inventory_bytes: bytes,
                max_inventory_entries: 1024,
                ..Config::default()
            },
        )
        .await
        .expect("valid inventory at both aggregate limits");
        assert!(transport.frames.is_empty());
    });
}
