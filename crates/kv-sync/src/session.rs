use btree::{BTree, BTreeResult};
use kv::KvStore;

use crate::{
    Config, Error, KvSnapshot, Message, SyncRole, SyncTransport, decode_frame, encode_frame,
};

const VERSION: u8 = 1;

pub async fn synchronize<B, T>(
    store: &KvStore<B>,
    transport: &mut T,
    role: SyncRole,
    config: Config,
) -> Result<(), Error<T::Error>>
where
    B: BTree<Vec<u8>, Vec<u8>>,
    T: SyncTransport,
{
    let snapshots = export(store).await.map_err(storage_error)?;
    match role {
        SyncRole::Initiator => {
            send(transport, Message::Hello { version: VERSION }, config).await?;
            expect_hello(receive(transport, config).await?)?;
            send_snapshots(transport, snapshots, config).await?;
            receive_snapshots(store, transport, config).await?;
            send(transport, Message::Finished, config).await?;
            expect_finished(receive(transport, config).await?)?;
        }
        SyncRole::Responder => {
            expect_hello(receive(transport, config).await?)?;
            send(transport, Message::Hello { version: VERSION }, config).await?;
            receive_snapshots(store, transport, config).await?;
            send_snapshots(transport, snapshots, config).await?;
            expect_finished(receive(transport, config).await?)?;
            send(transport, Message::Finished, config).await?;
        }
    }
    Ok(())
}

async fn export<B: BTree<Vec<u8>, Vec<u8>>>(store: &KvStore<B>) -> BTreeResult<Vec<KvSnapshot>> {
    let transaction = store.transaction().await?;
    let snapshots = transaction
        .export_snapshots()
        .await?
        .into_iter()
        .map(|(key, payload)| KvSnapshot { key, payload })
        .collect();
    transaction.rollback().await?;
    Ok(snapshots)
}

async fn receive_snapshots<B, T>(
    store: &KvStore<B>,
    transport: &mut T,
    config: Config,
) -> Result<(), Error<T::Error>>
where
    B: BTree<Vec<u8>, Vec<u8>>,
    T: SyncTransport,
{
    loop {
        match receive(transport, config).await? {
            Message::Snapshots(snapshots) => import_batch(store, snapshots)
                .await
                .map_err(storage_error)?,
            Message::End => return Ok(()),
            _ => return Err(Error::UnexpectedMessage),
        }
    }
}

async fn import_batch<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    snapshots: Vec<KvSnapshot>,
) -> BTreeResult<()> {
    let mut transaction = store.transaction().await?;
    for snapshot in snapshots {
        if let Err(error) = transaction
            .import_snapshot((snapshot.key, snapshot.payload))
            .await
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }
    }
    transaction.commit().await
}

async fn send_snapshots<T: SyncTransport>(
    transport: &mut T,
    snapshots: Vec<KvSnapshot>,
    config: Config,
) -> Result<(), Error<T::Error>> {
    for batch in snapshots.chunks(config.max_snapshots_per_batch.max(1)) {
        send(transport, Message::Snapshots(batch.to_vec()), config).await?;
    }
    send(transport, Message::End, config).await
}

async fn send<T: SyncTransport>(
    transport: &mut T,
    message: Message,
    config: Config,
) -> Result<(), Error<T::Error>> {
    let frame = encode_frame(&message, config).map_err(|error| match error {
        Error::FrameTooLarge => Error::FrameTooLarge,
        _ => Error::InvalidFrame,
    })?;
    transport.send(frame).await.map_err(Error::Transport)
}

async fn receive<T: SyncTransport>(
    transport: &mut T,
    config: Config,
) -> Result<Message, Error<T::Error>> {
    let frame = transport.receive().await.map_err(Error::Transport)?;
    decode_frame(&frame, config)
}

fn expect_finished<E>(message: Message) -> Result<(), Error<E>> {
    match message {
        Message::Finished => Ok(()),
        _ => Err(Error::UnexpectedMessage),
    }
}

fn expect_hello<E>(message: Message) -> Result<(), Error<E>> {
    match message {
        Message::Hello { version: VERSION } => Ok(()),
        Message::Hello { version } => Err(Error::UnsupportedVersion(version)),
        _ => Err(Error::UnexpectedMessage),
    }
}

fn storage_error<E: core::fmt::Debug, T>(error: E) -> Error<T> {
    Error::Storage(format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use btree::InMemoryBTree;
    use futures::{SinkExt, executor::block_on, join, stream::StreamExt};
    use futures_channel::mpsc;
    use kv::decode_document_id;

    use super::*;

    fn test_timestamp_provider() -> uuid::Timestamp {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

        let millis = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        uuid::Timestamp::from_unix_time(
            1_700_000_000 + millis / 1_000,
            (millis % 1_000) as u32 * 1_000_000,
            0,
            0,
        )
    }

    struct Channel {
        sender: mpsc::UnboundedSender<Vec<u8>>,
        receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    }

    impl SyncTransport for Channel {
        type Error = ();

        async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
            self.receiver.next().await.ok_or(())
        }

        async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
            self.sender.send(frame).await.map_err(|_| ())
        }
    }

    #[test]
    fn failed_received_batch_rolls_back_all_snapshots() {
        block_on(async {
            let source = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
            let mut tx = source.transaction().await.unwrap();
            tx.set("valid", b"value".to_vec(), None).await.unwrap();
            let (key, payload) = tx.export_snapshot("valid").await.unwrap().unwrap();
            let snapshot = KvSnapshot { key, payload };
            tx.commit().await.unwrap();
            let mut invalid = snapshot.clone();
            invalid.payload = vec![0xff];

            let (peer_sender, peer_receiver) = mpsc::unbounded();
            let (out_sender, mut out_receiver) = mpsc::unbounded();
            peer_sender
                .unbounded_send(
                    encode_frame(&Message::Hello { version: VERSION }, Config::default()).unwrap(),
                )
                .unwrap();
            peer_sender
                .unbounded_send(
                    encode_frame(
                        &Message::Snapshots(vec![snapshot, invalid]),
                        Config::default(),
                    )
                    .unwrap(),
                )
                .unwrap();
            let mut responder = Channel {
                sender: out_sender,
                receiver: peer_receiver,
            };
            let destination = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
            let result = synchronize(
                &destination,
                &mut responder,
                SyncRole::Responder,
                Config::default(),
            )
            .await;
            assert!(matches!(result, Err(Error::Storage(_))));
            assert_eq!(
                decode_frame::<()>(&out_receiver.next().await.unwrap(), Config::default()).unwrap(),
                Message::Hello { version: VERSION }
            );
            let tx = destination.transaction().await.unwrap();
            assert_eq!(tx.get("valid", i64::MIN).await.unwrap(), None);
            tx.rollback().await.unwrap();
        });
    }

    #[test]
    fn exchanges_snapshots_in_both_directions_and_repeats_idempotently() {
        block_on(async {
            let left = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
            let right = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
            let mut tx = left.transaction().await.unwrap();
            tx.set("left", b"L".to_vec(), None).await.unwrap();
            tx.set("expired", b"E".to_vec(), Some(5)).await.unwrap();
            tx.commit().await.unwrap();

            let mut tx = right.transaction().await.unwrap();
            tx.set("right", b"R".to_vec(), None).await.unwrap();
            tx.set("deleted", b"D".to_vec(), None).await.unwrap();
            tx.delete("deleted").await.unwrap();
            tx.set("recreated", b"old".to_vec(), None).await.unwrap();
            tx.delete("recreated").await.unwrap();
            tx.set("recreated", b"new".to_vec(), None).await.unwrap();
            tx.commit().await.unwrap();

            for round in 0..2 {
                let (left_tx, left_rx) = mpsc::unbounded();
                let (right_tx, right_rx) = mpsc::unbounded();
                let mut initiator = Channel {
                    sender: left_tx,
                    receiver: right_rx,
                };
                let mut responder = Channel {
                    sender: right_tx,
                    receiver: left_rx,
                };
                let (a, b) = join!(
                    synchronize(
                        &left,
                        &mut initiator,
                        SyncRole::Initiator,
                        Config::default()
                    ),
                    synchronize(
                        &right,
                        &mut responder,
                        SyncRole::Responder,
                        Config::default()
                    )
                );
                a.unwrap();
                b.unwrap();
                if round == 0 {
                    let mut tx = left.transaction().await.unwrap();
                    tx.set("left", b"updated".to_vec(), None).await.unwrap();
                    tx.commit().await.unwrap();
                }
            }

            let tx = left.transaction().await.unwrap();
            assert_eq!(
                tx.get("right", i64::MIN).await.unwrap(),
                Some(b"R".to_vec())
            );
            assert_eq!(tx.get("deleted", 0).await.unwrap(), None);
            assert_eq!(tx.get("recreated", 0).await.unwrap(), Some(b"new".to_vec()));
            assert_eq!(tx.get("expired", 5).await.unwrap(), None);
            let snapshots = tx.export_snapshots().await.unwrap();
            assert_eq!(snapshots.len(), 5);
            assert!(
                snapshots
                    .iter()
                    .all(|(key, _)| decode_document_id(&key.id).is_ok())
            );
            tx.rollback().await.unwrap();
            let tx = right.transaction().await.unwrap();
            assert_eq!(
                tx.get("left", i64::MIN).await.unwrap(),
                Some(b"updated".to_vec())
            );
            assert_eq!(tx.get("deleted", 0).await.unwrap(), None);
            assert_eq!(tx.get("recreated", 0).await.unwrap(), Some(b"new".to_vec()));
            assert_eq!(tx.get("expired", 5).await.unwrap(), None);
            assert_eq!(snapshots, tx.export_snapshots().await.unwrap());
            tx.rollback().await.unwrap();
        });
    }

    #[test]
    fn dropped_transport_and_malformed_frames_fail_explicitly() {
        block_on(async {
            let store = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
            let (out_sender, _out_receiver) = mpsc::unbounded();
            let (_closed_sender, closed_receiver) = mpsc::unbounded();
            drop(_closed_sender);
            let mut dropped = Channel {
                sender: out_sender,
                receiver: closed_receiver,
            };
            assert!(matches!(
                synchronize(&store, &mut dropped, SyncRole::Responder, Config::default()).await,
                Err(Error::Transport(()))
            ));

            let (input_sender, input_receiver) = mpsc::unbounded();
            let (out_sender, _out_receiver) = mpsc::unbounded();
            input_sender.unbounded_send(vec![0xff]).unwrap();
            let mut malformed = Channel {
                sender: out_sender,
                receiver: input_receiver,
            };
            assert!(matches!(
                synchronize(
                    &store,
                    &mut malformed,
                    SyncRole::Responder,
                    Config::default()
                )
                .await,
                Err(Error::InvalidFrame)
            ));

            let (input_sender, input_receiver) = mpsc::unbounded();
            let (out_sender, _out_receiver) = mpsc::unbounded();
            let hello =
                encode_frame(&Message::Hello { version: VERSION }, Config::default()).unwrap();
            input_sender.unbounded_send(hello.clone()).unwrap();
            input_sender.unbounded_send(hello).unwrap();
            let mut duplicate_control = Channel {
                sender: out_sender,
                receiver: input_receiver,
            };
            assert!(matches!(
                synchronize(
                    &store,
                    &mut duplicate_control,
                    SyncRole::Responder,
                    Config::default()
                )
                .await,
                Err(Error::UnexpectedMessage)
            ));

            let (input_sender, input_receiver) = mpsc::unbounded();
            let (out_sender, _out_receiver) = mpsc::unbounded();
            for message in [
                Message::Hello { version: VERSION },
                Message::End,
                Message::End,
            ] {
                input_sender
                    .unbounded_send(encode_frame(&message, Config::default()).unwrap())
                    .unwrap();
            }
            let mut duplicate_end = Channel {
                sender: out_sender,
                receiver: input_receiver,
            };
            assert!(matches!(
                synchronize(
                    &store,
                    &mut duplicate_end,
                    SyncRole::Responder,
                    Config::default()
                )
                .await,
                Err(Error::UnexpectedMessage)
            ));
        });
    }

    #[test]
    fn empty_peers_complete_a_session() {
        block_on(async {
            let left = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
            let right = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
            let (left_tx, left_rx) = mpsc::unbounded();
            let (right_tx, right_rx) = mpsc::unbounded();
            let mut initiator = Channel {
                sender: left_tx,
                receiver: right_rx,
            };
            let mut responder = Channel {
                sender: right_tx,
                receiver: left_rx,
            };
            let (a, b) = join!(
                synchronize(
                    &left,
                    &mut initiator,
                    SyncRole::Initiator,
                    Config::default()
                ),
                synchronize(
                    &right,
                    &mut responder,
                    SyncRole::Responder,
                    Config::default()
                )
            );
            a.unwrap();
            b.unwrap();
        });
    }
}
