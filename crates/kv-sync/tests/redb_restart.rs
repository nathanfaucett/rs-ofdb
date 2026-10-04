use std::sync::Arc;

use btree::{BTree, BTreeResult, InMemoryBTree};
use btree_redb::{Bytes, RedbByteBTree, table_definition};
use futures::{SinkExt, executor::block_on, join, stream::StreamExt};
use futures_channel::mpsc;
use kv::KvStore;
use ofdb_kv_sync::{Config, SyncRole, SyncTransport, synchronize};
use redb::Database;
use uuid::Uuid;
use value::Value;

const TABLE: &str = "kv-sync";

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

fn redb_store(path: &std::path::Path, create: bool) -> (Arc<Database>, KvStore<RedbByteBTree>) {
    let database = Arc::new(if create {
        Database::create(path).unwrap()
    } else {
        Database::open(path).unwrap()
    });
    if create {
        let transaction = database.begin_write().unwrap();
        transaction
            .open_table(table_definition::<Bytes, Vec<u8>>(TABLE))
            .unwrap();
        transaction.commit().unwrap();
    }
    let store = KvStore::new(
        RedbByteBTree::new(database.clone(), TABLE),
        test_timestamp_provider,
    );
    (database, store)
}

async fn synchronize_pair<A, B>(left: &KvStore<A>, right: &KvStore<B>) -> BTreeResult<()>
where
    A: BTree<Vec<u8>, Vec<u8>>,
    B: BTree<Vec<u8>, Vec<u8>>,
{
    let (left_sender, left_receiver) = mpsc::unbounded();
    let (right_sender, right_receiver) = mpsc::unbounded();
    let mut initiator = Channel {
        sender: left_sender,
        receiver: right_receiver,
    };
    let mut responder = Channel {
        sender: right_sender,
        receiver: left_receiver,
    };
    let (left_result, right_result) = join!(
        synchronize(left, &mut initiator, SyncRole::Initiator, Config::default()),
        synchronize(
            right,
            &mut responder,
            SyncRole::Responder,
            Config::default()
        )
    );
    left_result.map_err(|error| btree::BTreeError::custom(format!("{error:?}")))?;
    right_result.map_err(|error| btree::BTreeError::custom(format!("{error:?}")))?;
    Ok(())
}

#[test]
fn sync_retries_after_redb_peer_reopen() {
    block_on(async {
        let path = std::env::temp_dir().join(format!("kv-sync-{}.db", Uuid::now_v7()));
        let source = KvStore::new(
            InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
            test_timestamp_provider,
        );
        let mut source_tx = source.transaction().await.unwrap();
        source_tx
            .set("value", Value::Blob(vec![9]), None)
            .await
            .unwrap();
        source_tx
            .set("deleted", Value::Blob(vec![1]), None)
            .await
            .unwrap();
        source_tx.delete("deleted").await.unwrap();
        source_tx.commit().await.unwrap();

        let (database, destination) = redb_store(&path, true);
        synchronize_pair(&source, &destination).await.unwrap();
        drop(destination);
        drop(database);

        let (database, reopened) = redb_store(&path, false);
        synchronize_pair(&source, &reopened).await.unwrap();
        let transaction = reopened.transaction().await.unwrap();
        assert_eq!(
            transaction.get("value", 0).await.unwrap(),
            Some(Value::Blob(vec![9]))
        );
        assert_eq!(transaction.get("deleted", 0).await.unwrap(), None);
        transaction.rollback().await.unwrap();
        drop(reopened);
        drop(database);
        std::fs::remove_file(path).unwrap();
    });
}
