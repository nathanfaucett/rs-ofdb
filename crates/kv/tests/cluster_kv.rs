use std::{sync::Arc, time::Duration};

mod kv_common;

use btree::{BTree, InMemoryBTree};
use btree_redb::{Bytes, RedbByteBTree, table_definition};
use futures::join;
use kv_common::{start_client_server, stop_server};
use kv_store::KvStore;
use kv_sync::{
    Config, Error, KvSnapshot, Message, SyncRole, SyncTransport, encode_frame, synchronize,
};
use redb::Database;
use tokio::sync::mpsc;
use value::Value;

struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl SyncTransport for Channel {
    type Error = ();

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.recv().await.ok_or(())
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sender.send(frame).map_err(|_| ())
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

async fn run_sync<B>(
    store: &KvStore<B>,
    mut channel: Channel,
    role: SyncRole,
    config: Config,
) -> Result<(), Error<()>>
where
    B: BTree<Vec<u8>, Vec<u8>>,
{
    synchronize(store, &mut channel, role, config).await
}

async fn synchronize_pair<A, B>(left: &KvStore<A>, right: &KvStore<B>)
where
    A: BTree<Vec<u8>, Vec<u8>>,
    B: BTree<Vec<u8>, Vec<u8>>,
{
    let (left_sender, left_receiver) = mpsc::unbounded_channel();
    let (right_sender, right_receiver) = mpsc::unbounded_channel();
    let mut initiator = Channel {
        sender: left_sender,
        receiver: right_receiver,
    };
    let mut responder = Channel {
        sender: right_sender,
        receiver: left_receiver,
    };
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        join!(
            synchronize(left, &mut initiator, SyncRole::Initiator, Config::default()),
            synchronize(
                right,
                &mut responder,
                SyncRole::Responder,
                Config::default()
            )
        )
    })
    .await
    .expect("KV sync pair should finish");
    assert!(result.0.is_ok(), "initiator failed: {:?}", result.0);
    assert!(result.1.is_ok(), "responder failed: {:?}", result.1);
}

#[tokio::test]
async fn sync_exchanges_offline_writes_and_selects_greatest_generation() {
    let left = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let right = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );

    let mut tx = left.transaction().await.expect("start left transaction");
    tx.set("left-only", Value::Blob(vec![1]), None)
        .await
        .expect("set left key");
    tx.set("shared", Value::Blob(vec![2]), None)
        .await
        .expect("set left shared key");
    tx.commit().await.expect("commit left writes");

    let mut tx = right.transaction().await.expect("start right transaction");
    tx.set("right-only", Value::Blob(vec![3]), None)
        .await
        .expect("set right key");
    tx.set("shared", Value::Blob(vec![4]), None)
        .await
        .expect("set right shared key");
    tx.commit().await.expect("commit right writes");

    synchronize_pair(&left, &right).await;

    for store in [&left, &right] {
        let tx = store.transaction().await.expect("start verification");
        assert_eq!(
            tx.get("left-only", 0).await.expect("read left key"),
            Some(Value::Blob(vec![1]))
        );
        assert_eq!(
            tx.get("right-only", 0).await.expect("read right key"),
            Some(Value::Blob(vec![3]))
        );
        assert_eq!(
            tx.get("shared", 0).await.expect("read winner"),
            Some(Value::Blob(vec![4]))
        );
        tx.rollback().await.expect("finish verification");
    }
}

fn older_timestamp_provider() -> uuid::Timestamp {
    uuid::Timestamp::from_unix_time(1_700_000_000, 0, 0, 0)
}

fn newer_timestamp_provider() -> uuid::Timestamp {
    uuid::Timestamp::from_unix_time(1_700_000_001, 0, 0, 0)
}

#[tokio::test]
async fn the_greater_generation_wins_in_either_sync_direction() {
    for reverse in [false, true] {
        let left = KvStore::new(
            InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
            older_timestamp_provider,
        );
        let right = KvStore::new(
            InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
            newer_timestamp_provider,
        );
        for (store, value) in [
            (&left, Value::Blob(vec![1])),
            (&right, Value::Blob(vec![2])),
        ] {
            let mut tx = store.transaction().await.expect("start transaction");
            tx.set("same", value, None).await.expect("set value");
            tx.commit().await.expect("commit value");
        }
        if reverse {
            synchronize_pair(&right, &left).await;
        } else {
            synchronize_pair(&left, &right).await;
        }
        for store in [&left, &right] {
            let tx = store.transaction().await.expect("start verification");
            assert_eq!(
                tx.get("same", 0).await.expect("read winner"),
                Some(Value::Blob(vec![2]))
            );
            tx.rollback().await.expect("finish verification");
        }
    }
}

struct FailingChannel {
    channel: Channel,
    fail_send_at: usize,
    send_count: usize,
}

impl SyncTransport for FailingChannel {
    type Error = ();

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.channel.receive().await
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.send_count += 1;
        if self.send_count == self.fail_send_at {
            return Err(());
        }
        self.channel.send(frame).await
    }
}

#[tokio::test]
async fn failed_transport_session_can_be_repaired_by_a_fresh_session() {
    let source = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let destination = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let mut tx = source
        .transaction()
        .await
        .expect("start source transaction");
    tx.set("repair-me", Value::Blob(vec![4]), None)
        .await
        .expect("set key");
    tx.commit().await.expect("commit source key");

    let (left_sender, left_receiver) = mpsc::unbounded_channel();
    let (right_sender, right_receiver) = mpsc::unbounded_channel();
    let mut initiator = FailingChannel {
        channel: Channel {
            sender: left_sender,
            receiver: right_receiver,
        },
        fail_send_at: 3,
        send_count: 0,
    };
    let mut responder = Channel {
        sender: right_sender,
        receiver: left_receiver,
    };
    let failed = tokio::time::timeout(Duration::from_secs(3), async {
        join!(
            synchronize(
                &source,
                &mut initiator,
                SyncRole::Initiator,
                Config::default()
            ),
            synchronize(
                &destination,
                &mut responder,
                SyncRole::Responder,
                Config::default()
            )
        )
    })
    .await;
    assert!(
        failed.is_err(),
        "peer should be bounded after the failed frame"
    );
    drop((initiator, responder));

    let destination_tx = destination
        .transaction()
        .await
        .expect("inspect partial state");
    let partial_value = destination_tx
        .get("repair-me", 0)
        .await
        .expect("read partial state");
    destination_tx
        .rollback()
        .await
        .expect("finish partial-state read");
    assert!(partial_value.is_none() || partial_value == Some(Value::Blob(vec![4])));

    synchronize_pair(&source, &destination).await;
    let tx = destination
        .transaction()
        .await
        .expect("verify repaired state");
    assert_eq!(
        tx.get("repair-me", 0).await.expect("read repaired value"),
        Some(Value::Blob(vec![4]))
    );
    tx.rollback().await.expect("finish verification");
}

#[tokio::test]
async fn delayed_snapshot_cannot_restore_a_deleted_generation() {
    let source = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let destination = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let mut tx = source
        .transaction()
        .await
        .expect("start source transaction");
    tx.set("key", Value::Blob(vec![1]), None)
        .await
        .expect("set original value");
    let stale = tx
        .export_snapshot("key")
        .await
        .expect("export stale snapshot")
        .expect("snapshot exists");
    tx.delete("key").await.expect("delete original value");
    tx.set("key", Value::Blob(vec![2]), None)
        .await
        .expect("recreate key");
    tx.commit().await.expect("commit recreated key");
    synchronize_pair(&source, &destination).await;

    let mut tx = destination.transaction().await.expect("start stale import");
    tx.import_snapshot(stale)
        .await
        .expect("ignore stale snapshot");
    assert_eq!(
        tx.get("key", 0).await.expect("read current value"),
        Some(Value::Blob(vec![2]))
    );
    tx.rollback().await.expect("finish stale import");
}

#[tokio::test]
async fn divergent_same_generation_history_converges() {
    let seed = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let mut tx = seed.transaction().await.expect("start seed transaction");
    tx.set("fork", Value::Blob(vec![0]), None)
        .await
        .expect("set common value");
    let snapshot = tx
        .export_snapshot("fork")
        .await
        .expect("export common snapshot")
        .expect("snapshot exists");
    tx.rollback().await.expect("finish seed transaction");

    let left = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let right = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    for (store, value) in [
        (&left, Value::Blob(vec![1])),
        (&right, Value::Blob(vec![2])),
    ] {
        let mut tx = store.transaction().await.expect("start branch transaction");
        tx.import_snapshot(snapshot.clone())
            .await
            .expect("import common history");
        tx.commit().await.expect("commit common history");
        let mut tx = store.transaction().await.expect("start fork transaction");
        tx.set("fork", value, None)
            .await
            .expect("create divergent history");
        tx.commit().await.expect("commit divergent history");
    }

    synchronize_pair(&left, &right).await;
    let left_tx = left.transaction().await.expect("read left merge");
    let right_tx = right.transaction().await.expect("read right merge");
    assert_eq!(
        left_tx.get("fork", 0).await.expect("left value"),
        right_tx.get("fork", 0).await.expect("right value")
    );
    assert_eq!(
        left_tx.export_snapshots().await.expect("left history"),
        right_tx.export_snapshots().await.expect("right history")
    );
    left_tx.rollback().await.expect("finish left read");
    right_tx.rollback().await.expect("finish right read");
    synchronize_pair(&right, &left).await;
}

#[tokio::test]
async fn invalid_snapshot_batch_is_rejected_without_partial_import() {
    let source = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let mut tx = source
        .transaction()
        .await
        .expect("start source transaction");
    tx.set("valid", Value::Blob(vec![1]), None)
        .await
        .expect("set valid value");
    let (key, payload) = tx
        .export_snapshot("valid")
        .await
        .expect("export valid snapshot")
        .expect("snapshot exists");
    tx.commit().await.expect("commit source value");
    let valid = KvSnapshot {
        key: key.clone(),
        payload,
    };
    let invalid = KvSnapshot {
        key,
        payload: vec![0xff],
    };
    let destination = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let (peer_sender, peer_receiver) = mpsc::unbounded_channel();
    let (out_sender, _out_receiver) = mpsc::unbounded_channel();
    peer_sender
        .send(
            encode_frame(&Message::Hello { version: 2 }, Config::default()).expect("encode hello"),
        )
        .expect("queue hello");
    peer_sender
        .send(
            encode_frame(&Message::Snapshots(vec![valid, invalid]), Config::default())
                .expect("encode invalid batch"),
        )
        .expect("queue invalid batch");
    let mut responder = Channel {
        sender: out_sender,
        receiver: peer_receiver,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        synchronize(
            &destination,
            &mut responder,
            SyncRole::Responder,
            Config::default(),
        ),
    )
    .await
    .expect("invalid batch handling should finish");
    assert!(matches!(result, Err(Error::Storage(_))));
    let tx = destination
        .transaction()
        .await
        .expect("check batch rollback");
    assert_eq!(
        tx.get("valid", 0)
            .await
            .expect("read uncommitted batch value"),
        None
    );
    tx.rollback().await.expect("finish batch check");
}

#[tokio::test]
async fn frame_limit_rejects_session_without_applying_data() {
    let source = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let destination = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let mut tx = source
        .transaction()
        .await
        .expect("start source transaction");
    tx.set("limited", Value::Blob(vec![1]), None)
        .await
        .expect("set source value");
    tx.commit().await.expect("commit source value");
    let (left_sender, left_receiver) = mpsc::unbounded_channel();
    let (right_sender, right_receiver) = mpsc::unbounded_channel();
    let initiator = Channel {
        sender: left_sender,
        receiver: right_receiver,
    };
    let responder = Channel {
        sender: right_sender,
        receiver: left_receiver,
    };
    let config = Config {
        max_frame_bytes: 1,
        ..Config::default()
    };
    let results = tokio::time::timeout(Duration::from_secs(2), async {
        join!(
            run_sync(&source, initiator, SyncRole::Initiator, config),
            run_sync(&destination, responder, SyncRole::Responder, config)
        )
    })
    .await
    .expect("small frame rejection should finish");
    assert!(matches!(results.0, Err(Error::FrameTooLarge)));
    let tx = destination
        .transaction()
        .await
        .expect("check frame rejection");
    assert_eq!(
        tx.get("limited", 0).await.expect("read rejected value"),
        None
    );
    tx.rollback().await.expect("finish frame-limit check");
}

#[tokio::test]
async fn sync_transfers_empty_values_tombstones_and_expired_snapshots() {
    let left = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let right = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );

    let mut tx = left.transaction().await.expect("start source transaction");
    tx.set("empty", Value::Blob(Vec::new()), None)
        .await
        .expect("set empty value");
    tx.set("deleted", Value::Blob(vec![1]), None)
        .await
        .expect("set soon-to-be-deleted value");
    tx.delete("deleted").await.expect("delete value");
    tx.set("expired", Value::Blob(vec![2]), Some(10))
        .await
        .expect("set expired value");
    tx.commit().await.expect("commit source state");

    synchronize_pair(&left, &right).await;

    for store in [&left, &right] {
        let tx = store.transaction().await.expect("start verification");
        assert_eq!(
            tx.get("empty", 0).await.expect("read empty value"),
            Some(Value::Blob(vec![]))
        );
        assert_eq!(tx.get("deleted", 0).await.expect("read tombstone"), None);
        assert_eq!(
            tx.get("expired", 10).await.expect("read expired value"),
            None
        );
        let snapshots = tx.export_snapshots().await.expect("export snapshots");
        assert_eq!(snapshots.len(), 3);
        tx.rollback().await.expect("finish verification");
    }

    let left_tx = left.transaction().await.expect("start left snapshot check");
    let left_snapshots = left_tx
        .export_snapshots()
        .await
        .expect("export left snapshots");
    left_tx
        .rollback()
        .await
        .expect("finish left snapshot check");
    let right_tx = right
        .transaction()
        .await
        .expect("start right snapshot check");
    let right_snapshots = right_tx
        .export_snapshots()
        .await
        .expect("export right snapshots");
    right_tx
        .rollback()
        .await
        .expect("finish right snapshot check");
    assert_eq!(left_snapshots, right_snapshots);

    synchronize_pair(&left, &right).await;
    let tx = right
        .transaction()
        .await
        .expect("start repeated sync check");
    assert_eq!(
        tx.export_snapshots()
            .await
            .expect("export repeated snapshots"),
        left_snapshots
    );
    tx.rollback().await.expect("finish repeated sync check");
}

#[tokio::test]
async fn three_stores_replicate_state_and_tombstones_in_stages() {
    let a = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let b = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );
    let c = KvStore::new(
        InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
        test_timestamp_provider,
    );

    let mut tx = a.transaction().await.expect("start A transaction");
    tx.set("shared", Value::Blob(vec![1]), None)
        .await
        .expect("set A value");
    tx.commit().await.expect("commit A value");
    synchronize_pair(&a, &b).await;
    synchronize_pair(&b, &c).await;

    let mut tx = a.transaction().await.expect("start A delete transaction");
    tx.delete("shared").await.expect("delete A value");
    tx.commit().await.expect("commit A deletion");
    synchronize_pair(&a, &b).await;
    synchronize_pair(&b, &c).await;

    for store in [&a, &b, &c] {
        let tx = store.transaction().await.expect("start verification");
        assert_eq!(tx.get("shared", 0).await.expect("read tombstone"), None);
        assert_eq!(
            tx.export_snapshots().await.expect("export snapshots").len(),
            1
        );
        tx.rollback().await.expect("finish verification");
    }
}

#[tokio::test]
async fn redb_replica_restarts_and_syncs_again_through_grpc() {
    const TABLE: &str = "cluster-kv-restart";
    let directory = tempfile::tempdir().expect("create Redb directory");
    let path = directory.path().join("kv.redb");
    let database = Arc::new(Database::create(&path).expect("create Redb database"));
    let table_tx = database.begin_write().expect("begin table creation");
    table_tx
        .open_table(table_definition::<Bytes, Vec<u8>>(TABLE))
        .expect("create KV table");
    table_tx.commit().expect("commit table creation");

    let source_backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
    let source = KvStore::new(source_backend.clone(), test_timestamp_provider);
    let redb_backend = RedbByteBTree::new(database.clone(), TABLE);
    let redb_store = KvStore::new(redb_backend.clone(), test_timestamp_provider);
    let (source_client, source_shutdown, source_server) = start_client_server(source_backend).await;
    let (replica_client, replica_shutdown, replica_server) =
        start_client_server(redb_backend).await;
    source_client
        .set("durable".into(), Value::Blob(vec![6]), None)
        .await
        .expect("write source value");
    synchronize_pair(&source, &redb_store).await;
    assert_eq!(
        replica_client
            .get("durable".into())
            .await
            .expect("read synced value"),
        Some(Value::Blob(vec![6]))
    );

    replica_shutdown
        .send(())
        .expect("replica server is running");
    tokio::time::timeout(Duration::from_secs(3), replica_server)
        .await
        .expect("replica should stop")
        .expect("replica task should complete");
    drop(replica_client);
    drop(redb_store);
    drop(database);

    let database = Arc::new(Database::open(&path).expect("reopen Redb database"));
    let redb_backend = RedbByteBTree::new(database.clone(), TABLE);
    let redb_store = KvStore::new(redb_backend.clone(), test_timestamp_provider);
    let (replica_client, replica_shutdown, replica_server) =
        start_client_server(redb_backend).await;
    assert_eq!(
        replica_client
            .get("durable".into())
            .await
            .expect("read after restart"),
        Some(Value::Blob(vec![6]))
    );
    source_client
        .delete("durable".into())
        .await
        .expect("delete source value");
    synchronize_pair(&source, &redb_store).await;
    assert_eq!(
        replica_client
            .get("durable".into())
            .await
            .expect("read replicated deletion"),
        None
    );

    source_shutdown.send(()).expect("source server is running");
    replica_shutdown
        .send(())
        .expect("replica server is running");
    tokio::time::timeout(Duration::from_secs(3), source_server)
        .await
        .expect("source should stop")
        .expect("source task should complete");
    tokio::time::timeout(Duration::from_secs(3), replica_server)
        .await
        .expect("replica should stop")
        .expect("replica task should complete");
    drop(source_client);
    drop(replica_client);
    drop(redb_store);
    drop(database);
}

#[tokio::test]
async fn client_writes_replicate_to_peer_client() {
    let left_backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
    let right_backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
    let left_store = KvStore::new(left_backend.clone(), test_timestamp_provider);
    let right_store = KvStore::new(right_backend.clone(), test_timestamp_provider);
    let (left_client, left_shutdown, left_server) = start_client_server(left_backend).await;
    let (right_client, right_shutdown, right_server) = start_client_server(right_backend).await;

    left_client
        .set("replicated".into(), Value::Blob(vec![7, 8]), None)
        .await
        .expect("write through left client");
    synchronize_pair(&left_store, &right_store).await;
    assert_eq!(
        right_client
            .get("replicated".into())
            .await
            .expect("read replicated value"),
        Some(Value::Blob(vec![7, 8]))
    );
    assert_eq!(
        right_client
            .scan_prefix("rep".into())
            .await
            .expect("scan replicated prefix"),
        vec![("replicated".into(), Value::Blob(vec![7, 8]))]
    );

    right_client
        .delete("replicated".into())
        .await
        .expect("delete through right client");
    synchronize_pair(&right_store, &left_store).await;
    assert_eq!(
        left_client
            .get("replicated".into())
            .await
            .expect("read replicated deletion"),
        None
    );

    left_shutdown.send(()).expect("left server is running");
    right_shutdown.send(()).expect("right server is running");
    stop_server(left_server).await;
    stop_server(right_server).await;
}

#[tokio::test]
async fn repeated_sessions_merge_nested_maps_lists_and_delete_update_races() {
    use value::{JsonNumber, JsonValue};

    fn state(a: i64, b: i64) -> Value {
        let number = |value| JsonValue::Number(JsonNumber::I64(value));
        Value::Json(JsonValue::Object(
            [
                (
                    "nested".into(),
                    JsonValue::Object([("a".into(), number(a)), ("b".into(), number(b))].into()),
                ),
                ("list".into(), JsonValue::Array(vec![number(a), number(b)])),
            ]
            .into(),
        ))
    }
    let left = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
    let right = KvStore::new(InMemoryBTree::new(), test_timestamp_provider);
    let mut tx = left.transaction().await.expect("start seed write");
    tx.set("key", state(0, 0), None)
        .await
        .expect("write seed map");
    tx.commit().await.expect("commit seed");
    synchronize_pair(&left, &right).await;
    for round in 1..=3 {
        for (store, value) in [
            (&left, state(round, round - 1)),
            (&right, state(round - 1, round)),
        ] {
            let mut tx = store.transaction().await.expect("start branch write");
            tx.set("key", value, None).await.expect("edit branch");
            tx.commit().await.expect("commit branch");
        }
        if round % 2 == 0 {
            synchronize_pair(&right, &left).await;
        } else {
            synchronize_pair(&left, &right).await;
        }
        for store in [&left, &right] {
            let tx = store.transaction().await.expect("verify merged map");
            assert_eq!(
                tx.get("key", 0).await.expect("read map"),
                Some(state(round, round))
            );
            tx.rollback().await.expect("finish verification");
        }
        synchronize_pair(&left, &right).await;
    }
    let mut tx = left.transaction().await.expect("start delete");
    tx.delete("key").await.expect("delete generation");
    tx.commit().await.expect("commit deletion");
    let mut tx = right.transaction().await.expect("start concurrent update");
    tx.set("key", state(4, 4), Some(100))
        .await
        .expect("edit deleted generation offline");
    tx.commit().await.expect("commit concurrent update");
    for _ in 0..2 {
        synchronize_pair(&right, &left).await;
        for store in [&left, &right] {
            let tx = store.transaction().await.expect("verify delete wins");
            assert_eq!(tx.get("key", 0).await.expect("read deleted key"), None);
            assert!(tx.scan_all(0).await.expect("scan deleted key").is_empty());
            tx.rollback().await.expect("finish deleted read");
        }
    }
}
