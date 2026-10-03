use std::time::Duration;

mod kv_common;

use std::sync::Arc;

use btree::InMemoryBTree;
use btree_redb::{Bytes, RedbByteBTree, table_definition};
use kv::KvStore;
use kv_client::Client;
use kv_common::{start_client_server, stop_server};
use kv_server::Server;
use redb::Database;

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

#[tokio::test]
async fn remote_client_supports_kv_operations_and_preserves_empty_values() {
    let backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
    let (client, shutdown, server) = start_client_server(backend).await;

    assert_eq!(
        client.get("missing".into()).await.expect("get missing key"),
        None
    );
    client
        .set("empty".into(), Vec::new(), None)
        .await
        .expect("set empty value");
    assert_eq!(
        client.get("empty".into()).await.expect("get empty value"),
        Some(Vec::new())
    );
    client
        .set("test:1234".into(), vec![1, 2], None)
        .await
        .expect("set prefixed value");
    client
        .set("prefix|get|5".into(), vec![3], None)
        .await
        .expect("set delimited prefix value");
    client
        .set("雪\0:key".into(), vec![4], None)
        .await
        .expect("set Unicode key with NUL");
    client
        .set("expired".into(), vec![5], Some(1))
        .await
        .expect("set expired value");
    client
        .set("future".into(), vec![6], Some(i64::MAX))
        .await
        .expect("set far-future value");
    client
        .set("remove-expiry".into(), vec![7], Some(1))
        .await
        .expect("set expiring value");
    client
        .set("remove-expiry".into(), vec![8], None)
        .await
        .expect("remove expiry");

    assert_eq!(
        client
            .scan_prefix("test:".into())
            .await
            .expect("scan literal prefix"),
        vec![("test:1234".into(), vec![1, 2])]
    );
    assert_eq!(
        client
            .scan_prefix("prefix|get|".into())
            .await
            .expect("scan delimited prefix"),
        vec![("prefix|get|5".into(), vec![3])]
    );
    assert_eq!(
        client
            .get("雪\0:key".into())
            .await
            .expect("get Unicode key"),
        Some(vec![4])
    );
    assert_eq!(
        client.get("expired".into()).await.expect("get expired key"),
        None
    );
    assert!(
        client
            .scan_prefix("expired".into())
            .await
            .expect("scan expired prefix")
            .is_empty()
    );
    assert!(
        client
            .scan("expired".into(), "expiredx".into())
            .await
            .expect("scan expired range")
            .is_empty()
    );
    assert_eq!(
        client.get("future".into()).await.expect("get future key"),
        Some(vec![6])
    );
    assert_eq!(
        client
            .get("remove-expiry".into())
            .await
            .expect("get key with removed expiry"),
        Some(vec![8])
    );
    assert_eq!(
        client
            .scan("empty".into(), "prefix".into())
            .await
            .expect("scan range"),
        vec![("empty".into(), Vec::new()), ("future".into(), vec![6])]
    );
    assert_eq!(
        client.scan_all().await.expect("scan all"),
        vec![
            ("empty".into(), Vec::new()),
            ("future".into(), vec![6]),
            ("prefix|get|5".into(), vec![3]),
            ("remove-expiry".into(), vec![8]),
            ("test:1234".into(), vec![1, 2]),
            ("雪\0:key".into(), vec![4]),
        ]
    );

    client.delete("test:1234".into()).await.expect("delete key");
    assert_eq!(
        client
            .get("test:1234".into())
            .await
            .expect("get deleted key"),
        None
    );
    client
        .set("test:1234".into(), vec![10], None)
        .await
        .expect("recreate deleted key");
    assert_eq!(
        client
            .get("test:1234".into())
            .await
            .expect("get recreated key"),
        Some(vec![10])
    );

    shutdown.send(()).expect("server is awaiting shutdown");
    stop_server(server).await;
}

#[tokio::test]
async fn remote_expiry_transition_and_stopped_server_fail_within_bounds() {
    let backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
    let (client, shutdown, server) = start_client_server(backend).await;
    let expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_millis() as i64
        + 250;
    client
        .set("short-lived".into(), vec![1], Some(expiry))
        .await
        .expect("set short-lived value");
    assert_eq!(
        client
            .get("short-lived".into())
            .await
            .expect("read live value"),
        Some(vec![1])
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(client.get("short-lived".into()).await, Ok(None)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("value should expire");

    shutdown.send(()).expect("server is awaiting shutdown");
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("server shutdown should finish")
        .expect("server task should complete");
    assert!(
        tokio::time::timeout(Duration::from_secs(3), client.get("short-lived".into()))
            .await
            .expect("client failure should be bounded")
            .is_err()
    );
}

#[tokio::test]
async fn redb_values_and_deletions_survive_server_restart() {
    const TABLE: &str = "remote-kv-restart";
    let directory = tempfile::tempdir().expect("create temporary Redb directory");
    let path = directory.path().join("kv.redb");
    let database = Arc::new(Database::create(&path).expect("create Redb database"));
    let write = database.begin_write().expect("begin table creation");
    write
        .open_table(table_definition::<Bytes, Vec<u8>>(TABLE))
        .expect("create KV table");
    write.commit().expect("commit table creation");

    let (client, shutdown, server) =
        start_client_server(RedbByteBTree::new(database.clone(), TABLE)).await;
    client
        .set("persisted".into(), vec![3, 4], None)
        .await
        .expect("set persisted value");
    client
        .set("deleted".into(), vec![5], None)
        .await
        .expect("set value to delete");
    client
        .delete("deleted".into())
        .await
        .expect("delete persisted key");
    shutdown.send(()).expect("server is awaiting shutdown");
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("first server should stop")
        .expect("first server task should complete");
    drop(client);
    drop(database);

    let database = Arc::new(Database::open(&path).expect("reopen Redb database"));
    let (client, shutdown, server) =
        start_client_server(RedbByteBTree::new(database.clone(), TABLE)).await;
    assert_eq!(
        client
            .get("persisted".into())
            .await
            .expect("read persisted value"),
        Some(vec![3, 4])
    );
    assert_eq!(
        client
            .get("deleted".into())
            .await
            .expect("read persisted tombstone"),
        None
    );
    shutdown.send(()).expect("server is awaiting shutdown");
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("reopened server should stop")
        .expect("reopened server task should complete");
    drop(client);
    drop(database);
}

#[cfg(unix)]
#[tokio::test]
async fn unix_client_server_supports_get_set_and_prefix_scan() {
    let directory = tempfile::tempdir().expect("create temporary socket directory");
    let socket = directory.path().join("kv.sock");
    let server_socket = socket.clone();
    let (shutdown, signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::unix(
            server_socket,
            KvStore::new(
                InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
                test_timestamp_provider,
            ),
        )
        .serve(async {
            let _ = signal.await;
        })
        .await
        .expect("Unix KV server should stop cleanly");
    });
    let client = Client::lazy_unix(socket.clone()).expect("valid Unix client");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if client.get("__readiness__".into()).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Unix KV server should become ready");

    client
        .set("unix:key".into(), vec![9], None)
        .await
        .expect("set value over Unix socket");
    assert_eq!(
        client
            .get("unix:key".into())
            .await
            .expect("get value over Unix socket"),
        Some(vec![9])
    );
    assert_eq!(
        client
            .scan_prefix("unix:".into())
            .await
            .expect("scan prefix over Unix socket"),
        vec![("unix:key".into(), vec![9])]
    );

    shutdown.send(()).expect("server is awaiting shutdown");
    stop_server(server).await;
}
