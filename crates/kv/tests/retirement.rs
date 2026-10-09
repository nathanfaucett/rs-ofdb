#![cfg(feature = "redb")]
use ofdb_kv::{Database, Value};

#[tokio::test]
async fn retirement_revokes_live_clients_and_transaction_and_preserves_neighbor() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let other = dir.path().join("other.redb");
    let database = Database::open_with_table(&path, "kv").expect("open named store");
    let client = database.client();
    let clone = client.clone();
    client
        .set("key", Value::Blob(vec![1]), None)
        .await
        .expect("write");
    let neighbor = Database::open_with_table(&other, "kv").expect("neighbor");
    neighbor
        .client()
        .set("keep", Value::Blob(vec![2]), None)
        .await
        .expect("write neighbor");
    let mut transaction = client.transaction().await.expect("live transaction");
    database.retire_and_remove().await.expect("remove resource");
    assert!(!path.exists());
    assert!(clone.get("key").await.is_err());
    assert!(clone.set("key", Value::Blob(vec![3]), None).await.is_err());
    assert!(clone.delete("key").await.is_err());
    assert!(clone.scan("a", "z").await.is_err());
    assert!(clone.scan_prefix("").await.is_err());
    assert!(clone.scan_all().await.is_err());
    assert!(database.client().transaction().await.is_err());
    assert!(
        transaction
            .set("late", Value::Blob(vec![3]), None)
            .await
            .is_err()
    );
    assert!(transaction.commit().await.is_err());
    database.retire_and_remove().await.expect("repeat");
    assert!(Database::open_with_table(&path, "kv").is_err());
    assert!(
        !path.exists(),
        "denied reopen must not recreate persistence"
    );
    Database::remove_retired(&path).expect("restart retry");
    assert_eq!(
        neighbor
            .client()
            .get("keep")
            .await
            .expect("neighbor retained"),
        Some(Value::Blob(vec![2]))
    );
}

#[tokio::test]
async fn admitted_queued_work_drains_after_idle_transaction_is_revoked() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let database = Database::open_with_table(&path, "kv").expect("open");
    let client = database.client();
    let transaction = client.transaction().await.expect("idle transaction");
    let mut queued = Box::pin(client.get("key"));
    assert!(futures::poll!(queued.as_mut()).is_pending());
    let (removed, queried) = tokio::join!(database.retire_and_remove(), queued);
    removed.expect("retire without blocking native thread");
    assert_eq!(queried.expect("admitted query finishes"), None);
    assert!(transaction.rollback().await.is_err());
}

#[tokio::test]
async fn failed_removal_is_fenced_and_retryable_without_touching_replacement() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let saved = dir.path().join("saved.redb");
    let database = Database::open_with_table(&path, "kv").expect("open");
    let client = database.client();
    std::fs::rename(&path, &saved).expect("inject replacement");
    std::fs::write(&path, b"unrelated replacement").expect("replacement");
    assert!(database.retire_and_remove().await.is_err());
    assert!(client.get("key").await.is_err());
    assert!(Database::open_with_table(&path, "kv").is_err());
    assert_eq!(
        std::fs::read(&path).expect("replacement retained"),
        b"unrelated replacement"
    );
    assert!(Database::remove_retired(&path).is_err());
    std::fs::remove_file(&path).expect("remove fixture replacement");
    std::fs::rename(&saved, &path).expect("restore exact file");
    database.retire_and_remove().await.expect("retry");
    drop(database);
    drop(client);
    Database::remove_retired(&path).expect("restart after completion");
    assert!(!path.exists());
}

#[tokio::test]
async fn opener_paused_before_native_construction_cannot_recreate_retired_resource() {
    struct PausedTable {
        entered: std::sync::mpsc::Sender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    }
    impl From<PausedTable> for String {
        fn from(table: PausedTable) -> Self {
            table.entered.send(()).expect("signal paused opener");
            table.resume.recv().expect("resume opener");
            "kv".to_owned()
        }
    }
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let database = Database::open_with_table(&path, "kv").expect("open");
    let (entered, paused) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    let opening_path = path.clone();
    let opener = std::thread::spawn(move || {
        Database::open_with_table(
            &opening_path,
            PausedTable {
                entered,
                resume: resumed,
            },
        )
    });
    paused
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("opener paused");
    database.retire_and_remove().await.expect("retire");
    resume.send(()).expect("resume opener");
    assert!(
        opener.join().expect("opener thread").is_err(),
        "a concurrent opener must not bypass the retirement fence"
    );
    assert!(
        !path.exists(),
        "denied opener must not recreate persistence"
    );
}

#[cfg(feature = "sync")]
#[tokio::test]
async fn checked_and_unchecked_sync_waits_cancel_on_retirement() {
    use ofdb_kv::{SyncConfig, SyncRole, SyncTransport};
    struct Pending;
    impl SyncTransport for Pending {
        type Error = ();
        async fn send(&mut self, _: Vec<u8>) -> Result<(), ()> {
            Ok(())
        }
        async fn receive(&mut self) -> Result<Vec<u8>, ()> {
            core::future::pending().await
        }
    }
    for checked in [false, true] {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("resource.redb");
        let database = Database::open_with_table(&path, "kv").expect("open");
        let mut transport = Pending;
        let mut sync = Box::pin(async {
            if checked {
                database
                    .synchronize_checked(
                        &mut transport,
                        SyncRole::Initiator,
                        SyncConfig::default(),
                        || async { Ok(()) },
                    )
                    .await
            } else {
                database
                    .synchronize(&mut transport, SyncRole::Initiator, SyncConfig::default())
                    .await
            }
        });
        assert!(futures::poll!(sync.as_mut()).is_pending());
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            database.client().set("local", Value::Blob(vec![1]), None),
        )
        .await
        .expect("network wait does not hold native write transaction")
        .expect("local work during sync");
        let (removed, synchronized) = tokio::join!(database.retire_and_remove(), sync);
        removed.expect("remove without transport progress");
        assert!(synchronized.is_err());
        assert!(database.export_snapshots().await.is_err());
        assert!(database.import_snapshots(vec![]).await.is_err());
        assert!(
            database
                .synchronize(&mut Pending, SyncRole::Initiator, SyncConfig::default())
                .await
                .is_err()
        );
    }
}

#[cfg(feature = "server")]
#[tokio::test]
async fn unmanaged_native_hosting_refuses_retirement_instead_of_claiming_handle_release() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let database = Database::open_with_table(&path, "kv").expect("open");
    let mut hosted = Box::pin(database.serve_tcp(
        "127.0.0.1:0".parse().expect("address"),
        core::future::pending(),
    ));
    assert!(futures::poll!(hosted.as_mut()).is_pending());
    assert_eq!(
        database
            .retire_and_remove()
            .await
            .expect_err("unmanaged hosting")
            .kind(),
        std::io::ErrorKind::Unsupported
    );
    database
        .client()
        .get("key")
        .await
        .expect("failed retirement leaves hosted owner active");
    drop(hosted);
    assert_eq!(
        database
            .retire_and_remove()
            .await
            .expect_err("backend handles may have escaped")
            .kind(),
        std::io::ErrorKind::Unsupported
    );
    assert!(path.exists());
}
