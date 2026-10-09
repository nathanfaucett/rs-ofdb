#![cfg(all(feature = "redb", feature = "sql"))]
use ofdb_sql::{Database, Row};

#[tokio::test]
async fn retirement_revokes_database_client_clones_and_live_transaction() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let other = dir.path().join("neighbor.redb");
    let database = Database::open(&path).expect("open resource");
    let clone = database.clone();
    let client = database.client();
    let client_clone = client.clone();
    client
        .execute_sql("CREATE TABLE data (id UUID PRIMARY KEY, value TEXT)", None)
        .await
        .expect("schema");
    let neighbor = Database::open(&other).expect("neighbor");
    neighbor
        .client()
        .execute_sql("CREATE TABLE keep (id UUID PRIMARY KEY, value TEXT)", None)
        .await
        .expect("neighbor schema");
    #[cfg(feature = "sync")]
    let unit = database
        .export_sync_state()
        .await
        .expect("state before retirement")
        .into_iter()
        .next()
        .expect("schema state unit");
    let mut transaction = client.transaction().await.expect("live transaction");
    database.retire_and_remove().await.expect("remove");
    assert!(!path.exists());
    assert!(
        client_clone
            .execute_sql("SELECT * FROM data", None)
            .await
            .is_err()
    );
    assert!(clone.client().transaction().await.is_err());
    assert!(
        transaction
            .execute_sql("SELECT * FROM data", None)
            .await
            .is_err()
    );
    assert!(
        transaction
            .row_conflicts("data", &Row::new(vec![]))
            .await
            .is_err()
    );
    assert!(
        transaction
            .row_conflict_values("data", &Row::new(vec![]))
            .await
            .is_err()
    );
    assert!(transaction.commit().await.is_err());
    #[cfg(feature = "sync")]
    {
        assert!(database.sync_manifest().await.is_err());
        assert!(database.export_sync_state().await.is_err());
        assert!(database.apply_sync_state(unit).await.is_err());
    }
    clone.retire_and_remove().await.expect("repeat");
    assert!(Database::open(&path).is_err());
    assert!(
        !path.exists(),
        "denied reopen must not recreate persistence"
    );
    Database::remove_retired(&path).expect("restart retry");
    drop(neighbor);
    Database::open(&other)
        .expect("neighbor reopens")
        .client()
        .execute_sql("SELECT * FROM keep", None)
        .await
        .expect("neighbor retained");
}

#[cfg(feature = "native-stage")]
#[tokio::test]
async fn retirement_cancels_pending_native_sync_and_releases_only_owned_stages() {
    run_pending_stage_retirement(false, None).await;
}

#[cfg(feature = "native-stage")]
#[tokio::test]
async fn native_stage_cleanup_failure_is_reported_and_restart_retryable() {
    run_pending_stage_retirement(true, None).await;
}

#[cfg(feature = "native-stage")]
async fn run_pending_stage_retirement(fail_cleanup: bool, crash_root: Option<&std::path::Path>) {
    use ofdb_sql::{SessionConfig, SyncRole, SyncTransport};
    struct Channel {
        sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
        receiver: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    }
    impl SyncTransport for Channel {
        type Error = &'static str;
        async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
            self.sender.send(frame).map_err(|_| "closed")
        }
        async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
            self.receiver.recv().await.ok_or("closed")
        }
    }
    let dir = tempfile::tempdir().expect("temporary directory");
    let root = crash_root.unwrap_or(dir.path());
    let path = root.join("resource.redb");
    let stages = root.join("stages");
    std::fs::create_dir(&stages).expect("stage directory");
    std::fs::write(stages.join("unrelated"), b"keep").expect("neighbor stage file");
    let database = Database::open(&path).expect("open");
    let config = SessionConfig::default();
    let source = Database::open(root.join("source.redb")).expect("source");
    source
        .client()
        .execute_sql("CREATE TABLE data (id UUID PRIMARY KEY, value TEXT)", None)
        .await
        .expect("source schema");
    let (sender_a, receiver_a) = tokio::sync::mpsc::unbounded_channel();
    let (sender_b, receiver_b) = tokio::sync::mpsc::unbounded_channel();
    let mut source_transport = Channel {
        sender: sender_a,
        receiver: receiver_b,
    };
    let mut transport = Channel {
        sender: sender_b,
        receiver: receiver_a,
    };
    let mut sending =
        Box::pin(source.synchronize(&mut source_transport, &config, SyncRole::Initiator));
    let entered = std::sync::atomic::AtomicBool::new(false);
    let mut sync = Box::pin(database.synchronize_with_stage_directory_checked(
        &mut transport,
        &config,
        SyncRole::Responder,
        &stages,
        || async {
            entered.store(true, std::sync::atomic::Ordering::SeqCst);
            core::future::pending().await
        },
    ));
    for _ in 0..20 {
        assert!(futures::poll!(sending.as_mut()).is_pending());
        assert!(futures::poll!(sync.as_mut()).is_pending());
        if entered.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
    }
    assert!(
        entered.load(std::sync::atomic::Ordering::SeqCst),
        "paused pre-apply with live native stages"
    );
    assert!(
        std::fs::read_dir(&stages)
            .expect("live native stages")
            .count()
            > 1
    );
    if crash_root.is_some() {
        let mut retirement = Box::pin(database.retire_and_remove());
        assert!(
            futures::poll!(retirement.as_mut()).is_pending(),
            "live session must drain after durable fence"
        );
        std::process::exit(0);
    }
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        database
            .client()
            .execute_sql("CREATE TABLE local_work (id UUID PRIMARY KEY)", None),
    )
    .await
    .expect("sync waits without a native writer")
    .expect("local operation during pending sync");
    let obstructed = if fail_cleanup {
        let stage = std::fs::read_dir(&stages)
            .expect("stage entries")
            .map(|entry| entry.expect("entry").path())
            .find(|path| path.file_name().expect("filename") != "unrelated")
            .expect("owned stage");
        std::fs::remove_file(&stage).expect("unlink fixture stage while handle is alive");
        std::fs::create_dir(&stage).expect("inject cleanup failure");
        Some(stage)
    } else {
        None
    };
    let (removed, synchronized) = tokio::join!(database.retire_and_remove(), sync);
    assert!(synchronized.is_err());
    if let Some(stage) = obstructed {
        assert!(
            removed.is_err(),
            "stage failure must not report resource removal success"
        );
        assert!(
            path.exists(),
            "persistence retained until stage cleanup succeeds"
        );
        assert!(database.client().transaction().await.is_err());
        assert!(Database::open(&path).is_err());
        assert_eq!(
            Database::remove_retired(&path)
                .expect_err("stage still obstructed, but native persistence is released")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        std::fs::remove_dir(stage).expect("remove fixture obstruction");
        drop(database);
        Database::remove_retired(&path)
            .expect("restart cleanup without opening retired persistence");
    } else {
        removed.expect("retire while pre-apply remains pending");
    }
    assert!(!path.exists());
    assert_eq!(
        std::fs::read_dir(&stages).expect("stages released").count(),
        1
    );
    assert_eq!(
        std::fs::read(stages.join("unrelated")).expect("unrelated retained"),
        b"keep"
    );
}

#[tokio::test]
async fn admitted_operation_finishes_and_canceled_retirement_resumes_after_reopen_denial() {
    use ofdb_sql::{QueryParams, SqlTranslator, Statement, TranslateError, Translator};
    struct Paused(tokio::sync::Notify);
    impl Translator for Paused {
        async fn translate_with_params(
            &self,
            query: &str,
            params: Option<&QueryParams>,
        ) -> Result<Vec<Statement>, TranslateError> {
            self.0.notified().await;
            SqlTranslator.translate_with_params(query, params).await
        }
    }
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let database = Database::open(&path).expect("open");
    let client = database.client();
    let translator = Paused(tokio::sync::Notify::new());
    let mut operation = Box::pin(
        client.translate_and_execute("CREATE TABLE done (id UUID PRIMARY KEY)", &translator),
    );
    assert!(futures::poll!(operation.as_mut()).is_pending());
    let mut retirement = Box::pin(database.retire_and_remove());
    assert!(
        futures::poll!(retirement.as_mut()).is_pending(),
        "admitted operation must drain"
    );
    assert!(path.exists());
    assert!(
        Database::open(&path).is_err(),
        "fence is durable before draining"
    );
    assert!(
        Database::remove_retired(&path).is_err(),
        "restart cleanup refuses a live native owner"
    );
    assert!(client.transaction().await.is_err());
    drop(retirement);
    translator.0.notify_one();
    operation.await.expect("already admitted write finishes");
    drop(client);
    drop(database);
    Database::remove_retired(&path).expect("restart resume after canceled retirement");
    Database::remove_retired(&path).expect("idempotent restart retry");
    assert!(!path.exists());
    assert!(Database::open(&path).is_err());
}

#[cfg(feature = "native-stage")]
#[tokio::test]
async fn abrupt_retirement_child() {
    if let Some(root) = std::env::var_os("OFDB_RETIREMENT_CRASH_FIXTURE") {
        run_pending_stage_retirement(false, Some(std::path::Path::new(&root))).await;
        panic!("child must exit without running destructors");
    }
}

#[cfg(feature = "native-stage")]
#[test]
fn abrupt_process_exit_after_fence_resumes_exact_database_and_stage_cleanup() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "abrupt_retirement_child", "--nocapture"])
        .env("OFDB_RETIREMENT_CRASH_FIXTURE", dir.path())
        .env("RUST_MIN_STACK", "16777216")
        .status()
        .expect("crash fixture child");
    assert!(status.success());
    let path = dir.path().join("resource.redb");
    let stages = dir.path().join("stages");
    assert!(path.exists());
    assert!(std::fs::read_dir(&stages).expect("orphan stages").count() > 1);
    assert!(Database::open(&path).is_err());
    Database::remove_retired(&path).expect("resume in a fresh process after abrupt owner exit");
    Database::remove_retired(&path).expect("idempotent retry");
    assert!(!path.exists());
    assert_eq!(
        std::fs::read_dir(&stages)
            .expect("exact stage cleanup")
            .count(),
        1
    );
    assert_eq!(
        std::fs::read(stages.join("unrelated")).expect("unrelated retained"),
        b"keep"
    );
    let source =
        Database::open(dir.path().join("source.redb")).expect("unrelated native resource reopens");
    futures::executor::block_on(source.client().execute_sql("SELECT * FROM data", None))
        .expect("unrelated source schema retained");
}

#[cfg(feature = "server")]
#[tokio::test]
async fn unmanaged_native_hosting_refuses_retirement() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let database = Database::open(&path).expect("open");
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
    drop(hosted);
    assert!(path.exists());
}

#[tokio::test]
async fn admitted_transaction_operation_drains_before_native_removal() {
    use ofdb_sql::{QueryParams, SqlTranslator, Statement, TranslateError, Translator};
    struct Paused(tokio::sync::Notify);
    impl Translator for Paused {
        async fn translate_with_params(
            &self,
            query: &str,
            params: Option<&QueryParams>,
        ) -> Result<Vec<Statement>, TranslateError> {
            self.0.notified().await;
            SqlTranslator.translate_with_params(query, params).await
        }
    }
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("resource.redb");
    let database = Database::open(&path).expect("open");
    let mut transaction = database.client().transaction().await.expect("transaction");
    let translator = Paused(tokio::sync::Notify::new());
    let mut operation = Box::pin(
        transaction
            .translate_and_execute("CREATE TABLE admitted (id UUID PRIMARY KEY)", &translator),
    );
    assert!(futures::poll!(operation.as_mut()).is_pending());
    let mut retirement = Box::pin(database.retire_and_remove());
    assert!(futures::poll!(retirement.as_mut()).is_pending());
    assert!(
        path.exists(),
        "admitted transaction still owns native persistence"
    );
    assert!(database.client().transaction().await.is_err());
    assert!(
        Database::remove_retired(&path).is_err(),
        "live native transaction must prevent unlink"
    );
    translator.0.notify_one();
    operation
        .await
        .expect("admitted transaction operation finishes");
    retirement.await.expect("drain then remove");
    assert!(
        transaction.commit().await.is_err(),
        "later commit is a new operation, not previously admitted work"
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn retired_transaction_denies_before_calling_user_translation() {
    use ofdb_sql::{QueryParams, Statement, TranslateError, Translator};
    struct MustNotRun;
    impl Translator for MustNotRun {
        async fn translate_with_params(
            &self,
            _: &str,
            _: Option<&QueryParams>,
        ) -> Result<Vec<Statement>, TranslateError> {
            panic!("retired transaction must not start translation");
        }
    }
    let dir = tempfile::tempdir().expect("temporary directory");
    let database = Database::open(dir.path().join("resource.redb")).expect("open");
    let mut transaction = database.client().transaction().await.expect("transaction");
    database.retire_and_remove().await.expect("retire");
    assert!(
        transaction
            .translate_and_execute("ignored", &MustNotRun)
            .await
            .is_err()
    );
    assert!(
        transaction
            .translate_and_execute_with_params("ignored", None, &MustNotRun)
            .await
            .is_err()
    );
}
