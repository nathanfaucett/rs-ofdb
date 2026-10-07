#![cfg(feature = "native-stage")]

use sync::{FileSyncStageFactory, SyncStage, SyncStageFactory};

#[tokio::test]
async fn truncated_records_and_failed_creation_release_resources() {
    for bytes in [vec![0], vec![255; 8], vec![0, 0, 0, 0, 0, 0, 0, 2, 1]] {
        let dir = tempfile::tempdir().expect("directory");
        let mut stage = FileSyncStageFactory::new(dir.path())
            .create()
            .await
            .expect("stage");
        let path = std::fs::read_dir(dir.path())
            .expect("listing")
            .next()
            .expect("entry")
            .expect("path")
            .path();
        std::fs::write(path, bytes).expect("fault injection");
        stage.finish().await.expect("finish");
        assert!(stage.next_record().await.is_err());
        drop(stage);
        assert_eq!(std::fs::read_dir(dir.path()).expect("listing").count(), 0);
    }
    let dir = tempfile::tempdir().expect("directory");
    let path = dir.path().join("not-a-directory");
    std::fs::write(&path, b"keep").expect("fixture");
    assert!(FileSyncStageFactory::new(path).create().await.is_err());
    assert_eq!(std::fs::read_dir(dir.path()).expect("listing").count(), 1);
}

#[tokio::test]
async fn canceled_owner_cleans_creation_append_and_replay() {
    for phase in 0..3 {
        let dir = tempfile::tempdir().expect("directory");
        let factory = FileSyncStageFactory::new(dir.path());
        let mut future = Box::pin(async {
            let mut stage = factory.create().await.expect("stage");
            if phase > 0 {
                stage.append(vec![1]).await.expect("append");
            }
            if phase > 1 {
                stage.finish().await.expect("finish");
                stage.next_record().await.expect("replay");
            }
            core::future::pending::<()>().await;
            drop(stage);
        });
        assert!(futures::poll!(future.as_mut()).is_pending());
        assert_eq!(std::fs::read_dir(dir.path()).expect("listing").count(), 1);
        drop(future);
        assert_eq!(std::fs::read_dir(dir.path()).expect("listing").count(), 0);
    }
}

#[tokio::test]
async fn explicit_close_reports_cleanup_failure() {
    let dir = tempfile::tempdir().expect("directory");
    let stage = FileSyncStageFactory::new(dir.path())
        .create()
        .await
        .expect("stage");
    let path = std::fs::read_dir(dir.path())
        .expect("listing")
        .next()
        .expect("entry")
        .expect("path")
        .path();
    std::fs::remove_file(&path).expect("remove fixture");
    std::fs::create_dir(&path).expect("block unlink");
    assert!(stage.close().is_err());
}

#[tokio::test]
async fn native_records_lifecycle_and_drop_cleanup() {
    let dir = tempfile::tempdir().expect("directory");
    let factory = FileSyncStageFactory::new(dir.path());
    let mut stage = factory.create().await.expect("stage");
    assert!(stage.next_record().await.is_err());
    stage.append(vec![1, 2]).await.expect("first");
    stage.append(vec![]).await.expect("empty");
    stage.append(vec![3]).await.expect("third");
    stage.finish().await.expect("finish");
    stage.finish().await.expect("idempotent finish");
    assert!(stage.append(vec![4]).await.is_err());
    assert_eq!(stage.next_record().await.expect("first"), Some(vec![1, 2]));
    assert_eq!(stage.next_record().await.expect("empty"), Some(vec![]));
    assert_eq!(stage.next_record().await.expect("third"), Some(vec![3]));
    assert_eq!(stage.next_record().await.expect("end"), None);
    drop(stage);
    assert_eq!(std::fs::read_dir(dir.path()).expect("listing").count(), 0);
}
