#![cfg(feature = "shared-stage")]

use btree_redb::SharedDatabase;
use history::{NativeSyncStageFactory, SyncStage, SyncStageFactory};

#[test]
fn shared_stages_store_ordered_records_in_the_host_only() {
    futures::executor::block_on(async {
        let path = std::env::temp_dir().join(format!("shared-stage-{}", uuid::Uuid::now_v7()));
        let host = SharedDatabase::create(&path).expect("host");
        let factory =
            NativeSyncStageFactory::shared(host.namespace("sql:resource").expect("namespace"));
        let mut stage = factory.create().await.expect("stage");
        stage.append(b"first".to_vec()).await.expect("first record");
        stage
            .append(b"second".to_vec())
            .await
            .expect("second record");
        stage.finish().await.expect("finish");
        assert!(stage.append(vec![]).await.is_err());
        assert_eq!(
            stage.next_record().await.expect("first"),
            Some(b"first".to_vec())
        );
        assert_eq!(
            stage.next_record().await.expect("second"),
            Some(b"second".to_vec())
        );
        assert_eq!(stage.next_record().await.expect("end"), None);
        drop(stage);
        drop(factory);
        drop(host);
        std::fs::remove_file(path).expect("remove single host file");
    });
}
