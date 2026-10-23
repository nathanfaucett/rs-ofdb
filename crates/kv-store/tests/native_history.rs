use btree::InMemoryBTree;
use futures::executor::block_on;
use ofdb_kv_store::{ChangeApply, KvStore};
use value::Value;

fn timestamp() -> uuid::Timestamp {
    uuid::Timestamp::from_unix_time(1_700_000_000, 0, 0, 0)
}

#[test]
fn native_history_survives_snapshot_rewrite_and_validates_claims_and_dependencies() {
    block_on(async {
        let source = KvStore::new(InMemoryBTree::new(), timestamp);
        let target = KvStore::new(InMemoryBTree::new(), timestamp);
        let mut tx = source.transaction().await.expect("source transaction");
        tx.set("key", Value::Integer(1), None).await.expect("seed");
        tx.set("key", Value::Integer(2), None).await.expect("edit");
        let history = tx.history("key").await.expect("history").expect("document");
        assert_eq!(history.changes.len(), 3);
        let changes = tx
            .export_changes(&history.id, &[])
            .await
            .expect("native changes");
        assert_eq!(changes.len(), 3);
        assert!(changes[0].deps.is_empty());
        assert_eq!(changes[1].deps, vec![changes[0].hash]);
        let snapshot = tx
            .export_snapshot("key")
            .await
            .expect("snapshot")
            .expect("document");
        tx.commit().await.expect("commit source");

        let mut tx = target.transaction().await.expect("target transaction");
        assert_eq!(
            tx.import_changes(&history.id, &changes[1..])
                .await
                .expect("gap"),
            ChangeApply::MissingDependencies
        );
        assert_eq!(tx.get("key", 0).await.expect("gap made no writes"), None);
        let mut forged = changes.clone();
        forged[0].hash = [42; 32];
        assert!(tx.import_changes(&history.id, &forged).await.is_err());
        let mut forged = changes.clone();
        forged[1].deps.clear();
        assert!(tx.import_changes(&history.id, &forged).await.is_err());
        assert_eq!(
            tx.import_changes(&history.id, &changes)
                .await
                .expect("apply"),
            ChangeApply::Applied
        );
        assert_eq!(
            tx.get("key", 0).await.expect("read"),
            Some(Value::Integer(2))
        );
        tx.commit().await.expect("commit target");
        let mut tx = target.transaction().await.expect("rewrite");
        tx.import_snapshot(snapshot).await.expect("snapshot merge");
        assert_eq!(
            tx.history("key").await.expect("history").expect("document"),
            history
        );
        assert_eq!(
            tx.export_changes(&history.id, &[])
                .await
                .expect("snapshot-contained export"),
            changes
        );
        assert!(
            tx.export_changes(
                &history.id,
                &history
                    .changes
                    .iter()
                    .map(|change| change.hash)
                    .collect::<Vec<_>>()
            )
            .await
            .expect("known history")
            .is_empty()
        );
        tx.rollback().await.expect("rollback");
    });
}
