use std::sync::Arc;

use btree::ChangeSubscription;
use btree_redb::RedbByteBTree;
use futures::{FutureExt, executor::block_on};
use ofdb_kv_store::KvStore;
use value::Value;

fn store() -> (KvStore<RedbByteBTree>, ChangeSubscription) {
    let database = redb::Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .expect("memory backend");
    let backend = RedbByteBTree::new(Arc::new(database), "kv");
    let subscription = backend.subscribe_changes();
    (
        KvStore::new(backend, || uuid::Timestamp::now(uuid::NoContext)),
        subscription,
    )
}

fn assert_quiet(subscription: &mut ChangeSubscription) {
    assert!(
        subscription.changed().now_or_never().is_none(),
        "no committed wakeup"
    );
}

fn assert_changed(subscription: &mut ChangeSubscription) {
    assert!(
        matches!(subscription.changed().now_or_never(), Some(Ok(()))),
        "committed wakeup available without waiting"
    );
}

#[test]
fn incoming_snapshot_wakes_fanout_but_known_history_and_expiry_do_not() {
    block_on(async {
        let (source, mut source_changes) = store();
        let (destination, mut destination_changes) = store();
        let mut transaction = source.transaction().await.expect("source write");
        transaction
            .set("a", Value::from("one"), Some(10))
            .await
            .expect("value");
        let snapshot = transaction
            .export_snapshot("a")
            .await
            .expect("export")
            .expect("snapshot");
        transaction.commit().await.expect("source commit");
        assert_changed(&mut source_changes);
        let mut transaction = destination
            .transaction()
            .await
            .expect("incoming transaction");
        transaction
            .import_snapshot(snapshot.clone())
            .await
            .expect("import");
        assert_quiet(&mut destination_changes);
        transaction.commit().await.expect("incoming commit");
        assert_changed(&mut destination_changes);
        let mut transaction = destination.transaction().await.expect("known history");
        transaction
            .import_snapshot(snapshot.clone())
            .await
            .expect("duplicate");
        assert_eq!(
            transaction.get("a", 9).await.expect("before expiry"),
            Some(Value::from("one"))
        );
        assert_eq!(transaction.get("a", 10).await.expect("at expiry"), None);
        assert_eq!(
            transaction
                .export_snapshot("a")
                .await
                .expect("expiry keeps history"),
            Some(snapshot.clone())
        );
        transaction.commit().await.expect("no-op commit");
        assert_quiet(&mut destination_changes);
        let mut transaction = destination
            .transaction()
            .await
            .expect("invalid import batch");
        transaction
            .set("b", Value::from("discard"), None)
            .await
            .expect("staged value");
        let mut malformed = snapshot;
        malformed.1 = vec![0, 1, 2];
        assert!(transaction.import_snapshot(malformed).await.is_err());
        transaction
            .rollback()
            .await
            .expect("caller rolls back failed import");
        assert_quiet(&mut destination_changes);
        let transaction = destination
            .transaction()
            .await
            .expect("read after rollback");
        assert_eq!(
            transaction.get("b", 0).await.expect("discarded value"),
            None
        );
        transaction.rollback().await.expect("read rollback");
    });
}

#[test]
fn kv_commit_is_coalesced_independent_and_rollback_is_silent() {
    block_on(async {
        let (store, mut first) = store();
        let mut second = first.clone();
        let mut transaction = store.transaction().await.expect("transaction");
        transaction
            .set("a", Value::from("one"), None)
            .await
            .expect("first write");
        transaction
            .set("b", Value::from("two"), None)
            .await
            .expect("second write");
        assert_quiet(&mut first);
        transaction.commit().await.expect("commit both writes");
        assert_changed(&mut first);
        assert_quiet(&mut first);
        assert_changed(&mut second);
        let mut new_peer = first.clone();
        assert_quiet(&mut new_peer);
        for explicit_rollback in [true, false] {
            let mut transaction = store.transaction().await.expect("transaction");
            transaction
                .set("a", Value::from("discard"), None)
                .await
                .expect("staged write");
            if explicit_rollback {
                transaction.rollback().await.expect("rollback");
            } else {
                drop(transaction);
            }
            assert_quiet(&mut first);
            assert_quiet(&mut second);
        }
        let transaction = store.transaction().await.expect("read");
        assert_eq!(
            transaction.get("a", 0).await.expect("a"),
            Some(Value::from("one"))
        );
        assert_eq!(
            transaction.get("b", 0).await.expect("b"),
            Some(Value::from("two"))
        );
        transaction.commit().await.expect("read-only commit");
        assert_quiet(&mut first);
    });
}
