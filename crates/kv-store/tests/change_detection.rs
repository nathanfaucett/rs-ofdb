use std::sync::Arc;

use btree::ChangeSubscription;
use btree_redb::RedbByteBTree;
use futures::{FutureExt, executor::block_on};
use ofdb_kv_store::{KvInvalidation, KvStore};
use value::Value;

fn timestamp() -> uuid::Timestamp {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let millis = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    uuid::Timestamp::from_unix_time(
        1_700_000_000 + millis / 1_000,
        (millis % 1_000) as u32 * 1_000_000,
        0,
        0,
    )
}

fn store() -> (KvStore<RedbByteBTree>, ChangeSubscription) {
    let database = redb::Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .expect("memory backend");
    let backend = RedbByteBTree::new(Arc::new(database), "kv");
    let subscription = backend.subscribe_changes();
    (KvStore::new(backend, timestamp), subscription)
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
        source_changes.acknowledge(source_changes.select().expect("initial source"));
        destination_changes.acknowledge(destination_changes.select().expect("initial destination"));
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
        let initial = destination_changes.select().expect("incoming creation");
        destination_changes.acknowledge(initial);
        let mut update = source.transaction().await.expect("update source");
        update
            .set("a", Value::from("two"), Some(10))
            .await
            .expect("edit");
        let edited = update
            .export_snapshot("a")
            .await
            .expect("export")
            .expect("snapshot");
        update.commit().await.expect("commit");
        let mut incoming = destination.transaction().await.expect("incoming edit");
        incoming
            .import_snapshot(edited.clone())
            .await
            .expect("merge");
        incoming.commit().await.expect("commit incoming");
        let selected = destination_changes.select().expect("fanout identity");
        assert!(
            matches!(destination.committed_invalidations("kv", &selected).as_slice(), [KvInvalidation::Generation { key, .. }] if key == "a")
        );
        destination_changes.acknowledge(selected);
        assert_changed(&mut destination_changes);
        let snapshot = edited;
        let mut transaction = destination.transaction().await.expect("known history");
        transaction
            .import_snapshot(snapshot.clone())
            .await
            .expect("duplicate");
        assert_eq!(
            transaction.get("a", 9).await.expect("before expiry"),
            Some(Value::from("two"))
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
fn replacement_generation_invalidates_key_selection_and_stale_import_is_silent() {
    block_on(async {
        let (store, mut changes) = store();
        let mut tx = store.transaction().await.expect("transaction");
        tx.set("key", Value::from("old"), None).await.expect("seed");
        let old = tx
            .export_snapshot("key")
            .await
            .expect("export")
            .expect("snapshot");
        tx.commit().await.expect("commit");
        changes.acknowledge(changes.select().expect("initial"));
        let mut tx = store.transaction().await.expect("transaction");
        tx.delete("key").await.expect("delete");
        tx.set("key", Value::from("new"), None)
            .await
            .expect("recreate");
        tx.commit().await.expect("commit replacement");
        let selected = changes.select().expect("replacement");
        let invalidations = store.committed_invalidations("kv", &selected);
        assert_eq!(
            invalidations.len(),
            2,
            "removed generation and new generation"
        );
        assert!(
            invalidations
                .iter()
                .all(|item| matches!(item, KvInvalidation::Generation { key, .. } if key == "key"))
        );
        changes.acknowledge(selected);
        let mut tx = store.transaction().await.expect("stale import");
        tx.import_snapshot(old)
            .await
            .expect("stale generation ignored");
        tx.commit().await.expect("no-op");
        assert!(changes.select().is_none());
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
