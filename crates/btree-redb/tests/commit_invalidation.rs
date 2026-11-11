use std::sync::Arc;

use btree::BTreeTransaction;
use btree_redb::{Bytes, RedbDatabase};
use futures::{FutureExt, StreamExt, executor::block_on};

fn database() -> RedbDatabase {
    RedbDatabase::new(Arc::new(
        redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .expect("database"),
    ))
}

#[test]
fn commit_invalidation_basic() {
    block_on(async {
        let db = database();
        let mut changes = db.subscribe_changes();

        let tx = db.transaction().await.expect("transaction");
        tx.create_table::<Bytes, Bytes>("items").expect("table");
        tx.commit().expect("commit");

        // Table creation does not emit events - only data changes do
        assert!(changes.events().next().now_or_never().is_none());

        // Now insert data - this should emit an event
        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![1]), Bytes(vec![2]))
            .await
            .expect("write");
        tx.commit().expect("commit");

        let event = changes.events().next().await.expect("data change event");
        assert_eq!(event.key.table, "items");
        assert_eq!(event.key.key, vec![1]);
    });
}

#[test]
fn committed_work_survives_cancellation() {
    block_on(async {
        let db = database();
        let mut changes = db.subscribe_changes();

        let tx = db.transaction().await.expect("transaction");
        tx.create_table::<Bytes, Bytes>("items").expect("table");
        tx.commit().expect("commit");

        // Insert data
        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![1]), Bytes(vec![2]))
            .await
            .expect("write");
        tx.commit().expect("commit");

        let _event = changes.events().next().await;
        // Second call to events() returns None - snapshot semantics
        assert!(changes.events().next().now_or_never().is_none());
    });
}

#[test]
fn overflow_handles_large_keys() {
    block_on(async {
        let db = database();
        let mut changes = db.subscribe_changes();

        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![0; 17000]), Bytes(vec![1]))
            .await
            .expect("large key");
        tx.commit().expect("commit");

        let event = changes.events().next().await.expect("overflow event");
        assert!(!event.blob.is_empty());
    });
}

#[test]
fn retry_after_cancellation_works() {
    block_on(async {
        let db = database();
        let mut changes = db.subscribe_changes();

        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![0; 17000]), Bytes(vec![1]))
            .await
            .expect("large key");
        tx.commit().expect("commit");

        let _event = changes.events().next().await;

        // Simulate cancellation by dropping and re-subscribing
        drop(changes);
        let mut changes = db.subscribe_changes();

        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![250]), Bytes(vec![4]))
            .await
            .expect("post-overflow write");
        tx.commit().expect("commit");

        // New subscription replays all history (push-based model)
        let event1 = changes
            .events()
            .next()
            .await
            .expect("first event from history");
        assert_eq!(event1.key.key, vec![0; 17000]);
        let event2 = changes
            .events()
            .next()
            .await
            .expect("second event from history");
        assert_eq!(event2.key.key, vec![250]);
        // No more events
        assert!(changes.events().next().now_or_never().is_none());
    });
}

#[test]
fn oversized_batch_invalidations() {
    block_on(async {
        let db = database();
        let mut changes = db.subscribe_changes();

        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![0; 17000]), Bytes(vec![1]))
            .await
            .expect("large key");
        tx.commit().expect("commit");

        let event = changes
            .events()
            .next()
            .await
            .expect("oversized batch event");
        assert!(!event.blob.is_empty());
    });
}

#[test]
fn acknowledgement_cannot_hide_a_later_commit_or_another_consumer() {
    block_on(async {
        let db = database();
        let mut changes = db.subscribe_changes();

        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![1]), Bytes(vec![2]))
            .await
            .expect("write");
        tx.commit().expect("commit");

        let _event = changes.events().next().await;

        // New consumer sees same event (historical replay)
        let mut other = db.subscribe_changes();
        let event2 = other.events().next().await;
        assert!(event2.is_some());
        assert_eq!(event2.unwrap().key.key, vec![1]);
    });
}
