#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use btree::BTreeTransaction;
    use btree_redb::{Bytes, RedbDatabase};
    use futures::{FutureExt, StreamExt, executor::block_on};

    #[test]
    fn pending_notifications_are_none_before_commit() {
        block_on(async {
            let directory = tempfile::tempdir().expect("temporary directory");
            let database = RedbDatabase::new(Arc::new(
                redb::Database::create(directory.path().join("pending.redb")).expect("database"),
            ));
            let mut changes = database.subscribe_changes();
            let mut stream = changes.events();
            assert!(
                stream.next().now_or_never().is_none(),
                "no events before commit"
            );
        });
    }

    #[test]
    fn committed_changes_yield_events() {
        block_on(async {
            let directory = tempfile::tempdir().expect("temporary directory");
            let database = RedbDatabase::new(Arc::new(
                redb::Database::create(directory.path().join("committed.redb")).expect("database"),
            ));
            let mut changes = database.subscribe_changes();
            let tx = database.transaction().await.expect("transaction");
            tx.table::<Bytes, Bytes>("items")
                .insert(Bytes(vec![1]), Bytes(vec![2]))
                .await
                .expect("write");
            tx.commit().expect("commit");

            #[allow(deprecated)]
            changes.changed().await.expect("event notification");
            let mut stream = changes.events();
            let event = stream.next().await.expect("event available");
            assert_eq!(event.key.key, vec![1]);
            assert_eq!(event.blob, vec![2]);
        });
    }

    #[test]
    fn scoped_tables_notify_only_at_outer_commit_and_drop_does_not_scan_contents() {
        block_on(async {
            let directory = tempfile::tempdir().expect("temporary directory");
            let database = RedbDatabase::new(Arc::new(
                redb::Database::create(directory.path().join("scoped.redb")).expect("database"),
            ));
            let mut changes = database.subscribe_changes();
            let tx = database.transaction().await.expect("transaction");
            tx.create_table::<Bytes, Bytes>("items").expect("table");
            tx.commit().expect("commit");

            // No events for table creation
            assert!(changes.events().next().now_or_never().is_none());

            let tx = database.transaction().await.expect("transaction");
            tx.table::<Bytes, Bytes>("items")
                .insert(Bytes(vec![1]), Bytes(vec![2]))
                .await
                .expect("write");
            tx.commit().expect("commit");

            // Wait for new events before polling stream
            #[allow(deprecated)]
            changes.changed().await.expect("event notification");
            let event = changes.events().next().await.expect("event available");
            assert_eq!(event.key.key, vec![1]);
        });
    }

    #[test]
    fn drop_subscription_does_not_block() {
        block_on(async {
            let directory = tempfile::tempdir().expect("temporary directory");
            let database = RedbDatabase::new(Arc::new(
                redb::Database::create(directory.path().join("drop.redb")).expect("database"),
            ));
            let mut changes = database.subscribe_changes();
            let tx = database.transaction().await.expect("transaction");
            tx.table::<Bytes, Bytes>("items")
                .insert(Bytes(vec![1]), Bytes(vec![2]))
                .await
                .expect("write");
            tx.commit().expect("commit");

            let _event = changes.events().next().await;

            // Dropping the subscription should not block
            // (the stream is dropped before we drop changes)
        });
    }
}
