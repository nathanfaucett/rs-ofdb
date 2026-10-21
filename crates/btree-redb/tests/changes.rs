use std::sync::Arc;

use btree::{BTree, BTreeRead, BTreeTransaction, ChangeSubscription};
use btree_redb::{Bytes, RedbByteBTree, RedbDatabase};
use futures::{FutureExt, StreamExt, executor::block_on};

fn pending(changes: &mut ChangeSubscription) {
    assert!(
        changes.changed().now_or_never().is_none(),
        "no committed change"
    );
}
fn changed(changes: &mut ChangeSubscription) {
    assert!(
        changes
            .changed()
            .now_or_never()
            .expect("committed change")
            .is_ok()
    );
}

#[test]
fn byte_tree_notifies_changed_commits_across_clones() {
    block_on(async {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = Arc::new(
            redb::Database::create(directory.path().join("bytes.redb")).expect("database"),
        );
        let tree = RedbByteBTree::new(database, "bytes");
        let clone = tree.clone();
        let mut changes = tree.subscribe_changes();
        let mut other = clone.subscribe_changes();
        let mut tx = clone.transaction().await.expect("transaction");
        tx.insert(vec![1], vec![2]).await.expect("insert");
        pending(&mut changes);
        tx.commit().await.expect("commit");
        changed(&mut changes);
        changed(&mut other);

        let mut tx = tree.transaction().await.expect("transaction");
        tx.insert(vec![1], vec![2]).await.expect("identical put");
        tx.remove(&vec![9]).await.expect("absent remove");
        tx.update(vec![1], |_| Ok(()))
            .await
            .expect("identical update");
        tx.commit().await.expect("no-op commit");
        pending(&mut changes);

        let mut tx = tree.transaction().await.expect("transaction");
        tx.insert(vec![1], vec![3]).await.expect("change");
        tx.rollback().await.expect("rollback");
        let mut tx = tree.transaction().await.expect("transaction");
        tx.insert(vec![1], vec![3]).await.expect("change");
        drop(tx);
        pending(&mut changes);

        let mut tx = tree.transaction().await.expect("transaction");
        tx.remove_range(..)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("remove range");
        tx.insert(vec![1], vec![2]).await.expect("restore");
        tx.commit().await.expect("net no-op");
        pending(&mut changes);

        let mut tx = tree.transaction().await.expect("transaction");
        tx.update(vec![1], |value| {
            *value = vec![4];
            Ok(())
        })
        .await
        .expect("update");
        tx.commit().await.expect("update commit");
        changed(&mut changes);
        assert_eq!(tree.get(&vec![1]).await.expect("read"), Some(vec![4]));
        drop(tree);
        drop(clone);
        assert!(changes.changed().await.is_err());
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
        let clone = database.clone();
        let tx = clone.transaction().await.expect("transaction");
        tx.create_table::<Bytes, Bytes>("a").expect("create");
        tx.table::<Bytes, Bytes>("a")
            .insert(Bytes(vec![1]), Bytes(vec![2]))
            .await
            .expect("insert");
        tx.table::<Bytes, Bytes>("a")
            .commit()
            .await
            .expect("scoped commit");
        pending(&mut changes);
        tx.commit().expect("outer commit");
        changed(&mut changes);

        let tx = database.transaction().await.expect("transaction");
        tx.create_table::<Bytes, Bytes>("a")
            .expect("ensure existing");
        tx.table::<Bytes, Bytes>("a")
            .insert(Bytes(vec![1]), Bytes(vec![2]))
            .await
            .expect("identical put");
        tx.commit().expect("unchanged commit");
        pending(&mut changes);

        let tx = database.transaction().await.expect("transaction");
        tx.drop_table::<Bytes, Bytes>("a").expect("drop");
        tx.rollback().expect("rollback");
        pending(&mut changes);
        let tx = database.transaction().await.expect("transaction");
        tx.drop_table::<Bytes, Bytes>("a").expect("drop");
        tx.create_table::<Bytes, Bytes>("a")
            .expect("recreate empty");
        tx.commit().expect("commit");
        changed(&mut changes);
        let tx = database.transaction().await.expect("transaction");
        assert!(
            !tx.drop_table::<Bytes, Bytes>("missing")
                .expect("absent drop")
        );
        tx.commit().expect("commit");
        pending(&mut changes);
    });
}
