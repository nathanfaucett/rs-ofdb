use automerge::{ActorId, AutoCommit, ROOT, transaction::Transactable};
use btree::{BTree, BTreeRead, BTreeTransaction, InMemoryBTree};
use btree_automerge::{AutomergeBTree, DocumentChangeKey, hash_heads, run_compaction};
use futures::{StreamExt, executor::block_on};

#[test]
fn incremental_labels_hash_heads_while_compacted_snapshots_retain_individual_changes() {
    block_on(async {
        let storage = InMemoryBTree::<DocumentChangeKey, Vec<u8>>::new();
        let tree = AutomergeBTree::new(storage.clone());
        let id = vec![7; 16];
        let mut document = AutoCommit::new().with_actor(ActorId::from(vec![1]));
        document.put(ROOT, "base", true).expect("base");
        let mut transaction = tree.transaction().await.expect("insert transaction");
        transaction
            .insert(id.clone(), document)
            .await
            .expect("snapshot");
        transaction.commit().await.expect("snapshot commit");
        let mut transaction = tree.transaction().await.expect("update transaction");
        transaction
            .update(id.clone(), |document| {
                document.put(ROOT, "first", true).expect("first change");
                document.commit();
                document.put(ROOT, "second", true).expect("second change");
                document.commit();
                Ok(())
            })
            .await
            .expect("two changes in one stored incremental");
        transaction.commit().await.expect("update commit");
        let mut document = tree.get(&id).await.expect("reconstruct").expect("document");
        let changes = document.get_changes(&[]);
        assert_eq!(changes.len(), 3);
        let records = storage
            .range(..)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("stored records");
        assert_eq!(records.len(), 2);
        let (incremental, payload) = records
            .iter()
            .find(|(key, _)| key.r#type().is_incremental())
            .expect("stored incremental");
        assert_eq!(incremental.change_hash(), &hash_heads(document.get_heads()));
        assert!(
            changes
                .iter()
                .all(|change| &change.hash().0 != incremental.change_hash()),
            "a digest of heads is not an Automerge change identity"
        );
        assert!(
            automerge::Change::from_bytes(payload.clone()).is_err(),
            "payload contains more than one change"
        );
        let before = changes
            .iter()
            .map(automerge::Change::hash)
            .collect::<Vec<_>>();
        let mut transaction = storage.transaction().await.expect("compaction transaction");
        run_compaction(&mut transaction, &id, &mut document)
            .await
            .expect("compaction");
        transaction.commit().await.expect("compaction commit");
        let records = storage
            .range(..)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("compacted records");
        assert_eq!(records.len(), 1);
        assert!(records[0].0.r#type().is_snapshot());
        let mut reopened = AutomergeBTree::new(storage)
            .get(&id)
            .await
            .expect("reopen")
            .expect("snapshot");
        assert_eq!(
            reopened
                .get_changes(&[])
                .iter()
                .map(automerge::Change::hash)
                .collect::<Vec<_>>(),
            before
        );
        assert_eq!(reopened.get_heads(), document.get_heads());
    });
}
