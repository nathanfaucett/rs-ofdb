use automerge::{ActorId, AutoCommit, ROOT, ReadDoc, transaction::Transactable};
use btree::{BTree, BTreeRead, BTreeTransaction, InMemoryBTree};
use btree_automerge::{
    AutomergeBTree, AutomergeChangeStore, DocumentChangeKey, DocumentId, DocumentType, hash_heads,
};
use futures::{StreamExt, executor::block_on, pin_mut};

fn inner_tree() -> InMemoryBTree<DocumentChangeKey, Vec<u8>> {
    InMemoryBTree::new()
}

#[test]
fn missing_document_returns_none() {
    block_on(async {
        let tree = AutomergeBTree::new(inner_tree());

        assert!(tree.get(&DocumentId::from([1])).await.unwrap().is_none());
    });
}

#[test]
fn metadatas_are_included_in_snapshot_and_incremental_ranges() {
    block_on(async {
        let inner = inner_tree();
        let id = DocumentId::from([1]);
        let mut document = AutoCommit::new();
        let snapshot = DocumentChangeKey::new_snapshot(id.clone(), [0; 32]);
        let incremental = DocumentChangeKey::new_incremental(id.clone(), [1; 32]);
        let metadata = DocumentChangeKey::new_metadata(id.clone());

        let mut tx = inner.transaction().await.unwrap();
        tx.insert(snapshot, document.save()).await.unwrap();
        document.put(ROOT, "value", "changed").unwrap();
        tx.insert(incremental, document.save_incremental())
            .await
            .unwrap();
        tx.insert(metadata, Vec::new()).await.unwrap();
        tx.commit().await.unwrap();

        let tree = AutomergeBTree::new(inner.clone());
        assert!(tree.get(&id).await.unwrap().is_none());
        let stream = tree.range(id.clone()..=id.clone());
        pin_mut!(stream);
        assert!(stream.next().await.is_none());

        let range_id = id.clone();
        let raw = inner.range(DocumentChangeKey::range_for(&range_id));
        pin_mut!(raw);
        assert_eq!(
            raw.next().await.unwrap().unwrap().0.r#type(),
            DocumentType::Snapshot
        );
        assert_eq!(
            raw.next().await.unwrap().unwrap().0.r#type(),
            DocumentType::Incremental
        );
        assert_eq!(
            raw.next().await.unwrap().unwrap().0.r#type(),
            DocumentType::Metadata
        );
        assert!(raw.next().await.is_none());

        let key = DocumentChangeKey::new(id, DocumentType::Metadata, [0; 32]);
        assert_eq!(
            DocumentChangeKey::decode_ordered(&key.encode_ordered()).unwrap(),
            key
        );
    });
}

#[test]
fn change_store_range_honors_included_and_excluded_bounds() {
    block_on(async {
        let inner = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
        let ids = [vec![1], vec![2], vec![3]];
        let mut tx = inner.transaction().await.unwrap();
        for id in &ids {
            tx.insert(
                DocumentChangeKey::new_snapshot(id.clone(), [0; 32]).encode_ordered(),
                Vec::new(),
            )
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();

        let store = AutomergeChangeStore::new(inner);
        let entries = store.range(
            DocumentChangeKey::min_for_id(ids[1].clone())
                ..=DocumentChangeKey::max_for_id(ids[1].clone()),
        );
        pin_mut!(entries);
        assert_eq!(entries.next().await.unwrap().unwrap().0.id(), &ids[1]);
        assert!(entries.next().await.is_none());

        let entries = store.range((
            std::ops::Bound::Excluded(DocumentChangeKey::max_for_id(ids[0].clone())),
            std::ops::Bound::Excluded(DocumentChangeKey::min_for_id(ids[2].clone())),
        ));
        pin_mut!(entries);
        assert_eq!(entries.next().await.unwrap().unwrap().0.id(), &ids[1]);
        assert!(entries.next().await.is_none());
    });
}

#[test]
fn multiple_sequential_updates_reconstruct_after_reopening() {
    block_on(async {
        let inner = inner_tree();
        let tree = AutomergeBTree::new(inner.clone());
        let id = DocumentId::from([1]);

        let mut tx = tree.transaction().await.unwrap();
        tx.insert(id.clone(), AutoCommit::new()).await.unwrap();
        tx.commit().await.unwrap();

        for value in ["first", "second", "third"] {
            let mut tx = tree.transaction().await.unwrap();
            tx.update(id.clone(), |document| {
                document.put(ROOT, "value", value).unwrap();
                Ok(())
            })
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }

        let reopened = AutomergeBTree::new(inner);
        let document = reopened.get(&id).await.unwrap().unwrap();
        let (value, _) = document.get(ROOT, "value").unwrap().unwrap();
        assert_eq!(value.to_string(), "\"third\"");
    });
}

#[test]
fn concurrent_incrementals_reconstruct_both_changes() {
    block_on(async {
        let inner = inner_tree();
        let id = DocumentId::from([7]);
        let mut base = AutoCommit::new();
        base.put(ROOT, "base", true).unwrap();
        let snapshot = base.save();
        let mut left = AutoCommit::load(&snapshot)
            .unwrap()
            .with_actor(ActorId::from(vec![1]));
        let mut right = AutoCommit::load(&snapshot)
            .unwrap()
            .with_actor(ActorId::from(vec![2]));
        left.put(ROOT, "left", true).unwrap();
        right.put(ROOT, "right", true).unwrap();
        let left_heads = hash_heads(left.get_heads());
        let right_heads = hash_heads(right.get_heads());
        let mut tx = inner.transaction().await.unwrap();
        tx.insert(
            DocumentChangeKey::new_snapshot(id.clone(), hash_heads(base.get_heads())),
            snapshot,
        )
        .await
        .unwrap();
        tx.insert(
            DocumentChangeKey::new_incremental(id.clone(), left_heads),
            left.save_incremental(),
        )
        .await
        .unwrap();
        tx.insert(
            DocumentChangeKey::new_incremental(id.clone(), right_heads),
            right.save_incremental(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let document = AutomergeBTree::new(inner).get(&id).await.unwrap().unwrap();
        assert!(document.get(ROOT, "base").unwrap().is_some());
        assert!(document.get(ROOT, "left").unwrap().is_some());
        assert!(document.get(ROOT, "right").unwrap().is_some());
    });
}

#[test]
fn compaction_keeps_document_readable_and_replaces_history_with_snapshot() {
    block_on(async {
        let inner = inner_tree();
        let tree = AutomergeBTree::new(inner.clone());
        let id = DocumentId::from([1]);

        let mut tx = tree.transaction().await.unwrap();
        tx.insert(id.clone(), AutoCommit::new()).await.unwrap();
        tx.commit().await.unwrap();
        let mut tx = tree.transaction().await.unwrap();
        tx.update(id.clone(), |document| {
            document.put(ROOT, "value", "compacted").unwrap();
            Ok(())
        })
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let mut document = tree.get(&id).await.unwrap().unwrap();
        let mut tx = inner.transaction().await.unwrap();
        btree_automerge::run_compaction(&mut tx, &id, &mut document)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let document = tree.get(&id).await.unwrap().unwrap();
        let (value, _) = document.get(ROOT, "value").unwrap().unwrap();
        assert_eq!(value.to_string(), "\"compacted\"");

        let entries = inner.range(DocumentChangeKey::range_for(&id));
        pin_mut!(entries);
        let mut types = Vec::new();
        while let Some(entry) = entries.next().await {
            types.push(entry.unwrap().0.r#type());
        }
        assert_eq!(types, [DocumentType::Snapshot]);
    });
}

#[test]
fn bounded_document_range_respects_excluded_and_included_ids() {
    block_on(async {
        let tree = AutomergeBTree::new(inner_tree());
        for id in [vec![1], vec![2], vec![3]] {
            let mut tx = tree.transaction().await.unwrap();
            tx.insert(id, AutoCommit::new()).await.unwrap();
            tx.commit().await.unwrap();
        }

        let documents = tree.range((
            std::ops::Bound::Excluded(DocumentId::from([1])),
            std::ops::Bound::Included(DocumentId::from([3])),
        ));
        pin_mut!(documents);
        assert_eq!(documents.next().await.unwrap().unwrap().0, [2]);
        assert_eq!(documents.next().await.unwrap().unwrap().0, [3]);
        assert!(documents.next().await.is_none());
    });
}

#[test]
fn remove_range_skips_malformed_documents_and_removes_their_changes() {
    block_on(async {
        let inner = inner_tree();
        let malformed_id = DocumentId::from([1]);
        let valid_id = DocumentId::from([2]);
        let malformed_key = DocumentChangeKey::new_incremental(malformed_id.clone(), [0; 32]);
        let valid_key = DocumentChangeKey::new_snapshot(valid_id.clone(), [1; 32]);
        let mut document = AutoCommit::new();

        let mut inner_tx = inner.transaction().await.unwrap();
        inner_tx
            .insert(malformed_key.clone(), vec![0])
            .await
            .unwrap();
        inner_tx
            .insert(valid_key.clone(), document.save())
            .await
            .unwrap();
        inner_tx.commit().await.unwrap();

        let tree = AutomergeBTree::new(inner.clone());
        let mut tx = tree.transaction().await.unwrap();
        {
            let stream = tx.remove_range(malformed_id.clone()..=valid_id.clone());
            pin_mut!(stream);

            assert!(stream.next().await.unwrap().is_err());
            assert_eq!(stream.next().await.unwrap().unwrap().0, valid_id);
            assert!(stream.next().await.is_none());
        }
        tx.commit().await.unwrap();

        assert_eq!(inner.get(&malformed_key).await.unwrap(), None);
        assert_eq!(inner.get(&valid_key).await.unwrap(), None);
    });
}
