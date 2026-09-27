use automerge::{AutoCommit, ROOT, transaction::Transactable};
use btree::{BTreeRead, InMemoryBTree};
use btree_automerge::{DocumentChangeKey, DocumentType, hash_heads};
use futures::{StreamExt, executor::block_on};
use kv::{KvSnapshot, KvStore, encode_document_id};
use uuid::Uuid;

fn uuid(timestamp: u128) -> Uuid {
    Uuid::from_u128((timestamp << 80) | (7 << 76) | (2 << 62))
}

fn store() -> KvStore<InMemoryBTree<Vec<u8>, Vec<u8>>> {
    KvStore::new(InMemoryBTree::new())
}

#[test]
fn converges_in_both_orders_and_prunes_old_generations() {
    block_on(async {
        for reverse in [false, true] {
            let source = store();
            let destination_backend = InMemoryBTree::new();
            let destination = KvStore::new(destination_backend.clone());
            let mut tx = source
                .transaction_with_uuid_generator(|| uuid(1))
                .await
                .unwrap();
            tx.set("k\0雪", vec![1], None).await.unwrap();
            tx.commit().await.unwrap();
            let tx = source.transaction().await.unwrap();
            let old = tx.export_snapshot("k\0雪").await.unwrap().unwrap();
            tx.rollback().await.unwrap();
            let mut tx = source.transaction().await.unwrap();
            tx.delete("k\0雪").await.unwrap();
            tx.commit().await.unwrap();
            let mut tx = source
                .transaction_with_uuid_generator(|| uuid(2))
                .await
                .unwrap();
            tx.set("k\0雪", vec![2], Some(10)).await.unwrap();
            tx.commit().await.unwrap();
            let tx = source.transaction().await.unwrap();
            let new = tx.export_snapshot("k\0雪").await.unwrap().unwrap();
            tx.rollback().await.unwrap();

            for snapshot in if reverse {
                [new.clone(), old.clone()]
            } else {
                [old.clone(), new.clone()]
            } {
                let mut tx = destination.transaction().await.unwrap();
                tx.import_snapshot(snapshot).await.unwrap();
                tx.commit().await.unwrap();
            }
            let mut tx = destination.transaction().await.unwrap();
            tx.import_snapshot(new.clone()).await.unwrap();
            tx.import_snapshot(old).await.unwrap();
            assert_eq!(tx.get("k\0雪", 9).await.unwrap(), Some(vec![2]));
            assert_eq!(tx.get("k\0雪", 10).await.unwrap(), None);
            assert_eq!(tx.export_snapshot("k\0雪").await.unwrap(), Some(new));
            tx.commit().await.unwrap();
            assert_eq!(
                destination_backend
                    .range(..)
                    .collect::<Vec<_>>()
                    .await
                    .len(),
                1
            );
        }
    });
}

#[test]
fn tombstone_reaches_peer_that_missed_live_value() {
    block_on(async {
        let source = store();
        let destination = store();
        let mut tx = source
            .transaction_with_uuid_generator(|| uuid(1))
            .await
            .unwrap();
        tx.set("k", vec![1], None).await.unwrap();
        tx.delete("k").await.unwrap();
        let tombstone = tx.export_snapshot("k").await.unwrap().unwrap();
        tx.commit().await.unwrap();
        let mut tx = destination.transaction().await.unwrap();
        tx.import_snapshot(tombstone.clone()).await.unwrap();
        tx.commit().await.unwrap();
        let tx = destination.transaction().await.unwrap();
        assert_eq!(tx.get("k", 0).await.unwrap(), None);
        assert_eq!(tx.export_snapshot("k").await.unwrap(), Some(tombstone));
        tx.rollback().await.unwrap();
    });
}

#[test]
fn missed_tombstone_cannot_delete_a_new_generation() {
    block_on(async {
        let source = store();
        let destination = store();
        let mut tx = source
            .transaction_with_uuid_generator(|| uuid(1))
            .await
            .unwrap();
        tx.set("k", vec![1], None).await.unwrap();
        tx.delete("k").await.unwrap();
        let tombstone = tx.export_snapshot("k").await.unwrap().unwrap();
        tx.commit().await.unwrap();
        let mut tx = source
            .transaction_with_uuid_generator(|| uuid(2))
            .await
            .unwrap();
        tx.set("k", vec![2], None).await.unwrap();
        let newer = tx.export_snapshot("k").await.unwrap().unwrap();
        tx.commit().await.unwrap();
        let mut tx = destination.transaction().await.unwrap();
        tx.import_snapshot(newer.clone()).await.unwrap();
        tx.import_snapshot(tombstone).await.unwrap();
        assert_eq!(tx.export_snapshot("k").await.unwrap(), Some(newer));
        assert_eq!(tx.get("k", 0).await.unwrap(), Some(vec![2]));
        tx.commit().await.unwrap();
    });
}

#[test]
fn equal_generation_accepts_newer_and_ignores_stale_but_rejects_forks() {
    block_on(async {
        let source = store();
        let destination = store();
        let mut tx = source
            .transaction_with_uuid_generator(|| uuid(1))
            .await
            .unwrap();
        tx.set("k", vec![1], None).await.unwrap();
        tx.commit().await.unwrap();
        let tx = source.transaction().await.unwrap();
        let old = tx.export_snapshot("k").await.unwrap().unwrap();
        tx.rollback().await.unwrap();
        let mut tx = source.transaction().await.unwrap();
        tx.set("k", vec![2], None).await.unwrap();
        tx.commit().await.unwrap();
        let tx = source.transaction().await.unwrap();
        let new = tx.export_snapshot("k").await.unwrap().unwrap();
        tx.rollback().await.unwrap();
        let mut tx = destination.transaction().await.unwrap();
        tx.import_snapshot(old.clone()).await.unwrap();
        tx.import_snapshot(new.clone()).await.unwrap();
        tx.import_snapshot(old.clone()).await.unwrap();
        assert_eq!(tx.export_snapshot("k").await.unwrap(), Some(new));
        tx.commit().await.unwrap();

        let fork = store();
        let mut tx = fork.transaction().await.unwrap();
        tx.import_snapshot(old).await.unwrap();
        tx.commit().await.unwrap();
        let mut tx = fork.transaction().await.unwrap();
        tx.set("k", vec![3], None).await.unwrap();
        tx.commit().await.unwrap();
        let tx = fork.transaction().await.unwrap();
        let forked = tx.export_snapshot("k").await.unwrap().unwrap();
        tx.rollback().await.unwrap();
        let mut tx = destination.transaction().await.unwrap();
        let before = tx.export_snapshot("k").await.unwrap();
        assert!(tx.import_snapshot(forked).await.is_err());
        assert_eq!(tx.export_snapshot("k").await.unwrap(), before);
        tx.rollback().await.unwrap();
    });
}

#[test]
fn independent_replicas_exchange_winning_tombstone_and_ignore_delayed_loser() {
    block_on(async {
        for reverse in [false, true] {
            let left = store();
            let right = store();
            let mut tx = left
                .transaction_with_uuid_generator(|| uuid(1))
                .await
                .unwrap();
            tx.set("same", vec![1], None).await.unwrap();
            tx.commit().await.unwrap();
            let tx = left.transaction().await.unwrap();
            let old = tx.export_snapshot("same").await.unwrap().unwrap();
            tx.rollback().await.unwrap();
            let mut tx = left.transaction().await.unwrap();
            tx.set("same", vec![3], None).await.unwrap();
            tx.commit().await.unwrap();
            let tx = left.transaction().await.unwrap();
            let delayed = tx.export_snapshot("same").await.unwrap().unwrap();
            tx.rollback().await.unwrap();

            let mut tx = right
                .transaction_with_uuid_generator(|| uuid(2))
                .await
                .unwrap();
            tx.set("same", vec![2], None).await.unwrap();
            tx.delete("same").await.unwrap();
            tx.commit().await.unwrap();
            let tx = right.transaction().await.unwrap();
            let winner = tx.export_snapshot("same").await.unwrap().unwrap();
            tx.rollback().await.unwrap();

            let mut tx = right.transaction().await.unwrap();
            for snapshot in if reverse {
                [delayed.clone(), old.clone()]
            } else {
                [old.clone(), delayed.clone()]
            } {
                tx.import_snapshot(snapshot).await.unwrap();
            }
            assert_eq!(
                tx.export_snapshot("same").await.unwrap(),
                Some(winner.clone())
            );
            tx.commit().await.unwrap();

            let mut tx = left.transaction().await.unwrap();
            tx.import_snapshot(winner.clone()).await.unwrap();
            tx.import_snapshot(delayed).await.unwrap();
            tx.import_snapshot(old).await.unwrap();
            assert_eq!(tx.get("same", 0).await.unwrap(), None);
            assert_eq!(tx.export_snapshot("same").await.unwrap(), Some(winner));
            tx.commit().await.unwrap();
        }
    });
}

#[test]
fn batch_import_rolls_back_if_a_later_snapshot_is_invalid() {
    block_on(async {
        let source = store();
        let destination = store();
        let mut tx = source
            .transaction_with_uuid_generator(|| uuid(1))
            .await
            .unwrap();
        tx.set("first", vec![1], None).await.unwrap();
        let valid = tx.export_snapshot("first").await.unwrap().unwrap();
        tx.commit().await.unwrap();
        let mut invalid = valid.clone();
        invalid.key.id = vec![0];

        let mut tx = destination.transaction().await.unwrap();
        tx.import_snapshot(valid).await.unwrap();
        assert!(tx.import_snapshot(invalid).await.is_err());
        tx.rollback().await.unwrap();
        let tx = destination.transaction().await.unwrap();
        assert_eq!(tx.get("first", 0).await.unwrap(), None);
        tx.rollback().await.unwrap();
    });
}

#[test]
fn rejects_invalid_ids_types_hashes_payloads_and_incrementals_without_mutation() {
    block_on(async {
        let source = store();
        let destination = store();
        let mut tx = source
            .transaction_with_uuid_generator(|| uuid(1))
            .await
            .unwrap();
        tx.set("k", vec![1], None).await.unwrap();
        let valid = tx.export_snapshot("k").await.unwrap().unwrap();
        tx.commit().await.unwrap();
        let mut seed = destination.transaction().await.unwrap();
        seed.import_snapshot(valid.clone()).await.unwrap();
        seed.commit().await.unwrap();
        let mut invalid = Vec::new();
        let mut bad = valid.clone();
        bad.key.id = encode_document_id("k", uuid(2));
        bad.payload = vec![1, 2, 3];
        invalid.push(bad);
        let mut bad = valid.clone();
        bad.key.id = vec![0];
        invalid.push(bad);
        let mut bad = valid.clone();
        bad.key.id = encode_document_id("k", Uuid::nil());
        invalid.push(bad);
        let mut bad = valid.clone();
        bad.key.r#type = DocumentType::Incremental;
        invalid.push(bad);
        let mut bad = valid.clone();
        bad.key.change_hash = [0; 32];
        invalid.push(bad);
        let mut bad = valid.clone();
        bad.payload = vec![1, 2, 3];
        invalid.push(bad);
        let mut bad = valid.clone();
        bad.payload = Vec::new();
        invalid.push(bad);
        let mut doc = AutoCommit::new();
        doc.put(ROOT, "value", vec![1_u8]).unwrap();
        doc.put(ROOT, "tombstone", false).unwrap();
        let incremental = doc.save_incremental();
        invalid.push(KvSnapshot {
            key: DocumentChangeKey::new_snapshot(valid.key.id.clone(), hash_heads(doc.get_heads())),
            payload: incremental,
        });
        doc.put(ROOT, "value", vec![2_u8]).unwrap();
        invalid.push(KvSnapshot {
            key: DocumentChangeKey::new_snapshot(valid.key.id.clone(), hash_heads(doc.get_heads())),
            payload: doc.save_incremental(),
        });
        let mut doc = AutoCommit::new();
        doc.put(ROOT, "tombstone", true).unwrap();
        doc.put(ROOT, "value", vec![1_u8]).unwrap();
        invalid.push(KvSnapshot {
            key: DocumentChangeKey::new_snapshot(valid.key.id.clone(), hash_heads(doc.get_heads())),
            payload: doc.save(),
        });
        for snapshot in invalid {
            let mut tx = destination.transaction().await.unwrap();
            assert!(tx.import_snapshot(snapshot).await.is_err());
            assert_eq!(tx.export_snapshot("k").await.unwrap(), Some(valid.clone()));
            tx.commit().await.unwrap();
        }
    });
}
