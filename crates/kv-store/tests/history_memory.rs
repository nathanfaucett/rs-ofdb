use std::time::Instant;

use automerge::{AutoCommit, ROOT, transaction::Transactable};
use btree::{BTree, BTreeTransaction, InMemoryBTree};
use btree_automerge::{DocumentChangeKey, hash_heads};
use futures::executor::block_on;
use ofdb_kv_store::{KvStore, encode_document_id};
use value::Value;

const KEYS: usize = 160;
const CHANGES: usize = 24;
const VALUE_BYTES: usize = 8192;

fn timestamp() -> uuid::Timestamp {
    uuid::Timestamp::from_unix_time(1_700_000_000, 0, 0, 0)
}

#[test]
#[ignore = "native peak-RSS diagnostic; run the compiled test alone under /usr/bin/time -v"]
fn histories_peak_rss() {
    block_on(async {
        let setup = Instant::now();
        let backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
        let mut document = AutoCommit::new().with_actor(automerge::ActorId::from(vec![1]));
        document.put(ROOT, "format_version", 2_u64).expect("format");
        document
            .put(ROOT, "tombstone", false)
            .expect("live document");
        let mut state = 1u32;
        for _ in 0..CHANGES {
            let text: String = (0..VALUE_BYTES)
                .map(|_| {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    char::from(b'!' + ((state >> 24) % 90) as u8)
                })
                .collect();
            autosurgeon::reconcile_prop(&mut document, ROOT, "value", Value::Text(text))
                .expect("value");
            document.commit();
        }
        let heads = hash_heads(document.get_heads());
        let snapshot = document.save();
        drop(document);
        let snapshot_bytes = snapshot.len();
        let generation = uuid::Uuid::from_u128((1 << 80) | (7 << 76) | (2 << 62));
        let mut tx = backend.transaction().await.expect("seed transaction");
        for n in 0..KEYS {
            let id = encode_document_id(&format!("key-{n:04}"), generation);
            let key = DocumentChangeKey::new_snapshot(id, heads);
            tx.insert(key.encode_ordered(), snapshot.clone())
                .await
                .expect("seed snapshot");
        }
        tx.commit().await.expect("commit seed");
        drop(snapshot);
        println!(
            "keys={KEYS} changes_per_key={CHANGES} value_bytes={VALUE_BYTES} snapshot_bytes_per_key={snapshot_bytes} setup_seconds={:.6}",
            setup.elapsed().as_secs_f64()
        );

        let store = KvStore::new(backend, timestamp);
        let tx = store.transaction().await.expect("history transaction");
        let started = Instant::now();
        let histories = tx.histories("").await.expect("histories");
        let elapsed = started.elapsed();
        assert_eq!(histories.len(), KEYS);
        assert!(
            histories
                .iter()
                .all(|history| history.changes.len() == CHANGES)
        );
        println!(
            "history_changes={} histories_seconds={:.6}",
            KEYS * CHANGES,
            elapsed.as_secs_f64()
        );
        tx.rollback().await.expect("rollback");
    });
}
