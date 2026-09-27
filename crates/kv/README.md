# kv

`kv` stores opaque byte values under UTF-8 keys. Reads take an explicit Unix-millisecond timestamp. Mutations and reads are visible within a transaction until commit; rollback discards them. Expiry is exclusive (`now >= expires_at`). It provides explicit snapshot exchange between peers; network transport and peer state are not included.

## In-memory

Enable the `in-memory` feature on `btree`, then:

```rust,ignore
let store = kv::KvStore::new(btree::InMemoryBTree::<Vec<u8>, Vec<u8>>::new());
let mut tx = store.transaction().await?;
tx.set("user:1", vec![1, 2, 3], None).await?;
assert_eq!(tx.get("user:1", now_ms).await?, Some(vec![1, 2, 3]));
tx.commit().await?;
```

## redb

Create the table before constructing the KV store. `RedbByteBTree` adapts redb's ordered byte key wrapper to the `BTree<Vec<u8>, Vec<u8>>` expected by `kv`:

```rust,ignore
let db = std::sync::Arc::new(redb::Database::create("store.redb")?);
let setup = db.begin_write()?;
setup.open_table(btree_redb::table_definition::<btree_redb::Bytes, Vec<u8>>("kv"))?;
setup.commit()?;
let tree = btree_redb::RedbByteBTree::new(db, "kv");
let store = kv::KvStore::new(tree);
```

Use `KvStore::transaction()` to generate UUIDv7 generations or `transaction_with_uuid_generator()` when deterministic generation is needed. A supplied UUID must be version 7 and greater than the current generation when recreating a tombstoned key.

## Snapshot exchange

Export the **current winning generation**, even if it is tombstoned or expired. Exchange `KvSnapshot` values with peers using your own transport, then import and commit in a transaction:

```rust,ignore
let source_tx = source.transaction().await?;
let snapshot = source_tx.export_snapshot("user:1").await?;
source_tx.rollback().await?;
if let Some(snapshot) = snapshot {
    let mut destination_tx = destination.transaction().await?;
    destination_tx.import_snapshot(snapshot).await?;
    destination_tx.commit().await?;
}
```

`KvSnapshot` contains an ordered `DocumentChangeKey` and a self-contained Automerge snapshot. Import validates the document ID, snapshot type, hash, payload, and value schema before writing. Incrementals are rejected rather than queued: each export contains all dependencies, so retry by exporting the latest snapshot. Repeated imports and delayed lower generations cannot resurrect pruned data. A higher generation removes older records atomically on commit; a winning tombstone is retained. For several snapshots, import them all in **one** transaction and commit once; roll back on any error. Same-generation snapshots must be identical or causally ordered; divergent histories are rejected. There is no automatic propagation: peers converge after they exchange and commit their latest snapshots. UUIDv7 order is deterministic but does not account for clock skew.
