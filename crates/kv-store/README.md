# ofdb-kv-store

`ofdb-kv-store` is the lower-level storage implementation used by the public `ofdb-kv` facade. Applications that need the supported embedded and remote query interface should use [`ofdb-kv`](../kv/README.md).

The store holds shared `value::Value` values under UTF-8 keys. It supports primitives, blobs, and nested JSON objects and arrays. Automerge reconciles the value property, so concurrent changes to different nested fields can merge. Reads take an explicit Unix-millisecond timestamp. Mutations and reads are visible within a transaction until commit; rollback discards them. Expiry is exclusive (`now >= expires_at`). It provides explicit snapshot exchange between peers; network transport and peer state are not included.

## In-memory

Enable the `in-memory` feature on `btree`, then:

```rust,ignore
let store = kv::KvStore::new(
    btree::InMemoryBTree::<Vec<u8>, Vec<u8>>::new(),
    || uuid::Timestamp::now(uuid::NoContext),
);
let mut tx = store.transaction().await?;
tx.set("user:1", vec![1, 2, 3], None).await?;
assert_eq!(tx.get("user:1", now_ms).await?, Some(vec![1, 2, 3]));
tx.commit().await?;
```

## redb

Create the table before constructing the KV store. `RedbByteBTree` adapts redb's ordered byte key wrapper to the `BTree<Vec<u8>, Vec<u8>>` expected by `ofdb-kv-store`:

```rust,ignore
let db = std::sync::Arc::new(redb::Database::create("store.redb")?);
let setup = db.begin_write()?;
setup.open_table(btree_redb::table_definition::<btree_redb::Bytes, Vec<u8>>("kv"))?;
setup.commit()?;
let tree = btree_redb::RedbByteBTree::new(db, "kv");
let store = kv::KvStore::new(tree, || uuid::Timestamp::now(uuid::NoContext));
```

`KvStore::new` requires a timestamp provider. There is no default provider, so construction without one does not compile, with or without `std`. The store uses it to create UUIDv7 generations internally; callers cannot supply IDs. Use a controlled timestamp source in tests. UUIDv7 order does not account for clock skew.

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

`KvSnapshot` contains an ordered `DocumentChangeKey` and a self-contained Automerge snapshot. Import validates the document ID, snapshot type, hash, payload, format version, and value schema before writing. Documents from the old bytes-only format are rejected without migration. Incrementals are rejected rather than queued: each export contains all dependencies, so retry by exporting the latest snapshot. Repeated imports and delayed lower generations cannot resurrect pruned data. A higher generation removes older records atomically on commit; a winning tombstone is retained. For several snapshots, import them all in **one** transaction and commit once; roll back on any error. Same-generation concurrent snapshots merge through Automerge. Tombstones win concurrent delete/update races. There is no automatic propagation: peers converge after they exchange and commit their latest snapshots. UUIDv7 order is deterministic but does not account for clock skew.
