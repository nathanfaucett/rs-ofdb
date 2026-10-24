# ofdb-kv-store

`ofdb-kv-store` is the lower-level storage implementation used by the public `ofdb-kv` facade. Applications that need the supported embedded and remote query interface should use [`ofdb-kv`](../kv/README.md).

The store holds shared `value::Value` values under UTF-8 keys. It supports primitives, blobs, and nested JSON objects and arrays. Automerge reconciles the value property, so concurrent changes to different nested fields can merge. Reads take an explicit Unix-millisecond timestamp. Mutations and reads are visible within a transaction until commit; rollback discards them. Expiry is exclusive (`now >= expires_at`). It provides native history and snapshot exchange between peers; network transport and peer state are not included.

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

`KvSnapshot` contains an ordered `DocumentChangeKey` and a self-contained Automerge snapshot. Import validates the document ID, snapshot type, hash, payload, format version, and value schema before writing. Documents from the old bytes-only format are rejected without migration. `import_snapshot` rejects incremental physical records. Native changes use the separate validated `import_changes` API; each snapshot export contains all dependencies. Repeated imports and delayed lower generations cannot resurrect pruned data. A higher generation removes older records atomically on commit; a winning tombstone is retained. For several snapshots, import them all in **one** transaction and commit once; roll back on any error. Same-generation concurrent snapshots merge through Automerge. Tombstones win concurrent delete/update races. There is no automatic propagation: peers converge after they exchange and commit their latest snapshots. UUIDv7 order is deterministic but does not account for clock skew.

## Native history and committed selection

`KvTransaction::history(key)` reads the latest generation's ID, heads, and dependency-ordered native change summaries without exporting a snapshot. `histories()` does the same for the whole store. `export_changes(id, known_hashes)` exports missing `NativeChange { hash, deps, payload }` values from the reconstructed document, including changes inside snapshots or bundled physical incrementals. Physical record labels are not native Automerge hashes.

`import_changes(id, changes)` validates every claimed hash and dependency list, checks dependency order/completeness before writing, validates the complete candidate, then merges through the same store path as snapshots. It preserves latest-generation selection, stale-generation rejection, tombstone wins, and concurrent history. `ChangeApply::MissingDependencies` makes no writes for that call; callers must **roll back the enclosing batch** before snapshot recovery, since earlier operations in the same transaction may have written. All other errors likewise require caller rollback. Known history is silent.

`KvStore::committed_invalidations(storage_table, work)` maps a token from this backend's own subscription to `KvInvalidation::{All, Generation { key, generation }}`. Any generation invalidates latest-generation selection for its **logical key**. Capture work before reads and acknowledge only successful reconciliation; retain independent subscriptions per peer. New/gapped/unknown work forces broad comparison. The signal retains 128 commits capped at 128 physical invalidations / 16 KiB of identity bytes per commit, replacing oversized commits with All; selected tokens may combine retained commits. These are notification-retention limits, not transaction/session-memory bounds. Invalidations are process-local hints, not durable history; shared backend wrappers do not monitor external writes. `changed()` consumes wakeups without acknowledging work. Reconnect/restart requires a fresh broad subscription. Expiry emits no notification. A local subscription cannot observe remote-only commits; callers supply remote wakeups or explicit reconciliation.
