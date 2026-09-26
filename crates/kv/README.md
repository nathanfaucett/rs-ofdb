# kv

`kv` stores opaque byte values under UTF-8 keys. Reads take an explicit Unix-millisecond timestamp. Mutations and reads are visible within a transaction until commit; rollback discards them. Expiry is exclusive (`now >= expires_at`). The crate is local-only; it does not synchronize peers.

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
