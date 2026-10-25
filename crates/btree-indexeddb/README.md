# Browser byte persistence

`ofdb-btree-indexeddb` provides local, single-threaded `wasm32-unknown-unknown`
IndexedDB storage in Window and Dedicated Worker contexts. It does not require a
Tokio runtime, shared Wasm memory or cross-origin isolation. Native targets compile
without browser exports; existing native persistence remains unchanged.

For the public KV facade, enable only the explicit backend:

```toml
ofdb-kv = { version = "0.1", default-features = false, features = ["indexeddb"] }
```

```rust,ignore
let database = ofdb_kv::Database::open_indexeddb("application").await?;
let client = database.client();
let mut transaction = client.transaction().await?;
transaction.set("offline", true, None).await?;
// Unrelated browser awaits are allowed here.
transaction.commit().await?;
```

Clients and their clones own shared storage after the Embedded Handle is dropped.
Names and store kinds have disjoint physical namespaces (`ofdb:<UTF-8 kind byte
length>:<kind>:<name>`). The KV kind is `kv`, with logical table `ofdb-kv`.
An incompatible format/version or denied open returns an error, never memory.
Connections close on versionchange; subsequent calls on closed handles fail.
Dropping the last connection owner closes it; issued commits retain ownership
until native completion.

## Storage and transaction contract

The fixed version-1 IndexedDB schema has `meta`, `tables`, and `records` object
stores. `meta` holds format `ofdb-byte-storage-1` and a decimal-string u64 revision;
logical table names are data, not schema upgrades. Record keys are compound
`[table name, Uint8Array byte key]` keys; values are `Uint8Array` bytes. Arbitrary
binary keys are never encoded as JavaScript strings. Revisions use BigInt inside
native callbacks and checked u64 parsing in Rust, not imprecise JS numbers.
Revision exhaustion is an error.

`Database::snapshot()` loads metadata, table names, binary keys and values in
one readonly native transaction. `Snapshot` provides owned table maps, creation,
drop and rollback; all tables in a snapshot participate in **one** commit. The
`ByteBTree` adapter uses this same staging for the existing byte BTree contract.
No native transaction is kept alive across caller operations or awaits.

A mutating commit opens one readwrite transaction over all three physical stores.
Its revision-success callback compares the persisted revision and queues the
complete table/record diff and next revision while the transaction is active.
Another tab/worker/connection with an old snapshot gets `BTreeError::Conflict`;
there is no automatic retry or replay. Unchanged snapshots, net byte no-ops, and
stale read-only commits do not write, advance revisions or publish invalidation.
Rollback and dropping uncommitted staging do not issue persistence work.
Table-drop invalidation is conservative, including drop/recreation in one batch.

Request success is not commit success. The native `complete`/`abort` events settle
the Promise. An independently scheduled local completion observer publishes
successful committed invalidation and resolves the caller channel. Dropping the
caller future cannot drop that observer, the connection or its outcome; only the
issued persistence and its invalidation remain owned, not the Rust snapshot.
Timeout/cancellation after issue does **not** establish rollback. Native abort,
quota/access exceptions and version errors propagate without partial persistence.

Notifications are process-local to clones of one opened adapter. Independently
opened tabs/handles are protected by the persisted revision but do not receive
cross-context notifications. Public facade continuous-sync subscription wiring
and browser/native replication are subsequent-ticket work; the facade's
`sync_changes()` still returns Unsupported for IndexedDB in this ticket.

## Initial cost and limits

Every transaction starts by reading the **entire physical database**, including
all logical tables and persisted history. Raw read/copy volume and memory scale
with database size; ordered-map reconstruction adds O(N log N) CPU work. The
initial implementation retains two Rust copies (base and staging) plus transient
IndexedDB/JS materialization. Commits write only net differences, but building the
diff also visits the snapshot. Selective sync does not imply selective physical
reads. Incremental loading and finer conflict detection are deferred.

A diagnostic Chrome 156 Dedicated Worker run with the optimized browser fixture
and its documented 16 MiB Wasm stack measured **11 ms / 102,800 raw key+value bytes**
for 100 records and **96 ms / 1,028,000 bytes** for 1,000 records (1 KiB values,
4-byte keys). These are single samples, not latency guarantees, peak-heap
measurements, a minimum stack claim, or a release-build benchmark.

Persistence follows the browser's IndexedDB completion/durability and eviction
policies. Completion is not a power-loss guarantee or protection against site-data
clearing. There is no automatic migration, quota reservation, memory fallback,
keepalive loop or background retry. Safari/WebKit and browser-process/OS crash
recovery are not validated. See the [real-browser harness](../../tests/browser/README.md).

## Binding choice

Checked registry API/features for `indexed_db_futures 0.6.4` and `idb 0.6.5`, and
inspected their transaction/listener sources. The former has opt-in `tx-done`,
`typed-arrays`, `cursors`, `version-change` (the latter adds Tokio macros); its
transaction borrows the database and dropping it frees listeners/aborts unless
already finalized. The latter defaults to `builder`, which enables `futures` and
Tokio; callback-only use can disable all features but owns Wasm closures in request
and transaction wrappers and offers `forget_callbacks` with leak caveats. Neither
high-level future alone supplies the independent issued-commit ownership contract.

The chosen small platform adapter uses native JS callbacks/Promises for dependent
request scheduling and completion, with a Rust-owned independent observer. It
uses the already-resolved `wasm-bindgen`, `js-sys`, `wasm-bindgen-futures` and
`futures` APIs; no additional binding, serializer framework, deprecated serde
feature or executor package is introduced. JS callbacks are GC-owned, with no
Rust closure-forgetting or unsafe Send/Sync assertions.
