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
stores. `meta` holds format `ofdb-byte-storage-1`, the exact `kind` marker and a decimal-string u64 revision;
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
cross-context notifications. Both SQL and KV facade committed subscriptions,
checked sync and caller-polled continuous runners are supported. External-write gaps require
broad reconciliation rather than relying only on local invalidation.

## Initial cost and limits

Every transaction starts by reading the **entire physical database**, including
all logical tables and persisted history. Raw read/copy volume and memory scale
with database size; ordered-map reconstruction adds O(N log N) CPU work. The
initial implementation retains two Rust copies (base and staging) plus transient
IndexedDB/JS materialization. Commits write only net differences, but building the
diff also visits the snapshot. Selective sync does not imply selective physical
reads. Incremental loading and finer conflict detection are deferred.

See the current [snapshot samples and reproduction](../../docs/browser-validation.md#snapshot-samples)
for Chrome/Firefox in both contexts, raw payload sizes and measured Wasm linear
memory capacity. Capacity is not live/peak heap; single-sample timings are not
latency guarantees, a minimum-stack claim or release-build benchmarks. The public
[browser guide](../../docs/browser.md) includes SQL/KV quickstarts and support limits.

Persistence follows the browser's IndexedDB completion/durability and eviction
policies. Completion is not a power-loss guarantee or protection against site-data
clearing. There is no automatic migration, quota reservation, memory fallback,
keepalive loop or background retry. Browser-process/OS crash recovery is not
validated. See the [real-browser harness](../../tests/browser/README.md).

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

## Lifecycle and browser failures

SQL (`sql`) and KV (`kv`) share this policy, not a database namespace. Open checks
physical version 1, exactly the three expected object stores, out-of-line keys,
no auto-increment or physical indexes, and format/kind/revision metadata before
returning a handle. Missing markers, unsupported versions and malformed schemas
are errors. Earlier experimental databases without the kind marker are also
incompatible: there is no migration, reset, deletion or memory fallback. Keep the
original database for a compatible reader or choose a new name explicitly.

A blocked event fails immediately. A **5,000 ms browser timer** also bounds open
and metadata validation, including an open queued behind another context's
blocked deletion (which may never emit its own blocked event). Close other tabs,
iframes and workers using the origin's database, then retry explicitly. Browser
suspension and throttling can delay timer delivery: this is not a hard real-time
wall-clock guarantee. IndexedDB has no cancellation API for an open request;
a refused request retains only cleanup callbacks. A late upgrade is aborted
before stores are created, and any late successful connection is closed.
This construction policy is independent of optional query deadlines.

On version change or deletion, connections close immediately. All client clones
sharing that connection are stale; new queries and commits (including unchanged
staging) fail with an explicit closure error. There is no automatic reopen,
transaction replay or transfer of staged writes. Already-owned Rust staging is
not silently discarded or rewritten; discard it explicitly and construct a new
compatible handle. Already-issued native transactions still settle through their
normal complete/abort observer: closure or cancellation is not proof of rollback.
Dropping Database alone does not close a connection owned by Clients; dropping
the last owner closes it after any issued commit's independent observer finishes.

Native failure names (for example SecurityError, QuotaExceededError and
AbortError) receive operation-specific, actionable messages without copying
browser exception messages, database names, keys or stored values. Revision
conflicts retain BTree Conflict / KV Conflict. Other browser failures preserve the
existing facade Storage category and include abort/quota/access/closure detail;
SQL's existing error model also categorizes persistence conflicts as Storage.
No new public or remote wire error category is introduced. A failed native
transaction leaves records, logical-table metadata and revision unchanged, and
publishes no successful-commit invalidation. Injected abort/quota tests validate
atomic failure handling, **not** actual disk-quota exhaustion.

Storage belongs to the browser origin (scheme, host and port), profile and any
browser partitioning policy; the same name on another origin is another database.
Private browsing may deny IndexedDB, impose different quotas or remove data when
the session ends. The browser may evict non-persistent origin storage; users may
clear even persistent storage. Applications may use the browser StorageManager
API to inspect estimates/request persistence, but permission is not guaranteed
and this library neither reserves quota nor requests it automatically. Handle
quota/access errors and keep an application-appropriate backup or replica.
IndexedDB completion is browser transaction completion, not native filesystem
fsync durability, an eviction guarantee or a native retirement fence. No native
retirement emulation is provided. Browser-process crashes, OS/power failure and
private-mode/quota-exhaustion behavior have not been validated here.
