# Browser SQL and KV

Both Rust facades support persistent, offline queries in single-threaded
`wasm32-unknown-unknown` **Window and Dedicated Worker** contexts. IndexedDB stores
bytes; the existing SQL engine, KV values, Automerge history and sync protocols
still define database behavior. No Tokio runtime, shared Wasm memory or cross-origin
isolation is required. The adapter uses `std`; this is not a `no_std` I/O backend.

## Supported baseline and feature selection

Defaults are empty. Select a backend explicitly; browser construction is async.

| Facade     | Browser features                         | Capability                                                              |
| ---------- | ---------------------------------------- | ----------------------------------------------------------------------- |
| `ofdb-kv`  | `indexeddb`                              | Typed values, scans, expiry and explicit transactions                   |
| `ofdb-kv`  | `indexeddb,sync`                         | Also checked broad/selected sessions and caller-polled continuous sync  |
| `ofdb-sql` | `indexeddb`                              | Typed statements and explicit transactions, without the text translator |
| `ofdb-sql` | `indexeddb,sql`                          | Also SQL-text translation                                               |
| `ofdb-sql` | `indexeddb,sync` or `indexeddb,sql,sync` | Also checked broad/selected/scoped sessions and continuous sync         |

`native-stage` is unnecessary in a browser. Even if explicitly enabled with SQL
sync, file staging APIs and dependencies remain absent on this target. Sync's
memory-stage limit does **not** limit the storage snapshot described below.
Do not enable `redb`, `remote`, `server`, Iroh, native TCP/Unix/gRPC or native file
staging for a supported browser build. Compile the specific facade, not the native
CLI/server workspace with Wasm `--all-features`.

The release validation baseline is **Chrome/Chromium engine 156 (tested using
Google Chrome 156.0.8078.47) and Firefox 158.0.1**, in both contexts, on Linux.
These are tested configurations, not established minimum versions or a claim for
all later browsers. The supported validation scope is Chromium and Firefox;
WebKit is out of scope and is not a release requirement.
Mobile, private-mode storage behavior and older engines are also unvalidated.
See [validation evidence and reproduction](browser-validation.md).

## Application setup

These are Rust APIs to call from an async browser entry point or event handler
using your existing Wasm integration. For a complete runnable KV page, see the
[persistent browser example](../examples/browser/README.md) (`just browser-example-serve`).
There is no public JavaScript SDK, npm package or bundled browser transport. Build with
`--target wasm32-unknown-unknown` and install that target using rustup.

Packages have **not been published**. Until registry prerequisites exist, use
source path dependencies from your application (example: an application adjacent
to an `ofdb` checkout):

```toml
[dependencies]
ofdb-kv = { version = "0.1", path = "../ofdb/crates/kv", default-features = false, features = ["indexeddb"] }
ofdb-sql = { version = "0.1", path = "../ofdb/crates/sql", default-features = false, features = ["indexeddb", "sql"] }
```

Use only the facade(s) you need; add `sync` explicitly if required. Once packages
are available from a registry, omit `path`. Do not assume the existing interop
derives constitute a stable JavaScript API.

The validated SQL harness uses an optimized dev profile and a **16 MiB Wasm linear
stack**. Configure an adequate stack in the application's `.cargo/config.toml`:

```toml
[target.wasm32-unknown-unknown]
rustflags = ["-C", "link-arg=-zstack-size=16777216"]
```

```toml
# Application Cargo.toml
[profile.dev]
opt-level = 1
```

This is a supported test configuration, not a measured minimum. Default-stack SQL
previously trapped. Stack memory is additional to database snapshots, engine
state, sync staging and JS allocations. No default-stack or release memory ceiling
is claimed. Cargo discovers `.cargo/config.toml` from its working directory, not
from an arbitrary `--manifest-path`; pass `--config` when building from elsewhere.

## KV quickstart

`Database` constructs storage; `Client` performs every ordinary query and creates
explicit transactions. This function can run again against the same origin/name.

```rust
pub async fn kv_quickstart() -> Result<(), ofdb_kv::Error> {
    let database = ofdb_kv::Database::open_indexeddb("preferences").await?;
    let client = database.client();
    client.set("theme", "dark", None).await?;
    assert_eq!(client.get("theme").await?, Some(ofdb_kv::Value::from("dark")));

    let mut transaction = client.transaction().await?;
    transaction.set("theme", "light", None).await?;
    transaction.set("offline", true, None).await?;
    // Application awaits are allowed between operations; this is Rust staging.
    transaction.commit().await?;
    assert_eq!(client.scan_prefix("theme").await?.len(), 1);
    Ok(())
}
```

`None` means no expiry; an expiry is an absolute millisecond timestamp, not a
relative TTL. Shared values include primitives, exact integer/float representations,
blobs and nested JSON. No JavaScript-number conversion is required. Explicit
`rollback().await?` or dropping an uncommitted transaction discards staging.
A standalone set/delete commits as its own transaction. Cloned clients share
storage and remain usable after `Database` is dropped.

## SQL quickstart

With `indexeddb,sql`, the same Client/Transaction APIs execute translated statements:

```rust
pub async fn sql_quickstart() -> Result<(), ofdb_sql::Error> {
    let database = ofdb_sql::Database::open_indexeddb("address-book").await?;
    let client = database.client();
    client.execute_sql(
        "CREATE TABLE IF NOT EXISTS people (name TEXT PRIMARY KEY, active BOOLEAN)",
        None,
    ).await?;
    let mut transaction = client.transaction().await?;
    transaction.execute_sql("DELETE FROM people WHERE name = 'Ada'", None).await?;
    transaction.execute_sql("INSERT INTO people VALUES ('Ada', TRUE)", None).await?;
    transaction.commit().await?;
    let results = client.execute_sql("SELECT name FROM people WHERE active = TRUE", None).await?;
    assert!(results[0].rows.contains(&ofdb_sql::Row::from(["Ada"])));
    Ok(())
}
```

Each standalone statement batch is atomic. Explicit transactions enlist catalogs,
rows, history, Tombstones and derived indexes in one commit. Execution failures
retain SQL's aborted-transaction rules; committing an aborted transaction cannot
persist earlier writes. Translation failures before execution are not execution
failures. Roll back/drop staging when abandoning a transaction.

For **typed statements**, omit `sql`; use `Client::execute(Vec<Statement>)` or
`Transaction::execute` instead of `execute_sql`. For example, a translator-free
explicit insert against an existing `people` schema is:

```rust
pub async fn typed_insert(client: &ofdb_sql::Client) -> Result<(), ofdb_sql::Error> {
    use ofdb_sql::{Query, QueryInsert, Row, Statement};
    let mut transaction = client.transaction().await?;
    transaction.execute(vec![Statement::Query(Query::Insert(QueryInsert {
        table: "people".into(),
        row: Row::from([ofdb_sql::Value::from("Grace"), ofdb_sql::Value::from(true)]),
        returning: Some(vec!["name".into()]),
    }))]).await?;
    transaction.commit().await?;
    Ok(())
}
```

Typed DDL/schema construction and translator-free selection are exercised in
[`typed_sql_statements_persist_without_translation`](../tests/browser/src/sql_indexeddb.rs).
The release matrix independently compiles `indexeddb` without translator feature
unification. Typed SQL, natural/composite primary keys, constraints, conflict
inspection and explicit Resolution use the same domain contracts as native SQL.

## Transaction and error contract

- A transaction loads one consistent committed snapshot plus its revision. Reads
  observe that snapshot and pending writes across arbitrary caller awaits. Native
  IndexedDB transactions are short-lived; there is no keepalive loop.
- A mutating commit compares the global persisted revision and atomically writes
  the complete diff in one native transaction. Independent handles/tabs cannot
  silently overwrite newer commits. A stale writer fails; no automatic replay.
  Global revision detection can reject writes to unrelated keys/tables.
- Read-only and net byte no-op commits do not advance revisions or publish changes.
  Success is reported only after native transaction **completion**, not request
  success. Injected abort/quota tests prove atomic failure handling, not real
  disk-quota exhaustion or power-loss durability.
- KV exposes structured `Conflict` for revision conflicts. SQL categorizes backend
  conflicts as `Storage`; other access/quota/abort/closure failures retain facade
  storage errors with safe actionable detail. Raw browser messages, keys and
  stored values are not copied into adapter error details. Do not message-parse
  errors into new categories or silently select volatile storage on failure.
- `Client::with_deadline(Duration)` sets an optional per-operation budget, with no
  fixed default. Clones keep independent settings; transactions inherit settings
  with a fresh budget per operation. SQL translation/execution share one budget.
  Construction and caller idle time are excluded. Timers are cooperative and can
  be throttled; they cannot hard-interrupt synchronous computation.
- **Timeout/cancellation does not prove rollback.** Once native commit is issued,
  its independently owned completion observer can persist and publish after the
  caller stops polling. Dropping unissued staging is different from undoing an
  issued write. Reconcile/read the actual state before deciding how to recover.

## Caller-owned synchronization

Enable `indexeddb,sync` (plus `sql` for text). Keep transport, peers, authorization,
remote wakeups and runner lifetime in the application. SQL wire v7 and KV wire v3
are unchanged; committed native-generated fixtures validate protocol/history
interoperability, not a live network transport. Browser-local callback futures may
own `Rc` and do not need `Send`.

For one-shot checked sessions see [SQL selected sessions](../crates/sql/README.md#selected-per-peer-sessions)
and [KV selected sessions](../crates/kv/README.md#selected-per-peer-sessions).
For continuous broad sessions the shared runner can be used with an application's
existing authorized exchange callback:

```rust
pub async fn run_browser_sync<F, Fut, E>(
    database: &ofdb_kv::Database,
    synchronize: F,
) -> std::io::Result<()>
where
    F: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<(), E>>,
{
    let changes = database.sync_changes()?;
    changes.run(synchronize).await;
    Ok(())
}
```

The application must poll/await the future (or select it against shutdown). Merely
constructing it starts no task. Return `Err` when work remains undelivered and
report callback errors yourself. The runner performs initial reconciliation,
coalesces commits, retains changes arriving during a pass, and retries from 100 ms
up to 30 seconds, resetting after success. Dropping it cancels callback/retry
polling, not previously committed persistence. Last-backend closure also stops it;
a callback holding Database/Client handles keeps that backend alive.

SQL and KV share `ChangeSubscription`; merge them only for one **broad** callback
that exchanges both stores. For selective sessions keep independent unmerged
subscriptions per database/peer, select before reads, and acknowledge only after
successful delivery. Fresh consumers, retention overflow/gaps or unknown external
writes require broad recovery. Notifications retain at most 128 commits with
per-commit identity limits; they are local invalidation, not a durable replay log.

**Independent opens, other tabs/workers and remote-only writes do not wake local
subscriptions.** Supply authorized remote wakeups and explicit broad recovery
when external writes may have been missed. The local continuous runner alone
cannot guarantee bidirectional real-time synchronization. Selected logical work
does not avoid the whole-database physical snapshot.

## Browser storage/lifecycle limits

Names are scoped to origin (scheme/host/port), browser profile and partitioning
policy. SQL and KV use disjoint physical namespaces; the same name is not a shared
store across kinds or origins. Private browsing can deny access, change quota or
delete data at session end. Quota is not reserved. Browser eviction and user-cleared
site data can remove storage; StorageManager estimates/persistence requests are
application policy and offer no unconditional protection. Keep a suitable backup
or replica. IndexedDB completion is **not** native filesystem fsync equivalence.

Open validates physical version, stores, format, kind and revision metadata;
incompatible or older experimental formats fail without reset/migration. A
blocked event fails immediately; a 5,000 ms browser timer additionally bounds
queued opens and validation. Throttling/suspension can delay callback delivery.
Close blocking peer contexts and retry explicitly. Late connections are closed;
refused late upgrades cannot silently create storage.

On version change/deletion, connections close and all sharing clients become
stale. New queries and commits, even read-only staging, fail clearly; there is no
automatic reopen, replay or staging transfer. Issued commits still settle normally.
Choose a compatible fresh handle explicitly and decide how to handle old staging.
See [the physical adapter policy](../crates/btree-indexeddb/README.md#lifecycle-and-browser-failures).

## Snapshot cost and exclusions

Every transaction initially reads/copies the **whole physical database**, including
history and all logical tables. Base and staged snapshots retain two Rust copies
plus ordered-map overhead and transient IndexedDB/JS data. Read/copy/memory cost is
O(database size); ordered reconstruction adds O(N log N) CPU work. Building the
commit diff visits the snapshot, even though only net changes are written.
[Measured samples](browser-validation.md#snapshot-samples) describe transaction
snapshot startup, not application module/open startup or a peak-heap ceiling.
Budget stack, engine/history, JS and concurrent transactions separately. There is
no established safe maximum database size; profile your workload before adopting
this initial correctness-first backend for large histories.

Not included: JavaScript SDK, automatic cross-tab observation/coordination,
SharedWorker hosting, bundled browser transport, native gRPC, OPFS/filesystem
emulation, native retirement fence, Redb import/migration, implicit format upgrade,
automatic retry of queries, selective physical reads or a durable peer cursor.
Whole-browser/OS crash, power-loss, real eviction/quota exhaustion and private-mode
persistence have not been validated. Native memory/Redb support remains separate.
