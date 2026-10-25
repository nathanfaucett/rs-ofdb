# ofdb-sql

The SQL facade provides an embedded control handle, `Database`, and a query and transaction `Client` for embedded or remote access. Enable only the features needed by your application.

- `in-memory` or `redb` enables a native embedded database backend; `in-memory` also runs in browsers.
- `indexeddb` enables browser persistence on `wasm32-unknown-unknown` with async `Database::open_indexeddb(name)`.
- `remote` enables the remote gRPC client and connection constructors.
- `sql` enables SQL-text translation. Typed statements do not require it.
- `sync` enables one-shot sessions and the continuous change-driven lifecycle. Transport and peer authorization remain caller-owned.
- `server` enables SQL request serving through `Database::serve_tcp` and, on Unix, `Database::serve_unix`.
- `native-stage` enables optional native sync staging; `sync` already provides memory and caller-defined staging. `Client::transaction()` starts an explicit SQL transaction for embedded and remote clients; transactions are not exposed on `Database`.

Default features are empty. Remote-only builds do not include a local SQL engine or sync implementation. Neither handle has a default request deadline. `Client::with_deadline(Duration)` applies to embedded and remote queries. A configured native embedded deadline requires a Tokio runtime with time enabled; browser deadlines use platform timers; an untimed embedded query does not require one. A timeout does not prove rollback or guarantee interruption of blocking storage work.

`serve_tcp` and `serve_unix` borrow the embedded database and serve a shared engine clone. The caller owns the database handle and shutdown future. The serving future owns the bound listener until shutdown. The caller owns Unix socket path cleanup. Sync is a separate explicit operation. An interrupted session can leave already-applied batches committed; run sync again to reconcile. SQL transactions use `Client::transaction()` for embedded and remote access. A remote transaction holds one server-side engine transaction across calls; dropping its handle rolls it back. Each execute batch is atomic within the transaction. Commit applies the transaction as one engine operation; rollback discards it. Remote calls do not share a transaction unless they use the same transaction handle.

## Continuous synchronization

With `sync`, `Database::sync_changes()` returns `std::io::Result<ChangeSubscription>` for both SQL memory and Redb backends. This opaque type belongs to the sync API: it exposes no Tokio receiver or revision bookkeeping. Obtain it before starting reconciliation, then await `changes.run(synchronize)`, where the callback implements `FnMut() -> Future<Output = Result<(), E>>` and supplies an authorized sync session. The sync library owns the initial pass, change coalescing, race-free rescheduling for commits during a pass, and retry of failed work without waiting for another commit. Retry backoff starts at 100 ms, doubles to a 30-second cap, and resets on success. The callback reports/logs its errors and must return `Err` when delivery is still pending, including unavailable peers.

### Selected per-peer sessions

SQL wire **v7** is incompatible with earlier versions. Both peers use selected negotiation and compare the union of their invalidations; either All forces broad discovery. `Database::synchronize_selected_checked` uses memory staging; `synchronize_selected_with_stage_factory_checked` accepts caller staging. Selection is not authorization: these APIs require whole-database admission. Restricted history/value/candidate policies use the existing table-scoped validated APIs.

```rust,ignore
let mut peer_changes = database.sync_changes()?; // fresh peer => broad
let work = peer_changes.select(); // before any reconciliation reads
let invalidations = match &work {
    Some(work) => database.sync_invalidations(work).await?,
    None => Vec::new(), // still reconcile on a remote wakeup
};
database.synchronize_selected_checked(
    &mut transport, &config, role, &invalidations,
    || core::future::ready(Ok(())), // use real caller admission/cancellation
).await?;
if let Some(work) = work {
    assert!(peer_changes.acknowledge(work));
}
```

Keep an independent, unmerged subscription per database/peer; recreate it on reconnect/restart or peer changes. Select before reads; acknowledge only after that work is included in a successful pass. Errors/cancellation leave work retryable; discard failed transports. Re-select before waiting: later commits remain pending even if observed during the pass. `changed()` consumes wakeups, not selected work. Fresh/cloned consumers, gaps, overflow and unknown writes require All; table invalidation permits a table scan and catalog/dependency changes require broad discovery.

The shared signal retains 128 commits, each capped at 128 physical invalidations / 16 KiB of identity bytes; oversized batches become All. Selected tokens can combine retained commits. This bounds notification retention, not document history or session memory. The facade re-exports `ChangeWork` and `SqlInvalidation`; map tokens only against their corresponding database.

The engine owns `RowReconciler` history/state capabilities; SQL sync owns protocol units and orchestration. Transaction-consistent native summaries avoid snapshot serialization for discovery. Exact-row work avoids unrelated row-ID scans. Ordinary metadata-free rows send missing native changes; bootstrap, metadata mismatches and dependency recovery send state plus metadata and merge concurrent history. Complete-row/catalog apply, scoped validation, derived indexes and caller checks remain enforced. Sessions are not globally frozen or atomic. SQL frames/apply batches are limited to 1 MiB; `max_session_bytes` defaults to 64 MiB and transports must bound reads before buffering.

**Local notifications cannot observe remote-only commits.** Callers provide authorized remote wakeups/receive delivery or explicitly invoke both sides, including empty local selections. If remote pending work cannot be supplied reliably, request All. Incoming apply then wakes local fan-out; known history settles without echo. No automatic bidirectional realtime delivery or durable peer cursor is promised.

SQL and KV re-export the **same** `ChangeSubscription`. Use `sql.sync_changes()?.merge(kv.sync_changes()?)` and one **broad** callback that exchanges SQL and KV to avoid duplicate runners. Merged subscriptions are not suitable for selective logical mapping. `changed().await -> std::io::Result<()>` remains available for admission invalidation, not for implementing another sync scheduler. Cloning creates an independent consumer requiring initial broad reconciliation.

The library spawns no task. Poll `run` on a Tokio runtime with time enabled; dropping its future cancels the current callback or retry timer. A caller can select the runner against its shutdown/admission future, and create a fresh runner when its authorized peer scope changes. The runner also stops when all subscribed backends close, including retirement; `changed` returns `BrokenPipe` after closure and pending invalidations. A subscription alone does not retain storage, though its callback can. Existing clients can keep storage alive after `Database` is dropped.

The committed signal covers backend clones, hosted queries, explicit transactions, conflict resolution and incoming sync. Read-only commits, identical byte writes, failed/rolled-back transactions and replayed sync are silent. Touched-key tracking suppresses net byte no-ops, including index rebuilds; a successful table drop is conservatively dirty even if recreated in the same transaction, without scanning/copying the dropped table. Visible row equality does not imply equal replicated history. Notifications are process-local coalesced invalidations, not a durable log, and do not monitor writes outside the shared backend.

```rust,ignore
let database = ofdb_sql::Database::in_memory();
let client = database.client();
client
    .execute_sql("CREATE TABLE items (id UUID PRIMARY KEY)", None)
    .await?;

let mut transaction = client.transaction().await?;
transaction
    .execute_sql(
        "INSERT INTO items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
        None,
    )
    .await?;
transaction.commit().await?;

let remote = ofdb_sql::Client::connect("http://127.0.0.1:50051").await?;
remote.execute_sql("SELECT id FROM items", None).await?;

// With the `server` feature, serve the same embedded database while retaining
// the handle for sync. Queries and transactions use the Client.
database
    .serve_tcp(address, shutdown_signal())
    .await?;
```

## Browser SQL persistence

Enable `indexeddb` for typed statements, and add `sql` only for SQL-text translation:

```toml
[dependencies]
ofdb-sql = { version = "0.1", default-features = false, features = ["indexeddb", "sql"] }
```

```rust,ignore
let database = ofdb_sql::Database::open_indexeddb("application").await?;
let client = database.client();
client.execute_sql("CREATE TABLE IF NOT EXISTS people (name TEXT PRIMARY KEY)", None).await?;
client.execute_sql("INSERT INTO people VALUES ('Ada')", None).await?;
```

The existing engine and Automerge Row Reconciler execute all statements; IndexedDB
only persists bytes. Catalogs, rows, Tombstones, constraints and derived indexes
commit together in one database-wide native transaction. Logical DDL changes data,
not the physical IndexedDB schema. SQL names use `ofdb:3:sql:<name>`; KV has a
separate namespace. Failed/unsupported opens return errors, never volatile storage.

Explicit transactions own a consistent snapshot plus staged writes across browser
event-loop awaits. Commit rejects a stale mutating snapshot without replaying it;
SQL currently categorizes storage conflicts/commit failures as `ErrorKind::Storage`,
retaining the backend detail in the error message. Rollback/drop discard staging;
a failed SQL statement aborts its explicit transaction. Commit success means native
IndexedDB completion. Canceling an issued commit does not prove rollback; the shared
adapter continues observing completion independently of the query future.

Snapshot loading is O(database size) memory/I/O and ordered reconstruction adds
O(N log N) work, retaining two Rust copies plus transient browser data. This is not
selective physical I/O. Names are origin-scoped; private-mode restrictions, quota,
eviction and user-cleared site data apply. Native completion does not promise
filesystem-equivalent durability or protection from eviction/power loss. See the
[byte adapter](../btree-indexeddb/README.md) for format/lifecycle details.

Window and Dedicated Worker tests run in Chrome and Firefox without a Tokio
runtime, native networking or Redb. Consumers must provision the documented
[Wasm stack budget](../../tests/browser/README.md#stack-budget) (the harness uses
16 MiB). No WebKit support or default-stack guarantee is claimed. SQL browser/native
sync interoperability and facade committed subscriptions remain ticket 08 work;
`sync_changes()` on IndexedDB currently returns Unsupported. Cross-tab wakeups are
not provided; revision checks still prevent stale writes across independent opens.
