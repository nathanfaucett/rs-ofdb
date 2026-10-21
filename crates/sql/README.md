# ofdb-sql

The SQL facade provides an embedded control handle, `Database`, and a query and transaction `Client` for embedded or remote access. Enable only the features needed by your application.

- `in-memory` or `redb` enables an embedded database backend.
- `remote` enables the remote gRPC client and connection constructors.
- `sql` enables SQL-text translation. Typed statements do not require it.
- `sync` enables one-shot sessions and the continuous change-driven lifecycle. Transport and peer authorization remain caller-owned.
- `server` enables SQL request serving through `Database::serve_tcp` and, on Unix, `Database::serve_unix`.
- `sync` exposes explicit sessions on the embedded `Database`. `Client::transaction()` starts an explicit SQL transaction for embedded and remote clients; transactions are not exposed on `Database`.

Default features are empty. Remote-only builds do not include a local SQL engine or sync implementation. Neither handle has a default request deadline. `Client::with_deadline(Duration)` applies to embedded and remote queries. A configured embedded deadline requires a Tokio runtime with time enabled; an untimed embedded query does not require one. A timeout does not prove rollback or guarantee interruption of blocking storage work.

`serve_tcp` and `serve_unix` borrow the embedded database and serve a shared engine clone. The caller owns the database handle and shutdown future. The serving future owns the bound listener until shutdown. The caller owns Unix socket path cleanup. Sync is a separate explicit operation. An interrupted session can leave already-applied batches committed; run sync again to reconcile. SQL transactions use `Client::transaction()` for embedded and remote access. A remote transaction holds one server-side engine transaction across calls; dropping its handle rolls it back. Each execute batch is atomic within the transaction. Commit applies the transaction as one engine operation; rollback discards it. Remote calls do not share a transaction unless they use the same transaction handle.

## Continuous synchronization

With `sync`, `Database::sync_changes()` returns `std::io::Result<ChangeSubscription>` (`Unsupported` for an in-memory handle). This opaque type belongs to the sync API: it exposes no Tokio receiver or revision bookkeeping. Obtain it before starting reconciliation, then await `changes.run(synchronize)`, where the callback implements `FnMut() -> Future<Output = Result<(), E>>` and supplies an authorized sync session. The sync library owns the initial pass, change coalescing, race-free rescheduling for commits during a pass, and retry of failed work without waiting for another commit. Retry backoff starts at 100 ms, doubles to a 30-second cap, and resets on success. The callback reports/logs its errors and must return `Err` when delivery is still pending, including unavailable peers.

SQL and KV re-export the **same** `ChangeSubscription`. Use `sql.sync_changes()?.merge(kv.sync_changes()?)` and one callback that exchanges SQL and KV to avoid duplicate runners. `changed().await -> std::io::Result<()>` remains available for admission invalidation, not for implementing another sync scheduler. Cloning a subscription starts observing at the current state, like a fresh subscription.

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
