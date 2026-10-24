# ofdb-kv

`ofdb-kv` is a KV-only facade with an embedded `Database` and a query/transaction `Client` for embedded or remote access. Default features are empty. Enable `in-memory` or `redb` for embedded storage, `remote` for network clients, `server` for hosting, and `sync` for one-shot sessions and the continuous change-driven lifecycle.

`Client` provides `get`, `set`, `delete`, `scan`, `scan_prefix`, and `scan_all` for both embedded and remote stores. `Client::transaction()` provides the same operations on one transaction, with explicit `commit` and `rollback`, for embedded and remote stores. `Database` constructs and controls the embedded store; use `Database::client()` for queries and transactions. Values use the shared `ofdb-value::Value` type. It supports primitives, blobs, and nested JSON objects and arrays. Automerge reconciles nested values; concurrent edits to different map fields can merge, while concurrent scalar edits can produce a conflict. Expiry is an optional absolute millisecond deadline. Reads and scans use the local process clock for embedded access and the host clock for remote access. Callers do not provide clocks or timestamps.

A standalone write commits as one local transaction. Standalone reads and scans use one read transaction each. Operations inside a client transaction share one transaction and become durable together on commit. Remote calls outside a transaction are independent; a lost write response does not prove that the operation failed. Dropping an uncommitted remote transaction rolls it back when the server observes stream closure.

Create an embedded client with `Database::client()`. It shares storage with the database and remains usable after the database handle is dropped. `Database::open(path)` uses the `ofdb-kv` Redb table; `Database::open_with_table(path, name)` selects a table for existing Redb integrations. A remote client connects eagerly with `Client::connect` or `Client::connect_unix`. `Client::with_deadline(Duration)` configures a deadline for embedded or remote queries; no deadline is set by default. Configured embedded deadlines require a Tokio runtime with time enabled. A timeout does not prove rollback or guarantee interruption of blocking storage work.

The breaking value-format change uses a new persisted-document version and a new gRPC service name. Existing database documents and old clients are rejected; there is no migration path. Errors use the facade's `Error` and `ErrorKind` types; they do not expose Tonic statuses. Local storage errors and remote server responses retain KV categories through structured error details, not message parsing. Timeout and transport failures remain distinct from query failures. A lost write response does not prove that the write failed.

## Continuous synchronization

With `sync`, `Database::sync_changes()` returns `std::io::Result<ChangeSubscription>` (`Unsupported` for an in-memory handle). This opaque type belongs to the sync API: it exposes no Tokio receiver or revision bookkeeping. Obtain it before starting reconciliation, then await `changes.run(synchronize)`, where the callback implements `FnMut() -> Future<Output = Result<(), E>>` and supplies an authorized sync session. The sync library owns the initial pass, change coalescing, race-free rescheduling for commits during a pass, and retry of failed work without waiting for another commit. Retry backoff starts at 100 ms, doubles to a 30-second cap, and resets on success. The callback reports/logs its errors and must return `Err` when delivery is still pending, including unavailable peers.

### Selected per-peer sessions

`Database::synchronize_selected_checked(&mut changes, &mut transport, role, config, check)` selects a watermark before reads, maps committed invalidations using the actual physical table name, reconciles the union of both peers' affected logical keys/latest generations, and acknowledges **only after success**. Keep one unmerged subscription per database/peer; create it anew on connection/reconnection. Incoming apply remains pending for other peers, and commits during a pass cannot be accidentally acknowledged. `Database::sync_invalidations(&ChangeWork)` also exposes the logical mapping for caller-managed selection. The facade re-exports `ChangeWork` and `KvInvalidation`.

```rust,ignore
let mut peer_changes = database.sync_changes()?;
database.synchronize_selected_checked(
    &mut peer_changes, &mut transport, ofdb_kv::SyncRole::Initiator,
    ofdb_kv::SyncConfig::default(), || core::future::ready(Ok(())),
).await?;
```

The shared signal retains 128 commits, each capped at 128 physical invalidations / 16 KiB of identity bytes; oversized commits become All. Tokens can combine retained commits: these are notification-retention limits, not session-memory limits. `changed()` consumes wakeups without acknowledging selected work. Tokens/subscriptions must belong to this database; they do not retain storage or durable history. Fresh subscriptions, unknown writes, gaps and overflow select broad comparison. `synchronize_checked` remains the broad one-shot API, now exchanging native missing history where possible. KV wire v3 is incompatible with earlier versions. Hash/dependency validation, atomic complete batches, snapshot recovery/merge, generation replacement and tombstone semantics apply to both APIs. Memory still returns `Unsupported` from `sync_changes`; explicitly invoke broad sessions there.

**Local subscriptions do not observe remote-only writes.** Callers must provide authorized remote wakeups/receive delivery or explicitly invoke reconciliation even with no local selected work. Neither a local runner nor an idle local subscription alone guarantees bidirectional realtime delivery. After incoming application, local notifications drive fan-out. Repeated known history settles without no-op commits/echo. Selected inventories scale with selected document histories, not the whole database; a document inventory/delta must fit a configured frame.

SQL and KV re-export the **same** `ChangeSubscription`. Use `sql.sync_changes()?.merge(kv.sync_changes()?)` and one **broad** callback that exchanges SQL and KV to avoid duplicate runners. Merged subscriptions are not suitable for selective logical mapping; keep each store/peer's subscription separate for selected sessions. `changed().await -> std::io::Result<()>` remains available for admission invalidation, not for implementing another sync scheduler. Cloning creates an independent consumer requiring initial broad reconciliation.

The library spawns no task. Poll `run` on a Tokio runtime with time enabled; dropping its future cancels the current callback or retry timer. A caller can select the runner against its shutdown/admission future, and create a fresh runner when its authorized peer scope changes. The runner also stops when all subscribed backends close, including retirement; `changed` returns `BrokenPipe` after closure and pending invalidations. A subscription alone does not retain storage, though its callback can. Existing clients can keep storage alive after `Database` is dropped.

The committed signal covers shared clients, hosted queries, explicit transactions and incoming sync. Read-only commits, identical byte writes, failed/rolled-back transactions and replayed sync are silent. Touched-key tracking suppresses net byte no-ops; structural table drops are conservatively dirty without copying their contents. A history change can notify even when the visible value is unchanged; elapsed expiry alone is not a commit and does not notify. Notifications are process-local coalesced invalidations, not a durable log, and do not monitor writes outside the shared backend.

```rust,ignore
let database = ofdb_kv::Database::in_memory();
let client = database.client();
client.set("name", b"Ada".to_vec(), None).await?;
let value = client.get("name").await?;
let mut transaction = client.transaction().await?;
transaction.set("greeting", b"hello".to_vec(), None).await?;
transaction.commit().await?;

let remote = ofdb_kv::Client::connect("http://127.0.0.1:50051").await?;
let remote_value = remote.get("name").await?;
```

When `server` and an embedded backend are enabled, `Database::serve_tcp` or `Database::serve_unix` hosts the database. These methods serve a shared clone of the underlying store while the `Database` remains available for explicit sync. The serving future owns the bound listener until shutdown. The caller owns the shutdown future, transport, peer selection and authorization; `ChangeSubscription::run` owns continuous scheduling and retry. The caller also removes Unix socket paths after shutdown. Sync batches commit separately; interruption does not roll back earlier batches, and a later explicit session resumes reconciliation. Opening or serving does not automatically start replication; explicitly poll the sync runner.
