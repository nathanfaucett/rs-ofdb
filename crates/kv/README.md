# ofdb-kv

`ofdb-kv` is a KV-only facade with an embedded `Database` and a query/transaction `Client` for embedded or remote access. Default features are empty. Enable `in-memory` or `redb` for embedded storage, `remote` for network clients, `server` for hosting, and `sync` for explicit sync sessions.

`Client` provides `get`, `set`, `delete`, `scan`, `scan_prefix`, and `scan_all` for both embedded and remote stores. `Client::transaction()` provides the same operations on one transaction, with explicit `commit` and `rollback`, for embedded and remote stores. `Database` constructs and controls the embedded store; use `Database::client()` for queries and transactions. Values are opaque bytes. Expiry is an optional absolute millisecond deadline. Reads and scans use the local process clock for embedded access and the host clock for remote access. Callers do not provide clocks or timestamps.

A standalone write commits as one local transaction. Standalone reads and scans use one read transaction each. Operations inside a client transaction share one transaction and become durable together on commit. Remote calls outside a transaction are independent; a lost write response does not prove that the operation failed. Dropping an uncommitted remote transaction rolls it back when the server observes stream closure.

Create an embedded client with `Database::client()`. It shares storage with the database and remains usable after the database handle is dropped. `Database::open(path)` uses the `ofdb-kv` Redb table; `Database::open_with_table(path, name)` selects a table for existing Redb integrations. A remote client connects eagerly with `Client::connect` or `Client::connect_unix`. `Client::with_deadline(Duration)` configures a deadline for embedded or remote queries; no deadline is set by default. Configured embedded deadlines require a Tokio runtime with time enabled. A timeout does not prove rollback or guarantee interruption of blocking storage work.

Errors use the facade's `Error` and `ErrorKind` types; they do not expose Tonic statuses. Local storage errors and remote server responses retain KV categories through structured error details, not message parsing. Timeout and transport failures remain distinct from query failures. A lost write response does not prove that the write failed.

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

When `server` and an embedded backend are enabled, `Database::serve_tcp` or `Database::serve_unix` hosts the database. These methods serve a shared clone of the underlying store while the `Database` remains available for explicit sync. The serving future owns the bound listener until shutdown. The caller owns the shutdown future, transport, peer selection, authorization, and sync scheduling. The caller also removes Unix socket paths after shutdown. Sync batches commit separately; interruption does not roll back earlier batches, and a later explicit session resumes reconciliation. The facade does not add background sync.
