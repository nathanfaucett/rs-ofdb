# ofdb-kv

`ofdb-kv` is a KV-only facade with an embedded `Database` and a query-only `Client` for embedded or remote access. Default features are empty. Enable `in-memory` or `redb` for embedded storage, `remote` for network clients, `server` for hosting, and `sync` for explicit sync sessions.

`Client` provides `get`, `set`, `delete`, `scan`, `scan_prefix`, and `scan_all` for both embedded and remote stores. `Database` constructs and controls the embedded store; use `Database::client()` for its queries. Values are opaque bytes. Expiry is an optional absolute millisecond deadline. Reads and scans use the local process clock for embedded access and the host clock for remote access. Callers do not provide clocks or timestamps.

Each set/delete call commits one local store transaction. Each scan/read uses one store transaction for its operation. Remote calls are independent; there are no transactions across calls. A lost write response does not prove that the operation failed.

Create an embedded client with `Database::client()`. It shares storage with the database and remains usable after the database handle is dropped. A remote client connects eagerly with `Client::connect` or `Client::connect_unix`. `Client::with_deadline(Duration)` configures a deadline for embedded or remote queries; no deadline is set by default. Configured embedded deadlines require a Tokio runtime with time enabled. A timeout does not prove rollback or guarantee interruption of blocking storage work.

Errors use the facade's `Error` and `ErrorKind` types; they do not expose Tonic statuses. Local storage errors and remote server responses retain KV categories through structured error details, not message parsing. Timeout and transport failures remain distinct from query failures. A lost write response does not prove that the write failed.

```rust,ignore
let database = ofdb_kv::Database::in_memory();
let client = database.client();
client.set("name", b"Ada".to_vec(), None).await?;
let value = client.get("name").await?;

let remote = ofdb_kv::Client::connect("http://127.0.0.1:50051").await?;
let remote_value = remote.get("name").await?;
```

When `server` and an embedded backend are enabled, `Database::serve_tcp` or `Database::serve_unix` hosts the database. These methods serve a shared clone of the underlying store while the `Database` remains available for explicit sync. The serving future owns the bound listener until shutdown. The caller owns the shutdown future, transport, peer selection, authorization, and sync scheduling. The caller also removes Unix socket paths after shutdown. Sync batches commit separately; interruption does not roll back earlier batches, and a later explicit session resumes reconciliation. The facade does not add background sync.
