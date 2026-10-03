# ofdb-kv

`ofdb-kv` is a KV-only facade with an embedded `Database` and remote `Client`. Default features are empty. Enable `in-memory` or `redb` for embedded storage, `remote` for clients, `server` for hosting, and `sync` for explicit sync sessions.

Both handles provide `get`, `set`, `delete`, `scan`, `scan_prefix`, and `scan_all`. Values are opaque bytes. Expiry is an optional absolute millisecond deadline. Reads and scans use the local process clock for embedded access and the host clock for remote access. Callers do not provide clocks or timestamps.

Each set/delete call commits one local store transaction. Each scan/read uses one store transaction for its operation. Remote calls are independent; there are no transactions across calls. A lost write response does not prove that the operation failed.

A remote client connects eagerly with `Client::connect` or `Client::connect_unix`. `Client::with_deadline(Duration)` configures a per-request deadline; no deadline is set by default. A deadline can cancel the client wait but does not prove that the host stopped or rolled back the operation.

Errors use the facade's `Error` and `ErrorKind` types; they do not expose Tonic statuses. Local storage errors and remote server responses retain KV categories through structured error details, not message parsing. Timeout and transport failures remain distinct from query failures. A lost write response does not prove that the write failed.

When `server` and an embedded backend are enabled, `Database::serve_tcp` or `Database::serve_unix` hosts the database. These methods serve a shared clone of the underlying store while the `Database` remains available for explicit sync. The serving future owns the bound listener until shutdown. The caller owns the shutdown future, transport, peer selection, authorization, and sync scheduling. The caller also removes Unix socket paths after shutdown. Sync batches commit separately; interruption does not roll back earlier batches, and a later explicit session resumes reconciliation. The facade does not add background sync.
