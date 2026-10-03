# ofdb-sql

The SQL facade provides an embedded `Database` and a remote `Client`. Enable only the features needed by your application.

- `in-memory` or `redb` enables an embedded database backend.
- `remote` enables the remote gRPC client.
- `sql` enables SQL-text translation. Typed statements do not require it.
- `sync` enables explicit SQL sync sessions. Transport and peer authorization remain caller-owned.
- `server` enables SQL request serving through `Database::serve_tcp` and, on Unix, `Database::serve_unix`.
- `sync` exposes explicit sessions on the embedded `Database`; remote `Client` has no sync or transaction methods.

Default features are empty. Remote-only builds do not include a local SQL engine or sync implementation. Both handles have no default request deadline; use `Client::with_deadline(Duration)` to set one for remote requests.

`serve_tcp` and `serve_unix` borrow the embedded database and serve a shared engine clone. The caller owns the database handle and shutdown future. The serving future owns the bound listener until shutdown. The caller owns Unix socket path cleanup. Sync is a separate explicit operation. An interrupted session can leave already-applied batches committed; run sync again to reconcile.

```rust,ignore
let database = ofdb_sql::Database::in_memory();
database.execute(statements).await?;

// With the `server` feature, serve the same embedded database while retaining
// the handle for sync and local transactions.
database
    .serve_tcp(address, shutdown_signal())
    .await?;
```
