# ofdb-sql

The SQL facade provides an embedded control handle, `Database`, and a query and transaction `Client` for embedded or remote access. Enable only the features needed by your application.

- `in-memory` or `redb` enables an embedded database backend.
- `remote` enables the remote gRPC client and connection constructors.
- `sql` enables SQL-text translation. Typed statements do not require it.
- `sync` enables explicit SQL sync sessions. Transport and peer authorization remain caller-owned.
- `server` enables SQL request serving through `Database::serve_tcp` and, on Unix, `Database::serve_unix`.
- `sync` exposes explicit sessions on the embedded `Database`. `Client::transaction()` starts an explicit SQL transaction for embedded and remote clients; transactions are not exposed on `Database`.

Default features are empty. Remote-only builds do not include a local SQL engine or sync implementation. Neither handle has a default request deadline. `Client::with_deadline(Duration)` applies to embedded and remote queries. A configured embedded deadline requires a Tokio runtime with time enabled; an untimed embedded query does not require one. A timeout does not prove rollback or guarantee interruption of blocking storage work.

`serve_tcp` and `serve_unix` borrow the embedded database and serve a shared engine clone. The caller owns the database handle and shutdown future. The serving future owns the bound listener until shutdown. The caller owns Unix socket path cleanup. Sync is a separate explicit operation. An interrupted session can leave already-applied batches committed; run sync again to reconcile. SQL transactions use `Client::transaction()` for embedded and remote access. A remote transaction holds one server-side engine transaction across calls; dropping its handle rolls it back. Each execute batch is atomic within the transaction. Commit applies the transaction as one engine operation; rollback discards it. Remote calls do not share a transaction unless they use the same transaction handle.

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
