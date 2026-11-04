# Offline First Database Workspace (`ofdb`)

`ofdb` is an embedded, local-first database workspace in Rust for offline-first applications, autonomous edge nodes, and decentralized peer-to-peer systems. It provides two self-contained database models:

- [**`ofdb-sql`**](crates/sql/README.md): Relational SQL engine with schema catalogs, typed statements, secondary indexes, and row-level CRDT synchronization.
- [**`ofdb-kv`**](crates/kv/README.md): Key-value store for shared typed values with Automerge-backed history, UUIDv7 key generations, and millisecond expiry.

Browser Rust applications can open persistent SQL and KV databases asynchronously with `indexeddb`. See [browser support and limits](docs/browser.md) and the [validation matrix](docs/browser-validation.md).

---

## Why Local-First

Traditional client-server architectures require continuous connectivity. When connectivity drops, operations block or fail. `ofdb` is built around the **Local-First principle**:

1. **Local Embedded Operations**: Reads and writes use local storage (Redb, in-memory, or browser IndexedDB) with backend-specific transaction guarantees. Applications stay fully functional offline.
2. **Conflict-Free Convergence**: Rows (SQL) and key generations (KV) are tracked as Automerge CRDT documents. Concurrent offline edits merge deterministically without a central coordinator.
3. **Transport-Neutral Sync**: Replication works over any byte-stream implementing [`SyncTransport`](crates/sql-sync/src/transport.rs) — TCP, Unix sockets, WebSockets, or P2P meshes like Iroh.
4. **Clean Engine/Network Split**: Transport, authentication, and scheduling belong to the application. `ofdb` governs only storage, transactions, and state reconciliation.

---

## Choosing `ofdb-sql` vs `ofdb-kv`

| Capability              | `ofdb-sql`                                                                          | `ofdb-kv`                                                                                                         |
| :---------------------- | :---------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------- |
| **Data Model**          | Relational tables, typed values (`UUID`, `TEXT`, `INTEGER`, `BLOB`, `JSON`)         | Shared `Value` types: primitives, blobs, nested JSON objects/arrays, keyed by UTF-8 strings                       |
| **Queries**             | SQL text (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `JOIN`) and typed statement trees | Key operations: `get`, `set`, `delete`, `scan`, `scan_prefix`, `scan_all`                                         |
| **Indexes**             | Primary keys (UUIDv7) and secondary unique/non-unique indexes                       | Ordered UTF-8 key prefixes and ranges                                                                             |
| **Conflict Resolution** | Per-column CRDT via Automerge; greatest UUIDv7 selects visible schema               | Nested values reconcile through Automerge; tombstones win delete/update races; greatest UUIDv7 selects generation |
| **TTL / Expiry**        | Not applicable (application logic)                                                  | Optional absolute millisecond deadline (`expires_at`)                                                             |
| **Use Cases**           | Structured apps, document/user records, multi-table graphs                          | Caches, blobs, session states, config stores, high-throughput lookups                                             |

---

## Architecture: The Handle Model

Both facades separate database management from query execution:

- **`Database` (Control Handle)**: Manages storage lifecycle (`open`, `in_memory`), hosts remote gRPC endpoints (`serve_tcp`, `serve_unix`), and runs sync sessions (`synchronize`). Does **not** execute queries.
- **`Client` (Query Handle)**: The sole interface for reading and writing. Get an embedded client via `database.client()` or connect remotely via `Client::connect("http://...")`. Same API for local memory, on-disk Redb, or network.
- **`Transaction`**: Explicit multi-statement transactions via `client.transaction().await?` with atomic `commit()` and `rollback()`. Remote transactions stream over one connection and roll back if the client handle drops.

---

## Quickstart

### Feature Flags

Crates use empty default features. Enable what you need:

**`ofdb-sql`**: `in-memory`, `indexeddb`, `redb`, `sql`, `remote`, `server`, `sync`, `macros`

**`ofdb-kv`**: `in-memory`, `indexeddb`, `redb`, `remote`, `server`, `sync`

### 1. Embedded SQL

```toml
[dependencies]
ofdb-sql = { version = "0.1", default-features = false, features = ["redb", "sql"] }
tokio = { version = "1.0", features = ["full"] }
```

```rust,no_run
use ofdb_sql::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = Database::open("app_data.db")?;
    let client = database.client();

    client.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", None).await?;

    let mut tx = client.transaction().await?;
    tx.execute_sql(
        "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Alice')",
        None,
    ).await?;
    tx.commit().await?;

    let results = client.execute_sql("SELECT id, name FROM users", None).await?;
    for row in &results[0].rows {
        println!("User: {:?}", row);
    }
    Ok(())
}
```

### 2. Embedded Key-Value

```toml
[dependencies]
ofdb-kv = { version = "0.1", default-features = false, features = ["redb"] }
tokio = { version = "1.0", features = ["full"] }
```

```rust,no_run
use ofdb_kv::{Database, JsonValue, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = Database::open("kv_data.db")?;
    let client = database.client();

    let theme = Value::Json(JsonValue::Object(
        [("mode".to_owned(), JsonValue::String("dark".to_owned()))].into(),
    ));
    client.set("config:theme", theme, None).await?;

    let theme = client.get("config:theme").await?;
    println!("{theme:?}");

    let entries = client.scan_prefix("config:").await?;
    for (key, val) in entries {
        println!("{key} => {val:?}");
    }
    Ok(())
}
```

### 3. Remote Hosting and gRPC Client

Serve over TCP/Unix (feature `server`):

```rust,no_run
use ofdb_sql::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = Database::open("shared.db")?;
    let shutdown = tokio::signal::ctrl_c();
    database.serve_tcp("127.0.0.1:50051".parse()?, async {
        shutdown.await.ok();
    }).await?;
    Ok(())
}
```

Query remotely (features `remote`, `sql`):

```rust,no_run
use ofdb_sql::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("http://127.0.0.1:50051").await?;
    let results = client.execute_sql("SELECT name FROM users", None).await?;
    println!("Remote results: {:?}", results);
    Ok(())
}
```

### 4. Replicating Between Nodes

With `sync`, `Database::sync_changes()` returns a continuous lifecycle runner: initial pass, coalesced committed changes, bounded exponential backoff retry, cancel on drop. SQL and KV subscriptions merge into one runner. KV in-memory returns `Unsupported`; SQL in-memory and IndexedDB support them.

For one-shot sessions, `Database::synchronize` accepts any `SyncTransport`:

```rust,no_run
use ofdb_sql::{Database, SessionConfig, SyncRole};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = Database::open("node_a.db")?;
    let config = SessionConfig::default();
    // database.synchronize(&mut transport, &config, SyncRole::Initiator).await?;
    Ok(())
}
```

---

## Query Deadlines

Both SQL and KV `Client::with_deadline(Duration)` set an optional per-operation budget (no fixed default, independent per clone). SQL translation and execution share one budget. Transactions inherit the setting and start fresh per operation; idle time between calls is not charged.

Browser Wasm uses platform timers without Tokio; native deadlines require Tokio time. Browser timers cannot hard-interrupt computation and may be delayed by event-loop scheduling. Timeout does not prove rollback: an issued IndexedDB commit can finish after the caller stops waiting. See the [browser deadline harness](tests/browser/README.md#browser-query-deadlines) for limits.

---

## Deployment

### Docker Server Images

`compose.yaml` runs persistent KV and SQL gRPC servers with separate named volumes:

```bash
docker compose up --build
```

KV listens on `8080`, SQL on `8081`. Both use statically linked Rustls in a `scratch` runtime. gRPC TLS is optional: set `OFDB_TLS_CERT`/`OFDB_TLS_KEY` for server TLS, `OFDB_TLS_CLIENT_CA` for mTLS. Clients use `--tls-ca`, `--tls-cert`, `--tls-key`.

P2P sync uses Iroh (off by default). Enable with `--sync-listen` or `--sync-peer <NODE_ID>`. Node keys persist as `.iroh-key` next to the database.

Plaintext gRPC must not be exposed to untrusted networks. Use mTLS or an authenticated proxy for public exposure.

### One-Shot CLI Utilities

- **`sql-client-cli`**: Run SQL query batches against local or remote databases, outputting tagged JSON.
- **`kv-client-cli`**: Get, set, delete, or scan keys against local or remote KV storage.

```bash
cargo run --bin sql-client-cli --features="redb,sql" -- \
  --database app.db --query "SELECT * FROM users"

cargo run --bin kv-client-cli --features="redb" -- \
  --database kv.db set my-key --value "Hello World"
```

---

## Workspace Layout

```
crates/
├── sql/                   # Public SQL facade (Database, Client, Transaction)
├── sql-client-cli/        # One-shot SQL client CLI
├── sql-server-cli/        # SQL server executable
├── sql-client/            # gRPC client for remote SQL hosts
├── sql-engine/            # Relational engine, schema catalogs, transaction coordinator
├── sql-engine-automerge/  # Automerge CRDT row codec
├── sql-engine-redb/       # Redb persistent storage kernel
├── sql-macros/            # Derive macro for FromRow
├── sql-proto/             # Protocol buffers for SQL gRPC
├── sql-protocol/          # Wire conversions for SQL query types
├── sql-query/             # AST, statement definitions, query builder
├── sql-schema/            # Table & index schema definitions
├── sql-server/            # gRPC server for SQL
├── sql-sync/              # Transport-neutral replication protocols
├── sql-test/              # Cluster, chaos, offline test harnesses
├── sql-translator/        # SQL text-to-statement parser
├── value/                 # Shared runtime Value types
├── kv/                    # Public KV facade (Database, Client, Transaction)
├── kv-client-cli/         # One-shot KV client CLI
├── kv-server-cli/         # KV server executable
├── kv-client/             # gRPC client for remote KV hosts
├── kv-proto/              # Protocol buffers for KV gRPC
├── kv-server/             # gRPC server for KV
├── kv-store/              # Automerge-backed KV store with UUIDv7 generations
├── kv-sync/               # Snapshot-based KV synchronization
├── btree/                 # Storage-agnostic async B-Tree abstraction
├── btree-automerge/       # Automerge change persistence adapter
└── btree-redb/            # Redb byte storage adapter for B-Tree
```

---

## Documentation

- [`GLOSSARY.md`](GLOSSARY.md): Domain terminology, generation semantics, catalog invariants, conflict rules.
- [`docs/design.md`](docs/design.md): Deep architectural specification and protocol details.
- [`docs/goal.md`](docs/goal.md): Project requirements and engine invariants.
- [`docs/browser.md`](docs/browser.md): Browser Wasm support, quickstarts, and limits.
- [`AGENTS.md`](AGENTS.md): Workspace coding standards and guidelines.
