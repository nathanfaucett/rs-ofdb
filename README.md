# Offline First Database Workspace (`ofdb`)

`ofdb` is an embedded, local-first database workspace in Rust designed for offline-first applications, autonomous edge nodes, and decentralized peer-to-peer systems.

It provides two distinct, self-contained database models:

- [**`ofdb-sql`**](crates/sql/README.md): A relational SQL database engine with schema catalogs, typed statement execution, secondary indexes, and row-level CRDT synchronization.
- [**`ofdb-kv`**](crates/kv/README.md): A key-value store for shared typed values, with Automerge-backed history, UUIDv7 key generations, and millisecond expiry.

---

## What It Is & Why It Exists

In traditional client-server architectures, applications require continuous internet connectivity to access or mutate database state. When connectivity drops, operations block or fail.

`ofdb` is built around the **Local-First principle**. KV currently supports the shared value model, including nested JSON objects and arrays. It does not expose every native Automerge object type, such as collaborative text or counters:

1. **Zero-Latency Local Operations**: All reads and writes hit local embedded storage ([Redb](crates/btree-redb) or in-memory) with ACID guarantees. Applications remain fully functional offline.
2. **Conflict-Free Convergence**: Individual rows (in SQL) and key generations (in KV) are tracked using [Automerge](https://automerge.org/) CRDT documents. Concurrent offline edits merge deterministically without requiring a central coordinator.
3. **Transport-Neutral Synchronization**: Replication protocols do not assume a specific network stack, server daemon, or discovery system. Sync operates over any byte-stream or framing abstraction implementing [`SyncTransport`](crates/sql-sync/src/transport.rs) (TCP, Unix sockets, Bluetooth, WebSockets, or P2P meshes like [Iroh](https://iroh.computer/)).
4. **Clean Decoupling of Engine and Network**: Network transport, endpoint identities, authentication, and scheduling belong to the application; `ofdb` strictly governs storage, transactions, and state reconciliation.

---

## When to Use `ofdb-sql` vs `ofdb-kv`

| Capability              | `ofdb-sql`                                                                                   | `ofdb-kv`                                                                                                                                     |
| :---------------------- | :------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------- |
| **Data Model**          | Relational tables, columns, typed values (`UUID`, `TEXT`, `INTEGER`, `BLOB`, `JSON`, etc.)   | Shared `Value` types, including primitives, blobs, and nested JSON objects/arrays, keyed by UTF-8 strings                                     |
| **Queries**             | SQL-text (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `JOIN`) and typed statement trees          | Key operations (`get`, `set`, `delete`, `scan`, `scan_prefix`, `scan_all`)                                                                    |
| **Indexes**             | Primary keys (UUIDv7) and secondary unique/non-unique indexes                                | Ordered UTF-8 key prefixes and ranges                                                                                                         |
| **Conflict Resolution** | Per-column CRDT resolution via Automerge; greatest UUIDv7 selects visible schema definitions | Nested values reconcile through Automerge; concurrent edits merge, tombstones win delete/update races; greatest UUIDv7 selects the generation |
| **TTL / Expiry**        | Not applicable (handled in application logic)                                                | Optional absolute millisecond deadline (`expires_at`)                                                                                         |
| **Use Cases**           | Structured applications, document & user records, multi-table relationship graphs            | Caches, blobs, session states, configuration stores, high-throughput key lookups                                                              |

---

## Core Architecture: The Handle Model

Both facades adhere to a strict separation between database management and query execution:

- **`Database` (Embedded Control Handle)**: Constructs and manages the storage lifecycle (`open`, `in_memory`), hosts remote gRPC endpoints (`serve_tcp`, `serve_unix`), and executes sync sessions (`synchronize`). `Database` does **not** execute ordinary queries or transactions.
- **`Client` (Unified Query Handle)**: The sole interface for reading and writing data. Obtain an embedded client via `database.client()`, or connect remotely via `Client::connect("http://...")`. The API remains identical whether queries run against local memory, an on-disk Redb file, or across the network.
- **`Transaction`**: Explicit multi-statement / multi-operation transactions created via `client.transaction().await?`, with support for atomic `commit()` and `rollback()`. Remote transactions stream across a single server connection and roll back automatically if the client handle is dropped.

---

## Feature Flags

Crates in this workspace use empty default features (`default-features = false`). You must explicitly enable the components needed for your target architecture:

### `ofdb-sql`

- `in-memory`: In-memory embedded SQL engine.
- `redb`: On-disk persistent embedded SQL engine using Redb.
- `sql`: Enables SQL string translation (`execute_sql`, `SqlTranslator`).
- `remote`: Enables the gRPC client to query remote SQL database hosts.
- `server`: Enables `serve_tcp` and `serve_unix` on `Database` to host gRPC services.
- `sync`: Enables peer synchronization protocols (`synchronize`) and session config.
- `macros`: Enables `#[derive(FromRow)]` for mapping SQL rows directly into Rust structs.

### `ofdb-kv`

- `in-memory`: In-memory embedded key-value store.
- `redb`: On-disk persistent key-value store using Redb.
- `remote`: Enables the gRPC client to query remote KV database hosts.
- `server`: Enables `serve_tcp` and `serve_unix` on `Database` to host gRPC services.
- `sync`: Enables snapshot synchronization protocols (`synchronize`).

---

## Quickstart

### 1. Embedded Relational SQL (`ofdb-sql`)

Add to `Cargo.toml`:

```toml
[dependencies]
ofdb-sql = { version = "0.1", default-features = false, features = ["redb", "sql"] }
tokio = { version = "1.0", features = ["full"] }
```

Execute SQL queries and transactions:

```rust,no_run
use ofdb_sql::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open or create an embedded Redb database
    let database = Database::open("app_data.db")?;

    // 2. Create a query client
    let client = database.client();

    // 3. Define schema
    client.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", None).await?;

    // 4. Run atomic multi-step transactions
    let mut tx = client.transaction().await?;
    tx.execute_sql(
        "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Alice')",
        None,
    ).await?;
    tx.commit().await?;

    // 5. Query data
    let results = client.execute_sql("SELECT id, name FROM users", None).await?;
    for row in &results[0].rows {
        println!("User: {:?}", row);
    }

    Ok(())
}
```

---

### 2. Embedded Key-Value Store (`ofdb-kv`)

Add to `Cargo.toml`:

```toml
[dependencies]
ofdb-kv = { version = "0.1", default-features = false, features = ["redb"] }
tokio = { version = "1.0", features = ["full"] }
```

Store and scan key-value pairs:

```rust,no_run
use ofdb_kv::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = Database::open("kv_data.db")?;
    let client = database.client();

    use ofdb_kv::{JsonValue, Value};

    let theme = Value::Json(JsonValue::Object(
        [("mode".to_owned(), JsonValue::String("dark".to_owned()))].into(),
    ));
    client.set("config:theme", theme, None).await?;

    // Nested JSON values retain their types when read.
    let theme = client.get("config:theme").await?;
    println!("{theme:?}");

    // Prefix scans return the same typed values.
    let entries = client.scan_prefix("config:").await?;
    for (key, val) in entries {
        println!("{key} => {val:?}");
    }

    Ok(())
}
```

---

### 3. Remote Hosting and gRPC Client

Serve an embedded database over TCP or Unix sockets:

```rust,no_run
use ofdb_sql::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = Database::open("shared.db")?;

    // Host gRPC server on 127.0.0.1:50051 (feature = "server")
    let shutdown = tokio::signal::ctrl_c();
    database.serve_tcp("127.0.0.1:50051".parse()?, async {
        shutdown.await.ok();
    }).await?;

    Ok(())
}
```

Query the database from another process or service:

```rust,no_run
use ofdb_sql::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to remote host (feature = "remote", "sql")
    let client = Client::connect("http://127.0.0.1:50051").await?;
    let results = client.execute_sql("SELECT name FROM users", None).await?;
    println!("Remote results: {:?}", results);

    Ok(())
}
```

---

### 4. Replicating Between Nodes (Sync Sessions)

Replication is explicit and caller-scheduled. `Database::synchronize` accepts any transport implementing `SyncTransport`:

```rust,no_run
use ofdb_sql::{Database, SessionConfig, SyncRole};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = Database::open("node_a.db")?;
    let config = SessionConfig::default();

    // `transport` represents a bidirectional frame transport (e.g., Iroh P2P, TCP stream)
    // database.synchronize(&mut transport, &config, SyncRole::Initiator).await?;

    Ok(())
}
```

---

## Database Server Images

`compose.yaml` runs the persistent KV and SQL gRPC servers. It stores database files in separate named volumes:

```bash
docker compose up --build
```

The KV server listens on port `8080`. The SQL server listens on port `8081`. Both images use a statically linked Rustls build and a `scratch` runtime. gRPC TLS is optional. Set `OFDB_TLS_CERT` and `OFDB_TLS_KEY` to PEM file paths and mount those files into the container. Clients can trust the server certificate with `--tls-ca <PEM>`. For mutual TLS, also set `OFDB_TLS_CLIENT_CA` on the server. The server then requires a client certificate issued by that CA. Clients pass the client certificate and key with `--tls-cert <PEM> --tls-key <PEM>`. The client CA option requires TLS to be enabled.

P2P sync uses Iroh. It is off until you set `--sync-listen` or one or more `--sync-peer <NODE_ID>` options. Start each peer with `--sync-listen` to print its public node ID. Then restart each peer with the other node ID as `--sync-peer`; each configured peer is also the inbound allowlist. The private node key is stored next to the database file as `.iroh-key`, so keep the database volume persistent. Iroh uses relays and address discovery; it does not require Docker host networking. Relay discovery needs outbound network access.

The gRPC listener is plaintext unless TLS files are set. Do not expose plaintext gRPC to untrusted networks. Server TLS without `OFDB_TLS_CLIENT_CA` does not authenticate clients. Use mTLS or an authenticated proxy before exposing gRPC publicly.

For example, connect with a private CA certificate:

```bash
cargo run -p ofdb-sql-client-cli --features="remote" -- \
  --endpoint https://db.example:8081 --tls-ca ca.pem \
  --tls-cert client.crt --tls-key client.key --query "SELECT 1"
```

## One-Shot CLI Utilities

The workspace includes single-command CLIs for scripting and testing:

- **`sql-client-cli`**: Runs a SQL query batch against a database file, in-memory instance, or remote gRPC endpoint, outputting tagged JSON.
- **`kv-client-cli`**: Gets, sets, deletes, or scans keys against local or remote KV storage.

```bash
# Query an embedded Redb database
cargo run --bin sql-client-cli --features="redb,sql" -- \
  --database app.db --query "SELECT * FROM users"

# Query in memory
cargo run --bin sql-client-cli --features="in-memory,sql" -- \
  --memory --query "CREATE TABLE t (id UUID PRIMARY KEY); SELECT * FROM t;"

# KV store operations
cargo run --bin kv-client-cli --features="redb" -- \
  --database kv.db set my-key --value "Hello World"

cargo run --bin kv-client-cli --features="redb" -- \
  --database kv.db get my-key
```

---

## Workspace Layout

```
crates/
├── sql/                   # Public SQL facade (Database, Client, Transaction)
├── sql-client-cli/        # One-shot SQL client CLI executable
├── sql-server-cli/        # SQL database server executable
├── sql-client/            # gRPC client for remote SQL hosts
├── sql-engine/            # Relational database engine, schema catalogs, transaction coordinator
├── sql-engine-automerge/  # Automerge CRDT row codec
├── sql-engine-redb/       # Redb persistent storage kernel
├── sql-macros/            # Derive macro for FromRow
├── sql-proto/             # Protocol buffers definitions for SQL gRPC
├── sql-protocol/          # Wire conversions for SQL query types
├── sql-query/             # AST, Query statement definitions, and query builder
├── sql-schema/            # Table & index schema definitions
├── sql-server/            # gRPC server implementation for SQL
├── sql-sync/              # Transport-neutral replication protocols (SyncMessage)
├── sql-test/              # Cluster, chaos, and offline test harnesses
├── sql-translator/        # SQL text-to-statement parser & translator
├── value/                 # Shared runtime Value types (UUID, JSON, Blobs, Decimals)
├── kv/                    # Public KV facade (Database, Client, Transaction)
├── kv-client-cli/         # One-shot KV client CLI executable
├── kv-server-cli/         # KV database server executable
├── kv-client/             # gRPC client for remote KV hosts
├── kv-proto/              # Protocol buffer definitions for KV gRPC
├── kv-server/             # gRPC server implementation for KV
├── kv-store/              # Automerge-backed key-value store with UUIDv7 generations
├── kv-sync/               # Snapshot-based KV synchronization
├── btree/                 # Storage-agnostic async B-Tree abstraction
├── btree-automerge/       # Automerge change persistence adapter
└── btree-redb/            # Redb byte storage adapter for B-Tree
```

---

## Documentation & Standards

- [`GLOSSARY.md`](GLOSSARY.md): Domain terminology, generation semantics, catalog invariants, and conflict rules.
- [`docs/design.md`](docs/design.md): Deep architectural specification and protocol details.
- [`docs/goal.md`](docs/goal.md): Project requirements and engine invariants.
- [`AGENTS.md`](AGENTS.md): Workspace coding standards, dependency constraints, and refactoring guidelines.
