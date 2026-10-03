# Design

## Overview

The `ofdb` workspace contains separate SQL and KV stores. SQL engine writes commit schema/catalog, row state, tombstones and derived indexes atomically. Replication is transport-neutral and implemented by each store's sync crate; peer discovery, resource authorization and network framing are caller responsibilities.

## SQL engine

Catalog DDL and sync use the ordinary row codec, transaction, and tombstone paths described below; the parallel schema-change and catalog-entry protocols have been removed.

The engine owns canonical database state and exposes transactions to local callers and sync. The four internal catalog tables (`__engine_tables`, `__engine_table_fields`, `__engine_indices`, `__engine_index_fields`) are the sole source of SQL schema. `crates/sql-engine/src/schema.rs` owns their storage names, plain value-row structs, and fixed built-in row layouts, and converts queried rows into `TableSchema` and `IndexSchema`. Table and index definitions are ordinary catalog rows: each logical name may have successive UUIDv7 primary-key identities, with the greatest UUIDv7 deciding visibility as for KV keys. The UUIDv7 generation is part of row identity, never a value column. A new identity must exceed the latest known identity, even if the clock moves backwards. Fields reference their parent catalog row identity; separately created columns with the same name have distinct UUIDv7 identities, and index fields at one position likewise have distinct UUIDv7 identities scoped to their index. An index field references the exact column identity, not merely its name; reusing the column name does not retarget an old index. User rows belong to a table identity, and derived index storage belongs to an index identity and its table identity. The engine maintains derived indexes atomically when local or incoming state is applied. Dropping a table tombstones its current catalog row, not its dependents; fields, rows, and index definitions tied to that table identity remain replication facts but are inaccessible. An index name can be reused with a new index identity. Derived index records for inactive identities are discarded or ignored, never treated as schema.

Concurrent row values may be retained as conflicts by the Automerge row codec. Deterministic canonical ordering determines the visible value; an explicit resolution settles a conflict. Deleting a table or index marks its current catalog row with the ordinary row-codec tombstone, never a `deleted` value column, through the same codec and sync path as a user row. A tombstoned greatest UUIDv7 keeps the logical name absent; tombstones participate in winner selection but do not appear in search results. An older live row or stale snapshot cannot restore it. Recreating a name creates a new, greater UUIDv7 row instead of clearing the tombstone. This needs no separate schema-version table, schema tombstone protocol, or compatibility path. Old database files must be discarded and recreated after the format changes.

## SQL sync boundary

`ofdb_sql::synchronize` coordinates a peer session over the caller-provided `SyncTransport`. `SyncMessage` protocol version 5 exchanges:

- hello and a digest manifest;
- row-change inventories;
- mismatched canonical state units for bootstrap and recovery;
- incremental changes identified by opaque sync change IDs;
- requests for snapshots when dependencies are missing; and
- completion or abort.

`SessionConfig::max_units_per_frame` bounds the number of state units or changes batched into a frame. The transport supplies ordered byte frames; it does not provide peer identity or authorization. Callers must authenticate and authorize the resource before invoking sync, and must impose any additional encoded-frame byte limit at the transport boundary.

Normal updates use incremental payloads. Full state units are exchanged for initial manifests and dependency recovery. A catalog change transfers its affected table/index dependency set across the four catalog tables as one logical batch, not the entire database catalog. The index definition consists of its index row and all index-field rows in that complete batch; no separate expected-field count is stored. The batch can span transport frames; complete applied batches commit atomically, not the entire session. An interrupted catalog batch is discarded and retried through state exchange; dependent user-row changes wait for its commit. Independent columns with the same position are ordered deterministically by name. A catalog batch with a known-invalid definition is rejected atomically with an actionable error. Independently created columns with the same name under one table identity have distinct UUIDv7 identities; the greatest identity selects the visible definition. The engine has no envelope log, causal frontier, checkpoint, quarantine, or transport state.

## KV store and sync

KV is a separate store of opaque byte values under UTF-8 keys. Each key has UUIDv7 generations with Automerge-backed history and tombstones. Snapshot exchange is explicit through `kv-sync`; transport and peer state belong to its caller. The generation with the greatest UUIDv7 determines the visible state. Same-generation divergent histories are rejected rather than resolved with last-writer-wins. Therefore a mutable authorization record must not be stored as one KV value and assumed to resolve concurrent grant/revoke safely.

## Facades and CLI

See [the implementation plan](facade-cli-plan.md) for the settled contracts, implementation status, and remaining validation.

The public facades are `ofdb-kv` for KV and `ofdb-sql` for SQL. The repository root is workspace-only. Facades live in `crates/sql` and `crates/kv`; the KV implementation lives in `crates/kv-store`. SQL and KV have separate client-only binaries, `sql-cli` and `kv-cli`, and share a publishing approach, not a data model or storage interface.

SQL-specific crate directories use `sql-*`; their Cargo package names use `ofdb-sql-*`. This includes engine, engine-automerge, engine-redb, query, schema, client, server, proto, protocol, sync, value, macros, and test. The `sql-translator` directory keeps its name; its package becomes `ofdb-sql-translator`. Shared B-tree package names use `ofdb-btree*`, without a SQL prefix. KV implementation/support packages use `ofdb-kv-*`, with `ofdb-kv-store` distinct from the public `ofdb-kv` facade. Remove the unused wasm and examples-util crates.

Each facade provides an embedded `Database` and a remote `Client`. Both provide query operations for their own store, with consistent query methods, result types, and structured error types within that facade. Use explicit embedded and remote constructors. Async `Client::connect` establishes a connection immediately; subsequent queries reuse it. Remote requests preserve store-specific error categories through structured error details rather than message matching. There is no shared SQL/KV query interface. Embedded and server interfaces also expose sync setup; remote client interfaces do not. SQL and KV clients remain separate: no library package exports both.

Default features are empty. Explicit features are `std`, `remote`, `in-memory`, `redb`, `server`, and `sync`, plus `sql` for SQL-text helpers. Storage features enable their required embedded dependencies; features may coexist without merging handle roles. Remote-only builds do not include the local engine, Redb, Automerge, or sync implementation.

Sync uses explicit sessions. Transport, peer selection, authorization, and scheduling remain caller responsibilities; the facades do not start background replication automatically.

One-shot binaries `sql-cli` and `kv-cli` live in `crates/sql-cli` and `crates/kv-cli`, with packages `ofdb-sql-cli` and `ofdb-kv-cli`. Each consumes only its corresponding facade for local and remote queries. Neither CLI starts a server, performs sync, or exposes administration commands. SQL query interfaces retain typed statements and SQL-text helpers; remote SQL initially uses existing statement execution. Callers do not supply query timestamps or timestamp providers; explicit KV expiry settings remain supported. Clients do not automatically retry requests. Library deadlines are caller-configurable, with no fixed default. CLI `--timeout` accepts positive integer seconds, with no fixed default; library deadline configuration uses `Duration`. One budget starts after reading input and covers connection plus query completion; it does not prove rollback or guarantee hard interruption. Each invocation requires exactly one of `--database PATH`, `--memory`, `--endpoint URL`, or `--unix-socket PATH`. An explicit database path uses open-or-create behavior, matching `Database::open(path)`; no default database is selected or created. SQL accepts exactly one of `--query SQL` or `--file PATH`, with `-` for stdin, and executes the full input as one batch. KV set accepts `--value TEXT` or `--file PATH`, with file/stdin bytes preserved. SQL results and KV scans use JSON. SQL JSON retains value tags, decimal-string integers, base64 blobs, and exact 64-bit hexadecimal floats, including nested JSON numbers. `--params-file PATH` uses the same codec with existing positional/named parameter containers. This codec does not change stored serialization. KV get outputs raw bytes without a newline. Successful set/delete produces no stdout and exits 0. Scans emit an ordered JSON array of key/value entries, with base64 values. Exit codes are 0 for success, 1 for query failure/missing get, and 2 for invalid usage; errors go to stderr. Empty SQL batches are rejected on both handles. Initially, remote transactions do not span multiple calls. Local transactions remain available on embedded handles. Each query operation must document its atomicity. Preserve existing KV range bounds, ordering, literal-prefix scans, delete-missing success, expiry clearing on set without expiry, and acceptance of past expiry deadlines. Do not add pagination, conditional writes, or TTL-duration options.

### Current implementation constraints

- The SQL engine executes a statement batch in one transaction. The standard server executor preserves that scope. SQL transaction commands are not supported; explicit local transactions use an embedded transaction handle.
- Each remote KV write commits one store transaction. Remote reads use server time; embedded reads currently require caller-supplied time. Expiry is an absolute Unix-millisecond deadline, not a relative TTL.
- KV sync already provides explicit transport sessions using full snapshots. Incoming batches commit separately; a complete sync session is not atomic.
- Remote-only SQL builds currently retain an engine dependency, including through `EngineError`. The query error contract must be separated from the local engine.
- Internal dependency declarations currently lack registry versions, and several embedded/sync dependencies prohibit publication. Published facades require a publishable dependency graph, not only publishable facade manifests.

## Ownership boundaries

- `ofdb` owns SQL-like and KV persistence and sync semantics.
- `of` owns application resource issuance, catalog management and client token authorization.
- The caller binds a sync session to an approved endpoint, selected resource ID and valid grant before frames are exchanged.
- `ofnet` supplies endpoint mesh transport and ALPN routing; it does not own database state or authorization policy.

File-backed database URIs describe `ofdb` persistence only. They are not filesystem IDs or filesystem storage.
