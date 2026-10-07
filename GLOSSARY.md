# Domain Context

SQL and KV are separate stores. The row, catalog, index, and engine terms below describe SQL; KV has its own keys, generations, and sync semantics.

## Facade Roles

The repository root is a workspace only, with no root crate. The public facades live in `crates/sql` and `crates/kv`; the underlying KV implementation lives in `crates/kv-store`. The public libraries are `ofdb-sql` for SQL and `ofdb-kv` for KV. Neither library exports the other store's client interface. SQL-specific crate directories use `sql-*`, and their Cargo package names use `ofdb-sql-*`; shared storage crates do not use a SQL prefix.

### Query Operation

A Query Operation is a store-specific read or write request. SQL operations execute statements; KV operations read or change shared `ofdb-value::Value` values under UTF-8 keys. KV values can be primitive values, blobs, or nested JSON objects and arrays; Automerge merges concurrent changes to nested values. SQL and KV do not share a query interface. Each store has structured query errors shared by its embedded and remote query interfaces. Remote requests preserve store-specific categories through structured error details, not message matching. An empty SQL statement batch is invalid, not a successful no-op.

### Embedded Handle

An Embedded Handle, named `Database` in each facade, constructs and controls a database in the caller's process. It provides explicit sync and hosting setup, but is not the facade query or transaction API. Callers use `Database::client()` for embedded queries and SQL transactions. Callers do not supply query timestamps or timestamp providers; explicit KV expiry settings remain supported.

### Query Handle

A Query Handle, named `Client` in each facade, is the only facade API for ordinary queries against embedded storage or a database hosted by another process. It includes reads and writes. SQL and KV clients create explicit transactions for embedded and remote databases. Clients do not provide hosting or sync setup. SQL and KV clients remain separate types with separate query interfaces.

`Client` supports embedded and remote queries. `Database::client()` is the only embedded client constructor. It creates an owned handle that shares storage and remains usable after the `Database` handle is dropped. Storage construction stays on `Database`; remote connection constructors stay on `Client`. `Database` does not expose duplicate query methods or SQL transaction creation. Both CLIs use `Client` for query dispatch.

A remote client's async connection constructor establishes a connection immediately. `Client::transaction()` establishes an SQL transaction for embedded or remote storage. Remote transactions use one server-side transaction stream across calls; dropping an uncommitted transaction rolls it back when the server observes stream closure. `Client::with_deadline(Duration)` applies to queries and transaction operations, with no default. Timeout does not prove rollback or guarantee interruption of blocking storage work. The CLI retains one budget for construction plus query.

Clients support `Clone`. Embedded clones share storage; remote clones share the connection. Each clone keeps its own deadline setting. SQL `Client` exposes typed statement execution, SQL-gated `execute_sql()`, generic translator helpers, and explicit transactions. KV `Client` exposes its existing get/set/delete/scan operations and explicit transactions. A transaction holds local operations until commit; a remote transaction uses one server-side stream and rolls back when an uncommitted client handle is dropped and the server observes stream closure. `Database` does not expose ordinary query methods or transaction creation. SQL-text queries use one deadline budget for translation plus execution. Library query deadlines do not include connection establishment. Embedded queries with a configured deadline require a Tokio runtime with time enabled; embedded queries without a deadline remain usable without Tokio.

### Database Host

A Database Host owns the embedded database used to answer remote query requests. Sync setup belongs to the host or embedded caller, not to remote clients.

### Sync Session

A Sync Session explicitly exchanges replicated state between stores of the same kind. Transport, peer selection, authorization, and scheduling belong to the caller. SQL and KV retain their separate sync protocols; the facades do not start background replication automatically.

### CLI

Two separate executables, `sql-client-cli` and `kv-client-cli`, use their corresponding published facades for embedded or remote queries. They are one-shot query clients only: neither exposes hosting, sync, administration commands, or an interactive shell. Each invocation selects exactly one explicit target: database path, memory, endpoint URL, or Unix socket path. An explicit database path may create a missing database; no default path is selected or created. SQL input is one query argument or one file/stdin input executed as a single statement batch. KV CLI values use an explicit tagged JSON encoding; files and stdin contain that encoding, not raw value bytes.

SQL results and KV scans use JSON. SQL value tags preserve type distinctions; integers use decimal strings, blobs use base64, and floats use exact 64-bit hexadecimal representations, including numbers nested inside JSON values. Optional `--params-file PATH` uses the same lossless value encoding with the existing positional/named parameter containers. KV get emits raw bytes without an added newline. Successful set/delete commands produce no stdout and exit 0. Scans emit an ordered JSON array of key/value entries, with base64 values. Exit codes are 0 for success, 1 for query failure or a missing KV get, and 2 for invalid command usage; errors go to stderr. Empty values and empty scans are successful. CLI request timeouts are optional positive integer seconds, with no fixed default. Library deadline configuration uses `Duration`. One timeout budget starts after input is read and covers connection plus query completion. Timeout is not proof of rollback or a guarantee of hard interruption of blocking storage work.

### Build Roles

Default features are empty. Remote, embedded storage, hosting, and sync dependencies are enabled explicitly. A remote-only build must not include the local engine, Redb, Automerge, or sync implementation. Each query operation must document its atomicity. Clients do not automatically retry requests. Library request deadlines are caller-configurable, with no fixed default. Embedded and remote construction is explicit, not selected through a combined URI constructor. SQL query interfaces support typed statements and SQL-text helpers; remote SQL starts with existing statement execution.

The separate facades, embedded and remote query clients, host entry points, and one-shot CLIs are implemented with settled contracts and focused validation. KV errors preserve store categories through facade-owned structured errors. Internal path dependencies have registry version requirements, and publishable packages have descriptions. Registry-backed package verification and external-consumer checks remain blocked until prerequisites are available from a registry; no packages have been published.

## Engine Transaction

One transaction owns one complete read snapshot or write set for an engine operation batch. A write transaction commits all enlisted catalog, schema, index, Automerge row, and tombstone changes together, or rolls them all back.

## Logical Row

A Logical Row is the row value the Engine reads, writes, and indexes. Its stored representation is selected by the Row Reconciler.

## Row Reconciler

A Row Reconciler stores and resolves Logical Rows through an Engine Transaction. The Automerge Row Reconciler uses one Automerge document per Logical Row with stable column keys. The Engine is the only path for applying local or incoming Automerge changes to an engine-managed Logical Row. Concurrent values for one column are retained as a Conflict; Automerge canonical ordering selects the visible value.

## Row Identity

Every SQL Logical Row, including a schema-definition row, has one immutable UUIDv7 primary key. The table name and that same UUID identify the row for storage and replication; a table's identity is its name, not its schema-definition row UUID.

## Schema Definition

A Schema Definition describes a named table or index and its fields. Competing table definitions select one complete schema by largest row UUID; deleting a losing definition does not drop the named table.

## Schema Compatibility

Compatible table schemas have equal field names, types, defaults, primary key, and unique constraints. Field order may differ, but index field order may not.

## Table Drop

A Table Drop deletes a named table's contents and dependent schema definitions, including stale writes made without observing its deletion. It differs from deletion of a losing schema-definition contender.

## Table Recreation

Table Recreation creates a new schema-definition row for the same table name after observing all relevant drops and starts with empty contents. It does not restore deleted rows or introduce a new logical table identity.

## Index Record

An Index Record is a derived mapping from an index key to a row UUID. For a unique key, the largest row UUID is the canonical contender, including when that contender is deleted.

## Conflict

A Conflict retains concurrent values within one Logical Row, with deterministic canonical ordering selecting the visible value. An explicit Resolution settles this conflict; collisions between distinct rows follow the separate Unique-Key Conflict rule.

## Unique-Key Conflict

A Unique-Key Conflict occurs when distinct Logical Rows claim the same unique-key value. The largest UUID wins, including tombstoned contenders; smaller contenders are permanently deleted rather than retained for promotion.

## Generation

SQL table names identify tables. SQL table, column, index, and index-field rows have independent UUID primary keys; ownership and references are stored as ordinary values. SQL does not use table generations. KV generations remain a separate KV Store concept.

## Sync State Unit

A Sync State Unit is sync-owned canonical transferable state for one Logical Row, Table, Column, Index, or Index Field. It contains its identity, state bytes, deletion metadata where applicable, and a digest. Sync asks the Engine to apply it atomically while the Engine maintains derived Index Records.

## Sync Manifest

A Sync Manifest maps each sync-owned state-unit identity to its digest. A Sync Session exchanges manifests, transfers mismatched units in batches, and retries by exchanging manifests again. Normal realtime updates transfer only missing incremental payloads identified by opaque sync change IDs; full state units are for bootstrap and dependency recovery. Automerge implements `sync::SyncRowCodec`, while the Engine stores no protocol state, frontier, checkpoint, envelope log, or quarantine state.

## Tombstone

A SQL Tombstone permanently marks one Logical Row as deleted. Its unique-key claim can be superseded only by a different row with the same unique-key value and a larger UUID; this does not restore the deleted row.

## SQL Storage Compatibility

UUID row IDs replace the previous tagged and composite SQL row identities. Persisted SQL data and replicated payloads that use the previous identities are incompatible; do not mix formats or migrate them implicitly.

## KV Store

A KV Store holds shared `ofdb-value::Value` values under UTF-8 keys, with Automerge-backed history and tombstones. Values include primitives, blobs, and nested JSON objects and arrays. Concurrent same-generation changes merge through Automerge; tombstones win delete/update races. The greatest UUIDv7 generation determines visible state. This is not a last-writer-wins store.

## KV Expiry

KV Expiry is an optional absolute Unix-millisecond deadline. A value is hidden when the read time reaches that deadline. Expiry does not tombstone the generation; it is not a relative TTL.

## KV Key

A KV Key is a UTF-8 label for a value. It may have multiple KV Generations; the generation with the greatest UUIDv7 determines the visible state.

## KV Generation

A KV Generation is an immutable UUIDv7 identity for one KV Key's Automerge value. A tombstone ends that generation; setting the same key again creates a new generation rather than restoring the old one.
