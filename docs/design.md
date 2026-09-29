# Design

## Overview

`ofdb` is a transactional database engine with SQL-like rows and a separate KV store. Each engine write commits schema/catalog, row state, tombstones and derived indexes atomically. Replication is transport-neutral and implemented by the sync crate; peer discovery, resource authorization and network framing are caller responsibilities.

## SQL engine

Catalog DDL and sync use the ordinary row codec, transaction, and tombstone paths described below; the parallel schema-change and catalog-entry protocols have been removed.

The engine owns canonical database state and exposes transactions to local callers and sync. The four internal catalog tables (`__engine_tables`, `__engine_table_fields`, `__engine_indices`, `__engine_index_fields`) are the sole source of SQL schema. `crates/engine/src/schema.rs` owns their storage names, plain value-row structs, and fixed built-in row layouts, and converts queried rows into `TableSchema` and `IndexSchema`. Table and index definitions are ordinary catalog rows: each logical name may have successive UUIDv7 primary-key identities, with the greatest UUIDv7 deciding visibility as for KV keys. The UUIDv7 generation is part of row identity, never a value column. A new identity must exceed the latest known identity, even if the clock moves backwards. Fields reference their parent catalog row identity; separately created columns with the same name have distinct UUIDv7 identities, and index fields at one position likewise have distinct UUIDv7 identities scoped to their index. An index field references the exact column identity, not merely its name; reusing the column name does not retarget an old index. User rows belong to a table identity, and derived index storage belongs to an index identity and its table identity. The engine maintains derived indexes atomically when local or incoming state is applied. Dropping a table tombstones its current catalog row, not its dependents; fields, rows, and index definitions tied to that table identity remain replication facts but are inaccessible. An index name can be reused with a new index identity. Derived index records for inactive identities are discarded or ignored, never treated as schema.

Concurrent row values may be retained as conflicts by the Automerge row codec. Deterministic canonical ordering determines the visible value; an explicit resolution settles a conflict. Deleting a table or index marks its current catalog row with the ordinary row-codec tombstone, never a `deleted` value column, through the same codec and sync path as a user row. A tombstoned greatest UUIDv7 keeps the logical name absent; tombstones participate in winner selection but do not appear in search results. An older live row or stale snapshot cannot restore it. Recreating a name creates a new, greater UUIDv7 row instead of clearing the tombstone. This needs no separate schema-version table, schema tombstone protocol, or compatibility path. Old database files must be discarded and recreated after the format changes.

## SQL sync boundary

`ofdb::sync::synchronize` coordinates a peer session over the caller-provided `SyncTransport`. `SyncMessage` protocol version 5 exchanges:

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

## Ownership boundaries

- `ofdb` owns SQL-like and KV persistence and sync semantics.
- `of` owns application resource issuance, catalog management and client token authorization.
- The caller binds a sync session to an approved endpoint, selected resource ID and valid grant before frames are exchanged.
- `ofnet` supplies endpoint mesh transport and ALPN routing; it does not own database state or authorization policy.

File-backed database URIs describe `ofdb` persistence only. They are not filesystem IDs or filesystem storage.
