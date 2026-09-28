# Design

## Overview

`ofdb` is a transactional database engine with SQL-like rows and a separate KV store. Each engine write commits schema/catalog, row state, tombstones and derived indexes atomically. Replication is transport-neutral and implemented by the sync crate; peer discovery, resource authorization and network framing are caller responsibilities.

## SQL engine

The catalog row lifecycle below is the target of `docs/catalog-row-unification-plan.md`; the current schema mutation and sync paths are not yet unified.

The engine owns canonical database state and exposes transactions to local callers and sync. The four internal catalog tables (`__engine_tables`, `__engine_table_fields`, `__engine_indices`, `__engine_index_fields`) are the sole source of SQL schema. Table and index definitions are ordinary catalog rows: each logical name may have successive UUIDv7 row identities, with the greatest UUIDv7 deciding visibility as for KV keys. A new identity must exceed the latest known identity, even if the clock moves backwards. Fields reference their parent catalog row identity; user rows belong to a table identity, and derived index storage belongs to an index identity and its table identity. The engine maintains derived indexes atomically when local or incoming state is applied.

Concurrent row values may be retained as conflicts by the Automerge row codec. Deterministic canonical ordering determines the visible value; an explicit resolution settles a conflict. Deleting a table or index tombstones its current catalog row through the same codec and sync path as a user row. A tombstoned greatest UUIDv7 keeps the logical name absent; an older live row or stale snapshot cannot restore it. Recreating a name creates a new, greater UUIDv7 row instead of clearing the tombstone. This needs no separate schema-version table, schema tombstone protocol, or compatibility path. Old database files must be discarded and recreated after the format changes.

## SQL sync boundary

`ofdb::sync::synchronize` coordinates a peer session over the caller-provided `SyncTransport`. `SyncMessage` protocol version 4 exchanges:

- hello and a digest manifest;
- row-change inventories;
- mismatched canonical state units for bootstrap and recovery;
- incremental changes identified by opaque sync change IDs;
- requests for snapshots when dependencies are missing; and
- completion or abort.

`SessionConfig::max_units_per_frame` bounds the number of state units or changes batched into a frame. The transport supplies ordered byte frames; it does not provide peer identity or authorization. Callers must authenticate and authorize the resource before invoking sync, and must impose any additional encoded-frame byte limit at the transport boundary.

Normal updates use incremental payloads. Full state units are exchanged for initial manifests and dependency recovery. A session retries through state exchange; the engine has no envelope log, causal frontier, checkpoint, quarantine, or transport state.

## KV store and sync

KV is a separate store of opaque byte values under UTF-8 keys. Each key has UUIDv7 generations with Automerge-backed history and tombstones. Snapshot exchange is explicit through `kv-sync`; transport and peer state belong to its caller. The generation with the greatest UUIDv7 determines the visible state. Same-generation divergent histories are rejected rather than resolved with last-writer-wins. Therefore a mutable authorization record must not be stored as one KV value and assumed to resolve concurrent grant/revoke safely.

## Ownership boundaries

- `ofdb` owns SQL-like and KV persistence and sync semantics.
- `of` owns application resource issuance, catalog management and client token authorization.
- The caller binds a sync session to an approved endpoint, selected resource ID and valid grant before frames are exchanged.
- `ofnet` supplies endpoint mesh transport and ALPN routing; it does not own database state or authorization policy.

File-backed database URIs describe `ofdb` persistence only. They are not filesystem IDs or filesystem storage.
