# Goal

Provide a local-first SQL-like database engine and KV store with durable, transport-neutral synchronization.

The engine must:

- commit each SQL engine write atomically, including catalog/schema, row state, tombstones and derived indexes;
- store SQL schema only in four internal catalog tables, use ordinary row tombstones to delete table/index definitions, and recreate a logical name with a new UUIDv7 row identity greater than the latest known identity; the greatest UUIDv7 (including a tombstone) determines visibility, so stale state cannot revive an older table/index or its user rows;
- expose canonical state units for bootstrap/recovery and incremental row changes for normal updates;
- synchronize SQL state through the versioned `SyncMessage` protocol over caller-supplied `SyncTransport`;
- store shared typed values in KV, reconcile nested values as Automerge changes, and synchronize snapshots explicitly while retaining UUIDv7 generations, Automerge history, and tombstones; and
- merge concurrent same-generation KV histories through Automerge, with tombstones winning delete/update races.

`ofdb` owns engine state, SQL/KV persistence, CRDT reconciliation, sync execution, staging, batching, atomic apply and cleanup. `of` production database consumers use only `ofdb-kv` and `ofdb-sql`. Transport framing, authenticated endpoint/resource binding, peer discovery, domain authorization decisions and scheduling belong to callers. The sync engine does not implement checkpoint/envelope replication or store transport/frontier state.

The [planned storage-sync migration](../.scratch/storage-sync/spec.md) places the smallest suitable generic authorization check before each atomic apply, including staged replay and recovery, and moves caller-owned concrete SQL staging into an optional native `ofdb` implementation. Already-authorized work may finish; validation, atomicity, cancellation cleanup and valid partial progress remain required. No new trait, custom quota, timing or freshness policy is mandated. All nodes can write locally offline and reconcile through CRDT semantics, without a global security-consistency promise.
