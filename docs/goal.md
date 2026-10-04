# Goal

Provide a local-first SQL-like database engine and KV store with durable, transport-neutral synchronization.

The engine must:

- commit each SQL engine write atomically, including catalog/schema, row state, tombstones and derived indexes;
- store SQL schema only in four internal catalog tables, use ordinary row tombstones to delete table/index definitions, and recreate a logical name with a new UUIDv7 row identity greater than the latest known identity; the greatest UUIDv7 (including a tombstone) determines visibility, so stale state cannot revive an older table/index or its user rows;
- expose canonical state units for bootstrap/recovery and incremental row changes for normal updates;
- synchronize SQL state through the versioned `SyncMessage` protocol over caller-supplied `SyncTransport`;
- store shared typed values in KV, reconcile nested values as Automerge changes, and synchronize snapshots explicitly while retaining UUIDv7 generations, Automerge history, and tombstones; and
- merge concurrent same-generation KV histories through Automerge, with tombstones winning delete/update races.

`ofdb` owns engine state, persistence and sync semantics. Transport framing, endpoint identity, peer discovery, resource authorization and scheduling belong to callers. The sync engine does not implement checkpoint/envelope replication or store transport/frontier state.
