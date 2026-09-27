# Goal

Provide a local-first SQL-like database engine and KV store with durable, transport-neutral synchronization.

The engine must:

- commit each SQL engine write atomically, including catalog/schema, row state, tombstones and derived indexes;
- preserve immutable row and schema-generation identities and deterministic conflict visibility;
- expose canonical state units for bootstrap/recovery and incremental row changes for normal updates;
- synchronize SQL state through the versioned `SyncMessage` protocol over caller-supplied `SyncTransport`;
- synchronize KV snapshots explicitly, retaining UUIDv7 generations, Automerge history and tombstones; and
- reject divergent histories for the same KV generation instead of silently choosing a winner.

`ofdb` owns engine state, persistence and sync semantics. Transport framing, endpoint identity, peer discovery, resource authorization and scheduling belong to callers. The sync engine does not implement checkpoint/envelope replication or store transport/frontier state.
