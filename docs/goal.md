# Goal

Provide a local-first SQL-like database engine and KV store with durable, transport-neutral synchronization.

The engine must:

- commit each SQL engine write atomically, including catalog/schema, row state, tombstones and derived indexes;
- store SQL schema only in four internal catalog tables, use ordinary row tombstones to delete table/index definitions, and recreate a logical name with a new UUIDv7 row identity greater than the latest known identity; the greatest UUIDv7 (including a tombstone) determines visibility, so stale state cannot revive an older table/index or its user rows;
- expose engine-owned, transaction-consistent row history and metadata capabilities, with canonical state units for bootstrap, metadata and recovery and native missing changes for ordinary updates;
- synchronize SQL state through the versioned `SyncMessage` protocol over caller-supplied `SyncTransport`;
- store shared typed values in KV, exchange native missing history with merge-preserving snapshot recovery, and retain UUIDv7 generations and tombstones;
- merge concurrent same-generation KV histories through Automerge, with tombstones winning delete/update races; and
- select committed logical invalidations independently per peer with bounded in-process watermarks, falling back to broad comparison for new/reconnected peers, gaps, overflow and unknown writes.

`ofdb` owns engine state, SQL/KV persistence, CRDT reconciliation, sync execution, staging, batching, atomic apply and cleanup, plus continuous sync scheduling: initial reconciliation, committed-change coalescing, race handling and retry/backoff. `of` production database consumers use only `ofdb-kv` and `ofdb-sql`. Transport framing, authenticated endpoint/resource binding, peer discovery, domain authorization decisions and the explicit lifetime of each sync runner belong to callers. The sync engine does not implement checkpoint/envelope replication or store transport/frontier state.

Checked sync APIs invoke caller authorization/cancellation before each atomic apply, including staged replay and recovery. SQL offers memory, caller-provided and optional native staging. Already-authorized work may finish; denial and cancellation preserve earlier valid committed progress. All nodes can write locally offline and reconcile through CRDT semantics, without a global security-consistency promise.

SQL wire v7 and KV wire v3 are separate, intentionally incompatible with their predecessors. Local subscriptions cannot observe remote-only writes: callers must supply remote wakeups/receive delivery or explicitly reconcile both peers. Neither durable replay nor automatic bidirectional realtime delivery is promised. See [current design](design.md) for feature requirements, bounded-work limits and retained broad-discovery cases.
