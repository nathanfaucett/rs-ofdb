# Sync memory and progress investigation

## Scope

Reviewed native SQL/KV sync in `ofdb` and its caller in
`of/crates/storage-server`. No application data or production memory profile was
available. The original process OOM has not been reproduced.

Change assessment: moderate complexity, high correctness risk in batching and
recovery, high priority because repeatable failures can prevent replication.
Tests cover frame limits, recovery failure, ordering, authorization and replay.

## Confirmed defects and fixes

### KV retained reconstructed documents during broad discovery

`crates/kv-store/src/store.rs` collected reconstructed Automerge documents for all
latest key generations before producing histories or snapshots. The documents
remained live together although only summaries or encoded snapshots were needed.

The scan now converts each document immediately. It retains results for the latest
generation, including validation errors, rather than reconstructed documents. This
preserves the rule that only the latest generation's validation result is returned.

A bounded native measurement used 160 keys, 24 changes per key, and 8 KiB
text values per change. Each key held the same valid snapshot under a distinct
identity. Three separate processes per implementation ran one `histories()` call.
The baseline restored only the old document-retaining history loop; compilation
was excluded. Each process had a 60-second timeout and 384 MiB virtual-memory limit.

| Median measurement | Old loop | Current loop |
| --- | ---: | ---: |
| Peak RSS | 65,408 KiB | 56,572 KiB |
| Process elapsed time | 0.73 s | 0.72 s |
| History scan time | 0.638154 s | 0.638245 s |

Peak RSS fell by 8.63 MiB (13.5%). No meaningful speed change was measured.
This is not an end-to-end sync benchmark or proof of the original OOM cause.
The reusable ignored diagnostic is `crates/kv-store/tests/history_memory.rs`.
Build it with `cargo test -p ofdb-kv-store --test history_memory --no-run`, then
run the reported test executable directly under `/usr/bin/time -v` with
`--ignored --exact histories_peak_rss --nocapture`. Running Cargo under time
would include build and Cargo memory in the measurement.

### Count-based batches could fail without making progress

SQL and KV split payloads by item count, not encoded byte size. Multiple valid
items could exceed a frame limit when grouped together. A SQL regression produced
a 1,800,185-byte frame against a 1,048,576-byte limit. Retrying the same unchanged
work repeated the failure.

Both protocols now split transfer and recovery batches by encoded bytes and item
count. KV also splits inventories by bytes. SQL checks encoded message size before
allocating the frame. A single oversized item still fails explicitly.

Split KV recovery collects all requested snapshots before one atomic apply. It
holds no transaction across network reads. Cumulative recovery frame bytes are
bounded by `max_inventory_bytes`. Tests verify that disconnect, invalid data,
denied apply and budget overflow after the first frame leave the original failed
change batch unapplied.

### SQL repeated history searches and read transactions

SQL searched the remote change vector for each local change, giving quadratic
comparison cost for long histories. KV delta export used the same search pattern.
Both now use ordered sets for membership checks without changing dependency order.

SQL opened a read transaction for each missing change. It now exports the missing
changes for one row in one transaction. A regression verifies that three missing
changes use one read transaction instead of three.

SQL inventories now omit rows with equal manifest digests. Inventories that cannot
fit one frame use the existing missing-inventory snapshot fallback. This avoids
that inventory frame failure, but can increase snapshot traffic.

KV no longer reopens transfer transactions for documents already covered by the
peer's inventory. A regression counts storage reads for unchanged sync.

## Subscription and caller findings

`crates/btree/src/changes.rs` uses a watch channel with bounded history: at most
128 commit batches, each limited to 128 identities and 16 KiB of identity bytes.
Lagging consumers select broad reconciliation. This is not an unbounded payload
queue. Subscription drop, retry and idle behavior passed runtime tests.

The Storage caller merges SQL/KV wakeups and performs broad SQL followed by broad
KV sync. It does not pass selected invalidations to the selected-sync interfaces.
Jobs are created per peer/resource; sessions for the same peer/resource are
serialized by `of/crates/storage-server/src/database_sessions.rs`. Different
peer/resource pairs can run concurrently.

No caller concurrency policy or retry policy was changed without measured evidence.
The 14 Storage replication runtime tests passed, including idle convergence,
reconnect, cancellation and policy fencing. Both session arbitration tests passed.

## Remaining limits and next measurements

- SQL still prepares outbound payloads and discovery summaries in memory. Native
  file staging does not bound those allocations or total process memory.
- SQL manifests and snapshot requests remain single-frame messages. Large stores
  can still hit these limits. Full hash discovery also scales with history depth.
- KV must still reconstruct individual documents. Full inventories remain in memory.
- Neither protocol fragments one oversized document/change into smaller records.
- A session byte budget is not a process-wide heap budget. Concurrent sessions,
  engine reconstruction, storage caches and frame buffers add to process memory.
- Selected discovery must preserve both peers' invalidations, reconnect recovery,
  remote wakeups and per-peer acknowledgements. Do not skip broad reconciliation
  by using only one peer's local changes.

To identify the original OOM, capture the failing revision, backend, key/row count,
history depth, payload sizes, peer/resource count and retry errors. Profile one
session, then increase history depth and concurrent sessions independently. Record
live heap and RSS separately, plus discovery/export/apply time and progress per
retry. Change process admission or discovery design only against this evidence.

## Validation

Passed:

- SQL sync tests and its full feature powerset, all targets.
- KV store and KV sync feature powersets, all targets.
- Clippy for the three changed crates, all features and targets, warnings denied.
- KV facade sync and subscription tests; continuous runtime tests.
- SQL native staged bootstrap/incremental/idle stack test.
- Storage replication runtime and session arbitration tests.
- Changed-file formatting and diff whitespace checks.

Incomplete or blocked:

- Workspace feature powerset stopped on existing `ofdb-btree` sharded-only test
  compile errors, including missing `ToString` imports.
- `just crap` timed out after 200 seconds during workspace coverage compilation;
  no final complexity score was produced.
- SQL facade pending-abort test overflowed its stack while inserting fixture data,
  before sync started. The same isolated test also overflowed with the original
  SQL sync implementation restored for comparison. No stack limit was increased.
- No browser or production-load profiling was run.
