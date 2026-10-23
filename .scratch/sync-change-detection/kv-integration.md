# Ticket04 KV integration handoff

## Scope and assessment

- Branch `sync-change-detection-04-kv`, worktree `.scratch/worktrees/04-kv`, started clean at integration `sync-change-detection` / `d2781eb`. Read spec, ticket04, ticket01 Comments and ticket03 Comments; loaded `tdd` and used the preapproved public transaction/store/sync/subscription seams.
- Priority high, complexity medium-high, correctness risk high around native history/dependencies, generation replacement, atomic recovery and per-peer acknowledgement. No new dependencies, shared protocol, B-tree, SQL, runtime, root glossary or ticket status changes. The parallel SQL half is separate.
- Merged latest `sync-change-detection` before handoff: already up to date at `d2781eb`. Ticket04 status intentionally left for the coordinator.

## Implemented interfaces

### Store

`KvTransaction::{history(key), histories(), export_changes(id, known_hashes), import_changes(id, changes)}` expose native Automerge history, not physical incremental labels. `KvHistory` includes document ID/latest generation, heads and dependency-ordered `ChangeSummary { hash, deps }`; `NativeChange` additionally includes payload. Inspection does not save/reload a snapshot for a digest. Export reconstructs actual history and can extract individual changes from snapshots and bundled storage records.

`import_changes` validates every incoming payload against its claimed native hash **and exact dependencies**, validates dependency completeness/order before writes, validates the final document, and applies via store snapshot merge. Latest generation wins; stale generations cannot resurrect data; tombstones win concurrent same-generation updates; same-generation concurrent history is retained. `ChangeApply::MissingDependencies` makes no writes for that call. The session rolls back the **whole batch**, including earlier keys, before requesting snapshots. Recovery requires the snapshots/current state to contain every promised change (or a newer generation); empty/incomplete recovery is rejected atomically.

### Sync

KV wire version **3**, with no old compatibility path. `synchronize` / `synchronize_checked` retain broad bidirectional discovery but now use native deltas for matching generations. `synchronize_selected_checked(store, invalidations, transport, role, config, check)` compares the union of both peers' selected logical keys and re-reads latest generations; invalidation generation is not an export filter. Either peer selecting `All` forces broad discovery. New generations/bootstrap and missing dependencies use merge-preserving snapshots. All incoming snapshot/delta/recovery batches run caller checks **before opening the apply transaction**; errors/gaps roll back complete batches; earlier successful batches remain committed.

Inventories contain actual native change hashes. Deltas carry actual hashes/deps and self-contained single-change payloads in dependency order. Ordinary established-key updates export no full snapshots; known history sends no data batches and performs no mutations. Transactions used for inventory/export are released before network I/O. SQL/KV wire semantics remain separate.

### Facade (public selected interface)

```rust,ignore
let mut peer_changes = database.sync_changes()?; // independent, unmerged, per peer
// Both peers invoke this, even with no local pending work on a remote wakeup.
database.synchronize_selected_checked(
    &mut peer_changes, &mut transport, SyncRole::Initiator,
    SyncConfig::default(), || core::future::ready(Ok(())),
).await?;
```

The facade selects `ChangeWork` **before reads**, maps it using the actual physical table (including `open_with_table`), and acknowledges **only after successful completion**. Failure/cancellation leaves that watermark retryable; later commits remain pending. `Database::sync_invalidations(&ChangeWork)` plus facade re-exports `ChangeWork` / `KvInvalidation` support explicit caller-managed selection. Tokens/subscriptions must belong to this database; merged subscriptions are for broad callbacks, not selective mapping. Fresh/clone/reconnect subscriptions start broad; overflow/gaps/unknown work map to `All`.

## Delivery contract and limitations

- **Caller-driven remote reconciliation/wakeup:** local subscriptions cannot observe writes committed only at a remote peer. Callers supply authorized remote wakeups/receive delivery or explicitly reconcile, including with empty local selection. Incoming application then publishes local invalidation for fan-out. No automatic task/transport or bidirectional realtime guarantee from a local subscription alone.
- Generic KV memory `sync_changes()` remains explicit `Unsupported`; callers use broad sessions. Shared Redb wrappers support bounded selected notifications. Raw/external writes outside those wrappers are not observed automatically.
- No durable peer cursor/frontier. Keep independent subscriptions per peer and create fresh ones on reconnect/restart. `ChangeSubscription::run` is still useful for broad callbacks; the selected facade method takes an explicit mutable peer subscription.
- Selection avoids whole-store work, but reconstructs selected documents and inventories their full native history. Apply reuses snapshot merge/persistence rather than introducing a new B-tree incremental-storage path. Per-document inventory/delta size and aggregate batch size must fit configured frames; oversized frames fail explicitly, so tune frame/count budgets. Broad fallback retains a whole-store inventory in memory. No chunked single-document payload protocol was added.
- A session is neither globally atomic nor a frozen global snapshot. Earlier complete batches can survive later failure; discard transport after errors and retry. Expiry remains read-time visibility and emits no timer notification.
- Automerge save byte order is not replication identity: the existing concurrent-history cluster test now compares generation, heads and sorted native hash/dependency summaries, while separately asserting visible-value convergence. Existing malformed snapshot validation coverage is retained.

## Executable evidence

Red/green slices observed missing native-history APIs and missing selective/delta wire APIs before implementation. The incomplete-recovery regression initially advanced past an empty recovery and failed with `UnexpectedMessage`; it now rejects recovery as a storage error before advancing. An initial bounded attempt at that test timed out after shared-target contention and an incomplete scripted transcript; the completed transcript and fixed test pass.

New/focused coverage:

- `kv-store/tests/native_history.rs`: snapshot-contained/bundled native history, topological dependencies, individual native export, forged hash/dependency rejection, no-write gap, successful apply and known-history export silence.
- `kv-sync/tests/selective.rs` (five tests): concurrent nested edits through native deltas; dependency-gap snapshot recovery preserving concurrent history/expiry; incomplete recovery rejection; malformed native hash **and deps** rolling back the complete multi-key batch; concurrent tombstone/update and latest-generation recreation even from old-generation invalidation.
- Deterministic cost fixture: **101 keys** per peer, one selected key, **four total inventory entries** across both directional comparisons, no snapshot frames, and **fewer than 100 physical record reads per peer** through a public backend test decorator. A full-store reconstruction/export would exceed that budget. Replay emits neither deltas nor snapshots.
- `kv/tests/changes.rs`: three-peer selected fan-out, independent acknowledgement, custom physical table mapping, deletion/recreation and stale imports, caller-driven remote-only delivery, reconnect broad comparison, denied batch retry without either peer acknowledgement, commit during apply remaining pending, overflow broad fallback and return to exact-key selection.
- Updated old handcrafted KV wire tests for v3 selection/inventory handshake and interruption after an actual data batch, not a handshake-frame count. Kept complete-batch rollback, earlier-progress preservation, broad one-shot, Redb restart, facade cancellation/nonblocking Abort, remote query and retirement coverage.

All build/test/check commands used 120-second bounds; compilation/test commands used `CARGO_INCREMENTAL=0` and the shared `/home/nathan/code/rust/aicacia/ofdb/target`. Disk checked before/after: approximately 335/331 GiB free; no cleanup or workspace-wide validation required.

Passed:

```text
cargo test -p ofdb-kv-store -p ofdb-kv-sync --all-targets
cargo test -p ofdb-kv --features sync,in-memory,redb,server --all-targets
cargo clippy -p ofdb-kv-store -p ofdb-kv-sync -p ofdb-kv --all-targets \
  --features ofdb-kv/sync,ofdb-kv/in-memory,ofdb-kv/redb,ofdb-kv/server \
  -- -D warnings -A clippy::needless_question_mark
cargo clippy -p ofdb-kv --no-default-features --features sync,in-memory --lib -- -D warnings
cargo check -p ofdb-kv --no-default-features --features sync
cargo fmt -p ofdb-kv-store -p ofdb-kv-sync -p ofdb-kv -- --check
git diff --check
```

The focused Redb-inclusive lint command allows only the pre-existing `btree-redb/src/btree.rs:95` `needless_question_mark` documented by tickets01/03; no out-of-scope lint fix. No workspace powerset or CRAP run, as requested.
