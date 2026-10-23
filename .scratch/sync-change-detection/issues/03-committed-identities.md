# 03 — Publish commit-scoped logical invalidation

Status: Implemented

Depends on: 01

## Purpose

Let reconciliation select changed logical state without treating storage records as a durable feed.

## Work

- Extend existing transaction bookkeeping where sound. Select a single tested publication strategy that cannot lose invalidation after a successful commit, including cancellation around commit completion.
- Cover SQL Logical Rows, catalog/dependency invalidation, metadata-only changes, indirect tombstones/cleanup and KV Key/Generation changes. Ignore derived index bytes as independent replicated identities.
- Publish after successful commit; discard on rollback/failure. Use conservative table/catalog or broader invalidation where exact row identity collection is not reliable.
- Retain generic storage wakeups where useful. Keep SQL/KV semantics out of generic B-tree interfaces.
- Define subscription initialization, selected-work watermark, success acknowledgement and retry behavior. Never consume writes that arrive during reconciliation accidentally.
- Give independent peers independent progress. Bound memory and represent overflow or unknown changes as an explicit broader-reconciliation requirement; no durable cursor promise.
- Cover incoming replication for fan-out, including metadata and generation replacement. Preserve existing caller-controlled runner lifetime and cancellation.

## Acceptance

Deterministic tests cover rollback, failed commit, multi-write commits, cancellation near commit, commits during reconciliation, two consumers at different progress, new subscriptions, overflow/gaps and incoming replication. A missed identity causes conservative reconciliation, never silent loss. Acknowledgement of one peer does not clear another peer's work.

## Validation

Run targeted backend, engine/KV and subscription tests as applicable. Record which backends support selective notifications and the explicit behavior for unsupported backends.

## Comments

### Assessment and scope

- Started clean on `sync-change-detection-03` in `.scratch/worktrees/03`, exactly integration `sync-change-detection` commit `902659f`; `merge-base --is-ancestor` confirmed the integration base. Read the spec and ticket01 mutation inventory/exact ownership before implementation. Priority high, complexity medium-high, risk high around persistence/publication, metadata omission and consumer progress.
- Used `tdd` and the approved engine/store transaction and committed-subscription seams. Observed red compiler failures for selected work, memory subscriptions, SQL mapping and KV mapping, then implemented each vertical slice. Tests use real transactions and documents, plus an actual Redb backend whose durable sync can fail; no production fault hooks or new dependencies.
- Left ticket02's codec, document/history types, Automerge manifest, SQL sync session/protocol/state and existing SQL characterization tests untouched. No edits to `sql-engine/lib.rs`. The codec-specific key decoder is a **new detection-only** `sql-engine-automerge/src/commit_invalidation.rs`, wired by its thin lib re-export; facade plumbing and new detection tests are ticket03 changes.

### Publication and bounded consumer contract

- Redb extends the existing net-physical-mutation inventory with `StorageInvalidation::{All, Table, Key}`. The native transaction persists and publishes synchronously before its writer-admission guard drops. There is no async postcommit publication step. Failed persistence, rollback, drop, read-only and net-no-op commits do not publish. Drop/recreation retains a table invalidation even if table existence is unchanged.
- The SQL in-memory kernel records touched tables/keys and publishes net differences while holding the successful optimistic-commit state lock; conflicting commits do not publish. `Kernel::subscribe_changes()` defaults to `None` for unsupported implementations; Engine delegates it. The no_std interface remains available without the notification dependency/features.
- One shared ring per signal retains **128 commits**, each capped at **128 physical invalidations / 16 KiB of identity bytes**, not values or documents. Each subscriber keeps independent acknowledged watermarks. A selected work item is a bounded copy, not a durable cursor or retained transaction. Overflow, oversized batches, unknown writes, new subscriptions and clones select `All`. Acknowledging an overflow pass permits selective work again; later commits are still pending.
- `select()` captures an opaque `ChangeWork` **before opening any export/read snapshot**. `acknowledge(work)` advances only those captured revisions and rejects another consumer's token (or a token predating merge). Drop the work on error/cancellation: selection retries without another commit. Acknowledge only after successful reconciliation. Out-of-order older acknowledgements cannot regress or advance past newer acknowledged work.
- `changed()`/legacy `consume()` coalesce wakeups only; neither acknowledges selected work. The existing `run()` callback signature, explicit lifetime, cancellation, closure and backoff behavior remain workable; successful passes acknowledge their captured work, failed passes do not. No background task is spawned.

### Logical selection and ticket04 handoff

- `engine_automerge::committed_invalidations(&engine, &work).await` returns `SqlInvalidation::{All, Table(String), Row { table, row }}`. It decodes physical document keys with the existing `DocumentChangeKey` codec, including metadata keys, so indirect unique-key tombstones and direct/raw mutations cannot escape the transaction inventory. Current catalog schemas classify table/index storage; known derived indexes are not replicated identities. Catalog/dependency edits and unknown/unclassifiable storage select `All`; malformed keys in a known user table select that table. Row entries covered by a table entry are removed.
- Mapping ordinary edits reads catalog schemas only, not every user document or a whole-database manifest. An established single-row edit is proven to select exactly that row, including with index maintenance. Ticket04 must actually use this selection for export/comparison; this ticket deliberately does not alter the SQL sync session or claim its existing broad reconciliation is already selective.
- `KvStore::committed_invalidations(physical_table_name, &work)` returns `KvInvalidation::{All, Generation { key, generation }}`. Each generation invalidates **latest-generation selection for its logical key**, including old generations removed during replacement. Unknown/mismatched tables or malformed keys require `All`. Use the store's actual physical table name, not a logical key. Incoming history, tombstones and generation replacement use the same backend boundary. Expiry remains read-time visibility, not a mutation.
- Facade: `Database::sync_changes()` now supports SQL memory as well as Redb; `Database::sync_invalidations(&work).await` maps selected work. `ChangeWork` and `SqlInvalidation` are exported by the SQL facade. Continuous-runtime subscriptions expose the same `select()` / `acknowledge()` methods without changing the existing `run()` callback.

Concrete SQL facade handoff (the marked reconciliation step is ticket04's responsibility):

```rust
let mut changes = database.sync_changes()?;
let work = changes.select().expect("new peer requires initial comparison");
let invalidations = database.sync_invalidations(&work).await?;
// Reconcile All, affected Table(s), or exact Row(s), including their metadata.
// Only when that selected pass succeeds:
assert!(changes.acknowledge(work));
// Select again before waiting: commits during that pass remain pending.
// If select() is None, changed().await waits for a coalesced wakeup.
```

Engine/store equivalents:

```rust
let mut changes = engine.subscribe_changes().expect("supported kernel");
let work = changes.select().expect("initial work");
let invalidations = engine_automerge::committed_invalidations(&engine, &work).await?;
// Successful selected reconciliation precedes acknowledge(work).

let work = kv_backend_subscription.select().expect("selected KV work");
let invalidations = kv_store.committed_invalidations("kv", &work);
// Re-read latest generation for every affected key; acknowledge only on success.
```

### Focused executable evidence

All commands were bounded to 120 seconds and used the shared repository target. Final reruns disabled incremental compilation (`CARGO_INCREMENTAL=0`).

- `cargo test -p ofdb-btree-redb --all-targets`: **17 passed** (3 new). Covers independent consumers, concurrent commit after selection, new/clone initial state, overflow, cancellation/retry of selected work, wrong-consumer ack, rollback/drop/net-no-op, oversized keys, injected durable failure, and caller cancellation immediately after synchronous commit.
- `cargo test -p ofdb-sql-engine-automerge --test commit_invalidation`: **4 passed**. Covers exact row and metadata-only selection, derived-index filtering, malformed raw-write table fallback, incoming requested winner **and indirect unique-key loser**, incoming metadata fan-out, replay silence, catalog/drop broad invalidation, failed explicit engine transaction, optimistic conflict, read-only/rollback silence and cancellation after commit.
- `cargo test -p ofdb-kv-store --test change_detection`: **3 passed**. Covers independent/coalesced subscriptions, incoming same-generation fan-out identities, known-history/expiry silence, caller rollback after failed import, generation replacement including removed/new generations, and stale-generation import silence. Timestamp fixtures are deterministic.
- `cargo test -p ofdb-sync-runtime -p ofdb-btree --features ofdb-btree/changes --lib`: **17 passed** (8 B-tree, 9 runtime), retaining race, retry/backoff, cancellation, closure, merged-store and small-runner-future behavior.
- `cargo test -p ofdb-sql --features sync,sql,redb,in-memory --test commit_invalidation --test changes`: **6 passed**. New facade test exercises selected row mapping and concurrent/independent progress against **both memory and Redb**; legacy continuous lifecycle tests remain green.
- `cargo check -p ofdb-sql-engine --no-default-features --features in-memory`, `cargo fmt --all -- --check`, and `git diff --check` passed.
- Strict focused Clippy reproduces ticket01's pre-existing `btree-redb/src/btree.rs:95` `needless_question_mark`. Focused affected-package all-target checks and SQL facade test checks pass with `-D warnings -A clippy::needless_question_mark`; no unrelated lint fix. An intermediate shared-target build hit disk exhaustion; removed only this ticket's generated `commit_invalidation` artifacts, then successful bounded reruns established the results above. No workspace feature-powerset/CRAP run, as explicitly requested.

### Limitations / integration

- This is bounded in-process invalidation, not durable replay. Restart/reconnect/new consumers require broad comparison. Generic B-tree memory storage (used by KV) still has no signal; its facade's explicit Unsupported/caller-driven broad fallback remains. SQL memory supports selected work under std; other std kernels default to `None`, and the no_std API has no committed-subscription capability.
- Signals cover writes through a shared backend wrapper. Independently constructed wrappers over one raw Redb database, raw writes outside those wrappers, or writes made only at a remote peer cannot be detected locally. Use shared handles and caller-owned remote receive/reconnect/reconciliation; local wakeups alone do not promise bidirectional realtime delivery.
- Catalog changes deliberately remain broad; raw unknown keys in a known user table permit conservative table scans. Physical compaction can cause harmless extra invalidation. Schema classification can choose broader work if a concurrent catalog change/error prevents classification; its commit remains pending after the selected token.
- Keep per-store subscriptions for selective mapping; merged subscriptions are useful for broad combined lifecycle callbacks. Tokens have no backend/storage ownership and must be applied to their corresponding database/store; retain no storage snapshot. Ticket04 still owns selective session behavior and the combined metadata/document transmission defect recorded in ticket01.
- Integration synchronization: `git merge --no-edit sync-change-detection` reported **Already up to date** at integration tip `902659f`. Ticket02 has not landed on that integration tip; no ticket02-owned semantic/session files need exports from `sql-engine/lib.rs` for this implementation. Recheck/merge integration after the ticket commit before final handoff.
