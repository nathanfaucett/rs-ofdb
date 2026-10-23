# Ticket04 SQL integration

Branch: `sync-change-detection-04-sql`; worktree `.scratch/worktrees/04-sql`; starting integration tip `d2781eb`.

## Scope and assessment

SQL sync/session, Automerge reconciliation tests, SQL facade and its tests only. No KV files, ticket04 edits, shared protocol/crate, dependency additions, compatibility adapters, transport runners or persistent peer frontiers. Used `tdd` and the approved public transaction, reconciliation and subscription seams. Priority high; complexity medium-high; correctness risk high around lost work, metadata and authorization. Existing broad validated apply/recovery machinery is reused rather than duplicated.

## Implementation

- SQL protocol **7**, intentionally incompatible with 6. Selected endpoints exchange a versioned `SyncMessage::Selection` before the normal hello. `SyncSelection::{All, Selected { tables, rows }}` represents discovery work, not an authorization scope. Both endpoints compare the union; either endpoint's All expands the pass to broad discovery. Exact row work never calls `row_ids` or discovers unrelated user tables. Catalog addresses conservatively promote selection to All.
- Discovery gathers `DocumentSummary` values in one engine read transaction. A row's metadata, digest and native history inventory come from that same snapshot. The manifest and inventory are derived from these summaries. Snapshot export is deferred until mismatched rows actually require it; full comparison also avoids exporting/reloading snapshots solely for digests. Removed the superseded separate session inventory/export/digest-discovery path.
- Missing native changes still use engine-owned export and validated atomic engine apply. Missing dependencies retain the existing staged snapshot request/retry path. No new apply path or schema bypass.
- A mismatched row with nonempty metadata uses a conservative complete snapshot plus metadata, even when document deltas are available. This fixes the combined document edit/deletion defect without changing incremental frames. The old regression is no longer ignored. Ordinary live rows without metadata still send native deltas and **zero snapshots**. If the remote inventory already contains all local native history, the metadata-free subset peer does not send a redundant snapshot back.
- Scoped history/value/candidate validation, preflight disclosure checks, complementary directions, staging cleanup, pre-apply authorization/cancellation and complete-row/catalog atomicity remain on their existing APIs and apply paths. Selected APIs are deliberately **whole-database admitted**, not restricted-table APIs. Invalidations must never be used to infer permission. Callers with restricted history/candidate policies must use the existing table-scoped validated APIs (which retain conservative scoped preflight exports); no selected/scoped policy shortcut was introduced.

## Public usage and lifetime

SQL facade provides:

- `Database::synchronize_selected_checked(transport, config, role, invalidations, check)` (memory staging).
- `Database::synchronize_selected_with_stage_factory_checked(transport, config, role, factory, invalidations, check)` (caller staging).

The low-level SQL sync entry point is `synchronize_selected_with_stage_factory_checked(engine, transport, config, role, factory, SyncSelection, check)`.

Both peers must use selected negotiation, each with its own pending selection. Keep a separate subscription for **each peer relationship**, not one global consumed frontier:

```rust
let mut changes = database.sync_changes()?; // new/reconnected peer => All
// Before every pass, before opening export/read snapshots:
let work = changes.select();
let invalidations = match &work {
    Some(work) => database.sync_invalidations(work).await?,
    None => Vec::new(),
};
database.synchronize_selected_checked(
    &mut transport, &config, role, &invalidations,
    || core::future::ready(Ok(())), // caller's real admission/cancellation check
).await?;
if let Some(work) = work {
    assert!(changes.acknowledge(work));
}
// Re-select before waiting: commits during discovery/export/apply stay pending.
```

Error/cancellation: do not acknowledge; discard failed transport and retry captured pending work with a fresh transport. Caller must not acknowledge work which was not actually included in successful reconciliation. Never reuse a peer subscription/frontier for an unknown or reconnected peer. Restart/reconnect/new subscribers and ring overflow request All. Explicitly supplying `[SqlInvalidation::All]` also forces broad comparison.

Local subscriptions cannot observe remote-only commits. The caller supplies remote wakeups, invokes the two-sided reconciliation when either side has work, or reconnects with All. A remote endpoint with a pending selected token contributes that work through negotiation even when the local selection is empty. If remote work cannot be supplied reliably, request All. There is no persistent transport, automatic background task or durable replay promise.

## TDD and deterministic evidence

- Removed the combined metadata/deletion ignore and reproduced its receiver-visibility failure before repairing selection; the formerly ignored test now passes normally.
- Replaced ticket01's broad one-row baseline assertions with exact selected-work assertions: both peers combined **2 summary reads, 0 table/row-ID scans, 0 snapshot exports, 0 separate history inventories, 0 digest reloads**. One native change frame still transfers. Broad comparison of already-known state also exports zero snapshots and reloads zero digests. Retained baseline notes: ticket01 49 snapshots / 26 inventories / 48 digest reloads; ticket02 25 / 26 / 24 plus 24 summaries. These are public reconciler-call counters, not a global count of all private catalog/index/apply work.
- Facade red-before-green compile failure established the selected API seam. Three-peer fan-out test uses real mapping/subscriptions and independent per-peer acknowledgements, remote-only edits, offline divergent updates plus deletion metadata, broad reconnect and no-op settling. Canonical convergence uses `sync_manifest`, not raw Automerge save bytes (equivalent concurrent histories may serialize differently).
- During new, established and reconnect sessions, a transport hook commits another row update and a new row after summary capture; acknowledgement of the earlier token leaves that commit pending, and the next selected pass converges.
- Authorization denial leaves receiver state unchanged and selected work retryable. Ring overflow (130 commits) and catalog drop/recreation map to All and converge. Redb selected sessions and reopen fallback are exercised through the facade too.
- Selected negotiation test reproduced invalid-configuration transport progress, then verified early rejection plus incompatible-version rejection without mutation.

## Validation

All builds use the shared `/home/nathan/code/rust/aicacia/ofdb/target`, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, `-j 2`, and bounded tool calls. Initial disk availability 335 GiB; subsequent check 331 GiB. No artifacts belonging to other work were deleted. Cold compilation and shared-target locks required bounded retries.

Completed:

- `cargo test -p ofdb-sql-engine-automerge -p ofdb-sql-sync --all-targets -- --skip oversized` (120-second bounded retries; final run passed).
- `cargo test -p ofdb-sql-sync --test catalog selected_negotiation -- --nocapture` (passed after red/green).
- `cargo test -p ofdb-sql --features sync,sql,redb,in-memory,native-stage --test facade_sync --test commit_invalidation --test changes`: **17 passed** (11 facade, 1 invalidation, 5 lifecycle).
- Focused affected-package all-target Clippy with `-D warnings -A clippy::needless_question_mark` passed. Allowance is ticket01's known unrelated `btree-redb/src/btree.rs:95` lint; no source suppression or unrelated fix.
- `cargo check -p ofdb-sql-engine -p ofdb-sql-sync --no-default-features` and `cargo check -p ofdb-sql --no-default-features --features remote` passed.
- Formatting and whitespace checks passed before final integration; final reruns recorded below.

The facade scoped suite reached **24 passing tests** before the overall 120-second bound, with `directional_partial_native_apply_cancellation_reopens_and_resumes_exactly` (950,000-byte payloads in both roles) unfinished. This is not a claimed full-suite pass. The engine/sync `oversized` tests are explicitly filtered, matching ticket02's bounded validation limit. No workspace feature powerset or CRAP run, as requested.

## Integration finalization

`GIT_EDITOR=true git merge --no-edit sync-change-detection` returned **Already up to date** at integration tip `d2781eb`. Final `cargo fmt --all -- --check` and `git diff --check` passed. The final seven-test `change_detection` run passed with zero ignored tests, including the combined metadata regression in both broad and selected modes. Final affected-package all-target Clippy passed with the single documented pre-existing lint allowance.

A separate scoped-suite retry filtering only the large-payload cancellation test hit the 120-second bound during recompilation (shared target/parallel work); it did not establish an additional complete run. The earlier 24 successful scoped tests and the completed engine/sync scoped/history/candidate suites remain the evidence; the large facade cancellation test remains unverified here. No failures were hidden by changing old assertions outside the requested deterministic baseline replacement.

This file intentionally replaces implementation comments on ticket04 to avoid the KV owner's ticket collision.
