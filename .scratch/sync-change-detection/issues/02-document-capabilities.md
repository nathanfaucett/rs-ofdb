# 02 — Separate document capabilities from protocol

Status: Done

Depends on: 01

## Purpose

Remove the Row Reconciler's need to depend on SQL wire/session types without making the sync crate a mere transport wrapper.

## Work

- Review `crates/sql-sync/src/codec.rs`, `crates/sql-engine-automerge/src/codec.rs` and engine Row Reconciler interfaces.
- Move the smallest history/state capability interface and required semantic types to the engine/Row Reconciler side. Keep `SyncMessage`, transport frames and session orchestration in `sql-sync`.
- Obtain document heads/history summary plus metadata from a consistent transaction snapshot, without save/load solely for digest discovery.
- Define actual Automerge change IDs independently from physical `DocumentChangeKey` hash labels. Cover multi-change payloads and dependency order.
- Evaluate exporting missing changes from document history contained in snapshots, retaining full-state fallback when necessary. Do not mandate physical incremental retention.
- Rewrite call sites and delete superseded interfaces; do not add compatibility adapters or a new sync-core crate.
- Preserve no_std/alloc behavior for engine interfaces and feature isolation for remote-only facades. Keep implementation logic out of lib.rs/mod.rs.

## Acceptance

The Automerge Row Reconciler no longer needs protocol-owned state-unit types for its semantic capabilities. Dependency direction is explicit and tested. Existing SQL validation and session behavior remain intact; tests establish equivalent digests/history interpretation and snapshot fallback.

## Validation

Run focused codec/session tests, affected no-default-features checks and relevant facade feature checks. Record any intentional interface or protocol break; do not assume a wire change is necessary.

## Comments

### Implementation and ownership

- Implemented in `.scratch/worktrees/02`, branch `sync-change-detection-02`, starting clean at `902659f`, exactly the integration `sync-change-detection` tip. Read the spec and ticket01 findings/ownership before implementation; used the agreed public transaction, reconciliation/history and sync seams with red-before-green tests.
- Assessment: high correctness priority, medium-high complexity, high risk in dependency validation and scoped-history security. No commit/backend/KV/runtime/engine mutation files owned by ticket03 were edited. No selective sessions, crate moves, or ticket04 metadata repair.
- Moved document capabilities into `sql-engine/src/document.rs`, exported from the thin engine `lib.rs`. SQL sync re-exports the semantic trait and evidence/identity/digest types under its existing public vocabulary; there is one implementation interface, no compatibility trait or adapter. Protocol units/messages, staging, peer selection, scoped admission and validated apply orchestration remain SQL-sync-owned. Wire layouts and protocol version 6 are unchanged.
- One necessary declaration-only extension to the suggested ownership list: `sql-sync/src/candidate.rs` now re-exports engine-owned row/table observation types instead of defining them. Its candidate validation logic is unchanged. Moving complete table observation without this declaration move would retain the production dependency cycle or require a conversion adapter. Other changes stay in the suggested 02 files, plus the explicitly authorized engine `lib.rs`.
- Removed production `sql-engine-automerge -> sql-sync`; sync is only a dev-dependency. Production dependency-tree assertions verify this and remote-only facade isolation.

### Public contracts for ticket04

- Engine exports `RowReconciler<T>: RowCodec<T>`, `DocumentChangeId(Vec<u8>)`, `DocumentDigest([u8; 32])`, `DocumentSummary { digest, metadata, changes }`, `RowStateView`, `TableStateView`, and `InspectedHistory`. SQL sync's `SyncRowCodec`, `SyncChangeId`, `StateDigest`, `SyncRowView`, and `SyncTableView` are direct re-exports of these semantic definitions, not separate implementations. `SyncStateUnit`, `SyncKey`, inventories and incremental frames remain protocol-owned.
- `history_summary(&T, table, row, max_bytes) -> EngineResult<Option<DocumentSummary>>` observes history and metadata in the caller's immutable transaction. `None` means neither document nor metadata exists. Automerge reconstructs once, computes its canonical heads-and-metadata digest directly, and enumerates actual change IDs in dependency order; no save/load for discovery. The returned metadata plus conservatively accounted inventory must fit the budget. Generic non-native codecs have the trait's state-based default; Automerge overrides it. Manifests now use this method.
- `change_inventory(&T, table, row, max_bytes)` lists actual native hashes in dependency order, including changes inside snapshots or bundled physical records. Physical record hash labels are not identities and no additional record scan is needed.
- `export_change(&T, table, row, &DocumentChangeId, max_bytes)` exports exactly one native change, including snapshot-contained/bundled history. Malformed hash length or over-budget payload errors; unknown/unavailable history returns `None` for caller-owned full-state recovery. No physical incremental retention is required.
- `apply_change(&mut T, table, row, &DocumentChangeId, payload)` accepts native single-change or concatenated multi-change chunks. It validates chunk framing/checksums/decoding, advertised hash membership, and all dependencies before writing. Unresolved dependencies return `EngineError::SyncDependencyUnavailable`; malformed payload/hash errors do not persist changes. Known complete history is idempotent, independent of physical keys. `merge_state` rejects incomplete incoming snapshot history and still merges concurrent complete histories. Engine-managed mutations must continue through the engine's validated atomic apply scope and index maintenance.
- Scoped complete-history inspection and candidate evidence behavior are preserved. Metadata must still accompany deltas or force full-state transfer in ticket04; the new summary does not itself fix outbound metadata selection.

### TDD and validation evidence

- Observed red then green for native export from snapshot-contained history; for the new summary API and digest/budget behavior; for a bundled matching hash hiding an unrelated unresolved child; and for a snapshot hiding unresolved history. Added coverage for stable transaction snapshots during a concurrent metadata commit, absent documents, native export budgets/unknown IDs, malformed/trailing bytes, forged and malformed IDs, and duplicate delivery.
- `cargo test -p ofdb-sql-engine-automerge --test change_detection -- --nocapture`: **6 passed, 1 deliberately ignored existing ticket04 defect**. Baseline session counts improve from 49 snapshot exports/48 digest reloads to **25/24**, with 24 direct summaries and 26 inventory calls. Remaining session exports are the existing broad state-selection path, not selective discovery. Manifest-only assertions show **zero snapshot exports and zero digest reloads**. Reconstruction/load lower bound is 99 versus ticket01's 123, not a global private-call count.
- `cargo test -p ofdb-sql-engine-automerge -p ofdb-sql-sync --all-targets -- --skip oversized`: passed, including scoped security/history/candidate, recovery, catalog, index, conflict and session tests. The unfiltered bounded run reached two existing oversized-payload tests that exceeded the 120-second budget; no unfiltered all-targets pass is claimed. The filter also excludes the SQL-sync oversized test.
- `cargo test -p ofdb-btree-automerge --test change_history`: **1 passed**, confirming compaction preserves actual native history independently of incremental labels.
- `cargo check -p ofdb-sql-engine -p ofdb-sql-sync --no-default-features`: passed. `cargo check -p ofdb-sql --no-default-features --features remote`: passed. Normal dependency-tree assertions confirm the remote facade excludes engine/Redb/Automerge/sync and the production Automerge reconciler excludes SQL sync.
- `cargo fmt --all -- --check` and `git diff --check`: passed. Focused `cargo clippy -p ofdb-sql-engine-automerge -p ofdb-sql-sync --all-targets -- -D warnings -A clippy::needless_question_mark`: passed; the one allowance is ticket01's documented pre-existing Redb lint, not a new suppression in source.
- Explicitly running `incremental_reconciliation_transfers_combined_document_and_deletion_metadata -- --ignored` still fails at the expected receiver deletion assertion (exit 101). Its ignore reason and regression remain intact for ticket04.
- Build constraints: shared target artifacts collided across parallel worktrees, so reliable final checks used `CARGO_TARGET_DIR=/home/nathan/code/rust/aicacia/ofdb/target/ticket02`, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_DEV_INCREMENTAL=false`, `-j 2`. Initial debug-artifact compilation exhausted disk; removed only this ticket's generated target subdirectory and rebuilt smaller. Cold compiles needed bounded retries; final commands above passed as stated. No workspace powerset or CRAP was run.
- Acceptance met: production dependency direction corrected, direct transaction-consistent summary implemented, native identities/export independent of physical representation, complete dependency validation added, and scoped security/session behavior retained with the known ticket04 regression preserved.
