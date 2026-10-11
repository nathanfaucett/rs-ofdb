# 02 — Own optional native SQL staging in ofdb

Status: ready-for-agent
Type: task
Work status: resolved
Owner: ofdb
Blocked by: None
Priority: High
Complexity / risk: Medium / medium

## Scope and deliverable

Add optional file-backed SQL staging and usable public ofdb-sql setup. Start with `crates/sql-sync/src/staging.rs`, its manifest, and `crates/sql/src/{api,database}.rs`. Reference caller `of/crates/storage-server/src/sync_stage.rs` for lifecycle/generic tests, not IdP IDs or custom policy constants. Exact feature/setup shape is an implementation choice.

## Acceptance

- Caller selects staging location/configuration through the facade without implementing a factory or record IO; ofdb owns creation/replay/cleanup.
- Keep memory staging and useful generic stage seams. Native IO/runtime dependencies are optional; core remains no_std/alloc without native/transport features. Embedded file IO is not claimed to be no_std.
- No IdP/Management dependencies or new caller quotas/timeouts/deadlines/freshness policy. Preserve existing protocol/configuration validation without copying custom stage policy knobs.
- Deterministic tests cover append order, lifecycle, malformed/truncated records, success/error/drop cleanup, cancellation during creation/append/replay, and handle closure. Failed creation must not leak a created file.
- Cleanup failures remain observable without masking primary errors; no drop-based crash-cleanup promise.
- Generic replay/rollback/cleanup coverage moves to ofdb; real authorization scenarios remain in of. Focused stage/facade and core/native checks pass with recorded commands/results.

## Exclusions and integration

No background cleanup service or legacy wrapper; no of edits. [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) replaces/deletes its concrete stage after coverage moves. Native authorization composition is 03/05.

## Evidence

Implementation commit: c936acc. Acceptance evidenced: optional native-stage feature, FileSyncStageFactory and Database::synchronize_with_stage_directory; memory/custom factory unchanged. Native IO is synchronous and non-suspending inside each stage operation (may block an executor; no hard interruption guarantee). tempfile RAII owns creation and file lifetime. Drop closes then unlinks, reports errors to stderr independently of primary session error; explicit FileSyncStage::close returns cleanup error. No copied budgets/domain types. SQL facade sync no longer enables Iroh automatically; optional lower-level adapter extraction remains coordinated external work.

- Red: `cargo test -p ofdb-sql-sync --features native-stage --test native_stage` rejected missing feature before implementation.
- Green: same command: 4 tests passed (order/lifecycle/drop; truncated prefix/body and invalid length; failed creation; cancellation at creation/append/replay boundaries; explicit cleanup failure).
- `cargo check -p ofdb-sql-sync --no-default-features`: FAILED on a subsequent isolated check in sql-engine (missing alloc Vec and related no_std errors). The earlier grouped command's successful final facade check was not evidence that this component check passed; corrected here. Unrelated engine no_std defects are not changed.
- `cargo check -p ofdb-sql --features native-stage,in-memory`: passed.
- `cargo test -p ofdb-sql --features native-stage,in-memory,sql --test facade_sync`: default test stack overflowed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,sql --test facade_sync`: 1 passed, native catalog/row replay, exact exported state and empty stage directory.

Additional 05 composition evidence: `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,sql --test facade_sync`: final 6 passed. Native success, both-role denial/cancellation, retry convergence, invalid catalog replay rollback, cancellation inside catalog transaction, staging disconnect cleanup all pass. `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --test catalog --test native_stage`: 17 + 4 passed, including native recovery checks and cleanup. Generic stage mechanism/rollback coverage now lives in ofdb; consumer concrete stage removal stays external.

Previous blocker (superseded by continuation below): no-native core compile failed in sql-engine. No crash cleanup or cancellation mid-blocking syscall guarantee is required or claimed.

### Continuation: core qualification resolved

Fix commit: be49a20d35f7d4a7a93ba174405433ab01bc3ddd. Added only `use alloc::vec::Vec;` in sql-engine/src/codec.rs. The missing import caused all 17 downstream errors; no engine refactor or forced std feature.

Red: `cargo check -p ofdb-sql-sync --no-default-features` reproduced 17 errors (60s bound).
Green and focused regressions (a chained 300s-bounded invocation, all commands succeeded):

- `cargo check -p ofdb-sql-sync --no-default-features`: passed.
- `cargo check -p ofdb-sql-sync --no-default-features --features native-stage`: passed.
- `cargo clippy -p ofdb-sql-sync --features native-stage --lib`: passed without warnings.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --lib --test catalog`: 9 + 17 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage`: 9 + 17 + 4 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sync,in-memory,sql --test facade_sync`: 2 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync`: 6 + 9 passed.

All ticket 02 owner-local acceptance is evidenced by the initial stage/composition evidence plus successful independent core/native checks. Resolved; full-workspace powerset/complexity qualification remains ticket 05, not this ticket. Integration synced before fix commit (3c4bf50, already up to date).

## Comments

- Continuation claimed on 2026-10-07: user explicitly authorized minimal SQL no_std import fixes as required ticket 02/03 public feature qualification. Existing public core compile check is the red/green seam; no unrelated refactor. Integration synced before work (3c4bf50, already up to date).

- Claimed by storage-sync-owners on 2026-10-07 after 01 (b144761). Medium complexity/risk: use optional tempfile ownership and synchronous native record IO inside non-suspending async stage operations to avoid file-creation cancellation leaks; no policy limits copied. Public SyncStage/Factory and facade sync are the requested seams.

- Plain SQL synchronize already uses memory staging; the observed native production path owns its factory in of.

- Historical planning audit (superseded by resolved acceptance above): canonical triage is ready-for-agent; work was open, with blockers recorded separately and no implementation yet claimed.
