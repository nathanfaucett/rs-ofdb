# 10 — Genuine unchanged SQL UPDATE is successful without a new change

Status: ready-for-agent
Work status: resolved
Owner: storage-sync-owners
Priority: High
Complexity / risk: Low / medium (mutation/delta identity)
Blocked by: None

## Machine-local validation scope

Apply the [common Ubuntu Linux x86_64 policy](../spec.md#machine-local-validation-scope). Use the bounded focused public unchanged-update regression with relevant native/sync/SQL features: direct and explicit-transaction no-ops preserve exact history/state, native reopen, later real mutation and same-host incremental convergence. Retain conflict/tombstone/hash checks; consumer no-rotation behavior still needs its owner's real rerun. This scope note preserves resolved status and historical evidence; no tests run in this docs-only checkpoint.

## Contract and acceptance

Consumer of02 comment49: active no-rotation client credential test reaches Incremental payload has no change after08. Same-value UPDATE must succeed, including explicit Transaction commit, without inventing timestamps, generating a new incremental record or ignoring integrity errors. Public facade TDD regression proves state/history unchanged; later genuine mutation and sync/reopen remain usable. Fix generic owner root cause with minimal changes, separate commit. Preserve conflicts/tombstone/hash validation and other work.

## Comments

- Claimed at25b9251 before implementation, public Client/Transaction test seams authorized. Reproduce in owner only; no consumer execution/edits.
- Resolved by separate commit `c368f16641d0039ac3d32edd1e271d997df3f5e9`. Exact RED/GREEN: `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,sync,sql --test conflicts unchanged_updates -- --nocapture`; RED reproduced `QueryError(Storage, "Incremental payload has no change")`, GREEN public `unchanged_updates_commit_without_new_history_and_remain_syncable` proves direct/Transaction no-op commits leave exact state/history unchanged, native reopen retains it, and later real mutation/incremental convergence works. Root merge_row guard returns existing row only for genuinely empty delta; nonempty invalid deltas still reject. No invented timestamp/change, ignored failure or weakened hash validation.
- Final public scoped/conflict suites10/10 repeats (110 tests, including no-op each run), codec all-targets67 passed, facade composition43 passed. Resume public scoped/conflict11 tests passed again. Memory-only conflict test passes without native runtime. Formatting/Clippy/no_std evidence and exact shared broad commands recorded in `docs/storage-sync-table-scope.md` and09: final powerset1200s timeout306/581 is incomplete, just crap exit0 has known LCOV aggregation limitation/high scores. Consumer no-rotation test remains its owner's rerun; no of changes or execution.
