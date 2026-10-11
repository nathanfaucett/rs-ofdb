# 08 — Repair incremental SQL change identity after concurrent/sequential writes

Status: ready-for-agent
Work status: resolved
Owner: storage-sync-owners
Priority: High
Complexity / risk: Medium / high (replicated history integrity)
Blocked by: None; 06 is downstream, not a gate.

## Machine-local validation scope

Apply the [common Ubuntu Linux x86_64 policy](../spec.md#machine-local-validation-scope). Use bounded focused kernel/session/public conflict tests with relevant host native/in-memory/core features and a small explicitly bounded repeat count. Isolated same-host peers exercise sequential/concurrent writes, causal inversion across frames, forged/duplicate IDs, atomic rejection, partial resolution, native reopen and real incremental convergence without relaxing hash validation. Pre-fix persisted overlapping-bundle upgrade histories remain unqualified, not declared unsupported or fixed by narrower testing. This scope note preserves resolved status and historical evidence; no tests run in this docs-only checkpoint.

## Scope

Consumer of02 comments 35–39 and subsequent evidence report intermittent `Incremental change hash does not match key` before resolution (2/5 repetitions). Owner 07 also observed failure after resolution/reopen. Reproduce at owner baseline 1f0fe5aedb41fb728ce5ad58182381d6e11bc950, isolate generic cause, fix in owner worktree only. Preserve prior commits and hash validation, no domain workaround or consumer edits.

## Acceptance

- Deterministic red/green at existing public owner SyncRowCodec/Engine seam proves exported ID and payload correspond after sequential writes; forged payload/ID rejected atomically, duplicates remain safe.
- Public facade native/in-memory incremental synchronization preserves concurrent values, explicit partial restrictive resolution and unresolved ownership, including native reopen and final incremental convergence. Repeated focused regressions pass with exact counts recorded.
- Minimal generic root-cause fix; no feature/dependency/API expansion, validation weakening, domain policy or unrelated refactoring. Relevant host core no_std checks and bounded focused suites/targeted lints pass with exact outcomes recorded; broad sweeps are follow-up qualification, not closure gates. Preserve all historical broad outcomes below.
- Sync current integration tip before commit/report; never merge owner into integration. Preserve other work and original dirty docs.

## Evidence

Tracked report `docs/storage-sync-incremental-identity.md` records exact commands and root-cause/red-green details. All own local criteria demonstrated; broad outcomes recorded separately, not an all-green claim. Public API signatures/dependencies/features unchanged; apply_change validation untouched.

- Baseline deterministic `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test kernel sequential_updates_export_only_the_identified_change -- --nocapture` failed LeftoverData for second payload, then passed after actual pre-update-head delta encoding in ordinary writes/resolution. Forged second ID carrying first payload rejects and rolls back entire batch; valid and duplicate batches converge.
- Baseline public tests with `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,sync,sql --test conflicts <filter>`: final summarized five repetitions of sequential_concurrent yielded1 pass/4 hash failures before resolution; native_conflicts yielded3 passes/2 hash failures after final incremental resolution/reopen. Both patterns reproduce at owner1f0fe5a implementation.
- First producer fix alone left3/5 sequential fixture failures: temporary trace proved advertised hash matched payload but prerequisite absent. Deterministic session fixture generates an explicit hash-order inversion before sync, failed RemoteAbort before fix. Stable catalog/table/row sort retains codec dependency order; receiving grouping rejects duplicate IDs instead of requiring lexical hashes. Existing integrity checks unchanged. Intermediate sender-only test timed out because old receiving hash-order guard rejected causal delivery; final coordinated change passes, diagnostic logging removed.
- Green `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,sync,sql --test conflicts`:4 tests,20/20 repeats=80 passed. `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test session incremental_session_preserves_dependency_order_not_hash_order`:20/20 repeats with one-unit frames. Public native-to-memory fixture preserves false/true and competing ownership values, partial explicit false resolution leaves ownership unresolved, incremental convergence after reopen and durable history after second reopen.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --all-targets`:65 passed. `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --all-targets --all-features`:32 passed. `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test conflicts --test client --test facade_sync --test cluster_sync --test local_sql`:36 passed, includes native staging/denial/cancellation/pending-Abort and offline recovery. Memory-only conflicts1 passed. All focused commands completed within120–300s bounds.
- `cargo check -p ofdb-sql --no-default-features`, same with `--features remote`; `cargo check -p ofdb-sql-engine --no-default-features`; `cargo check -p ofdb-sql-sync --no-default-features`:passed. `cargo clippy -p ofdb-sql-engine-automerge -p ofdb-sql-sync -p ofdb-sql --all-targets --all-features`, `cargo fmt --all -- --check`, `git diff --check`:passed.
- Sequential final broad commands each timeout_ms1200000: `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets` timed out compiling309/581,1–308 completed, no observed test/compiler failures; NOT full pass. `just crap` completed exit0,28 coverage commands/analyzer,199/1045 functions above30 and baseline2/96-file LCOV mismatch; not aggregate coverage/complexity pass. Logs target/storage-sync-08-{repeats,codec,sync,powerset,crap}.log.
- Restored native fixture final incremental exchange removes07 whole-state fallback; historical07 report retained with supersession link. Consumer must rerun domain test itself; no of edits/execution. Pre-fix persisted overlapping-bundle upgrade histories unqualified, no compatibility glue/migration introduced. Integration38696d9 merged before commit (Already up to date), ancestor verified; final hash appended after commit. Prior1f0fe5a preserved; original dirty docs untouched.

## Comments

- 2026-10-07: final implementation/evidence commit `25b92516df6ef884d9cdf723da65c64568acdbd0`. Worktree clean; prior1f0fe5a and integration38696d9 ancestors verified. Integration merge before/after commit Already up to date; no owners merge back into integration. Commit hook rustfmt passed. Git emitted pre-existing worktree GC/unreachable-object warning; no prune/cleanup performed. Local acceptance resolved with explicit full-powerset timeout/LCOV limits, not broad all-green qualification.
- 2026-10-07: claimed before implementation. TDD loaded; public facade and existing public owner codec/sync seams authorized by request. Original tracker briefly appeared absent during interruption recovery; it is now present again and historical files/maps are preserved, not reconstructed. Baseline worktree clean at 1f0fe5a; integration 38696d9 ancestor. Consumer sources read only.
