# 07 — Expose native SQL conflict inspection and resolution through the facade

Status: ready-for-agent
Type: task
Work status: resolved
Owner: ofdb
Blocked by: None
Priority: High
Complexity / risk: Medium / high (security-sensitive conflict visibility and transaction snapshot semantics)

## Machine-local validation scope

Apply the [common Ubuntu Linux x86_64 policy](../spec.md#machine-local-validation-scope). Use bounded focused public conflict/transaction tests on native and in-memory stores: real concurrent writes, exact names/values, atomic invalid resolution, tombstone rejection and native reopen. Same-host remote connections must return explicit Unsupported for conflict operations, not false clear results. Check relevant host no-default/native/memory/remote feature boundaries and targeted lints; incremental repair evidence remains ticket 08, not inferred from whole-state convergence. This scope note preserves resolved status and historical evidence; no tests run in this docs-only checkpoint.

## Consumer blocker and scope

Consumer of02's `of/.worktrees/public-persistence/docs/public-persistence.md` Remaining migration and integration gaps identifies missing Database/Client conflict functionality. Original `of/crates/db/src/native.rs` constructs Engine/RedbKernel/AutomergeRowCodec/raw Redb through facade reexports; IdP/Management cannot remove these backend dependencies without conflict-aware public operations. This is a concrete migration unblocker, not scope expansion. Ticket 06 is downstream handoff evidence and must not gate 07 or of02.

Actual original consumers inspected:

- `of/crates/idp-model/src/replica.rs`: row_conflicts denies authentication/authorization/token/tunnel access on any selected security-row conflict. Explicit administrator resolution refuses nil admin and unsafe revoked_at/consumed_at NULL or omitted conflicting monotonic fields; audits resolved column names. Keep all policy/auditing in of, not storage.
- `of/crates/management-service/src/replica/selection_policy.rs`: row_conflict_values chooses NULL for conflicting application/selection fields and false for admin_allowed/selected only when those concurrent values exist; other ownership/admin conflicts deny. Explicit resolve_row is not a projected last-writer-wins update. deselect_owned refuses ownership/admin conflicts.
- `of/crates/idp-service/src/replica/oauth2_refresh_token_repo.rs` and `replica_signer_repo.rs`: existing transactions combine reads/conditional mutations; transaction-local conflict reads must inspect that same snapshot without opening a second transaction.

## Minimum deliverable / public seams

Through Database::client(), Client exposes row_conflicts(table: &str, key: &Row), row_conflict_values(table: &str, key: &Row), resolve_row(table: &str, key: &Row, values: Vec<(String, Value)>). Return existing facade QueryError and owned String/Value collections, never engine/kernel/codec/Redb types. Public Transaction exposes the two inspection methods against its active snapshot. Use existing engine/executor behavior; no policy types or speculative conflict abstraction. Database remains construction/control, Client queries/mutations, Transaction snapshot queries. Native/in-memory supported; remote returns explicit Unsupported (no remote wire expansion or false empty conflict result). Existing optional features/dependency boundaries/no_std core preserved.

## Acceptance

- Public-only facade tests create real concurrent persisted SQL updates, inspect exact conflicting columns and concurrent values (including false/NULL/revocation/ownership), reopen native database with conflicts intact, explicitly resolve chosen fields without silently resolving unrelated ones, then reopen and reconcile complete state to convergence through the existing public state seam used by original consumer tests. Incremental-session qualification is recorded separately, never inferred from whole-state success.
- Preserve existing engine validation and atomic resolution: unknown, repeated, nonconflicted and primary-key fields/empty assignment/malformed keys/missing rows rejected; failed operation leaves valid prior state and conflicts unchanged. Resolution cannot resurrect tombstoned rows.
- Transaction-local names/values match active snapshot, see local writes, allow caller fail-closed checks; failed/closed transactions cannot return misleading clear results. No second transaction/deadlock or backend escape. Read-only snapshot errors must not cause hidden commits.
- Caller policy remains external: facade returns all values, no automatic safe/unsafe security decisions, audit changes or last-writer-wins projection. Client deadline behavior remains consistent with existing methods.
- Bounded focused host-local facade native/reopen and transaction tests pass after observed red/green. Relevant native/in-memory/no-default/remote-only feature checks and host no_std engine checks retain boundaries; targeted regressions/lints pass with exact commands recorded. Unrun/unsupported cases and timeouts remain explicit; broad sweeps are follow-up qualification, not closure gates, and historical outcomes below are not rewritten.
- Work only ofdb owner worktree and original ignored tracker/map. Preserve dirty original docs and prior owner commits. Merge current integration tip before committing/reporting; no owner-branch merge back into integration and no consumer edits.

## Evidence

Owner-local API acceptance demonstrated; tracked report `docs/storage-sync-public-conflicts.md` contains exact signatures, original consumer call-site rationale, command outcomes and limitations. Implementation delegates Client operations to the existing engine names/values/resolve functions and adds only active-engine-transaction inspection. Database remains control, Client query/mutation, Transaction snapshot. No backend/codec/Redb values escape, dependencies/features/no_std unchanged. Remote operations explicitly Unsupported, existing deadline handling reused.

Red/green (120s bounds): `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,sync,sql --test conflicts native_conflicts` first failed with12 missing Client-method errors, then passed1 after minimal delegation. Same command with filter `transaction_conflict` failed6 missing Transaction-method errors, then passed1 after active snapshot inspection.

Focused final chain completed within300s:

- `cargo check -p ofdb-sql --no-default-features`, same with `--features remote`, `--features redb`, and `--features in-memory`: all passed independently, no new remote-only warnings or sync/SQL-text requirement.
- `cargo check -p ofdb-sql-engine --no-default-features` and `cargo check -p ofdb-sql-sync --no-default-features`: passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test conflicts --test client --test facade_sync --test cluster_sync --test local_sql`: 3 conflict+5 client+7 facade sync+9 cluster+11 local SQL=35 passed; rerun120s passed, log target/storage-sync-07-focused.log.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features remote,server,redb,in-memory,sql --test remote`: 2 passed, including all five Client/Transaction conflict operations explicitly Unsupported on real remote connection with usable rollback.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features in-memory,sync,sql --test conflicts`: 1 passed without native storage or Tokio runtime.
- `cargo clippy -p ofdb-sql -p ofdb-sql-engine --all-targets --all-features`, `cargo fmt --all -- --check`, `git diff --check`: passed.

Additional300s chain: `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-engine --feature-powerset --all-targets` all12/12 passed; `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --all-features --doc` 3 compile-fail architecture tests passed; `cargo clippy --workspace --all-targets --all-features` passed with same2 unchanged KV CLI warnings. `cargo tree -p ofdb-sql --no-default-features -e normal --prefix none --features remote` names checked:85 packages, no SQL engine/automerge/redb/sync, Redb, Automerge, Iroh or tempfile.

Native tests preserve exact concurrent false/true, NULL/UUID, distinct revocation timestamps and competing ownership values across reopen. Invalid/duplicate/partial-invalid/nonconflicted/PK/empty assignments, malformed keys and missing rows/tables reject without clearing history; partial resolution leaves ownership conflict, final explicit resolution persists and whole-state reconciliation converges. Tombstone resolution rejects resurrection. Native transaction names/values match active snapshot, own newly-created catalog/row visible, rollback/aborted commit removes pending writes; aborted inspections error instead of false clear results. No domain policy/audits/history filtering implemented.

Final broad runs each tool timeout_ms1200000, sequential:

- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`:1200s timeout compiling309/581 (`default,in-memory,native-stage,redb,remote,sql` on SQL), 1–308 completed and no observed failures; NOT a full pass. Log target/storage-sync-07-powerset.log. Earlier05 full pass at its old code revision is not qualification of later API changes.
- `just crap`: completed exit0 in5m2.140s,28 coverage invocations/analyzer;199/1045 above30, same2/96-files LCOV scope mismatch. Log target/storage-sync-07-crap.log. Baseline recipe limitation/complexity risk recorded, no quality/full-workspace coverage pass claimed.

Observed gap: an initial final incremental sync after resolution/reopen failed with RemoteAbort("Incremental change hash does not match key (data change batch)") and reproduced with `RUST_BACKTRACE=1 RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,sync,sql --test conflicts native_conflicts -- --nocapture` on that test version. Check is in unchanged sql-engine-automerge::apply_change; nontransactional engine resolution/sync implementations untouched. No clean-baseline run of this new scenario was done, so no independently baseline-reproduced classification claimed. No integrity check was weakened or unrelated sync repair made. Report explicitly distinguishes whole-state convergence (passed) from incremental-session scenario (not qualified); reconstructed session-variant fixture retained at ignored target/storage-sync-07-incremental-repro.rs. This does not enable IdP/Management domain history exchange. Client API migration blocker is resolved, not a claim that all replication scenarios or full581 combinations at this revision passed.

Implementation/evidence commit and final owner HEAD: `1f0fe5aedb41fb728ce5ad58182381d6e11bc950`. Integration tip `38696d981e88bcfe20a475f49eb514f498f29b75` merged before/after commit: Already up to date; ancestry check passed. Owner worktree clean. No owner-branch merge back into integration, no of/other repo edits; original dirty docs preserved. Local API acceptance resolved with full-powerset timeout and incremental-session gap explicitly recorded, not all-green broad qualification.

## Comments

- 2026-10-07: historical incremental-session gap superseded by owner08 commit `25b92516df6ef884d9cdf723da65c64568acdbd0`. Baseline reproduced, overlapping producer deltas and hash-sorted causal delivery repaired with unchanged apply_change validation. Public native conflict fixture restored final real incremental exchange;20/20 full conflict-suite repeats and20/20 forced inversion sessions pass. Exact commands/broad timeout/coverage limits in docs/storage-sync-incremental-identity.md and08. API signatures/07 acceptance unchanged; no consumer-domain qualification or pre-fix persisted bundle migration claimed.

- Claimed by storage-sync-owners on 2026-10-07 at 38696d981e88bcfe20a475f49eb514f498f29b75 after reading actual original IdP/Management consumers and consumer blocker document. Public facade Client and Transaction are the requested test seams. Risk: never replace conflict history with current projected row; transaction inspection must use owned transaction state and preserve aborted-state checks. 01–05 stay resolved on their recorded acceptance, 06 stays downstream/unclaimed.
