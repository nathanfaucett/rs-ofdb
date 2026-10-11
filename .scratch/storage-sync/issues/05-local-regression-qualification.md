# 05 — Qualify integrated sync integrity and feature boundaries

Status: ready-for-agent
Type: task
Work status: resolved
Owner: ofdb
Blocked by: None (baseline CRAP coverage aggregation limitation recorded, not a local implementation blocker)
Priority: High
Complexity / risk: Medium / high

## Machine-local validation scope

Apply the [common Ubuntu Linux x86_64 policy](../spec.md#machine-local-validation-scope). Use bounded focused SQL catalog/native-stage/facade/cluster and KV facade/cluster tests plus the public-only persisted consumer fixture, with only relevant supported feature combinations. Isolated same-host stores/processes cover offline divergence, recovery/retry, both roles, invalid payload/replay, denial/disconnect/cancellation, complete-operation rollback, prior progress and separate SQL/KV scopes. Host no-default/no_std, remote-only dependency boundaries and targeted formatting/Clippy remain feasible; blanket sweeps are follow-up qualification, not closure gates. This scope note preserves resolved status and all historical broad results/limits; no tests run in this docs-only checkpoint.

## Scope and deliverable

Additional composition coverage for facade parity, native stages and checks. Extend existing SQL catalog/cluster and KV sync/facade suites. Record local qualification evidence; external of/offs implementation is not a prerequisite.

## Acceptance

- Transport-neutral fake hooks exercise native catalog/data replay, missing-dependency recovery/retry, both roles and denial between atomic operations, without domain or Iroh dependencies.
- Invalid/truncated replay, invalid catalogs, missing advertised recovery, malformed rows/snapshots, disconnect, cancellation and denial preserve current-operation atomicity and cleanup.
- Complete rows/catalog dependency batches are never partially visible. Earlier commits may survive later failure; retry recovers/converges without premature success.
- Nodes write locally while disconnected and reconcile divergent SQL/KV CRDT state, retaining existing conflict/deletion/catalog/generation semantics, without a global leader or synchronized authorization promise.
- SQL-then-KV failure respects separate scopes, not cross-store/session rollback.
- Deterministic cancellation/fault injection adds no production timeout/byte-budget policy.
- Verify no_std/alloc core lacks native/runtime dependencies, remote-only facade graphs lack local engine/Redb/Automerge/sync, and optional native/public-only consumer builds work.
- Record bounded focused host-local test/check results for the relevant suites and supported features, with targeted formatting and package/suite Clippy. No full Cargo Hack, CRAP or workspace/all-target/all-feature sweep is a current closure gate or authorized run. Explicitly distinguish missing tooling, unrun or unsupported cases, timeouts and failures; never claim unobserved passes. Historical broad qualification below remains exact revision-specific evidence.

## Exclusions and integration

No real auth tests in ofdb, broad doc rewrites or unrelated fixes. [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) consumes tickets 01–05; [of 03 — execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) owns real Iroh authentication and domain authorization evidence in of through protocol transport adapters built atop ofnet. ofnet/iroh-chain's reusable mesh endpoint/allowlist/routing/pairing networking governs connection admission, not resource permission. [offs 01](../../../../offs/.scratch/filesystem-sync/issues/01-remote-operation-admission.md) and [offs 02](../../../../offs/.scratch/filesystem-sync/issues/02-staged-content-write-integrity.md) are additional of 02 prerequisites, not local qualification blockers. of 02 and [offs 03 — Iroh extraction/offline reconnect](../../../../offs/.scratch/filesystem-sync/issues/03-iroh-extraction-offline-reconnect.md) co-deliver extraction; local 06 must not gate them.

## Evidence

Qualification commit: dbc8e2ccf4f637f01322d949975f91de2fc35275. See worktree branch `docs/storage-sync-qualification.md` for full public API, commands, observed results, feature-graph checks, and failures. At that checkpoint this ticket was **not resolved**; final qualification below supersedes that disposition.

Final `GIT_EDITOR=true git merge --no-edit storage-sync-integration` on storage-sync-owners: already up to date. Integration tip 3c4bf500e6eb4dfeac24d0a5c8907c149ec89c1e is an ancestor (`git merge-base --is-ancestor storage-sync-integration HEAD` passed). Worktree clean. No merge of owner branch into integration, no cross-repo changes; original dirty design/goal docs preserved. Git commit emitted existing gc/unreachable-object housekeeping warnings; no pruning performed.

Focused/local composition commands (120s per command unless combined cluster run noted):

- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync`: 5 facade + 9 cluster passed (180s combined run). Final added disconnect test: `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,sql --test facade_sync`: 6 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage`: 9 + 17 + 4 passed (180s combined cluster run); final `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --test catalog --test native_stage`: 17 + 4 passed.
- `cargo test -p ofdb-kv --features sync,in-memory,redb,server --test facade_sync --test cluster_kv`: 4 + 12 passed (180s combined cluster run).
- `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target`: 1 passed after seeding ignored fixture lock from workspace lock. Earlier independent cold resolutions hit 120s twice; no unobserved passes claimed.
- Native tests: both roles, denial/cancellation before catalog/data, exact check counts, prior catalog progress, retry convergence and empty staging directories; injected invalid replay and cancellation mid-catalog transaction restore pre-session catalog; disconnect after first staged state frame commits no partial catalog. Native recovery test sees six scopes and denies fallback/recovery without introducing recovered row.
- Offline SQL cluster and KV CRDT cluster suites retain conflict/deletion/catalog/generation behavior; no leader or globally synchronized authorization. Facade-only separate consumer has only public facade production deps, native SQL + named kv Redb, transactions without timestamps; SQL persists on reopen after KV denial.
- `cargo check -p ofdb-sql -p ofdb-kv --no-default-features --features ofdb-sql/remote,ofdb-kv/remote`: passed (60s).
- `cargo tree -p ofdb-sql --no-default-features -e normal --prefix none --features remote`: 85 packages, no engine/Redb/Automerge/sync/Iroh/tempfile. KV equivalent: 79 packages, no store/Redb/Automerge/sync/Iroh. `cargo tree -p ofdb-sql-sync --no-default-features -e normal --prefix none`: 47 packages, no Tokio/Redb/Iroh/tempfile. Graph checked programmatically, not a no_std compile claim.

Final requested broad commands:

- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`: 180s timeout at combination 10/581; no full pass.
- `just crap`: 180s timeout during cargo-hack llvm-cov compilation; no complexity report, cargo-crap step not reached.
- `cargo fmt --all -- --check`: passed after formatting new tests. Separate fixture rustfmt and git diff --check passed.
- `cargo clippy --workspace --all-targets --all-features`: 180s timeout at optional Iroh compile; no full pass.
- Targeted `cargo clippy -p ofdb-sql -p ofdb-sql-sync -p ofdb-kv -p ofdb-kv-sync --all-targets --features ofdb-sql/native-stage,ofdb-sql/in-memory,ofdb-sql/redb,ofdb-sql/sql,ofdb-kv/in-memory,ofdb-kv/redb,ofdb-kv/sync`: final passed without warnings after fixing introduced lint findings (120s).
- `cargo check -p ofdb-sql-sync --no-default-features`: fails in unchanged sql-engine, 17 errors starting missing alloc::vec::Vec at codec.rs:70. Isolated native-only lib Clippy also fails at that existing engine boundary. No engine files changed; no forced std workaround. Default SQL test stack overflowed; 16 MiB test-thread stack passes. Core/no_std and full broad qualification are unresolved, not external of/offs blockers.

### Continuation qualification (300s bounds)

Fix commits: be49a20 (one alloc Vec import, exact no_std check now passes) and e3620fb (existing SQL CLI transport caller missed by earlier facade change). The latter explicitly selects the existing optional lower-level Iroh adapter in CLI p2p; storage/sync execution still uses Database facade. No new concrete transport implementation, facade auto-Iroh feature or cross-workspace edits.

Observed final broad results:

- `cargo clippy --workspace --all-targets --all-features`: initial continuation run failed on four removed `ofdb_sql::IrohTransport` references in SQL CLI; after e3620fb **passed** within 300s. Two existing KV CLI useless_conversion warnings remain at p2p.rs:268/:332; no warning-free/-D warnings pass claimed.
- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`: 300s **timeout at combination 59/581**, KV facade default,in-memory,server. Not a full pass; no test failure observed before timeout.
- `just crap`: 300s **timeout during coverage compilation** in existing CLI/network deps. Full complexity report not produced; not a pass.
- `cargo fmt --all -- --check`, fixture rustfmt check and `git diff --check`: passed.

Additional package-scoped runs:

- `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-sync --feature-powerset --all-targets`: all **4/4** combinations passed (300s).
- `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-engine --feature-powerset --all-targets`: all **12/12** passed; chained `cargo hack test -p ofdb-kv-sync --feature-powerset --all-targets`: **1/1** passed, 8 unit + 1 restart tests (combined 300s).
- `RUST_MIN_STACK=16777216 cargo llvm-cov -p ofdb-sql-sync --features native-stage --lcov --output-path target/storage-sync-sql-sync.lcov` and then `cargo crap -p ofdb-sql-sync --lcov target/storage-sync-sql-sync.lcov --summary`: completed within 300s. Coverage tests 9 + 17 + 4 passed. Scoped CRAP: **85 functions, 5 above threshold 30, worst validate_catalog 163.3**. Source/LCOV mismatch warning: optional iroh_transport.rs missing coverage, trait-only codec.rs unmatched analyzed source. Report is not a quality-threshold pass or full workspace report; no broad refactor performed.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target`: 1 persisted separate SQL/KV scope test passed (120s).
- `cargo check -p ofdb-sql -p ofdb-kv --no-default-features --features ofdb-sql/remote,ofdb-kv/remote`: passed. Normal graph assertions rerun: SQL remote 85 packages, KV remote 79, core 47; same forbidden local/native/runtime package sets absent.
- Exact independent no_std/native checks and memory/native focused regressions passed as recorded in resolved 02/03 continuation. No new policy/budget features.

05 remains **in-progress**, not resolved: full workspace powerset and complexity incomplete. All owner-local 01–04 are now resolved. Default SQL stack limitation remains historical; successful reruns use explicit 16 MiB stack. Evidence update commit: a6e5f6a3cf58e5868df37aa158217209cead68cd. Integration synced before every continuation commit and again before reporting: `GIT_EDITOR=true git merge --no-edit storage-sync-integration` was already up to date; tip 3c4bf500e6eb4dfeac24d0a5c8907c149ec89c1e is an ancestor (`git merge-base --is-ancestor storage-sync-integration HEAD` passed). Owner worktree clean at a6e5f6a. No owner branch merge into integration, no other workspace edits; original dirty design/goal docs preserved.

### Merger P1 follow-up (supersedes prompt-failure evidence)

At a6e5f6a, merger found that local denial awaited remote Abort indefinitely, retaining native stages. 03/04 reopened and claimed, fixed separately in KV `f506052dceb1ee0e20f984364de158f912a06a15` and SQL `378f87e285b822e2550ddfcaa89d311ce10d98f0`, now requalified/resolved. 02 lifecycle implementation did not need change; failing ownership was SQL session's awaited notification. Pending-Abort regressions prove local failure and native cleanup before completed future drop/peer closure, prior commits remain, denied batch/recovered row absent, both roles/caller cancellation decisions, fresh transport retry convergence. No timers or production policies. Delivery is best effort, transport must be discarded after failure because a canceled frame send may partially write.

Exact red/green and complete command table are in worktree `docs/storage-sync-qualification.md` Merger P1 section and 03/04 above. Post-fix && chain bounded at 300s completed all commands: independent no-default SQL core/native checks; KV facade/memory/Redb 5+3+3; KV-sync 8+1; memory SQL facade 2; native SQL facade/cluster 7+9; memory SQL core/catalog 9+18; native SQL core/catalog/stage 9+18+4; public persisted consumer 1; format and diff checks. Commands are unchanged from prior qualification except new regression test counts.

- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync`: 7+9 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage`: 9+18+4 passed.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target`: 1 passed.
- Additional 300s chain: `cargo check -p ofdb-kv-sync --no-default-features` and `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-sync -p ofdb-kv-sync --feature-powerset --all-targets`: check and all 5 package combinations passed (SQL 4/KV 1).
- `cargo clippy --workspace --all-targets --all-features`: completed within 300s; same two unchanged KV CLI useless-conversion warnings.

05 remains in-progress: prior full workspace powerset/CRAP timeouts and scoped complexity risk are not passes. Full powerset/CRAP not rerun for P1; historical SQL default-stack limitation remains. Latest instruction prohibits integration merging; tip inspected read-only, prior commits preserved. No other repositories or dirty original design/goal docs changed.

P1 evidence report commit/final owner HEAD: `1cfdfc0ff5a99497323661e04a48cda702ada2e2`. Worktree clean. Read-only ancestry checks passed for a6e5f6a and integration tip `3c4bf500e6eb4dfeac24d0a5c8907c149ec89c1e`; no merge performed. 05 remains in-progress.

### Final broad qualification / resolution

Continuation claimed on 2026-10-07 at owner/integration `1cfdfc0ff5a99497323661e04a48cda702ada2e2` to complete required broad commands with tool `timeout_ms: 1200000`. No code changes or introduced failures; all builds sequential. Full detailed outcomes and acceptance matrix are in worktree `docs/storage-sync-qualification.md` Final ticket 05 broad qualification section.

Exact required broad runs:

- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`: initial 1200s timeout compiling 240/581; 1–239 completed, no observed failures. NOT a pass. After completing sequential partitions, **same exact unpartitioned command completed exit 0 in 4m34.079s, all 581/581 combinations passed**. Log `target/storage-sync-final-powerset-complete.log` checked to contain indices 1–581 in order.
- `just crap`: **completed exit 0 in 7m47.219s**, all 28 coverage invocations and analyzer completed. Recipe itself sets RUST_MIN_STACK=67108864. Report 1,033 functions, 199 above threshold 30, worst validate_catalog 3782.0. Analyzer warns only 2/96 files match LCOV (94 missing); output LCOV contains only final SQL server CLI main.rs/p2p.rs. Identical shared-output-path recipe verified at base 3c4bf50. Non-ticket baseline aggregation limitation; not a meaningful whole-workspace coverage/quality pass. No enforced failing gate: exit 0 despite high scores. Measured high complexity is recorded risk, not automatic acceptance failure. No unrelated recipe/refactor changes.
- `cargo crap --workspace --lcov /tmp/lcov.info --summary`: completed within 120s, confirmed counts/warning; SQL-sync 85 functions/20 above threshold in this pessimistic broad report.
- Final `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features`, `git diff --check`: all completed in 300s chain; same two unchanged KV CLI useless-conversion warnings p2p.rs:268/:332. Not a -D warnings pass.

Sequential powerset continuation commands (each 1200s bound; no competing coverage builds): `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets --partition 3/5` first timed out at330, retry completed 117/117 global235–351 in8m31.512s; same command `--partition 4/5` completed117/117 global352–468 in19m18.696s; `--partition 5/5` first timed out at560, retry completed113/113 global469–581 in6m4.192s. Combined successful initial runs/partitions cover all581 without gaps; final exact unpartitioned completion independently confirms the full pass. Earlier timeouts remain failures to complete, never pass evidence. All output logs retained in owner target/storage-sync-final-*.log and named in tracked report.

Local acceptance is demonstrated by already-recorded native catalog/data/retry/recovery both-role tests, invalid/truncated/malformed payload rejection, prompt native cleanup on pending Abort, drop cancellation, whole-operation rollback/prior commits/fresh retry, SQL/KV offline cluster reconciliation, persisted public-only separate-store composition, and independent core/native/remote feature boundaries. All581 feature combinations passed. Criterion24 required commands actually completed; baseline report mismatch, warnings, intentional ignored tests and historical default-stack limitation are explicit. **Resolved for owner-local implementation/qualification acceptance**, not all-green complexity/coverage, concrete transport extraction, domain auth or external consumer acceptance. 06 stays downstream/unclaimed.

Latest user request now authorizes integration-tip merge. `GIT_EDITOR=true git merge --no-edit storage-sync-integration` returned Already up to date at1cfdfc0 before report commit. No merge of owners into integration; no other repo edits or dirty original design/goal edits.

Final evidence commit/owner HEAD: `38696d981e88bcfe20a475f49eb514f498f29b75` (`docs(sync): complete ticket 05 broad local qualification`). Worktree clean. Integration tip `1cfdfc0ff5a99497323661e04a48cda702ada2e2` merged before and after report commit: Already up to date; ancestry check passed. 01–05 resolved locally; 06 unclaimed. No implementation/other-repository changes, original dirty docs preserved.

## Comments

- Continuation on 2026-10-07: user authorized 300s broad qualification or smaller per-target runs. SQL core blocker fixed minimally in be49a20; exact no-default and native-only checks plus memory/native regressions passed. Tickets 02/03 resolved on their own acceptance; full ticket 05 remains in-progress pending observed broad results. Integration synced before fix commit; no other workspaces touched.

- Claimed by storage-sync-owners on 2026-10-07 after sequential implementations 01 b144761, 02 c936acc, 03 8dddf07, 04 2d391a5. Performing requested local qualification despite unresolved core validation in 02/03; do not infer those gates are resolved. Composition risk high: native denial/cancellation/replay error and cross-store separate scopes. Commands bounded (focused 120s; final broad commands 180s or lower), no cross-repo edits.

- Blocked on 01–04; each implementation ticket still supplies its own focused tests. No Rust implementation/validation occurred during documentation planning.

- Historical planning audit (superseded by resolved acceptance above): canonical triage is ready-for-agent; work was open, with blockers recorded separately and no implementation yet claimed.
