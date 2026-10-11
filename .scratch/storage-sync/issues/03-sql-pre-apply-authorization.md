# 03 — Check generic authorization before every SQL apply

Status: ready-for-agent
Type: task
Work status: resolved
Owner: ofdb
Blocked by: None
Priority: High
Complexity / risk: High / high

## Machine-local validation scope

Apply the [common Ubuntu Linux x86_64 policy](../spec.md#machine-local-validation-scope). Use bounded focused SQL facade/catalog/recovery tests and host core/native feature checks. Isolated same-host peers cover both roles, every complete catalog/row/replay/retry/recovery apply, changing decisions, malformed payload rejection, denied-operation rollback, prior progress and pending-Abort/cancellation stage cleanup. Fake hooks prove check placement only; real authenticated peer/resource and domain decisions remain of-owned evidence, not mock qualification. This scope note preserves resolved status and historical evidence; no tests run in this docs-only checkpoint.

## Scope and deliverable

Thread the smallest suitable generic caller check through public SQL sync, atomic apply, staged replay, recovery and retries in `crates/sql-sync/src/session.rs` and `crates/sql/src/database.rs`. Prefer an existing seam or function/closure; no mandated new trait or shared policy framework. No IdP/Management types or policy model.

## Acceptance

- Check before each complete catalog dependency batch, complete-row data batch, replay, recovery and retry apply, in both roles. Session/receive-only checks are insufficient.
- Denial/hook error prevents that operation from mutating, stops later applies, returns an actionable error and releases stages; earlier valid commits remain.
- Already-authorized operations may finish; no mid-transaction/commit-time recheck.
- Preserve atomic catalog/schema/CRDT rows/tombstones/index scope. Do not split complete rows/catalog batches at frame/hook boundaries.
- Transport-neutral fake-hook tests prove allow/deny/error, changing decision between operations, and change during authorized work, with persisted-state/invocation-order assertions; no Iroh dependence.
- Preserve invalid-payload/catalog rejection and recovery completeness; authorization cannot make invalid data valid.
- Document direct public state-apply responsibility; production code cannot bypass authorization using unchecked shortcuts.
- Memory-stage tests verify independently of 02; native composition is 05. Focused sync/facade and no-native core checks pass; record commands/results.

## Exclusions and integration

No domain policy trait mandate, freshness/cache/revocation protocol, quotas, timing policy or global security consistency promise. [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) consumes the public hook; [of 03 — execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) supplies real domain trust/authorization decisions and real Iroh authentication/peer/resource integration tests in of through protocol transport adapters built atop ofnet. ofnet/iroh-chain owns reusable mesh endpoint/allowlist/routing/pairing networking; endpoint allowlists admit connections, not resource operations. [offs 03 — Iroh extraction/offline reconnect](../../../../offs/.scratch/filesystem-sync/issues/03-iroh-extraction-offline-reconnect.md) co-delivers transport extraction with of 02, not a local SQL prerequisite.

## Evidence

Implementation commit: 8dddf07. Public Database::synchronize_checked, synchronize_with_stage_factory_checked, synchronize_with_stage_directory_checked accept async FnMut() -> Result<(), String> (Send future). Caller context stays captured outside storage. SyncError::PreApply distinguishes admission failure without message parsing; error string is caller detail. Each mutate_tables scope is checked once before entering; same callback is threaded through grouped data, catalog replay, per-row fallback and recovery. No mid-transaction recheck. Direct unchecked apply/sync responsibility documented.

- Red: `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sync,in-memory,sql --test facade_sync` failed on missing checked API/error variant (and a test QueryResult spelling corrected).
- Green: same command: 2 tests passed, both roles allow/deny and catalog-before-data progress; decision changed from inside catalog replay still permits catalog commit but denies data.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --test catalog checks_retries_and_recovery -- --nocapture`: 1 passed, both roles, six observed scopes (catalog, data, failed grouped change, fallback retry, recovery catalog validation, recovery data), denial at retry/recovery preserves previous catalog/data, recovery row absent until authorized.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --lib --test catalog`: 9 unit + 17 catalog tests passed, including invalid catalog, incomplete definitions, whole-batch row rollback, frame interruption and recovery completeness.
- `cargo check -p ofdb-sql-sync --no-default-features`: fails in pre-existing sql-engine no_std missing alloc imports (17 errors); not fixed as unrelated.

05 native composition now passes: final SQL facade suite 6 tests, native catalog suite 17 + native stage suite 4. Public-only persisted consumer proves SQL success survives KV denial. Exact commands/results are in 05 and docs/storage-sync-qualification.md on the worktree branch.

Previous blocker (superseded): no-native core check failed in sql-engine. Full workspace qualification remains ticket 05; no claim that earlier timeouts passed.

### Continuation: core qualification resolved

Fix commit: be49a20d35f7d4a7a93ba174405433ab01bc3ddd. One explicit alloc::vec::Vec import fixes the 17 reproduced no_std errors without std-enabling the core or changing sync behavior.

Exact red `cargo check -p ofdb-sql-sync --no-default-features` reproduced the failure; same command after import passed. Also passed `cargo check -p ofdb-sql-sync --no-default-features --features native-stage` and `cargo clippy -p ofdb-sql-sync --features native-stage --lib` without warnings.

Focused regressions rerun together with 300s bound, every command succeeded:

- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --lib --test catalog`: 9 + 17 passed (memory core, both-role retry/recovery checks).
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage`: 9 + 17 + 4 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sync,in-memory,sql --test facade_sync`: 2 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync`: 6 + 9 passed.

All ticket 03 owner-local acceptance is evidenced by prior hook/invocation/state assertions and independently passing no-native core check. Resolved. Default-stack limitation and broad workspace validation belong to recorded 05 qualification; no unrelated changes made.

### Merger P1 correction / requalification

Fix commit: `378f87e285b822e2550ddfcaa89d311ce10d98f0`. Reopened and claimed at a6e5f6a because awaited best-effort Abort prevented local denial from returning and retained native stages. SQL error-path Abort notifications now poll once using existing FutureExt::now_or_never, including replay, fallback/recovery and structural/engine/accounting failure paths. Pending notifications are dropped; primary PreApply detail remains unchanged. No timers/dependencies/policy/wire changes. Public checked facade docs require discarding failed transport because canceled sends may partially write frames; peer notification is not guaranteed.

Deterministic red/green commands (120s each):

- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,sql --test facade_sync pending_abort`: failed with `local failure must not await Abort send`, then passed 1 test after shared abort fix.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --test catalog pending_abort`: first corrected driver assumption that sender must stay pending (recovery sender may complete); then failed on intended blocked-Abort assertion, then passed 1 test after recovery fix.

Both roles and caller denial/cancellation decisions are covered at catalog/data and checks 3–6 (grouped change, fallback retry, recovery validation/data). Native stage directories are asserted empty before dropping completed receiver future or peer; previously committed catalog/three data rows remain, denied recovered row is absent, and fresh-session retry converges. Existing pending-check/session-drop cancellation regressions pass too.

Exact requalification commands in a 300s && chain all passed:

- `cargo check -p ofdb-sql-sync --no-default-features`
- `cargo check -p ofdb-sql-sync --no-default-features --features native-stage`
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sync,in-memory,sql --test facade_sync`: 2 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync`: 7 + 9 passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --lib --test catalog`: 9 + 18 passed independently without native stage.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage`: 9 + 18 + 4 passed.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target`: 1 passed.

Additional 300s bounds: `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-sync -p ofdb-kv-sync --feature-powerset --all-targets`: all 5 combinations passed (SQL 4/KV 1), chained after passing `cargo check -p ofdb-kv-sync --no-default-features`. `cargo clippy --workspace --all-targets --all-features` passed with two unchanged KV CLI warnings. `cargo fmt --all -- --check` and `git diff --check` passed. Owner-local acceptance restored and ticket resolved. 05 full workspace powerset/CRAP gaps and historical default-stack limitation remain; no integration merge performed for this fix.

Evidence report commit/final owner HEAD: `1cfdfc0ff5a99497323661e04a48cda702ada2e2`. Worktree clean, a6e5f6a preserved as ancestor. Inspected integration tip 3c4bf50 already an ancestor; no merge performed.

## Comments

- Reopened/claimed for merger P1 at a6e5f6a on 2026-10-07: local PreApply failure awaits remote Abort indefinitely and retains native stages. Minimal fix must make local return/cleanup independent of peer progress, without timers; public facade and core recovery seams will get pending-Abort regressions.

- Continuation claimed on 2026-10-07: minimal missing alloc imports now explicitly in scope; exact no-default SQL sync check and existing public facade/core regressions will be rerun before resolving this ticket.

- Claimed by storage-sync-owners on 2026-10-07 after 02 implementation c936acc. High complexity/risk: hooks must be at mutate_tables boundaries, including fallback per-row retries and recovery, not receipt boundaries. Public facade checked sync with async FnMut closure; no domain context in storage.

- Authorization covers one atomic operation, neither an entire session nor one frame.

- Historical planning audit (superseded by resolved acceptance above): canonical triage is ready-for-agent; work was open, with blockers recorded separately and no implementation yet claimed.
