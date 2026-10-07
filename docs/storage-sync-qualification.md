# Storage sync local qualification

2026-10-07. Branch `storage-sync-owners`, worktree `.worktrees/storage-sync`, base `storage-sync-integration` at `3c4bf50`. Local qualification is **partial**, not an integration approval. Original tickets/evidence are in the ignored original checkout `.scratch/storage-sync`; original dirty `docs/design.md` and `docs/goal.md` were not edited.

## Public contract delivered

- KV parity: existing `Database::open_with_table(path, "kv")`, `Database::client()` and `Client::transaction()` require no caller timestamp or backend construction. Existing unsupported/missing tables are rejected, not replaced.
- SQL optional feature `native-stage`: `FileSyncStageFactory::new(directory)`, public `FileSyncStage`, and `Database::synchronize_with_stage_directory(transport, config, role, directory)`. Memory staging and the custom factory seam remain available.
- SQL checked sync: `Database::synchronize_checked(transport, config, role, check)`, `synchronize_with_stage_factory_checked(..., factory, check)` and `synchronize_with_stage_directory_checked(..., directory, check)`.
- KV checked sync: `Database::synchronize_checked(transport, role, config, check)` and facade-reexported `SyncError`.
- Checks are async `FnMut() -> Future<Output = Result<(), String>>`; SQL requires the closure and future to be `Send`. Caller captures peer/resource context. `SyncError::PreApply(String)` distinguishes check failure from storage/transport/protocol failure. Denial and cancellation decisions are caller-owned; pending check futures can be canceled by dropping the session.
- Checks occur before every engine mutation scope / KV batch transaction, not per frame: catalog dependency replay, complete-row batching, grouped/fallback retries, recovery catalog validation and recovered rows. Already-authorized operations may finish; earlier commits survive later failure. Existing protocol and framing validation remains unchanged.
- Direct state/snapshot apply and unchecked sync document caller admission responsibility. Production consumers with changing decisions should use checked sync, not direct unchecked shortcuts.

Native stage IO is synchronous and non-suspending inside stage operations; it can block the executor and cannot be interrupted mid-syscall. Optional `tempfile` owns unique files and creation lifetime, handles close before unlink, and no caller quotas/timeouts were copied. `FileSyncStage::close` returns cleanup errors; automatic drop cleanup reports failures to stderr independently of the primary sync error. This is observable cleanup, not a crash/power-loss cleanup guarantee. SQL facade `sync` no longer automatically enables Iroh. The existing optional lower-level Iroh adapter remains for the externally coordinated extraction; no concrete networking implementation was added.

## Sequential commits

| Ticket | Commit                                        | Local disposition                                                                   |
| ------ | --------------------------------------------- | ----------------------------------------------------------------------------------- |
| 01     | `b144761`                                     | Resolved: facade parity tests, no replacement API                                   |
| 02     | `c936acc` + `be49a20`                         | Resolved: native staging and standalone core/native checks                          |
| 03     | `8dddf07` + `be49a20`                         | Resolved: checked sync and standalone core qualification                            |
| 04     | `2d391a5`                                     | Resolved: per-batch checks and focused regressions                                  |
| 05     | `dbc8e2c` + `e3620fb` + continuation evidence | Partial: composition and workspace Clippy pass; full powerset/complexity incomplete |

## Focused evidence

Commands ran in this worktree. Normal focused bounds were 120 seconds; the combined cluster qualification below used 180 seconds. SQL test commands need `RUST_MIN_STACK=16777216` in this environment.

| Command                                                                                                                                                                                                         | Observed result                                                                                                                                    |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cargo test -p ofdb-kv --features redb,in-memory,sync --test embedded_redb_client --test embedded_client --test facade_sync`                                                                                    | Ticket 01: 7 passed; cold build first timed out at 120s, then a test enum spelling was corrected before the passing run                            |
| `cargo test -p ofdb-kv --features redb --test embedded_redb_client`                                                                                                                                             | 3 passed                                                                                                                                           |
| `cargo test -p ofdb-kv --features in-memory --test embedded_client`                                                                                                                                             | 3 passed                                                                                                                                           |
| `cargo test -p ofdb-sql-sync --features native-stage --test native_stage`                                                                                                                                       | Red: feature absent; green: 4 passed                                                                                                               |
| `cargo check -p ofdb-sql --features native-stage,in-memory`                                                                                                                                                     | Passed                                                                                                                                             |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sync,in-memory,sql --test facade_sync`                                                                                                               | Red: checked method/error absent; green: 2 passed                                                                                                  |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --lib --test catalog`                                                                                                                                      | 9 unit + 17 catalog tests passed                                                                                                                   |
| `cargo test -p ofdb-kv --features sync,in-memory --test facade_sync`                                                                                                                                            | Red: checked method/error absent; green: 4 passed                                                                                                  |
| `cargo test -p ofdb-kv-sync --lib`                                                                                                                                                                              | 8 passed                                                                                                                                           |
| `cargo test -p ofdb-kv --features sync,in-memory,redb --test embedded_redb_client --test embedded_client --test facade_sync`                                                                                    | 10 passed                                                                                                                                          |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync`                                                                              | 5 facade + 9 cluster tests passed before the final disconnect test was added                                                                       |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage`                                                                                          | 9 unit + 17 catalog + 4 native tests passed                                                                                                        |
| `cargo test -p ofdb-kv --features sync,in-memory,redb,server --test facade_sync --test cluster_kv`                                                                                                              | 4 facade + 12 cluster tests passed                                                                                                                 |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,sql --test facade_sync`                                                                                                       | Final 6 passed, including staging disconnect cleanup                                                                                               |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --test catalog --test native_stage`                                                                                                | Final 17 + 4 passed                                                                                                                                |
| `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target`                                                                               | 1 passed after seeding its ignored lockfile from the workspace lock; initial independently resolved cold dependency builds timed out twice at 120s |
| `cargo check -p ofdb-sql -p ofdb-kv --no-default-features --features ofdb-sql/remote,ofdb-kv/remote`                                                                                                            | Passed                                                                                                                                             |
| `cargo clippy -p ofdb-sql -p ofdb-sql-sync -p ofdb-kv -p ofdb-kv-sync --all-targets --features ofdb-sql/native-stage,ofdb-sql/in-memory,ofdb-sql/redb,ofdb-sql/sql,ofdb-kv/in-memory,ofdb-kv/redb,ofdb-kv/sync` | Final passed without warnings after correcting newly introduced redundant conversions/collapsible conditionals                                     |
| `cargo fmt --all -- --check`, `rustfmt --edition 2024 --check test-fixtures/storage-sync-consumer/consumer.rs`, `git diff --check`                                                                              | Passed after formatting new composition tests                                                                                                      |

Coverage is transport-neutral: both-role fake checks and buffered denial, native replay/retry/recovery denial, physical truncated/invalid-length records, creation/drop/explicit cleanup failures, cancellation at stage operation boundaries, cancellation inside catalog replay transaction, malformed snapshots/rows, incomplete catalogs/recovery, frame disconnect, and offline SQL/KV divergence/convergence. Native recovery observes six check scopes and confirms recovered rows are absent when denied. The public-only consumer fixture has only facade production dependencies, persists native SQL success, denies subsequent KV apply, reopens both stores and proves no cross-store rollback. Internal lower-level cluster suites retain their existing codec/backend fixtures; they are not claimed as facade-only consumers.

## Feature graphs

The following normal dependency graphs were checked programmatically by package name:

- `cargo tree -p ofdb-sql --no-default-features -e normal --prefix none --features remote`: 85 packages; no SQL engine, Redb, Automerge, SQL sync, Iroh or native stage dependency.
- `cargo tree -p ofdb-kv --no-default-features -e normal --prefix none --features remote`: 79 packages; no KV store, Redb, Automerge, KV sync or Iroh.
- `cargo tree -p ofdb-sql-sync --no-default-features -e normal --prefix none`: 47 packages; no Tokio, Redb, Iroh or tempfile. The graph was rechecked during continuation; the separate no_std compile check now also passes.

## Initial qualification failures (historical)

- Initial `cargo check -p ofdb-sql-sync --no-default-features` failed with 17 errors beginning with missing `alloc::vec::Vec` in engine `src/codec.rs:70`; isolated native-only Clippy failed at the same boundary. During continuation the user explicitly brought the import fix into scope. Commit `be49a20` adds one alloc import and resolves all 17 errors without forcing std or refactoring the engine.
- Without the stack environment above, the initial SQL native facade test aborts with stack overflow. With 16 MiB test-thread stack it passes. No default-stack success is claimed.
- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`: **180s timeout**, reached combination 10/581 (wasm btree-automerge), not a full powerset pass.
- `just crap`: **180s timeout** during cargo-hack llvm-cov compilation, not a CRAP result. The recipe's final cargo-crap step was not reached; availability of that tool is not inferred.
- `cargo clippy --workspace --all-targets --all-features`: **180s timeout**, reached optional Iroh compilation; targeted lint success does not replace the full workspace gate.
- No real Iroh authentication, domain authorization, consumer migration, optional lower-level transport extraction or external of/offs acceptance is claimed. Ticket 06 remains downstream and unclaimed.

## Continuation qualification

Tickets **02 and 03 are resolved** after their independent core/native gates passed. Ticket **05 remains unresolved**: complete workspace feature powerset and workspace complexity qualification have not completed. This section supersedes initial blockers above without treating historical timeouts as passes.

### Focused fixes

- `be49a20d35f7d4a7a93ba174405433ab01bc3ddd`: one `use alloc::vec::Vec;` in SQL engine codec. Exact red check reproduced all 17 errors before the change; the same check passed after it.
- `e3620fb5b622fdbf136ae4d9c176b2bd1ed5f50e`: fixes an in-repo caller missed by the earlier facade change. SQL server CLI now explicitly selects the already-existing optional SQL-sync Iroh adapter in its `p2p` feature and still uses `ofdb_sql::Database` for sync/storage. No concrete networking implementation or facade Iroh reexport was added. All-feature Clippy initially found four missing `ofdb_sql::IrohTransport` references; this caller update fixes them. No other workspace was changed.
- Integration was synced before both commits and again before recording results: local tip `3c4bf50`, already up to date. Final sync/ancestry is also recorded in the original tracker.

### Re-run evidence

Focused regression chain bounded at 300s; each command succeeded:

| Command                                                                                                                            | Observed result                                                                                                       |
| ---------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `cargo check -p ofdb-sql-sync --no-default-features`                                                                               | Passed after reproduced red failure                                                                                   |
| `cargo check -p ofdb-sql-sync --no-default-features --features native-stage`                                                       | Passed independently without embedded/std feature unification                                                         |
| `cargo clippy -p ofdb-sql-sync --features native-stage --lib`                                                                      | Passed without warnings                                                                                               |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --lib --test catalog`                                                         | 9 unit + 17 catalog tests passed                                                                                      |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage`             | 9 + 17 + 4 passed                                                                                                     |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sync,in-memory,sql --test facade_sync`                                  | 2 passed                                                                                                              |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync` | 6 + 9 passed                                                                                                          |
| `cargo clippy -p ofdb-sql-server-cli --all-targets --all-features`                                                                 | Passed after caller update (120s bound)                                                                               |
| `cargo check -p ofdb-sql-server-cli --no-default-features`                                                                         | Passed (same 120s chain)                                                                                              |
| `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target`  | 1 persisted separate-scope test passed (120s bound)                                                                   |
| `cargo check -p ofdb-sql -p ofdb-kv --no-default-features --features ofdb-sql/remote,ofdb-kv/remote`                               | Passed; normal dependency graphs rechecked with the same forbidden-package assertions, 85/79/47 packages respectively |
| `cargo fmt --all -- --check`, fixture `rustfmt --check`, `git diff --check`                                                        | Passed                                                                                                                |

Final broad runs, each bounded at 300s:

- `cargo clippy --workspace --all-targets --all-features`: **completed successfully** after caller update. Two existing KV server CLI `useless_conversion` warnings remain at `p2p.rs:268` and `:332`; not a warning-free pass and no `-D warnings` result is claimed.
- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`: **timed out at combination 59/581** (`default,in-memory,server` on KV facade). No full powerset pass; no test failure observed before the timeout.
- `just crap`: **timed out during coverage compilation** in existing CLI/network dependencies. No full workspace complexity report; not a pass.

Smaller package-scoped qualification:

- `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-sync --feature-powerset --all-targets`: **all 4 combinations completed successfully**, including optional native/transport combinations (300s bound).
- `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-engine --feature-powerset --all-targets`: **all 12 combinations completed successfully**. Chained `cargo hack test -p ofdb-kv-sync --feature-powerset --all-targets`: **1/1 completed**, 8 unit + 1 restart test passed (combined 300s bound).
- `RUST_MIN_STACK=16777216 cargo llvm-cov -p ofdb-sql-sync --features native-stage --lcov --output-path target/storage-sync-sql-sync.lcov` followed by `cargo crap -p ofdb-sql-sync --lcov target/storage-sync-sql-sync.lcov --summary`: both commands completed within 300s, coverage tests 9 + 17 + 4 passed. Scoped report: **85 functions, 5 above threshold 30, worst `validate_catalog` CRAP 163.3**. Analyzer warned of a source/LCOV scope mismatch: optional `iroh_transport.rs` had no coverage entry, while trait-only `codec.rs` had no matched analyzed source. This is a scoped risk report, not a threshold-quality pass or substitute for full workspace complexity qualification. No broad complexity refactor was made.

The default-stack limitation remains recorded from the initial run; subsequent SQL tests used the stated 16 MiB stack, not a claimed default-stack pass. Full powerset and full workspace complexity remain ticket 05 gaps. The optional existing Iroh adapter still belongs to the external coordinated extraction; updating an existing in-repo caller does not claim extraction or real domain-auth acceptance.

## Merger P1 correction: local failure must not wait for Abort

The earlier best-effort Abort implementation awaited peer progress. That invalidated the prompt local-failure/stage-release claim: a pending send could retain native stages indefinitely. Tickets 03/04 were reopened and claimed at `a6e5f6a`; this section supersedes that behavior and its earlier acceptance evidence.

Fix commits: KV `f506052dceb1ee0e20f984364de158f912a06a15`; SQL `378f87e285b822e2550ddfcaa89d311ce10d98f0`.

KV now polls the existing Abort send once using core polling; SQL uses the existing futures dependency's `now_or_never()` for all error-path Abort notifications, including catalog/data replay, fallback retry, recovery validation/apply, engine failure and session accounting. Pending notifications are dropped, preserving the primary local error and allowing owned stages to drop before local completion. No timers, tasks, dependencies, policy or wire changes were added. Checked facade documentation now explains the non-waiting Abort attempt; KV no longer incorrectly labels checked sync unchecked.

**Caller obligation:** Abort delivery is not guaranteed. A canceled send may have partially written a frame; discard/close the failed session transport, and retry using a fresh transport. Local completion does not imply the remote peer has observed failure. Existing stage cleanup IO/failure limitations above still apply.

### Deterministic red → green evidence

Each command was bounded at 120s, not by test timers:

- `cargo test -p ofdb-kv --features sync,in-memory --test facade_sync pending_abort`: red assertion `local failure must not await Abort send` (one pending send poll); after the minimal KV fix, 1 test passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,sql --test facade_sync pending_abort`: same red assertion; after fixing shared replay abort, 1 test passed.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --test catalog pending_abort`: test-driver assumption first failed because a recovery sender can complete before the receiver; driver was corrected to stop polling completed senders. Then the intended red assertion reproduced awaited recovery Abort. After the recovery/error-notification fix, 1 test passed.

Regressions cover both roles, explicit caller denial and cancellation decisions, first/later KV batches, SQL catalog/data checks and checks 3–6 (grouped changes, fallback retry, recovery validation/data). The SQL tests assert native directories empty **while the completed receiver future is still held**, without closing the peer transport. Denied operations are absent; prior KV batch or SQL catalog/three data rows remain committed. Fresh-transport retry converges. Existing pending-hook/session-drop cancellation tests also reran successfully.

### Post-fix focused qualification

One `&&`-chained run bounded at 300s completed every command below successfully:

| Exact command                                                                                                                      | Result                                            |
| ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `cargo check -p ofdb-sql-sync --no-default-features`                                                                               | Passed                                            |
| `cargo check -p ofdb-sql-sync --no-default-features --features native-stage`                                                       | Passed                                            |
| `cargo test -p ofdb-kv --features sync,in-memory,redb --test facade_sync --test embedded_client --test embedded_redb_client`       | 5 facade + 3 memory + 3 Redb passed               |
| `cargo test -p ofdb-kv-sync --all-targets`                                                                                         | 8 unit + 1 restart passed                         |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sync,in-memory,sql --test facade_sync`                                  | 2 memory facade passed                            |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync` | 7 native facade + 9 cluster passed                |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --lib --test catalog`                                                         | 9 unit + 18 memory catalog passed                 |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage`             | 9 unit + 18 native catalog + 4 stage passed       |
| `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target`  | 1 public-only persisted SQL/KV composition passed |
| `cargo fmt --all -- --check` and `git diff --check`                                                                                | Passed                                            |

Additional bounded runs:

- `cargo clippy --workspace --all-targets --all-features` (300s): passed, with the same two unchanged KV CLI useless-conversion warnings at `p2p.rs:268/:332`.
- `cargo check -p ofdb-kv-sync --no-default-features` followed by `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-sync -p ofdb-kv-sync --feature-powerset --all-targets` (300s chain): check passed; all 5 combinations passed (SQL-sync 4, KV-sync 1).

03/04 owner-local acceptance is restored by these regressions and existing focused coverage. Native stage implementation/lifecycle in 02 needed no change; composed failure ownership was fixed in SQL session code. 05 remains incomplete: the earlier full workspace powerset/CRAP timeouts and scoped complexity risks remain gaps, not passes; full powerset/CRAP were not rerun for this P1 fix. The default-stack limitation remains. Integration tip is inspected read-only; no integration merge is performed under the latest instruction. Prior owner commits are preserved, and no other repository or original dirty design/goal document is changed.
