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

| Ticket | Commit | Local disposition |
| --- | --- | --- |
| 01 | `b144761` | Resolved: facade parity tests, no replacement API |
| 02 | `c936acc` | Implemented; no-native compilation gate blocked |
| 03 | `8dddf07` | Implemented; no-native compilation gate blocked |
| 04 | `2d391a5` | Resolved: per-batch checks and focused regressions |
| 05 | qualification commit containing this file | Partial: composition passes, full gates incomplete |

## Focused evidence

Commands ran in this worktree. Normal focused bounds were 120 seconds; the combined cluster qualification below used 180 seconds. SQL test commands need `RUST_MIN_STACK=16777216` in this environment.

| Command | Observed result |
| --- | --- |
| `cargo test -p ofdb-kv --features redb,in-memory,sync --test embedded_redb_client --test embedded_client --test facade_sync` | Ticket 01: 7 passed; cold build first timed out at 120s, then a test enum spelling was corrected before the passing run |
| `cargo test -p ofdb-kv --features redb --test embedded_redb_client` | 3 passed |
| `cargo test -p ofdb-kv --features in-memory --test embedded_client` | 3 passed |
| `cargo test -p ofdb-sql-sync --features native-stage --test native_stage` | Red: feature absent; green: 4 passed |
| `cargo check -p ofdb-sql --features native-stage,in-memory` | Passed |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sync,in-memory,sql --test facade_sync` | Red: checked method/error absent; green: 2 passed |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --lib --test catalog` | 9 unit + 17 catalog tests passed |
| `cargo test -p ofdb-kv --features sync,in-memory --test facade_sync` | Red: checked method/error absent; green: 4 passed |
| `cargo test -p ofdb-kv-sync --lib` | 8 passed |
| `cargo test -p ofdb-kv --features sync,in-memory,redb --test embedded_redb_client --test embedded_client --test facade_sync` | 10 passed |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,redb,sql --test facade_sync --test cluster_sync` | 5 facade + 9 cluster tests passed before the final disconnect test was added |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --lib --test catalog --test native_stage` | 9 unit + 17 catalog + 4 native tests passed |
| `cargo test -p ofdb-kv --features sync,in-memory,redb,server --test facade_sync --test cluster_kv` | 4 facade + 12 cluster tests passed |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features native-stage,in-memory,sql --test facade_sync` | Final 6 passed, including staging disconnect cleanup |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage --test catalog --test native_stage` | Final 17 + 4 passed |
| `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target` | 1 passed after seeding its ignored lockfile from the workspace lock; initial independently resolved cold dependency builds timed out twice at 120s |
| `cargo check -p ofdb-sql -p ofdb-kv --no-default-features --features ofdb-sql/remote,ofdb-kv/remote` | Passed |
| `cargo clippy -p ofdb-sql -p ofdb-sql-sync -p ofdb-kv -p ofdb-kv-sync --all-targets --features ofdb-sql/native-stage,ofdb-sql/in-memory,ofdb-sql/redb,ofdb-sql/sql,ofdb-kv/in-memory,ofdb-kv/redb,ofdb-kv/sync` | Final passed without warnings after correcting newly introduced redundant conversions/collapsible conditionals |
| `cargo fmt --all -- --check`, `rustfmt --edition 2024 --check test-fixtures/storage-sync-consumer/consumer.rs`, `git diff --check` | Passed after formatting new composition tests |

Coverage is transport-neutral: both-role fake checks and buffered denial, native replay/retry/recovery denial, physical truncated/invalid-length records, creation/drop/explicit cleanup failures, cancellation at stage operation boundaries, cancellation inside catalog replay transaction, malformed snapshots/rows, incomplete catalogs/recovery, frame disconnect, and offline SQL/KV divergence/convergence. Native recovery observes six check scopes and confirms recovered rows are absent when denied. The public-only consumer fixture has only facade production dependencies, persists native SQL success, denies subsequent KV apply, reopens both stores and proves no cross-store rollback. Internal lower-level cluster suites retain their existing codec/backend fixtures; they are not claimed as facade-only consumers.

## Feature graphs

The following normal dependency graphs were checked programmatically by package name:

- `cargo tree -p ofdb-sql --no-default-features -e normal --prefix none --features remote`: 85 packages; no SQL engine, Redb, Automerge, SQL sync, Iroh or native stage dependency.
- `cargo tree -p ofdb-kv --no-default-features -e normal --prefix none --features remote`: 79 packages; no KV store, Redb, Automerge, KV sync or Iroh.
- `cargo tree -p ofdb-sql-sync --no-default-features -e normal --prefix none`: 47 packages; no Tokio, Redb, Iroh or tempfile. This graph result is not a no_std compile pass.

## Failures and unfinished gates

- `cargo check -p ofdb-sql-sync --no-default-features` fails with 17 errors in existing `crates/sql-engine`, beginning with missing `alloc::vec::Vec` in `src/codec.rs:70`, plus downstream inference errors. `cargo clippy -p ofdb-sql-sync --features native-stage --lib` fails at the same engine boundary when no embedded/std feature is selected. Engine files have no diff from integration. These unrelated failures were not repaired or hidden by forcing std onto the core.
- Without the stack environment above, the initial SQL native facade test aborts with stack overflow. With 16 MiB test-thread stack it passes. No default-stack success is claimed.
- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`: **180s timeout**, reached combination 10/581 (wasm btree-automerge), not a full powerset pass.
- `just crap`: **180s timeout** during cargo-hack llvm-cov compilation, not a CRAP result. The recipe's final cargo-crap step was not reached; availability of that tool is not inferred.
- `cargo clippy --workspace --all-targets --all-features`: **180s timeout**, reached optional Iroh compilation; targeted lint success does not replace the full workspace gate.
- No real Iroh authentication, domain authorization, consumer migration, optional lower-level transport extraction or external of/offs acceptance is claimed. Ticket 06 remains downstream and unclaimed.

Tickets 02/03 and 05 must not be marked resolved on this evidence. Remaining owner-local gates are standalone no_std compilation and complete broad feature/complexity/lint qualification. The optional existing Iroh adapter is an external coordinated extraction item, not authorization to edit another repository here.
