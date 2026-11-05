# SQL migrations: final integration validation

## Result

Implemented all three SQL migration tickets on `implement/sql-migrations`.
Runtime changes and review fixes are integrated through commit `46410a4`.
The parent spec is unchanged. Replication and sync policy are out of scope.

The review covered code/QA, design, architecture, and security. It found five
issues, all fixed and rechecked:

- Reject malformed remote transaction result counts with a Protocol error.
- Preserve translator detail in Rust error sources while keeping migration
  Display/Debug and browser errors free of SQL and data.
- Report post-execution history errors with the active version and Record phase.
- Validate each plan entry through the existing Client deadline runner.
- Return after the final commit without another history transaction.

The follow-up review found no remaining actionable issues. This is not proof
that every workspace feature combination or browser environment is supported.

## Passed after review fixes

Commands ran from the repository root unless noted. Long commands used bounded
180-second or 240-second timeouts. Native tests used a 64 MiB thread stack.

- `RUST_MIN_STACK=67108864 cargo test -p ofdb-sql --features sql,in-memory,redb,remote,server --test migration_validation --test migration_outcomes --test migrations --test client --test remote`: 26 tests passed.
- `RUST_MIN_STACK=67108864 cargo test -p ofdb-sql-server --test grpc`: 7 tests passed.
- `cargo fmt --all -- --check`.
- `cargo clippy -p ofdb-sql --features sql,in-memory,redb,remote,server --all-targets -- -D warnings`.
- Native SQL client and SQL server Clippy with `--all-targets -- -D warnings`.
- `cargo clippy -p ofdb-sql --target wasm32-unknown-unknown --features indexeddb,sql --no-deps -- -D warnings`.
- `cargo check -p ofdb-sql --no-default-features --features sql`.
- `cargo check -p ofdb-sql --no-default-features --features remote,sql`.
- `pnpm --filter @ofsdk/sql run build:all`.
- Package TypeScript checks, strict consumer declaration checks, and focused
  migration validation-test Biome checks.
- All three migration browser test files plus transaction tests: 17 Chromium
  tests passed using the existing SQL harness.
- `git diff --check`.

Browser tests used `/snap/bin/chromium` and a temporary local config on port
18094 to avoid the existing shared Portless/TLS setup problem. The config was
removed. No shared server was stopped. Tests ran against rebuilt bindings.

Review regressions were added to existing test targets, not a separate
`migration_review` target. An initial command naming that nonexistent target
failed before running tests; the corrected combined command above passed.

## Workspace failures and limits

- `RUST_MIN_STACK=67108864 timeout --kill-after=5s 240s cargo hack test --feature-powerset --all-targets`
  failed with exit 1 in existing `ofdb-btree` tests with
  `--no-default-features --features sharded`. Eight compile errors include
  unavailable `std`, a gated `InMemoryBTreeShardFactory`, and missing `ToString`.
  The complete workspace feature matrix did not pass.
- `RUST_MIN_STACK=67108864 timeout --kill-after=5s 240s just crap`
  timed out with exit 124 during coverage compilation at package 17/33,
  `ofdb-sql-client`. No final complexity score was obtained.
- Earlier integration validation found a native `scan_layout` timing failure.
  The serial rerun took 32.77 seconds against an 18-second limit in
  `native_conflict_checks_reuse_raw_table_catalog_within_budget`. No migration
  regression was established. This unrelated timing test was not changed.
- Dependency-wide Wasm Clippy remains blocked by existing platform time lints.
  Focused facade Wasm Clippy passed with `--no-deps`.
- Package-wide lint reports existing restricted `Error` identifier violations.
  Standalone browser-test type checking also reports missing Node declarations
  and existing shared fixture errors. Package source checks and actual Chromium
  tests passed.
- Existing Wasm base64 deprecation warnings remain.

No full installed-package qualification or other browser-engine qualification
is claimed. Earlier reports contain detailed per-ticket commands and test-first
evidence. Final implementation acceptance is complete; workspace-wide validation
is not clean for the reasons above.

## Operational limits

Each migration commits independently. A later failure does not undo earlier
versions. Failed commits have an unknown durable outcome; there is no automatic
retry. Explicit subsequent calls read history before deciding what remains.
Existing deadline semantics do not guarantee interruption of synchronous work.
