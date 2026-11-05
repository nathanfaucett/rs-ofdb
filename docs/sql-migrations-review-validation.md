# SQL migrations: verified review fixes

## Base and scope

Worktree: `.scratch/worktrees/sql-migrations-review`.
Branch: `implement/sql-migrations-review`.
Initial HEAD and integration branch `implement/sql-migrations` both pointed to
`99a371f1da7e864b66da6799f418341a261a2a55`. The worktree was clean.

Loaded the TDD skill. Read the spec, all three tickets, glossary, and prior
migration validation reports. Tests use the agreed public Rust and TypeScript
Client boundaries. No spec or ticket files were changed.

Complexity: moderate. Risk: high because history validation protects persisted
schema and data. Priority: fix all five verified findings without new APIs,
dependencies, timeout policies, or compatibility code.

## Changes and test-first evidence

1. Remote transaction execution validates one response result per statement.
   The facade's typed and translated transaction paths both forward to this
   operation. The other direct consumer is SQL server integration tests.
   Empty responses at each of the three initial history reads now produce
   `History`/`Protocol`, not an indexing panic. Extra results also reject.
   History reads use checked first-result access as a second guard.
   The first regression run reproduced the original index-out-of-bounds panic;
   the same test passed after the fix.
2. Plan translation uses the existing query error conversion pattern:
   `Validation` with `Translate error: {error}`. The Rust source matches the
   ordinary SQL error. `MigrationError` Display/Debug still omit source detail.
   Browser checks include the TypeScript error, Wasm cause, and JSON properties
   and reject SQL/secret leakage. The source-parity test failed on the old
   generic message, then passed. Existing Rust validation assertions now check
   safe migration formatting, not sanitization of the intentionally detailed
   Rust source.
3. Every failed post-execution history read gets the active migration version
   and `Record` phase while retaining its source. Rust embedded/remote matrices
   and Chromium cover migration 2 altering the history schema or changing
   migration 1's fingerprint. Public schema/data/history snapshots stay equal;
   the original plan still succeeds without replay. The Rust matrix initially
   failed with `History` instead of `Record`. A controlled remote test also
   rejects empty responses at all three post-execution reads and checks the
   original `Protocol` source, version 2, rollback, and explicit resume.
4. Each migration's validation operation runs translation and hashing inside
   the existing Client deadline runner, made crate-visible. There is no new
   whole-list timeout. A public native test first failed because invalid-plan
   validation bypassed the normal timed SQL runtime requirement. It now verifies
   the same Tokio timer requirement as ordinary timed SQL. Timed Chromium
   migration calls also pass without Tokio. This does not prove hard
   interruption of synchronous translation/hashing: the existing timeout
   implementation and its polling semantics remain unchanged.
5. After the final pending migration commits, return its successful result
   without opening another history transaction. Initial empty/already-current
   validation still runs. A controlled remote executor rejects all begins after
   the final commit; the regression first failed with `Begin`/`Rejected`, then
   returned version 2 and applied versions `[2]`. The next explicit begin still
   rejects, proving the fault was armed. Existing cleanup tests no longer wait
   for rollback of the removed final transaction.

No implementation was added to `lib.rs` or `mod.rs`. Feature gates and no_std
build paths remain unchanged.

## Passed validation

From the worktree root:

```sh
pnpm install --frozen-lockfile
cargo fmt --all -- --check
RUST_MIN_STACK=67108864 cargo test -p ofdb-sql --features sql,in-memory,redb,remote,server --test migration_validation --test migration_outcomes --test migrations --test client --test remote
RUST_MIN_STACK=67108864 cargo test -p ofdb-sql-server --test grpc
cargo clippy -p ofdb-sql --features sql,in-memory,redb,remote,server --all-targets -- -D warnings
cargo clippy -p ofdb-sql-client --all-targets -- -D warnings
cargo check -p ofdb-sql --no-default-features --features sql
cargo check -p ofdb-sql --no-default-features --features remote,sql
cargo clippy -p ofdb-sql --target wasm32-unknown-unknown --features indexeddb,sql --no-deps -- -D warnings
pnpm --filter @ofsdk/sql run build:all
git --no-pager diff --check
```

The combined facade run passed 26 tests: 4 validation, 9 outcome, 6 migration,
5 client, and 2 remote tests. The SQL server run passed 7 more tests, including
cross-call transaction commit and drop rollback. Existing Redb reopen,
concurrency, deadlines, and uncertain-commit coverage remains green.

From `packages/sql`:

```sh
pnpm run check
pnpm exec biome check browser-tests/migration-validation.test.ts
BROWSER_PATH=/snap/bin/chromium pnpm exec playwright test --config playwright.local.config.ts migrations.test.ts migration-validation.test.ts migration-outcomes.test.ts transactions.test.ts
```

All 17 Chromium tests passed. The temporary config imported the existing config
and changed only `use.baseURL` and `webServer` to `http://127.0.0.1:18094`, using
`PORT=18094 node browser-tests/server.mjs`. It was removed after the run. This
uses the existing harness and avoids the documented shared Portless/TLS issue.

## Failures, limits, and deferred checks

- `pnpm run lint` reports five existing `noShadowRestrictedNames` errors for the
  `Error` identifier in unchanged `src/client.ts`, `src/database.ts`,
  `src/error.ts`, `src/handle.ts`, and `src/transaction.ts`. The changed browser
  file passes its focused Biome check. No unrelated fixes were made.
- The first TypeScript check ran before generated Wasm output was available and
  failed on missing `pkg/ofdb_wasm.js` imports. It passed after `build:all`.
- The first Rust regression command reached the 120-second limit during initial
  compilation. Its rerun completed and reproduced the product panic.
- Wasm build still emits the two existing base64 deprecation warnings.
- Native tests use the existing 64 MiB stack setting; no default-stack guarantee
  is claimed. Wasm Clippy uses `--no-deps`, as in the prior reports.
- No deterministic elapsed-time interruption test is claimed for synchronous
  plan work. The regression verifies reuse of ordinary deadline semantics,
  including its native runtime requirement; it does not strengthen them.
- Full browser/installed-package suites and other browser engines were not run.
  Standalone strict browser-test declaration checking was not rerun; prior
  reports document its unrelated fixture/Node declaration errors. Package source
  TypeScript checking and actual Chromium execution passed.
- Per the review task, neither the full feature powerset nor `just crap` was
  run. The parent owns final full validation.
