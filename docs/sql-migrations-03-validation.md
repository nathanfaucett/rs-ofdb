# SQL migrations: ticket 03 validation

## Scope and base

Worktree: `.scratch/worktrees/sql-migrations-03`, branch
`implement/sql-migrations-03`. Before changes, HEAD and integration branch
`implement/sql-migrations` both pointed to ticket 01 commit
`1689c50aa7f65b710d3c9cf3eee726120a5a7919`. The worktree was clean.

Read `AGENTS.md`, `GLOSSARY.md`, the parent spec, ticket 03, ticket 01's
implementation, and `docs/sql-migrations-validation.md`. The spec confirms the
public SQL Client as the Rust and TypeScript test boundary. No spec or ticket
files were changed. Ticket 02's input/history validation is separate.

Complexity: moderate. Risk: high because uncertain responses can hide durable
writes. Priority: prove atomic rejection and explicit recovery without replay.

## Changes and evidence

- `crates/sql/tests/migration_outcomes.rs`: five public Client tests cover embedded
  and remote concurrent calls, a controlled remote stale-revision rejection,
  timeout after a staged transformation with server cleanup, and timeout after
  durable commit but before its response.
- `packages/sql/browser-tests/migration-outcomes.test.ts`: five real Chromium
  tests cover independent IndexedDB opens with a held stale snapshot, native put
  failure/abort, deadline during snapshot acquisition, and deadline after native
  commit while the completion callback is held. Each test closes owned handles;
  the existing fixture deletes the database and detects leaked connections.
- The data transformation moves `pending` into `amount`, then clears `pending`.
  Replaying it would change the expected amount from 11 to 0. An explicit call
  after a lost completion skips that version and preserves 11. Later versions
  stop at the first error and remain pending for the explicit follow-up.
- The stale browser attempt stages its own extra table. Its commit rejects with
  structured `Storage` context and version 2; its extra table is absent. A new
  public Client call reads the winning history and can write normally.
- Injected abort/quota failures leave application rows, schema, and history
  unchanged. The API still reports `outcome: "unknown"`; the test's controlled
  abort proves rollback in that case, not that every commit rejection does.
- Rust errors retain version, phase, and source kind. Actual Wasm/TypeScript
  errors retain version, phase, query kind, and uncertainty. A pre-commit timeout
  has no uncertain outcome; a commit timeout has an unknown outcome. No message
  matching is used to classify these errors.
- Usage docs explain explicit follow-up, result inspection, per-version commits,
  conflict rejection, deadlines, cleanup, and uncertain outcomes.

Tests were added before considering runtime changes. The initial Rust run failed
because the test used unsupported arithmetic UPDATE expressions; the fixture was
changed to the supported column-copy transformation above. That was a test error,
not a product defect. The existing runner, query deadlines, Wasm bridge, and
TypeScript error paths pass these tests without runtime changes. Only tests,
their internal protocol dev-dependency, and documentation changed.

The controlled remote executor forwards statements to real embedded Client
transactions. It pauses an UPDATE response after staging, or a commit response
after durable completion. Existing server cancellation and stream closure perform
cleanup. The browser uses the existing native IndexedDB failure/completion controls,
not a replacement migration runner. Holding completion does not delay or cancel
the native commit itself.

## Passed commands

From the worktree root:

```sh
pnpm install --frozen-lockfile
pnpm --filter @ofsdk/sql run build:all
cargo fmt --all -- --check
RUST_MIN_STACK=67108864 cargo test -p ofdb-sql --features sql,in-memory,remote,server --test migration_outcomes
RUST_MIN_STACK=67108864 cargo test -p ofdb-sql --features sql,in-memory,redb,remote,server --test migration_outcomes --test migrations --test client --test remote
cargo clippy -p ofdb-sql --features sql,in-memory,redb,remote,server --test migration_outcomes -- -D warnings
cargo clippy -p ofdb-sql --features sql,in-memory,redb,remote,server --all-targets -- -D warnings
cargo clippy -p ofdb-sql --target wasm32-unknown-unknown --features indexeddb,sql --no-deps -- -D warnings
git --no-pager diff --check
```

The new Rust target passes all five tests. The combined Rust command passes
18 tests: five outcome tests, six existing migration tests, five Client tests,
and two remote tests. The existing migration tests include SQL-failure rollback,
completion-insert rollback, explicit resume, remote execution, and Redb reopen.
The release build includes the actual Wasm adapter and TypeScript browser bundle.
Existing base64 deprecation warnings remain in `crates/wasm/src/value.rs`.

From `packages/sql`:

```sh
pnpm run check
pnpm exec biome check browser-tests/migration-outcomes.test.ts
BROWSER_PATH=/snap/bin/chromium pnpm exec playwright test --config playwright.local.config.ts migrations.test.ts migration-outcomes.test.ts transactions.test.ts
BROWSER_PATH=/snap/bin/chromium pnpm exec playwright test --config playwright.local.config.ts migration-outcomes.test.ts --repeat-each=3
```

The combined browser command passes 14 tests. All five new browser tests also
passed three consecutive runs (15 executions).

The temporary local Playwright config imports `playwright.config.ts` and changes
only `use.baseURL` and `webServer` to `http://127.0.0.1:18093`, with server command
`PORT=18093 node browser-tests/server.mjs`. It uses the existing harness and is
removed before commit. This avoids the previously documented shared Portless/TLS
setup issue without changing it. Installed Chromium is `/snap/bin/chromium`.

## Gaps and deferred validation

This extra standalone browser-test declaration check failed:

```sh
pnpm exec tsc --noEmit --strict --module nodenext --target es2022 --lib ES2022,DOM,ESNext.Disposable browser-tests/migration-outcomes.test.ts
```

It reports 89 errors: missing Node types in Playwright declarations and `process`,
plus existing shared `browser-tests/test.ts` fixture typing errors. The package's
`pnpm run check` passes. No unrelated dependency or fixture changes were made.

The full browser suite, installed-package suite, Firefox, and native Redb
migration deadline injection were not run. The Rust tests use the existing
64 MiB test stack setting; no default-stack guarantee is claimed. Wasm Clippy is
facade-only with `--no-deps`, as in ticket 01.

The parent owns final full validation. Neither
`cargo hack test --feature-powerset --all-targets` nor `just crap` was run for
this ticket. Full feature completion is not claimed. Replication and sync policy
remain out of scope.
