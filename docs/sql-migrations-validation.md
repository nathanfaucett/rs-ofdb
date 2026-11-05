# SQL migrations: ticket 01 validation

## Scope

Ticket 01 adds explicit `Client::migrate` in Rust and `Client.migrate` in
TypeScript. One Rust runner serves embedded, remote, and Wasm clients. It uses
existing explicit transactions and the existing SHA-256 dependency. No migration
server, protocol, CLI, automatic retry, or Database migration method was added.

Base confirmed before changes: `implement/sql-migrations-01` and integration
branch `implement/sql-migrations` both pointed to
`0e5addbc1afbba4eb3068cd944f5f06ef22f7bbb`. All changes were made in the assigned
`sql-migrations-01` worktree. The parent spec and tickets were not edited.

Complexity: moderate. Risk: high because SQL changes persisted application data.
The key boundary is one transaction containing migration SQL and its history
record. Priority: establish the shared runner before adding full validation and
failure-injection coverage in tickets 02 and 03.

## Test-first evidence

The first public Rust Client test failed to compile because `Migration` and
`Client::migrate` did not exist. After implementation, the same test passed.
Further slices cover rollback, failed completion insertion, persistence, remote
execution, and browser bindings through public APIs.

## Passed checks

Commands run from the worktree root unless stated otherwise:

- `cargo fmt --all -- --check`.
- `RUST_MIN_STACK=67108864 cargo test -p ofdb-sql --features sql,in-memory,redb,remote,server --test migrations --test client --test remote`:
  6 migration tests, 5 client tests, and 2 remote tests passed.
- `cargo clippy -p ofdb-sql --features sql,in-memory,redb,remote,server --all-targets -- -D warnings`.
- `cargo check -p ofdb-sql --no-default-features --features remote,sql`.
- `cargo check -p ofdb-sql --no-default-features --features sql`.
- `cargo clippy -p ofdb-sql --target wasm32-unknown-unknown --features indexeddb,sql --no-deps -- -D warnings`.
- `pnpm --filter @ofsdk/sql run build:all`: release Wasm, generated declarations,
  TypeScript, and browser bundle built. Existing Wasm base64 deprecation warnings
  remain.
- `pnpm --filter @ofsdk/sql run check` and `pnpm --filter @ofsdk/sql run build`.
- In `packages/sql`:
  `pnpm exec tsc --noEmit --strict --module nodenext --target es2022 --lib ES2022,DOM,ESNext.Disposable browser-tests/consumer.ts`.
- In `packages/sql`:
  `pnpm exec biome check src/migration.ts src/index.ts browser-tests/migrations.test.ts browser-tests/consumer.ts`
  and `pnpm exec biome format src/client.ts`.
- `git diff --check`.

The two new browser tests passed with:

```sh
BROWSER_PATH=/snap/bin/chromium pnpm exec playwright test --config playwright.local.config.ts migrations.test.ts
```

This command ran in `packages/sql`. A temporary, uncommitted config reused
`playwright.config.ts` and the existing server and tests, overriding only
`use.baseURL` to `http://127.0.0.1:18091` and `webServer` to
`PORT=18091 node browser-tests/server.mjs` at that URL. The config was removed
after validation. No alternate test seam or migration runner was used.

Browser coverage includes explicit empty initialization, sequential upgrade,
existing-row defaults, application changes preserved on repeat calls, page reload,
failed-batch schema/data/history rollback, stopping later work, explicit resume,
structured version/phase/query kind, safe error messages, and handle cleanup.

## Existing failures and environment limits

- Without `RUST_MIN_STACK=67108864`, migration tests overflow the default test
  thread stack. The existing `client` test target also overflows that stack in
  `embedded_client_exposes_generic_translator_helpers`. No general small-stack
  guarantee is claimed. The existing browser build reserves a 16 MiB Wasm stack.
- The checked-in Playwright setup timed out on the Portless TLS certificate
  check. Supplying `NODE_EXTRA_CA_CERTS=/home/nathan/.portless/ca.pem` then found
  the URL still served by an orphaned test server. The temporary local config
  avoided changing shared Portless state. The default Playwright browser binary
  was absent; the installed Chromium executable was used instead.
- The complete browser suite under that local config passed 16 of 17 tests.
  The existing dedicated-worker test failed: `browser-tests/worker.js` imports
  `SqlDatabase`, while the public package exports `Database`. That file was not
  changed.
- `node browser-tests/package.mjs` packed the artifact but failed with ENOENT:
  it expects `artifacts/ofdb-sql-0.1.0.tgz`, while pnpm produced
  `artifacts/ofsdk-sql-0.1.0.tgz`. The installed-consumer browser suite therefore
  did not run. Direct consumer declaration checks passed as listed above.
- Biome check of all changed TypeScript files still reports the existing
  `src/client.ts:1` import named `Error` (`noShadowRestrictedNames`). New files
  pass their focused checks; client formatting passes.
- Full dependency Wasm Clippy failed in unchanged `crates/platform/src/time.rs`
  on `manual_is_multiple_of` and `result_unit_err`. The SQL facade passes with
  `--no-deps`.
- `RUST_MIN_STACK=67108864 timeout --kill-after=5s 180s cargo hack test --feature-powerset --all-targets`
  failed at `ofdb-btree --no-default-features --features sharded`, with existing
  test compilation errors, including missing `ToString` imports. The complete
  workspace feature matrix did not pass.
- `timeout --kill-after=5s 180s just crap` timed out during instrumented dependency
  compilation (exit 124). No complexity score or coverage result was obtained.

## Follow-up scope

Ticket 02 must complete and test authoritative whole-plan validation (including
SQL batches with no statements), malformed/colliding metadata, immutable exact-SQL
fingerprints, and rejection without changes across Rust and browser bindings.
Basic ordering, whitespace-empty input, metadata shape, and history checks already
exist; this is not a claim that ticket 02 is complete.

Ticket 03 must test concurrent attempts, IndexedDB stale revisions, deadline
cleanup, uncertain commits, and explicit follow-up after lost completion. The
runner already uses existing transactions, preserves query kinds and commit
context, marks failed commits conservatively as unknown, and never retries.
Those paths do not yet have the required failure-injection proof.

Full feature validation remains pending those tickets and the unrelated failures
above. Replication and sync policy are outside this work.
