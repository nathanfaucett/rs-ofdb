# SQL migrations: ticket 02 validation

## Base and scope

Worktree: `.scratch/worktrees/sql-migrations-02`, branch
`implement/sql-migrations-02`. Before changes, HEAD and integration branch
`implement/sql-migrations` both pointed to
`1689c50aa7f65b710d3c9cf3eee726120a5a7919` (ticket 01).

Read the spec, ticket 02, AGENTS.md, GLOSSARY.md, ticket 01 implementation, and
`docs/sql-migrations-validation.md`. The spec and tickets were not changed.

Complexity: moderate. Risk: high because validation guards persisted schema and
data. Priority: reject the complete invalid plan before any mutation. Reuse the
Rust SQL translator and runner; no dependencies, second runner, replication, or
concurrency implementation were added.

## Test-first evidence

A public embedded Client test supplied a valid version one followed by a
statement-free batch. The original runner rejected the semicolon-only tail during
`Execute`, not `Plan`, after committing version one. The test failed on the phase
assertion. After adding whole-plan translation, the same test passed and schema
remained unchanged. Comment-only, whitespace-only, empty, and invalid SQL tails
are also covered.

The runner now translates every batch before opening a transaction. Invalid SQL
and batches with no statements fail with safe `Plan`/`Validation` context. Existing
transaction-bound metadata and history validation remains the single authority
for embedded, remote, and Wasm clients.

## Coverage

New tests use only the agreed public Client seams:

- `crates/sql/tests/migration_validation.rs` runs the same 38 rejection cases over
  embedded and TCP remote Clients. Cases cover invalid ordering, duplicates,
  gaps, zero and maximum u32 versions, statement-free and invalid SQL tails,
  changed applied SQL (including whitespace and comments), empty/older full lists,
  NULL and invalid fingerprints, missing history, zero/negative/overflow stored
  versions, metadata column/type/default/primary-key collisions, and duplicate
  stored versions in a colliding table. Existing application tables do not imply
  migration completion. A normal engine rejection retains its query kind.
- Each rejection compares public table and field catalog rows, application rows,
  and history rows before and after the call. Pending schema/data work must not
  run, and incompatible history must not be repaired. Original unchanged plans
  still return no applied versions after rejected changes.
- `packages/sql/browser-tests/migration-validation.test.ts` adds 23 invalid-input
  cases and 22 history/metadata rejection cases through the TypeScript Client and
  real IndexedDB/Wasm. This includes malformed JS shapes, noninteger/negative/
  nonfinite/overflow/string versions, authoritative Rust ordering and batch
  validation, safe typed errors, phase, version, query kind, and no commit-unknown
  outcome for validation failures. Public schema/data/history snapshots must
  remain equal. Handles are closed and the existing fixture deletes IndexedDB
  databases, checking cleanup.
- Existing ticket 01 tests retain initialization, upgrade, rollback, repeat-call,
  native persistence-reopen, and browser reload coverage.

The engine requires a primary key and rejects primary-key UPDATE. Malformed
history fixtures therefore use DELETE/INSERT. Duplicate versions use a colliding
metadata table whose primary key is `fingerprint`; a correct metadata table's
primary key prevents duplicate versions through public SQL.

## Passed commands

From the worktree root:

```sh
cargo fmt --all -- --check
RUST_MIN_STACK=67108864 cargo test -p ofdb-sql --features sql,in-memory,redb,remote,server --test migration_validation --test migrations --test client --test remote
cargo clippy -p ofdb-sql --features sql,in-memory,redb,remote,server --all-targets -- -D warnings
cargo check -p ofdb-sql --no-default-features --features remote,sql
cargo check -p ofdb-sql --no-default-features --features sql
cargo clippy -p ofdb-sql --target wasm32-unknown-unknown --features indexeddb,sql --no-deps -- -D warnings
pnpm --filter @ofsdk/sql run build:all
git diff --check
```

Rust results: 2 validation matrix tests, 6 migration tests, 5 Client tests, and
2 remote tests passed (15 tests total).

From `packages/sql`:

```sh
pnpm exec biome check browser-tests/migration-validation.test.ts
pnpm run check
BROWSER_PATH=/snap/bin/chromium pnpm exec playwright test --config playwright.local.config.ts migration-validation.test.ts migrations.test.ts
```

Browser results: 4 tests passed, including both new validation matrices and both
ticket 01 migration tests. The temporary local config imported the checked-in
Playwright config and changed only `use.baseURL` and `webServer` to
`http://127.0.0.1:18092` and `PORT=18092 node browser-tests/server.mjs`. This is the
existing browser harness, not a new test seam. The config was removed after use.
As in ticket 01, this avoids shared Portless state and uses installed Chromium.

## Gaps and limits

- Standalone strict browser-test declaration checking did not pass:
  `pnpm exec tsc --noEmit --strict --module nodenext --target es2022 --lib ES2022,DOM,ESNext.Disposable browser-tests/migration-validation.test.ts`.
  It reported 89 errors: missing Node declarations in Playwright and the test's
  existing `process.env` pattern, plus existing fixture typing errors in
  `browser-tests/test.ts`. Package source `pnpm run check` and the actual browser
  tests passed. No dependency or unrelated harness changes were made.
- The Wasm build reports existing deprecated base64 encode/decode warnings.
- Native tests use the ticket 01 stack setting; no small-stack guarantee is made.
- No installed-package browser suite, full browser suite, other browser engine,
  full feature powerset, or `just crap` run was performed here. Parent final review
  owns full feature and complexity validation.
- Ticket 03 owns concurrency, stale revisions, deadlines, and uncertain commits.
  No new coverage for those paths is claimed here.
