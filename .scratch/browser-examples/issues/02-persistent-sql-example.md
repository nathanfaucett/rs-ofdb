# 02: Add SQL to the same browser example

**What to build:** Extend the existing browser page with a small SQL demonstration that adds and lists persistent records through the public Rust facade. Extend the same build/serve workflow and browser smoke check rather than creating a second application or test framework.

**Blocked by:** 01 — Ship a runnable persistent KV example.

**Status:** complete

- [x] A separate SQL section accepts a text record, adds it through the SQL Client, and lists committed records in deterministic order, including a clear empty-result state.
- [x] Database construction, idempotent schema initialization, and queries stay in Rust using public Database and Client APIs with only indexeddb and SQL-text features required. User input uses bound parameters, not SQL string concatenation.
- [x] SQL uses stable example-specific IndexedDB storage distinct from KV and tests. Startup and repeated reloads neither delete records nor duplicate seed data, and displayed records come from fresh database reads.
- [x] The documented build explicitly applies the validated SQL Wasm stack and profile through the correct Cargo configuration. Do not claim default-stack support, a minimum memory requirement, or new browser guarantees.
- [x] SQL controls inherit accessible loading, pending, success, and actionable error handling. Prevent accidental duplicate submissions, show success only after commit, and render all returned text safely. Document any new handwritten JavaScript with JSDoc.
- [x] Extend the existing served-page smoke check in Chromium and Firefox to add a distinctive SQL record, await commit success, reload, and verify both the SQL record and KV value remain. Verify repeated startup preserves records without duplication and input containing markup and SQL punctuation remains literal data.
- [x] Preserve isolated test storage, bounded automation, nonzero failure reporting, and browser/server cleanup. Do not duplicate the library transaction, reconciliation, or sync test suites in UI tests.
- [x] Update build/serve and public usage instructions for both sections, verify clean-checkout reproduction, and run applicable example build/dependency, formatting, Clippy, JavaScript, keyboard, and controlled error-state checks.
- [x] Keep the result one minimal teaching application: no framework, arbitrary SQL console, sync UI, reset/migration tools, or public JS SDK. No dependency on the separate just migration is introduced.

## Comments

### Implementation and validation — 2026-10-28

- Branch `implement/browser-examples-sql`, separate worktree
  `.scratch/browser-examples/worktree-sql` in the original checkout, based on
  verified integration tip `6685427` (ticket 01). No library implementation changes,
  second application/framework, or dependency on the separate just migration.
- Complexity/risk assessment: small extension; principal risks are destructive
  startup, unbound text, premature commit success, incorrect Cargo config discovery,
  and misleading reload evidence. Reused the app, independent consumer workspace,
  WebDriver helper, server, isolated profiles, and bounded cleanup.
- TDD at the already user-confirmed served-page seam: extended smoke first and
  observed Chromium fail against ticket 01 assets because SQL controls were absent
  (`red.log`); implemented the public Rust facade bridge and SQL section to pass.
  Tests use visible public controls/results, not internal storage or fake IndexedDB.
- Rust owns SQL construction, idempotent `CREATE TABLE IF NOT EXISTS`, bound `$1`
  insertion, and fresh `SELECT ... ORDER BY id`. Stable SQL storage
  `ofdb-browser-example-sql` is distinct from KV and test storage. No seeds/reset.
- Explicit `examples/browser/.cargo/config.toml` sets a 16 MiB Wasm linear stack.
  The just build recipe changes into the example directory so Cargo discovers it;
  wasm-pack `--dev` uses `opt-level = 1`, `debug = 0`. Docs explain this validated
  configuration, not default-stack support or a measured minimum memory requirement.
- **Passed:** `just browser-example-smoke` in Chromium **156.0.8078.73** and Firefox
  **159.0**, fresh driver-managed profiles, same-session/origin reloads. Covers KV
  missing/save/retrieve, SQL empty/add, actual WebDriver Tab/Enter submissions,
  awaited commit success, guarded pending state, immediate duplicate submission
  producing only one additional row, deterministic two-row order, repeated reloads,
  explicit fresh list, preservation of KV alongside SQL, and literal markup-like
  text containing quotes/SQL punctuation. No executable markup was created.
- **Passed:** controlled IndexedDB-open denial reports actionable startup errors
  and disabled controls in both sections. Missing-Wasm negative run exits **1**
  after its bounded wait, retaining visible build/storage guidance; restored the
  artifact afterwards. No production failure injection API was added.
- **Passed:** clean tracked-file export (staged checkout-index snapshot, no prior
  targets/pkg) at ignored `.scratch/browser-tools/reproduction`: documented
  `just browser-example-build` and `just browser-example-smoke` both pass with
  committed locks and existing installed tool/browser prerequisites. This is a
  local clean-source reproduction, not a remote CI or fresh tool-install claim.
- **Passed:** workspace, example, and helper rustfmt; example Wasm Clippy (invoked
  inside example directory), helper all-targets Clippy, workspace all-targets
  Clippy, all with `-D warnings`; handwritten JS parse checks (`node --check` and
  module-mode stdin check), plus actual parsing/execution in both browsers.
  Reviewed JSDoc for all new async helpers/events, native labels, focus outlines,
  loading/pending states, accessible status/error text, text-only rendering and
  submit guards. Keyboard evidence is automated real key events, not an interactive
  manual session or screen-reader/visual accessibility audit.
- **Passed:** independent dependency graph inspection: only facade `indexeddb`
  and SQL `sql` features; no Redb, in-memory, sync, remote/server, native networking,
  or filesystem staging; helper dependencies remain in their separate workspace.
  Transitive Tokio is sync-only, not an application runtime. No new external direct
  dependency was added. `git diff --check` passes.
- **Broad gate blocked, unchanged:**
  `cargo hack test --feature-powerset --all-targets` fails in `ofdb-btree` with
  `--no-default-features --features sharded`: missing `std`, `async_stream`,
  `InMemoryBTreeShardFactory`, and `ToString` availability. No unrelated repair.
- **Broad gate not completed:** `just crap` timed out at **240 seconds** during
  instrumented dependency compilation; no complexity/coverage score claimed.
- Logs retained locally under ignored `.scratch/browser-tools/`: `red.log`,
  `build.log`, `smoke-final.log`, `expected-failure.log`, `clean-build.log`,
  `clean-smoke.log`, `dependencies.txt`, `wasm-clippy.log`, helper/workspace Clippy
  logs, `feature-powerset.log`, and `crap.log`. Process inspection after the smoke
  runs found no remaining test browser/driver processes. No WebKit, remote CI,
  actual quota exhaustion, default-stack SQL, or crash durability claim.
