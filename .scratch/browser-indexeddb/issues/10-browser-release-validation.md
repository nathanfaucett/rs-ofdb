# 10: Validate and document the supported browser feature matrix

**What to build:** Browser SQL and KV support ships with reproducible real-browser validation, public usage documentation and explicit capability and performance limits alongside preserved native support.

**Blocked by:** 05 — Make browser storage failures and connection lifecycle actionable; 09 — Run continuous browser synchronization with cancellation and retry.

**Status:** done

- [x] Automate package-specific Wasm checks for KV indexeddb, SQL indexeddb typed statements, optional SQL translation and supported browser sync combinations. Do not require native CLI/server workspace all-features builds under Wasm.
- [x] Run real-browser acceptance tests in headless Chromium and Firefox, including representative Window and Dedicated Worker usage. Node-only or mocked IndexedDB tests are not acceptance evidence.
- [x] Validate representative persistence and transaction-lifetime scenarios in WebKit before claiming WebKit support; otherwise document the unvalidated status explicitly.
- [x] Check supported normal dependency graphs exclude Redb, native filesystem resources, native temporary staging and native TCP/Unix/gRPC transport, while preserving remote-only native isolation contracts.
- [x] Run formatting, focused Clippy, browser checks/tests, native regressions, cargo hack test --feature-powerset --all-targets and just crap as final implementation validation. Record exact unavailable tools, timeouts and pre-existing failures rather than claiming a pass.
- [x] Publish browser quickstarts for asynchronous construction, existing Client queries and explicit transactions for both facades, plus caller-owned sync and runner lifetime guidance.
- [x] Document origin scoping, quota, eviction, private-mode limits, blocked/version-change policy, error behavior and timeout-versus-rollback semantics.
- [x] Record measured snapshot materialization memory/startup costs and the initial O(database-size) transaction snapshot ceiling; do not advertise selective physical reads without evidence.
- [x] Document feature selection, supported browser baseline and exclusions: no JavaScript SDK, automatic cross-tab observation, native retirement emulation or bundled browser transport.
- [x] Remove obsolete or misleading Wasm declarations encountered within this feature's scope rather than preserving compatibility glue; do not perform unrelated cleanup.

## Comments

### Implementation and validation — 2026-10-25

Confirmed clean `browser-indexeddb-10` at requested base
`f1d079740b9bf4fe338fc4e81eb7c1b3aab9500b`, matching integration
`browser-indexeddb`. The original external worktree was not editor-writable; the
user relocated it to `ofdb/.scratch/worktrees/browser-10`, branch/base unchanged.
Read AGENTS, GLOSSARY, the full specification, all tickets 01–09 with their notes,
and browser harness/facade/storage/sync docs. Loaded TDD, GitHub Actions and
ponytail skills. Used the user-approved public facade, existing storage diagnostic
and automation seams. Complexity: moderate automation/docs; risk: false-green
feature isolation, browser substitution and misleading memory/release claims;
priority: reproducible release evidence. No runtime implementation or other
agent's worktree was edited.

Added `scripts/browser_matrix.py`, Python standard library only: independent
package checks, normal dependency guards, four real-browser cells, measurements,
native regressions, final gates, process-group bounds, raw logs and JSON status.
Failed checks continue collecting evidence but make the command fail. Six tests
cover features/contexts, native dependency rejection, missing tools, nonzero exits
and timeouts. Initial red: importing the not-yet-created runner failed; green
followed implementation. No production red/green claim for existing acceptance tests.

Added least-privilege, SHA-pinned `.github/workflows/browser.yml`: independent
feature/native jobs, Chrome/Firefox × Window/Worker, tested tool/browser versions,
matching ChromeDriver, bounds and failure artifacts. Read workflow syntax and
secure-use references; verified action SHAs/inputs/outputs upstream. Actionlint
1.7.7 passed. No remote Actions run claimed; unrelated CI remains unchanged.

Published `docs/browser.md` (all four Rust blocks compile as an isolated Wasm
consumer) and `docs/browser-validation.md` (commands, support matrix, measurements,
full limits and exact failed/incomplete gates). Linked public workspace/facade/
storage/harness docs. Removed obsolete claims that SQL browser sync is future
work and all memory subscriptions are Unsupported. No obsolete Wasm declarations
requiring runtime edits were found in scope; no compatibility glue, dependencies
or unrelated BTree fixes were introduced.

#### Passing final evidence

Full `wasm-pack test --headless --chrome|--firefox tests/browser --features
sync|worker,sync --lib`, through the matrix script:

- Chrome 156.0.8078.47 Window **87 passed**, Worker **84 passed**.
- Firefox 158.0.1 Window **87 passed**, Worker **84 passed**.
- Real capped retry, persistence/restarted contexts, transaction lifetime,
  SQL/KV checked native fixtures, deadlines and lifecycle included. Three
  iframe/page-only tests do not run in Worker mode; nested-worker restarts do.
- Seven independent Wasm facade checks and normal graphs, native-stage fixture
  graph and native remote-only isolation graphs passed.
- Browser Clippy in both contexts and focused native facade/runtime Clippy passed.
- Native suites: **5** protocol/ownership/runner + **19** KV + **70** SQL + **10**
  runtime tests passed. Native SQL/thread checks used 16 MiB RUST_MIN_STACK.
- Workspace/fixture rustfmt, six Python tests, compiled public quickstarts,
  actionlint and `git diff --check` passed.

The fixture retains its optimized dev profile and 16 MiB Wasm stack, not a default/
minimum-stack claim. Runs reused ignored ticket-01 capabilities and matching
drivers; no machine configuration or browser binary was committed. Exact commands
and JSON live under ignored `target/browser-validation/{check-final,chrome-window,
firefox-window,workers,measure,native,final}` in this worktree. Public reproduction
instructions do not depend on those machine-local resources.

Snapshot diagnostics now report Wasm linear capacity, explicitly not live/peak
heap or JS/IDB memory. Filtered single-sample 100/1,000-record timings (4-byte keys,
1 KiB values): Chrome Window **4/35 ms**, Worker **4/34 ms**; Firefox Window
**3/19 ms**, Worker **3/20 ms**. All samples measured capacity
**17,891,328→18,022,400** and **18,939,904→19,988,480** bytes respectively. Two-copy
payload lower bounds **205,600/2,056,000** bytes are calculations, not measured
peak memory. Public report includes methods and `measure` reproduction. This
measures transaction snapshot startup, not app cold startup or selective I/O.

#### Exact failures and unvalidated limits

- `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 cargo hack test --feature-powerset --all-targets`:
  **failed after 39.01 s**, cargo-hack exit 1 / underlying Cargo 101, at
  `ofdb-btree --no-default-features --features sharded`: eight pre-existing errors
  (std, async_stream, gated InMemoryBTreeShardFactory, ToString). Already documented
  and base-reproduced by earlier tickets; no BTree code changed. Remaining
  combinations did not complete; no full-matrix pass.
- `CARGO_BUILD_JOBS=2 just crap`: **timed out at 240 s**, coverage package **6/32**
  `ofdb-value`, compiling Automerge/value dependencies. Recipe line 40 terminated
  by signal 15. No completed workspace coverage or CRAP score.
- Ticket 08's explicitly 2 MiB native sync_stack overflow is pre-existing; this
  focused suite does not include it or claim to fix/revalidate it.
- Safari/WebKit browser/driver unavailable: **explicitly unvalidated**, not a pass.
  Actual quota exhaustion, eviction/private-mode behavior, whole-browser/OS crash,
  power loss, live network peer and minimum-stack support remain unvalidated.
  Existing quota/abort/access scenarios use fixture injection.

Acceptance requires running and accurately recording broad gates, not pretending
those gates passed. Ticket 10 is done; broad native matrix/complexity release
readiness remains incomplete.

#### Integration handoff

After committing, ran `git merge --no-edit browser-indexeddb`: **Already up to date**.
Integration remained `f1d079740b9bf4fe338fc4e81eb7c1b3aab9500b` and
`git merge-base --is-ancestor browser-indexeddb HEAD` succeeded. No integration
changes were omitted, no conflicts occurred and no other agent's work was changed.
All staging used explicit ticket-owned file paths, never the containing nested
worktree directory. Final worktree was clean on `browser-indexeddb-10`.
