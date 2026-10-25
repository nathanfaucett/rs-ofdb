# 04: Preserve full SQL transaction and reconciliation semantics

**What to build:** Browser SQL callers use explicit multi-call transactions, relational constraints, conflict inspection and Resolution with the same persistent domain guarantees as existing embedded backends.

**Blocked by:** 03 — Run persistent SQL statements through the browser Query Handle.

**Status:** done

- [x] Public explicit transactions preserve stable snapshots and read-your-writes across unrelated browser awaits; stale writers fail atomically across independent handles.
- [x] SQL execution errors preserve existing aborted-transaction rules. Commit of an aborted transaction cannot persist earlier writes; rollback and dropped staging discard pending state.
- [x] Verify defaults, natural and composite primary keys, unique constraints and derived Index Record maintenance through browser facade tests.
- [x] Verify table/index drop and recreation preserve namespace and identity rules and cannot restore stale rows.
- [x] Use persisted concurrent-history fixtures to verify conflict inspection, losing values and explicit Resolution through public Query Handles and transaction interfaces.
- [x] Verify schema, row history, Tombstones and derived indexes remain consistent after reopen and actual browser-context restart.
- [x] Exercise multi-table failure and concurrent commit scenarios to prove catalog and index state cannot be partially persisted.
- [x] Reuse existing SQL acceptance scenarios where possible without porting filesystem or multi-thread test-harness assumptions into browser tests.

## Comments

### Implementation and validation — 2026-10-25

Confirmed clean worktree `/home/nathan/code/rust/aicacia/ofdb-browser-04`, branch
`browser-indexeddb-04`, at exactly requested base
`633634785aaa528e435e85da89cb02324b0b3a50`; integration `browser-indexeddb` initially
pointed to the same commit. Loaded TDD and ponytail; read AGENTS, GLOSSARY,
specification, this ticket, ticket 03's completed notes, browser README and harness,
and existing native SQL constraint/conflict/transaction acceptance scenarios.
Used the user-confirmed public Database → Client → Transaction seam, with existing
public sync APIs solely to construct/persist causal-history fixtures. Complexity:
moderate test work; integrity/concurrency risk high; priority: SQL semantic parity.

All new tests and test-only Wasm/inline-JS probes live in
`tests/browser/src/sql_semantics.rs`. Shared harness changes are one module
registration and an appended README section. No dependency changes, new facade,
platform/timer changes, lifecycle-adapter changes, or SQL-engine modifications.
Tests did not expose a demonstrated engine correctness defect requiring a fix.
No edits were made to parallel tickets 05/06's owned implementation files.

#### Acceptance coverage

- Multi-call transactions retain snapshot reads, local inserts/updates and derived
  indexes across real browser timer awaits. Independent committed writes remain
  outside the snapshot; stale mutating commit returns Storage; stale read-only
  commit succeeds. Successful multi-call commit survives reopen.
- Rollback, dropped transaction and execution-aborted commit discard earlier
  catalog/index creation, row updates and deletions. Aborted execute and both
  conflict-inspection methods fail. Reopen proves no partial state remains.
- Defaults, natural and composite primary keys, null/duplicate-key rejection,
  immutable key columns, single/composite uniqueness, failed unique-index updates,
  successful indexed updates/deletes and reuse of released values survive reopen.
- Table drop/recreation changes schema, drops dependent indexes and rejects an
  unobserved old transaction. Historical table/index namespace claims remain
  protected; same-kind index recreation works.
- Persisted fixtures contain concurrent text/boolean values, a deleted-row
  Tombstone and a dropped/recreated table with a different schema/index. Public
  conflict names/losing values, batch inspection and transaction snapshots are
  checked. Invalid/mixed Resolution assignments leave exported history unchanged;
  partial Resolution settles only its column, remains durable after reopen, and
  does not change an existing transaction's conflict snapshot. Final Resolution
  and stale row-history replay cannot restore conflicts, deleted rows or old DDL.
- Distinct unique-key contenders follow largest-row-UUID selection, not same-row
  Conflict semantics. Deleting the winner never promotes the loser, including
  stale replay after reopen; a larger fresh UUID may claim the deleted unique key.
- Real destroyed/recreated worker and iframe-page instances compare complete
  exported native history bytes/metadata/digests exactly before/after restart,
  both before and after Resolution. Indexed reads, recreated schema and Tombstones
  are also independently queried. These are execution-context restarts, not
  whole-browser/OS crashes or power-loss tests.
- Late constraint errors and injected native abort/QuotaExceededError roll back
  changes spanning two existing tables, newly created table/index/catalog rows,
  updated indexes and deletions. Catalog comparison and reopen observe no partial
  commit; failed names remain usable. These are injected faults, not actual
  disk-quota exhaustion.
- Two separately initialized Wasm workers stage overlapping multi-table/catalog
  writes before an explicit barrier releases both commits. Exactly one wins and
  one returns Storage. Both contexts are terminated before reopen verifies the
  complete winner's rows/indexes/catalog and absence of every losing effect.

#### Incremental test-first evidence and corrections

Built and ran scenarios in vertical slices (constraints, transaction lifecycle,
multitable failures/DDL, persisted history/Resolution, worker race, composite
uniqueness/unique-key reconciliation). Existing functionality frequently passed
new acceptance coverage immediately; no fabricated production red/green is claimed.
Initial failures were test assumptions or fixture construction errors: SQL
translation errors do not execute and therefore do not abort existing embedded
transactions; text IDs sort lexically; historical index names cannot become table
storage; catalogs include implicit constraint indexes. Those were corrected to
match existing native contracts, without changing engine behavior. Applying a
catalog export one unit at a time encountered an incomplete-catalog rejection;
fixture setup now uses public synchronization to apply dependency-complete state.
A filter passed directly to wasm-pack also failed because it forwarded the filter
to `cargo build`; focused runs use Cargo or the existing test runner instead.

#### Passing final validation

Paths below are relative to this worktree. Actual tool invocations ran from the
editor's `ofdb` root with absolute fixture/manifest/config/justfile paths because
the sibling worktree is not an editor root. Browser commands used:

```
WASM_BINDGEN_TEST_TIMEOUT=120
WASM_BINDGEN_TEST_WEBDRIVER_JSON=/home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/webdriver.local.json
DRIVER=/home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/target/chromedriver-linux64/chromedriver
RUNNER=/home/nathan/.cache/.wasm-pack/wasm-bindgen-4f04094f43ba0220/wasm-bindgen-test-runner
GECKODRIVER=/home/nathan/.cache/.wasm-pack/geckodriver-df9057ce9c9cc43e/geckodriver
```

These are recorded literal environment/tool paths, not committed machine config.
Chrome and Firefox binaries are the ticket-01 Flatpak installations selected in
that existing local WebDriver JSON. Existing optimized dev profile, wasm-bindgen
0.2.128 and documented 16 MiB Wasm stack remain unchanged.

- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features sync`:
  **51 passed**, Window (all 10 ticket-04 tests).
- `wasm-pack test --headless --firefox tests/browser --features sync`:
  **51 passed**, Window (all 10 ticket-04 tests).
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features worker,sync`:
  **48 passed**, Dedicated Worker (9 ticket-04 tests; iframe is Window-only).
- `wasm-pack test --headless --firefox tests/browser --features worker,sync`:
  **48 passed**, Dedicated Worker.
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser`:
  **35 passed**, default features (6 ticket-04 tests; history fixtures are sync-gated).
- Focused Firefox `RUNNER tests/browser/target/wasm32-unknown-unknown/debug/deps/ofdb_browser_tests-3b7b8061aa6f7911.wasm sql_semantics`:
  **10 passed**, Window; focused Chrome equivalent also **10 passed**.
- `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 cargo test --manifest-path tests/browser/Cargo.toml --test native --features sync`:
  **2 passed**, native Send/Sync and spawned query/transaction/sync regressions.
- `cargo clippy --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync -- -D warnings`: passed.
- Same Clippy command without `--features worker,sync`: passed.
- `cargo fmt --manifest-path Cargo.toml --all -- --check`: passed.
- `cargo fmt --manifest-path tests/browser/Cargo.toml --all -- --check`: passed.
- `git diff --check`: passed.

#### Incomplete broader gates and limits

- `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 cargo hack test --feature-powerset --all-targets`:
  failed on existing `ofdb-btree --no-default-features --features sharded`, with
  eight missing in-memory export/no_std String/ToString errors. Same failure was
  base-reproduced/documented in ticket 01 and recorded in ticket 03. No affected
  implementation file changed here; remaining workspace combinations did not run.
- `CARGO_BUILD_JOBS=2 just crap`: timed out after **240 seconds** during coverage
  compilation of package **4/32**, ofdb-btree-indexeddb. Earlier BTree/Automerge
  tests passed; no completed complexity score is claimed.
- Two initially concurrent full browser invocations timed out at 180 seconds
  waiting for the native build directory lock. The native build completed and
  browser reruns passed. An initial full Firefox Window run also timed out at
  180 seconds after 33 tests had passed; focused Firefox passed, and the later
  full Window rerun passed all 51 tests in 67.27 seconds of test execution.
- No WebKit, native/browser wire-interop fixture, browser-process crash, actual
  quota exhaustion, default-stack support or SQL memory ceiling is claimed.
  Checked SQL sync/subscription acceptance remains ticket 08; lifecycle/deadline
  behavior remains owned by tickets 05/06. This ticket's public sync calls are
  fixture setup/replay, not a replacement for those parallel acceptance suites.

All eight ticket-specific acceptance criteria are met by real-browser public
facade tests. Broader pre-existing/tool-timeout gates above remain explicitly
incomplete and are not represented as passing.

#### Integration handoff

After committing implementation, ran `git merge --no-edit browser-indexeddb`:
**Already up to date**. Integration tip remained
`633634785aaa528e435e85da89cb02324b0b3a50`; `git merge-base --is-ancestor
browser-indexeddb HEAD` succeeded. Worktree was clean on `browser-indexeddb-04`.
No parallel implementation commits were overwritten or excluded by this handoff.

### P2 review fix: cold SELECT must remain read-only — 2026-10-25

Review branch `browser-indexeddb-review`, worktree `.scratch/worktrees/review`,
started clean at integration base `3fbd651`. Read repository rules, glossary,
specification and relevant implementation/validation notes; loaded TDD and
ponytail. Used the preapproved facade/subscription and public storage snapshot
seams. Complexity: low; risk: shared executor transaction semantics; priority: P2.

Root cause: shared `execute_in_transaction` ensured persistent catalogs before
classifying SELECT-only batches. On a fresh IndexedDB database this staged four
empty logical tables, advancing revision 0 to 1 and publishing invalidation;
a cold explicit reader then incorrectly competed with independent DDL.

The shared executor now classifies first and initializes catalogs only for
mutating batches. Missing catalogs are already readable as empty by the
IndexedDB Kernel, so no virtual-table layer or open-time writes are needed.
Normal and explicit execution use the same fix. Kernel `ensure_table` and shared
snapshot commits retain intentional empty-table persistence; no blanket removal
of empty tables, format migration, dependencies or unrelated review fixes.

Three real-browser tests in `tests/browser/src/sql_sync.rs` cover:

- Acknowledged initial subscription work followed by ordinary and explicit cold
  catalog SELECTs: no work published, persisted revision stays zero, including open.
- Cold SELECT-only transaction, independent handle DDL, stable old snapshot and
  successful reader commit: only DDL advances the revision, with no reader signal.
- Ordinary and explicit empty table/index DDL: physical logical tables survive
  closing all owners/reopen, both definitions replicate, and settled replay is
  silent with identical exported state and queryable empty tables/index catalogs.

Red/green: the silence test failed on the original executor, passed after the
fix; the conflict test was separately run against the restored original executor
and failed, then passed with the fix restored. The initial direct wasm-pack test
filter was rejected by cargo build; focused runs instead used Cargo with the
matching wasm-bindgen 0.2.128 runner. The first complete matrix found a test-only
expected-index-list omission (implicit primary-key indexes); corrected the
expectation, ran the focused empty-DDL test successfully, then reran all four cells.

Final evidence and commands are appended to `docs/browser-validation.md` under
“P2 cold-read review fix”. Full rerun: **90 passed** per Chrome/Firefox Window,
**87 passed** per Dedicated Worker. Seven Wasm feature checks/graphs, browser
Clippy/quickstarts, native SQL **70**, KV **19**, protocol/ownership **5** and runtime
**10** tests, native Clippy and formatting passed. Powerset again failed on the
previously documented eight `ofdb-btree --features sharded` errors; `just crap`
timed out at 240 seconds during coverage compilation. Neither broad gate is green.
