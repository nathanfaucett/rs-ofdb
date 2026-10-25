# 03: Run persistent SQL statements through the browser Query Handle

**What to build:** A browser application asynchronously opens a named SQL Embedded Handle, obtains its existing Query Handle, creates a table, inserts and queries rows, and reopens those rows after restart using IndexedDB.

**Blocked by:** 02 — Persist KV queries and explicit transactions in IndexedDB.

**Status:** done

- [x] Implement the SQL IndexedDB Kernel adapter using the shared byte persistence implementation and the existing database-wide Kernel transaction seam.
- [x] Add the opt-in indexeddb facade feature and asynchronous Database::open_indexeddb(name) constructor without changing native path-based construction or empty default features.
- [x] Support typed statement execution and optional SQL-text translation through the existing Client interface; Database does not gain ordinary query methods.
- [x] Persist catalog, row, Tombstone and derived Index Record mutations together in one database-wide commit, never separately committed logical-table transactions.
- [x] Represent logical SQL table creation and deletion as transactional data changes, not physical IndexedDB object-store upgrades.
- [x] Preserve existing SQL key/value codecs and Automerge Row Reconciler behavior rather than introduce a separate browser SQL data model.
- [x] Isolate SQL, KV and unrelated named databases through physical namespaces or validated store-kind metadata, rejecting incompatible reuse without replacement.
- [x] Real-browser facade tests cover typed and SQL-text creation, insertion, selection, failed statement-batch atomicity and persistence after closing all owners and restarting a browser context.
- [x] Supported SQL browser dependency graphs exclude Redb, native filesystem ownership and native networking. Native SQL behavior remains available and unchanged.


## Comments

### Implementation and validation — 2026-10-25

Confirmed clean worktree `/home/nathan/code/rust/aicacia/ofdb-browser-03`, branch
`browser-indexeddb-03`, based exactly on integration `browser-indexeddb` at
`64f6ee544a902adfb63bfc6d96fb58741edd51f9` (tickets 01/02) before editing.
Loaded TDD and minimal-implementation skills; read AGENTS, GLOSSARY, specification,
tickets 01/02 and browser README. Used the specification's public SQL facade and
Kernel testing seams. Complexity/risk: moderate adapter work over ticket 02's
high-risk atomic persistence; priority: durable SQL vertical slice.

Added `ofdb-sql-engine-indexeddb`: a thin existing-Kernel implementation over the
shared byte adapter's database-wide Snapshot. Logical-table DDL, catalogs, index
records, Automerge rows/history and Tombstones use one revision-checked commit.
No per-table persistence transactions, new row model, physical DDL upgrades,
unsafe bounds, serializer framework, native-only ownership or new third-party
packages. SQL uses the disjoint `ofdb:3:sql:<name>` physical namespace.

The facade's empty-default opt-in `indexeddb` feature enables target-gated async
`Database::open_indexeddb(name)`, existing Client/Transaction typed operations,
optional translation helpers, and conflict inspection/Resolution dispatch.
Native constructors and thread-safety remain unchanged. Queries remain exclusively
on Client/Transaction. Failed opens do not fall back to memory. Existing SQL errors
categorize backend conflicts and commit failures as Storage, with backend detail;
no new remote wire category or automatic replay was introduced.

SQL browser tests are isolated in `sql_indexeddb.rs` and
`sql_indexeddb_helpers.js`. No KV facade, KV sync, KV test, or shared byte adapter
file was edited. Shared harness edits are limited to the SQL dependency/features,
SQL Kernel/schema/query test dependencies, one module declaration, matching lock
entries, and a short README intro correction plus appended SQL section. These
shared Cargo.toml/README/lock/lib.rs hunks may overlap ticket 07; retain both SQL
and KV additions if integration advances. No merge conflict occurred at the
integration tip checked during this work.

#### Red/green and browser evidence

The first public-facade reopen test failed to compile because
`Database::open_indexeddb` was missing; it passes after implementation in actual
browsers. Subsequent slices exercise typed creation/insertion/selection without
translation, SQL-text execution, closing every storage owner before reopen,
failed statement-batch catalog/index/row atomicity, unique constraints, stable
snapshots across real timer awaits, stale writers, rollback/drop/aborted explicit
transactions, table drop/recreation and SQL/KV/name isolation. Kernel tests cover
binary empty/NUL/prefix/high-byte keys, bounded scans, missing tables, atomic
multi-table staging, rollback and stale read-only commit.

Fault injection reuses ticket 02's fixture-only native API hooks: abort after an
actual record request succeeds; separately throw QuotaExceededError after an
earlier record write has been queued. Neither leaves catalog/index names or rows
persisted. Fresh module workers and fresh same-origin iframe pages verify indexed
reads, unique constraints and deleted UUID Tombstones after the writer context
has been terminated/removed. This is a new Wasm instance, not a handle-only reopen.

Two test-harness mistakes were corrected during development: trying to update an
immutable primary key (the engine correctly rejected it; tests now update a
non-key indexed column), and a static import nested inside the iframe probe's
try block (changed to dynamic import). Initial failing browser runs are not
claimed passing; final reruns below passed.

#### Passing validation

Commands are relative to this worktree. Because the editor exposes the integration
repository rather than this sibling as a root, actual calls used `--manifest-path
../ofdb-browser-03/...`, fixture path arguments or `--justfile` as appropriate.
Browser commands reused ignored machine-local ticket-01 resources:
`WASM_BINDGEN_TEST_WEBDRIVER_JSON=/home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/webdriver.local.json`
and Chrome `--chromedriver
/home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/target/chromedriver-linux64/chromedriver`.
The fixture retains wasm-bindgen 0.2.128 and the documented optimized dev profile
with a 16 MiB Wasm linear stack.

- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser`:
  **29 passed**, Window, Chrome 156 driver/browser installation from ticket 01.
- `wasm-pack test --headless --firefox tests/browser`: **29 passed**, Window.
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features worker,sync`:
  **27 passed**, Dedicated Worker; both facade sync features compiled.
- `wasm-pack test --headless --firefox tests/browser --features worker,sync`:
  **27 passed**, Dedicated Worker. Window-only page tests are excluded.
- `cargo check -p ofdb-sql --no-default-features --features indexeddb --lib --target wasm32-unknown-unknown`.
- Same check with `--features indexeddb,sql` (optional translation).
- `cargo check -p ofdb-sql --no-default-features --features indexeddb --lib`
  (native feature selection, without browser exports).
- `cargo check --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync`.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features in-memory,sql,redb --test local_sql`:
  **19 passed**, including native indexed CRUD/reopen and explicit transactions.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path tests/browser/Cargo.toml --test native --features sync`:
  **2 passed**, native spawned operations/sync and Send/Sync assertions.
- `cargo clippy -p ofdb-sql -p ofdb-sql-engine-indexeddb --features in-memory,sql,redb,indexeddb,sync -- -D warnings`.
- `cargo clippy --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync -- -D warnings`.
- Browser normal dependency graph with sync programmatically checked to exclude
  Redb, btree-redb, engine-redb, tonic, Iroh, tempfile, SQL/KV server and protocol
  packages. Default SQL dependency tree retains only query/schema/value/uuid.
- Workspace and fixture `cargo fmt --all -- --check`; `git diff --check`.

#### Limits and incomplete broader gates

- `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 cargo hack test --feature-powerset --all-targets`:
  **failed** on `ofdb-btree --no-default-features --features sharded` with the
  same 8 missing in-memory export/no_std String/ToString errors documented and
  base-reproduced in ticket 01. No BTree implementation changed. Remaining
  workspace feature combinations were not completed.
- `CARGO_BUILD_JOBS=2 just crap`: **timed out at 240 seconds** during native
  BTree-Redb coverage compilation, package 5/32. No completed CRAP score.
- Initial fresh standalone dependency compilation timed out at 180 seconds; a
  subsequent combined check/native-test build timed out at 240 seconds while
  compiling the native SQL test binary. Later standalone checks and the full
  focused native SQL suite completed successfully.
- Snapshot cost remains O(database size), including two Rust copies and transient
  browser materialization. No new peak-memory/SQL snapshot performance claim.
- No WebKit/Safari, whole-browser/OS restart, crash/power-loss durability, actual
  disk-quota exhaustion, minimum/default-stack support, or release memory ceiling
  is claimed. Quota/abort cases are injected through actual native IDB requests.
- SQL browser/native sync interoperability, conflict convergence under incoming
  sync, and facade committed subscriptions remain ticket 08 work; IndexedDB
  `sync_changes()` returns Unsupported. Kernel byte commit completion/invalidation
  is already independently observed by the reused ticket-02 adapter.
- No cross-tab wakeups, fine-grained conflict detection, implicit migration,
  automatic query replay or native networking support in browsers.

Ticket-03 acceptance behavior is complete; broader matrix/complexity gates are
explicitly incomplete as above. Integration tip is merged before final handoff;
its last checked revision remains the confirmed base unless advanced concurrently.
