# 02: Persist KV queries and explicit transactions in IndexedDB

**What to build:** A browser application asynchronously opens a named KV Embedded Handle, obtains its existing Query Handle, reads and writes typed values, transacts across browser event-loop turns, and reopens committed state after restart. This is the first durable vertical slice of the browser IndexedDB specification.

**Blocked by:** 01 — Make embedded storage safe for browser-local execution.

**Status:** done

- [x] Add the opt-in indexeddb facade feature and asynchronous Database::open_indexeddb(name) constructor. Default features remain empty and failed persistent opens never fall back to memory.
- [x] Implement shared byte persistence at the existing BTree seam using a fixed physical IndexedDB schema for logical tables, byte records and database metadata. Evaluate actual binding capabilities and features before adding minimal dependencies.
- [x] Preserve existing document-key encoding, shared typed values, nested JSON, exact numeric representations, ordered scans, literal prefixes, expiry, generations and Tombstones.
- [x] Verify byte-lexicographic ordering for empty keys, embedded zeros, prefixes and Unicode-derived keys without converting arbitrary binary keys to JavaScript strings.
- [x] Explicit transactions use one consistent committed snapshot and revision plus Rust-owned staging. Reads and scans observe that snapshot and pending writes across unrelated browser awaits.
- [x] Commit checks the persisted revision, applies the complete write set and advances revision metadata atomically inside one short IndexedDB read-write transaction. Independent handles and tabs cannot silently overwrite newer commits.
- [x] Schedule dependent requests within IndexedDB's active request/event lifecycle; no keepalive loops, unrelated awaits or caller work occur inside native persistence transactions.
- [x] Read-only and net byte no-op commits do not advance revision metadata. Rollback and dropping uncommitted staging persist nothing. A stale mutating transaction returns a conflict without automatic replay.
- [x] Report success only after native transaction completion, not request success. Independently observe issued commit completion after caller cancellation so later committed invalidation integration cannot lose that outcome.
- [x] Test aborts and commit failures for all-or-nothing persistence, transactions across event-loop delays, independent-handle/tab races, and actual page/worker restart persistence including Tombstones and history.
- [x] Document and measure the initial O(database-size) snapshot materialization cost; do not claim selective sync implies selective physical reads.


## Comments

### Implementation and validation — 2026-10-25

Confirmed clean worktree `/home/nathan/code/rust/aicacia/ofdb-browser-02`, branch
`browser-indexeddb-02`, based exactly on integration `browser-indexeddb` at
`42698d2195c2dc74da5b0bbe3f2a5f1884096411` (ticket 01) before editing. Ticket 02 owned
all changes; no other implementers were active. Complexity/risk: high (native
transaction lifetime, multi-context revision races and cancellation); priority:
first durable browser vertical slice. Loaded the TDD and minimal-implementation
skills; tested the ticket's agreed BTree, KV facade and native-history seams.

Added shared `ofdb-btree-indexeddb` byte persistence, database-wide Snapshot staging
and the byte BTree adapter. All logical tables, records and revision metadata
share one native transaction for reads and one for a mutating commit. Native
request callbacks schedule dependent work synchronously; no keepalive or unrelated
await runs inside a persistence transaction. Decimal u64 revisions are compared
and incremented exactly using BigInt, with overflow rejected. Empty-table changes
are transactional metadata, not IndexedDB upgrades. Binary compound keys retain
empty/NUL/prefix ordering without stringifying arbitrary bytes.

The KV facade has an explicit `indexeddb` feature and target-gated asynchronous
`Database::open_indexeddb(name)`. Existing KV value/document/history codecs,
expiry clock, generation and Tombstone semantics are retained. Query handles own
storage after Embedded Handle drop. Failed opens and closed/versionchanged
connections return errors; no memory fallback. Native/default facade behavior
and thread-safety contracts are unchanged.

Issued commits attach completion to an independent local task. It retains only
the native operation/connection and committed invalidation inputs, not the
Rust snapshot. Publication and caller success happen after native completion,
never request success. Cancellation cannot discard that observation. Read-only,
net no-op, failed and rolled-back staging do not publish. Table drops include
conservative table invalidation, even when recreated in the same staging batch.

Binding evaluation: ran `cargo info indexed_db_futures` (0.6.4) and `cargo info
idb` (0.6.5), inspected actual feature manifests and transaction/listener source.
The small platform callback adapter avoids adding binding/serialization/executor
packages and Rust callback leaks; exact features, tradeoffs and limitations are
recorded in `crates/btree-indexeddb/README.md`. No unsafe thread-safety assertions.

#### Red/green evidence

- First public-facade test failed to compile: `Database::open_indexeddb` missing.
  The same test passes in real browsers after implementation.
- The byte net-no-op test failed in Firefox: revision **1**, expected **0**, after
  insert/remove in a previously absent BTree table. Fixed implicit empty-table
  staging; the same test now passes and leaves no table or revision change.
- Fault injection aborts after an actual record request succeeds, and separately
  throws QuotaExceededError after an earlier record request has been queued.
  Both reject commit and retain the old records, table metadata and revision.
- Independent Wasm workers stage from the same revision before either commits:
  exactly one persists and one returns Conflict. Context restart tests destroy
  the writer iframe/worker and load a fresh Wasm instance; values, generation IDs,
  heads and native changes survive. Stale history cannot revive a persisted
  Tombstone; recreation selects a new generation.

#### Passing validation

Commands below are relative to the worktree root. Because the editor exposes the
integration repository as a root, actual invocations used its sibling via
`--manifest-path ../ofdb-browser-02/...`, fixture path arguments, or
`--justfile ../ofdb-browser-02/justfile`.

Browser commands used
`WASM_BINDGEN_TEST_WEBDRIVER_JSON=/home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/webdriver.local.json`.
Chrome used `--chromedriver /home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/target/chromedriver-linux64/chromedriver`.
These ignored, machine-local ticket-01 resources were reused, not committed.
The fixture resolves wasm-bindgen 0.2.128; workspace Wasm compile also passed with
its existing wasm-bindgen 0.2.125 lock. Verified browser executable versions again.

- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser`:
  **20 passed**, Chrome **156.0.8078.47**, Window.
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features worker,sync`:
  **19 passed**, same Chrome, Dedicated Worker.
- `wasm-pack test --headless --firefox tests/browser`:
  **20 passed**, Firefox **158.0.1**, Window.
- `wasm-pack test --headless --firefox tests/browser --features worker,sync`:
  **19 passed**, same Firefox, Dedicated Worker.
- `cargo check --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown`.
- `cargo check --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync`.
- `cargo check -p ofdb-kv --no-default-features --features indexeddb --lib --target wasm32-unknown-unknown`:
  standalone browser backend without the fixture's in-memory feature.
- `cargo check -p ofdb-kv --no-default-features --features indexeddb --lib`:
  native feature selection compiles without browser exports.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path tests/browser/Cargo.toml --test native --features sync`:
  **2 passed**, including native Send/Sync and spawned query/transaction/sync checks.
- `cargo test -p ofdb-kv --features in-memory --test embedded_client`:
  **3 passed**.
- `cargo clippy -p ofdb-kv -p ofdb-btree-indexeddb --features in-memory,indexeddb,sync -- -D warnings`.
- `cargo clippy --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync -- -D warnings`.
- `cargo tree --manifest-path tests/browser/Cargo.toml --target wasm32-unknown-unknown --edges normal --prefix none --format '{p}' --features sync`:
  programmatically checked absent Redb, btree-redb, engine-redb, tonic, Iroh,
  tempfile and native SQL/KV server/protocol packages.
- `cargo tree -p ofdb-kv --no-default-features --edges normal --depth 1`:
  only base64, ofdb-value and serde_json; no embedded/browser platform dependency.
- Workspace and fixture `cargo fmt --all -- --check`; `git diff --check`.

Snapshot diagnostic (direct cached wasm-bindgen-test-runner with compiled
Dedicated Worker module, filter `snapshot_materialization_measurement`,
`--nocapture`, the same WebDriver environment and CHROMEDRIVER path):
**1 passed**, Chrome 156: **100 records / 102,800 raw bytes / 11 ms**;
**1,000 records / 1,028,000 raw bytes / 96 ms**. Values are 1 KiB and keys 4 bytes.
One sample per size, not a performance guarantee or peak-heap measurement.
Snapshot reads/copies scale with whole-database size and ordered reconstruction
adds O(N log N) CPU work. Two Rust copies plus transient IDB/JS data are retained.

#### Failures, limits and outstanding work

- `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 cargo hack test --feature-powerset --all-targets`:
  **failed**, stopped on `ofdb-btree --no-default-features --features sharded`
  tests with the same 8 missing in-memory export/no_std String/ToString errors
  documented and base-reproduced in ticket 01. No BTree implementation changed
  here. The remaining workspace feature matrix is **not** claimed passing.
- `CARGO_BUILD_JOBS=2 just crap`: **timed out after 240 seconds** during
  coverage compilation (Automerge/BTree-Automerge). No completed CRAP score.
- Initial browser compilation exceeded 120 seconds; one later concurrent-build
  attempt exceeded 180 seconds waiting for a Cargo build lock. Initial Firefox
  startup also returned WebDriver HTTP 500/SIGKILL. Subsequent final runs above
  completed successfully. A first attempted wasm-pack test filter was rejected
  by cargo build; measurement filtering used the runner directly instead.
- Same documented optimized fixture/16 MiB linear stack as ticket 01. No claim of
  minimum/default-stack SQL support or a release memory ceiling.
- Page restart means destruction/recreation of a same-origin iframe Window;
  worker restart means terminate/new Worker with a fresh Wasm instance. No whole
  browser-process/OS crash, power-loss durability, Safari/WebKit, or actual disk
  quota exhaustion validation. Quota and access failures use fixture-only native
  API fault injection. Browser eviction/site-data deletion remain platform limits.
- Committed invalidation is local to clones of one opened byte adapter. Cross-tab
  notifications, public facade continuous-sync wiring, SQL persistence/Kernel
  adaptation and browser/native sync interoperability are later-ticket work.
  IndexedDB `sync_changes()` remains Unsupported at this ticket boundary.
- Global revision conflict detection and whole-database snapshot loading are
  deliberate initial constraints; no automatic retry, selective physical I/O,
  migration or quota reservation is implied.

All ticket-02 behavior is validated as above; broader release/complexity gates
remain explicitly incomplete, not silently waived. Integration tip is merged into
the implementation branch before reporting (already the confirmed base unless it
advances during finalization).
