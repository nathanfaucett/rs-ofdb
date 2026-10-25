# 05: Make browser storage failures and connection lifecycle actionable

**What to build:** Both public facades fail clearly and safely when browser persistence is unavailable, an incompatible database is opened, or another browser context changes the physical IndexedDB schema version.

**Blocked by:** 03 — Run persistent SQL statements through the browser Query Handle.

**Status:** done

- [x] Validate physical format versions and store-kind metadata on open; unsupported formats fail without destructive reset or implicit migration.
- [x] Define and implement a bounded, documented policy for blocked opens and schema upgrades so callers receive an actionable result rather than wait indefinitely.
- [x] Close stale connections on version-change events and make subsequent operations fail clearly without silently reopening or losing staged transaction state.
- [x] Preserve Client clone ownership and usability after Database drop while storage remains valid.
- [x] Preserve structured facade errors for access, abort, conflict and commit failures where supported, with useful browser failure details that do not disclose stored values.
- [x] Persistent open failures never silently select volatile storage.
- [x] Test blocked opens and version-change closure across actual browser contexts, incompatible format rejection, independent database isolation and client lifetime behavior for both stores.
- [x] Test failed commits for no partial persistence. Use focused injection where actual quota exhaustion is impractical and distinguish injected failures from real quota validation.
- [x] Document origin scoping, private-mode limitations, quota, eviction and user-cleared storage. Do not equate IndexedDB completion with native filesystem durability or implement native retirement emulation.


## Comments

### Implementation and validation — 2026-10-25

Confirmed clean worktree `/home/nathan/code/rust/aicacia/ofdb-browser-05`, branch
`browser-indexeddb-05`, exactly at requested integration base `6336347` before
editing. Read AGENTS, GLOSSARY, feature spec, this ticket, previous ticket notes,
and browser harness. Loaded TDD and minimal-implementation skills. User explicitly
confirmed the facade Database → Client → Transaction seams, existing byte-storage
API for injected failure/atomicity checks, and real Window/Worker contexts.
Complexity/risk: moderate, concentrated in open queue cleanup, stale ownership
and atomic commit outcomes; priority: safe durable browser construction.

Only shared byte-adapter lifecycle/errors, new `storage_lifecycle.rs` and its own
JS helper, storage docs, this ticket and minimal harness wiring/README were
changed. No SQL engine/semantics tests, public facade/deadline/platform timer
operations, dependencies or locks were edited (tickets 04/06 keep their scopes).

Open now validates physical schema and format/kind/u64 revision metadata within
one bounded JS open lifecycle, instead of returning a connection then taking an
unbounded whole-database snapshot just to validate metadata. Exact stores,
out-of-line keys, no physical indexes/auto-increment, format and store kind are
required. Older experimental databases missing kind fail explicitly; no implicit
migration, destructive reset or memory fallback is provided.

Blocked events fail immediately; a five-second browser timer additionally bounds
queued opens and metadata validation. Browser throttling/suspension may delay
callback delivery; this is not a hard real-time deadline. Refused late upgrades
abort before creating stores, and late successful connections close. Connections
close on versionchange/deletion and unexpected close is tracked. New queries
and staged commits, including read-only/net-no-op commits, fail on closed handles;
there is no automatic reopen or replay. Existing Rust staging is not silently
rewritten or lost. Issued transactions retain their normal completion observer;
closure/cancellation does not prove rollback.

Browser errors retain known native failure names and actionable operation hints
but omit raw browser messages (which can contain keys/values), stacks and names.
Conflict retains the existing BTree/KV structured category; access/abort/quota and
other persistence failures retain existing facade Storage categories and safe
detail. SQL's existing model uses Storage for backend conflicts as well. No new
wire category or shared BTree error variant was introduced solely for this ticket.
The existing CommitFailed mapping is unchanged.

Storage docs cover origin/profile/partition scoping, private-mode limitations,
quota, eviction, user-cleared data, explicit retry, older experimental metadata
incompatibility, timer throttling and browser-completion versus native durability.
There is no filesystem retirement emulation.

#### Red/green and acceptance evidence

- Wrong-kind facade test failed in real Chrome because SQL accepted incorrect
  metadata; passed after atomically stored/validated kind metadata.
- Schema test failed on non-actionable missing-schema behavior; passed after
  explicit store/keyPath/autoIncrement/index validation. Expanded cases cover
  extra/missing stores, indexes, auto-increment, missing kind, format/version and
  out-of-range revision. Sentinel metadata survives every failed open.
- An actual peer worker holds a connection and ignores versionchange while its
  deletion is blocked. The facade's later open receives no blocked event. The
  bounded test failed before adding the timer; both facades now fail at five
  seconds and a late open cannot create storage after the peer is released.
- Versionchange test failed because a closed read-only staged SQL commit reported
  success; passed after guarding the byte Snapshot commit before its no-op path.
  Both stores' staged mutating/read-only commits, clones after Database drop,
  isolated same-name SQL/KV, unrelated names and incompatible reopens are covered.
- Revision-exhaustion test caught helper-name shadowing during implementation;
  fixed and passed with unchanged final revision/no partial table persistence.
- Access-denial injection verifies structured Storage errors and omission of a
  private-message sentinel for both facades, never memory fallback. Abort after
  a successful native request and quota exception after a queued request verify
  reopened SQL indexed rows and KV values remain unchanged with no partial insert.
  These are **injected failures**, not actual quota exhaustion validation.
- Existing full harness continues to cover cross-worker stale conflicts, issued
  commit cancellation, independent completion publication and failure/no-op
  invalidation silence. No duplicate SQL semantic implementation was introduced.

#### Passing checks

All commands below were run from the editor's `ofdb` root, using sibling
`--manifest-path ../ofdb-browser-05/...` / `../ofdb-browser-05/tests/browser`
arguments as applicable. Browser runs used:

`WASM_BINDGEN_TEST_WEBDRIVER_JSON=/home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/webdriver.local.json`

Chrome additionally used `--chromedriver
/home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/target/chromedriver-linux64/chromedriver`.
Full runs used `WASM_BINDGEN_TEST_TIMEOUT=120`; profile and 16 MiB Wasm stack
remain unchanged. Native spawn test used `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2`.

- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser`:
  **36 passed**, Window.
- `wasm-pack test --headless --firefox tests/browser`: **36 passed**, Window.
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features worker,sync`:
  **46 passed**, Dedicated Worker.
- `wasm-pack test --headless --firefox tests/browser --features worker,sync`:
  **46 passed**, Dedicated Worker.
- Separate focused `lifecycle_` runs: **7 passed** in Chrome and Firefox Window.
  Worker full runs include these same 7 lifecycle tests and actual peer workers.
- `cargo test --manifest-path tests/browser/Cargo.toml --test native --features sync`:
  **2 passed**, native clients/transactions and sync remain spawnable/Send+Sync.
- `cargo clippy -p ofdb-btree-indexeddb --target wasm32-unknown-unknown -- -D warnings`.
- `cargo clippy --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync -- -D warnings`.
- Workspace and fixture `cargo fmt --all -- --check`, and `git diff --check`.

#### Incomplete broader gates / truthful limits

- A combined browser command group hit its 240-second bound while the final
  Firefox Worker WebDriver session was starting. Chrome Worker had completed
  with 46 passes in that group. Separate subsequent Firefox Window and Worker
  full commands completed successfully as listed above; the timed-out group is
  not itself reported as passing.
- `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 cargo hack test --feature-powerset --all-targets`:
  **timed out at 120 seconds**, progressing through ofdb-btree combinations
  (7/1035). This is not a complete feature-matrix pass or a diagnosed code failure.
- `just crap`: **timed out at 120 seconds**, during coverage collection at
  ofdb-value (6/32); no complete complexity/coverage score is claimed.
- Native SQL `cargo test -p ofdb-sql --features in-memory,sql,redb --test local_sql`:
  **timed out at 120 seconds compiling dependencies**, before tests ran. The
  chained native `kv_protocol` command therefore did not execute. Browser sync
  fixtures and the two native spawn regressions did pass as listed above.
- A focused native `cargo hack test -p ofdb-btree-indexeddb --feature-powerset
  --all-targets` attempt also timed out at 120 seconds after waiting on the shared
  build lock/compiling Tokio; no native matrix pass is claimed.
- No WebKit, real private-mode eviction, real quota exhaustion, browser-process
  crash, OS restart or power-loss testing. No durability equivalence claim.

Ticket-05 acceptance is complete. Broader native matrix/coverage gates remain
incomplete as explicitly recorded. Integration tip will be merged before handoff;
no integration advancement was present at the last pre-commit check (`6336347`).
