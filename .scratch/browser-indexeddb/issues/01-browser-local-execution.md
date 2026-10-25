# 01: Make embedded storage safe for browser-local execution

**What to build:** Run existing in-memory SQL and KV queries in a single-threaded browser Wasm environment without a Tokio runtime, while preserving native thread-safety guarantees. This is the initial cross-cutting portability prefactor for the browser IndexedDB specification.

**Blocked by:** None (can start immediately).

**Status:** done

- [x] Use a consistent target-aware strategy for storage traits, SQL Kernel, RowCodec, Row Reconciler, stream types, boxed transaction callbacks and sync staging. Browser-local JavaScript handles and futures must not require unsafe Send/Sync assertions.
- [x] Replace affected declarations and call sites coherently; do not retain temporary compatibility interfaces or duplicate browser engines.
- [x] Native builds retain existing Send/Sync contracts and support for spawned query and sync work.
- [x] Public in-memory SQL and KV Query Handles execute representative reads, writes and transactions in real browser tests without Tokio.
- [x] Verify browser wall-clock timestamps, KV expiry, UUID randomness and Automerge actor generation at runtime, with necessary feature propagation.
- [x] Add focused Wasm compile checks and a real-browser smoke-test harness usable by subsequent tickets in Window and Dedicated Worker contexts.
- [x] Supported browser checks do not acquire native-only development dependencies; target-gate those dependencies or isolate browser consumer tests as needed.
- [x] Run focused native regressions and record exact validation results. Compilation alone is not proof of browser clock or randomness support.


## Comments

### Implementation and validation — 2026-10-25

Confirmed clean worktree `ofdb-browser-01`, branch `browser-indexeddb-01`, based
exactly on integration branch `browser-indexeddb` at
`c69dc129459a1cb0ad999c8b817a2dd573512c72` before editing. No other agents edited
this ticket. Complexity/risk: high cross-cutting bounds; priority: foundational.
The ticket's existing facade, BTree and Kernel seams were used for testing.

Implemented one shared `ofdb-platform` execution contract. Native marker names
alias real `Send`/`Sync`; only browser Wasm accepts local handles/futures.
Storage, Kernel, RowCodec, Row Reconciler, streams, boxed engine callbacks,
checked sync callbacks/validators and sync staging use that contract coherently.
Removed obsolete unsafe thread-safety assertions instead of asserting JavaScript
handles are transferable. Replaced catalog stream boxing with `Either`.

Browser wall time uses `Date.now`, UUID JS support and Automerge browser randomness
propagate on the target, and facade deadlines/continuous retry timing share
platform timers. Native timeouts retain Tokio behavior. Facade defaults remain
empty and the platform dependency is optional there. In-memory construction no
longer directly enables facade Tokio timer dependencies.

The isolated consumer harness is documented in `tests/browser/README.md` and is
reusable for subsequent tickets. It tests SQL/KV ownership, queries, explicit
commit/rollback, expiry, browser event-loop awaits, deadlines, UUID timestamps,
distinct UUIDs and Automerge actor IDs. Non-Send `Rc` storage, values, streams and
boxed callbacks exercise the target-aware bounds. Runtime assertions distinguish
Window from Dedicated Worker.

#### Passing validation

Commands here are expressed relative to the worktree root. The editor exposes the
integration repository as a root, so commands were actually invoked from its
sibling with `--manifest-path ../ofdb-browser-01/...` or a fixture path argument.
Browser commands used `WASM_BINDGEN_TEST_WEBDRIVER_JSON` pointing to the ignored
machine-local capabilities file, selecting installed Flatpak browser binaries.

- `wasm-pack test --headless --firefox tests/browser`: **6 passed**, Firefox
  **158.0.1**, Window.
- `wasm-pack test --headless --firefox tests/browser --features worker,sync`:
  **6 passed**, Firefox **158.0.1**, Dedicated Worker; sync features compiled.
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser`:
  **6 passed**, Google Chrome **156.0.8078.47**, Window.
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features worker,sync`:
  **6 passed**, same Chrome, Dedicated Worker; sync features compiled.
  `DRIVER` was the matching **156.0.8078.47** chromedriver extracted under ignored
  `tests/browser/target/chromedriver-linux64/chromedriver`. Firefox used
  wasm-pack's cached geckodriver. The harness lock resolves wasm-bindgen **0.2.128**,
  matching the cached tool used by wasm-pack.
- `cargo check --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown`.
- `cargo check --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync`.
- `cargo check -p ofdb-sql -p ofdb-kv --features in-memory,sql,sync --target wasm32-unknown-unknown`.
- `cargo tree --manifest-path tests/browser/Cargo.toml --target wasm32-unknown-unknown --edges normal --prefix none --format '{p}' --features sync`:
  checked absent Redb, engine-redb, tonic, Iroh, tempfile and native SQL/KV
  server/protocol packages. Native fixture dev dependencies are target-gated.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path tests/browser/Cargo.toml --test native --features sync`:
  **2 passed**, including spawned SQL/KV queries, explicit transactions and sync
  futures, plus native Database/Client `Send + Sync` assertions.
- `cargo test -p ofdb-kv --features in-memory --test embedded_client`:
  **3 passed**.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features in-memory,sql,redb --test local_sql explicit_transactions_span_api_calls`:
  **1 passed**, 18 filtered.
- `cargo test -p ofdb-btree -p ofdb-btree-automerge -p ofdb-sql-engine -p ofdb-sync-runtime --lib`:
  **22 passed** (8 + 2 + 3 + 9).
- `cargo check --workspace`.
- `cargo check -p ofdb-btree -p ofdb-sql-engine --no-default-features`.
- `cargo tree -p ofdb-sql -p ofdb-kv --no-default-features --edges normal --depth 1`:
  neither default facade acquired platform timers or embedded storage.
- `cargo clippy -p ofdb-platform -p ofdb-sql -p ofdb-kv --features in-memory,sql,sync -- -D warnings`.
- `cargo clippy --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync -- -D warnings`.
- `cargo fmt --all -- --check`, fixture `cargo fmt --all -- --check`, and
  `git diff --check`.

#### Failures, limits and unclaimed validation

- Red phase: initial browser consumer target compilation failed in getrandom
  because browser randomness was not enabled. The final runtime checks above
  validate the fix; compilation alone was not used as acceptance evidence.
- **Stack requirement:** default-stack SQL trapped with an out-of-bounds Wasm
  access; native SQL also overflowed its default thread stack. The fixture uses
  an optimized dev build and a **16 MiB linear stack**, matching the existing
  bounded-stack native SQL harness. Native SQL/spawn checks use
  `RUST_MIN_STACK=16777216`. These are documented supported test configurations,
  not claims of default-stack support or a measured minimum/release memory
  ceiling. Reducing existing SQL async construction stack usage is separate work.
- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`:
  **failed**, stopped on `ofdb-btree --no-default-features --features sharded`
  tests: missing in-memory exports and no_std `String`/`ToString` imports.
  Reproduced the same 8 errors on untouched integration/base `c69dc12` using
  `cargo test --locked -p ofdb-btree --no-default-features --features sharded --all-targets`.
  This pre-existing matrix failure is not fixed here; the remaining matrix was
  not run to completion.
- `just crap` (actually `just --justfile ../ofdb-browser-01/justfile crap`):
  **timed out at 120 seconds** during coverage compilation, while processing
  BTree/Automerge dependencies. No CRAP score or complete coverage result exists.
- Initial build/tool-install attempts also hit 120-second limits. Chromium
  download via Playwright timed out; the official ChromeDriver zip download
  timed out at 90 seconds and was successfully resumed, enabling the actual
  Chrome runs above. No Playwright browser run is claimed.
- No WebKit/Safari, IndexedDB persistence/reload, native/browser sync exchange or
  full release-feature matrix validation is claimed for ticket 01. Those remain
  subsequent-ticket/spec work. There is no remaining ticket-01 behavioral blocker
  in the documented stack configuration; broader matrix/complexity gates remain
  incomplete as described above.
