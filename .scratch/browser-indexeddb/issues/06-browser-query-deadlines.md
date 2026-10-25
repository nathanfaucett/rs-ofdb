# 06: Support query deadlines on browser timers

**What to build:** SQL and KV callers configure optional query and transaction deadlines that work in the browser without a Tokio runtime, while native deadline behavior remains intact.

**Blocked by:** 03 — Run persistent SQL statements through the browser Query Handle.

**Status:** done

- [x] Use a small browser-compatible timer capability that can also support continuous sync retry timing; avoid a new general runtime framework.
- [x] Preserve the existing public with_deadline behavior, no fixed default deadline and independent settings on cloned clients.
- [x] SQL-text execution retains one deadline budget for translation plus execution; deadline semantics for transaction operations remain consistent with existing facade contracts.
- [x] Untimed and timed browser queries do not require a Tokio runtime. Native consumers retain existing native timer behavior.
- [x] Real-browser tests cover pending work timeout, independent client clones, SQL translation budgeting and explicit transaction operations for both facades.
- [x] Test cancellation before persistence starts and timeout during an issued commit. Observe completion independently of the caller so committed state is not mistaken for rollback.
- [x] Document that timeout or cancellation does not prove rollback, undo a completed write or guarantee hard interruption of computation.


## Comments

### Implementation and validation — 2026-10-25

Confirmed clean worktree `/home/nathan/code/rust/aicacia/ofdb-browser-06`, branch
`browser-indexeddb-06`, based exactly on integration `browser-indexeddb` at
`6336347` before editing. Used the requested public SQL/KV Client/Transaction
seams and fixture-only native IndexedDB fault injection. Complexity: low production
change, moderate test coverage; risk: high around cancellation/commit ambiguity;
priority: required browser facade correctness. Ticket05 owns persistence lifecycle;
ticket04 owns SQL semantics/engine. Neither implementation was edited. No new
dependencies, runtime framework, engine wrapper, or persistence API was needed.

Ticket01 already wired facade query/transaction deadlines and continuous retry
through `ofdb-platform`. The failing vertical slice exposed the remaining bug:
browser `sleep` armed its timer only when polled, after the operation's first poll.
Synchronous SQL translation therefore postponed timeout registration. The timer is
now armed before the operation is polled, retaining fractional-millisecond rounding,
signed-32-bit delay chunking and cancellation on future drop. Native timer re-exports
remain unchanged. Existing facade wrappers already preserve one translation/execution
budget and copied, independent clone settings, so no duplicate plumbing was added.

Added ten browser deadline tests in the new `tests/browser/src/deadlines.rs`:
public pending query entry points (including conflict inspection/resolution),
transaction-creation timeouts, inherited per-operation transaction budgets across
caller awaits, SQL pending translation in explicit transactions, translation CPU
and translation-plus-persistence budgeting, independent short/long/untimed clones,
pre-persistence timeout/cancellation, and both public query and explicit-transaction
commits after timeout/cancellation. Checks include ready zero-deadline work,
sub-millisecond pending work and a `Duration::MAX` wait that must not wrap early.

`deadline_helpers.js` holds only adapter completion observation. A separate native
`complete` listener observes actual persistence; native request scheduling and
atomic commit are not mocked. Cancellation uses ticket07's existing `afterNextWrite`
helper to stop polling during an actual issued write. After caller exit, releasing
observation allows independent KV committed publication (with `sync`); both stores
are independently reopened and their whole committed write checked. A native commit
can already have completed while observation is held: these tests intentionally
exercise ambiguous *commit observation*, not delayed physical disk persistence.
SQL browser `sync_changes()` is unsupported at this base and belongs to ticket08;
SQL completion uses the native observer plus reopen, not a claimed SQL subscription.

Updated public `with_deadline` docs, README, glossary's obsolete native-only timer
statement, and the harness documentation. Browser timers remain cooperative and can
be throttled. Timeout/cancellation does not prove rollback, undo completed writes,
or hard-interrupt computation. Explicit transactions have fresh operation budgets,
not a transaction-lifetime timeout; connection setup and caller idle time are excluded.

#### Red → green

- New CPU/pending SQL translation test on the unchanged timer: Chrome **29 passed,
  1 failed**; the timed DDL wrongly succeeded. With the timer fix: **30 passed**.
- Subsequent public-API coverage was added in vertical slices and run in Chrome.
- An initial SQL subscription assertion exposed the existing `Unsupported` result;
  it was replaced with the ticket06 independent native-completion/reopen check,
  keeping ticket08 scope separate rather than changing lifecycle/engine code.

#### Passing final validation

Commands below are worktree-relative. The editor mounts the sibling integration
root, so actual invocations used absolute `--manifest-path`/fixture paths. Browser
capabilities came from ignored ticket01 `tests/browser/webdriver.local.json`, selecting
installed Flatpak browsers; Chrome used ticket01's matching ignored chromedriver.
No machine-local browser configuration or binaries were committed.

- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features sync`:
  **51 passed**, Google Chrome **156.0.8078.47**, Window; independently reran the built
  Window module with `wasm-bindgen-test-runner`: **51 passed**.
- `wasm-pack test --headless --firefox tests/browser --features sync`:
  **51 passed**, Firefox **158.0.1**, Window.
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features worker,sync`:
  **49 passed**, same Chrome, Dedicated Worker.
- `wasm-pack test --headless --firefox tests/browser --features worker,sync`:
  **49 passed**, same Firefox, Dedicated Worker.
- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser` without
  `sync`: **39 passed**, Chrome Window, after final module wiring.
- Browser suites create no Tokio runtime. Tokio synchronization primitives remain
  part of notification dependencies, not browser timing/runtime requirements.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path tests/browser/Cargo.toml --test native --features sync`:
  **2 passed**, native Send/Sync and spawned query/transaction/sync regressions.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-kv --features in-memory --lib deadline_wraps_pending_work_and_is_independent_after_clone`:
  **1 passed**, 3 filtered.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features in-memory,sql --test client deadline_covers_translation_and_is_independent_per_clone`:
  **1 passed**, 3 filtered.
- `cargo clippy -p ofdb-platform -p ofdb-sql -p ofdb-kv --features in-memory,sql,sync -- -D warnings`.
- `cargo clippy --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync -- -D warnings`.
- Minimal `cargo check --no-default-features --target wasm32-unknown-unknown` for
  `ofdb-kv --features indexeddb`, `ofdb-sql --features indexeddb` (typed only), and
  `ofdb-sql --features indexeddb,sql`.
- Browser normal dependency tree with `sync`: checked absent Redb, native engine,
  tonic, Iroh, tempfile and SQL/KV server packages.
- Workspace and isolated fixture `cargo fmt --all -- --check`; `git diff --check`.
- `git merge --no-edit browser-indexeddb`: **Already up to date**, integration still
  `6336347` when checked before recording completion.

#### Broader gates and limits

- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`:
  **failed** at `ofdb-btree --no-default-features --features sharded`, eight existing
  compilation errors (std/async_stream/in-memory exports and ToString imports).
  Reproduced those same eight errors with
  `cargo test -p ofdb-btree --no-default-features --features sharded --all-targets`
  in untouched integration `6336347`. Remaining feature combinations were not run.
- `just crap`: **timed out at 120 seconds**, during coverage compilation at
  `ofdb-btree-redb` (5/32). No complete coverage result or CRAP score is claimed.
- One Firefox worker attempt timed out loading Wasm while heavy validation ran;
  a separate repeat at the same 120-second command limit passed all **49** tests.
  Initial native SQL compilation and a batch of minimal Wasm checks also reached
  120-second limits; separate subsequent invocations completed as listed above.
- The harness retains its documented **16 MiB Wasm stack** and native SQL/spawn
  checks use `RUST_MIN_STACK=16777216`; no default-stack or minimum-stack claim.
- No WebKit/Safari run, physical quota exhaustion, power-loss guarantee, SQL browser
  subscription support, or completed workspace-wide feature/complexity gate is claimed.

A supplemental default-feature Firefox run, following the passing default Chrome
run in one 120-second command, timed out during page navigation before reporting
any tests. No default-feature Firefox pass is claimed; the complete Firefox Window
and Worker suites with `sync` passed as recorded above.
