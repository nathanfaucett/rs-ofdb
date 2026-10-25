# 09: Run continuous browser synchronization with cancellation and retry

**What to build:** Both facades expose the existing shared change-driven sync lifecycle in the browser, driven by browser-compatible timers and an explicitly polled caller-owned runner.

**Blocked by:** 06 — Support query deadlines on browser timers; 07 — Synchronize browser KV replicas using the existing protocol; 08 — Synchronize browser SQL replicas using the existing protocol.

**Status:** done

- [x] The shared ChangeSubscription runner performs initial reconciliation, commit coalescing and retry/backoff without requiring Tokio in the browser.
- [x] Preserve retry timing from 100 ms to the 30-second cap, reset after success, and retry failed work without requiring a new commit.
- [x] Capture selected work before reconciliation reads and acknowledge only successful selected work; commits during a callback remain pending for another pass.
- [x] Dropping the runner cancels its in-flight callback or retry timer; closing all subscribed backends also stops in-flight work.
- [x] SQL and KV retain the same subscription type and can merge into one broad-session runner; merged subscriptions are not used for selective logical mapping.
- [x] Real-browser tests verify initial pass, idle behavior, coalescing, concurrent commits, failed retry, cancellation, backend closure and merged SQL/KV subscriptions.
- [x] The library does not automatically start replication tasks or choose transports, peers or authorization. Caller transport callbacks may use browser-local futures.
- [x] Document that cross-tab and remote-only writes need caller-provided wakeups or explicit broad recovery; do not add cross-tab coordination or claim durable replay.
- [x] Native continuous runner regressions preserve existing scheduling, shutdown and cancellation behavior.


## Comments

### Implementation and validation — 2026-10-25

Confirmed clean worktree `/home/nathan/code/rust/aicacia/ofdb-browser-09`, branch
`browser-indexeddb-09`, exactly at requested base
`0bae22318301d4e43bf766f8843cc9c81f1f4f58`, matching integration
`browser-indexeddb`. Read AGENTS/GLOSSARY, browser spec, prior ticket notes and
browser README. Loaded TDD; used the explicitly approved public Database → Client
and `ChangeSubscription::{run, select, acknowledge, merge}` seams without another
interview. Complexity: low production change, moderate acceptance-test work;
risk: shutdown side effects and lost work during reconciliation; priority: complete
the explicitly caller-owned browser sync lifecycle.

Tickets 01/06 already supplied the target-aware execution contract and shared
browser timer; reused them without new dependencies, runtimes or duplicate runners.
The first browser test exposed eager callback construction in `tokio::select!`:
already-closed storage still invoked the caller's transport constructor. The red
Chrome test observed 1 call instead of 0. Deferring the existing boxed callback
construction into the selected async branch fixes this on both targets. The same
test then passed with the full Chrome suite (81 tests at that stage). Native
regression also verifies no callback construction after closure, and the existing
large-callback future-size regression remains passing.

Added seven browser tests in `tests/browser/src/continuous.rs`, driven directly by
the caller (no spawn): initial reconciliation and idle silence; gated in-flight
snapshot reads with three later commits coalesced into the next pass; independent
consumer/selected watermark retention; failed-work retries without a new commit;
real 100/200/400/800/1600/3200/6400/12800/25600/30000/30000 ms browser delays, success
reset to 100 ms and idle after success; cancellation/drop during local `Rc`
callbacks and retry waits; merged SQL/KV lifetime with one backend/client still
alive, then last-backend closure during callback/retry; and actual merged checked
SQL/KV broad protocol exchanges with persisted peer-state and replay-idle assertions.
Reused the existing KV exchange helper and SQL transport support, not new transport
implementations. Native facade regression spawns the same runner, checks shared
subscription `Send + Sync`, real native retry, commit rescheduling and cancellation
of pending transport when the last storage-owning handle drops.

Updated sync-runtime, SQL and browser-test documentation for caller polling,
local futures, cooperative browser timers, handle retention, broad-only merges,
and external-tab/remote-only wakeup limits. No automatic replication, networking,
peer/authorization selection, cross-tab coordination or durable replay was added.
Cancellation cannot undo prior completed persistence. Browser suite timeout is now
240 seconds because the real capped-delay test takes about 112 seconds alone.

#### Passing validation

Commands are worktree-relative; the editor exposes the sibling integration root,
so actual invocations used absolute `--manifest-path`, fixture or `--justfile`
arguments. Reused ignored ticket-01 browser capabilities and matching ChromeDriver;
no machine-local paths/binaries/configuration were added to source. Verified browser
versions: Chrome **156.0.8078.47**, Firefox **158.0.1**. All browser runs use the
fixture's documented **16 MiB Wasm stack**, not a default-stack support claim.

- Chrome Window: **87 passed**; final direct `cargo test --lib` with the existing
  wasm-bindgen-test-runner, `--target wasm32-unknown-unknown --features sync`.
- Firefox Window: **87 passed**, same complete public suite and real retry test.
- Chrome Dedicated Worker: **84 passed**, `wasm-pack test --headless --chrome
  --chromedriver DRIVER tests/browser --features worker,sync`. After the final
  RefCell-borrow cleanup, the six non-backoff lifecycle tests also reran: **6 passed**;
  the long retry test was unchanged.
- Firefox Dedicated Worker: **84 passed**, `wasm-pack test --headless --firefox
  tests/browser --features worker,sync`, including the real capped retry test.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sync-runtime --lib`: **10 passed**,
  including all existing scheduling/backoff/closure/drop and future-size regressions.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path tests/browser/Cargo.toml
  --test native --features sync`: **3 passed**, including spawned runner Send,
  existing spawned SQL/KV query/transaction and one-shot sync regressions.
- `cargo clippy -p ofdb-sync-runtime --all-targets -- -D warnings`.
- Browser fixture Clippy: `--tests --target wasm32-unknown-unknown --features
  worker,sync -- -D warnings`; native fixture `--test native --features sync` too.
- Browser normal dependency graph with `sync`: programmatically checked absence
  of Redb, native SQL/BTree backends, tonic, Iroh, tempfile and native SQL/KV
  server/protocol packages.
- Standalone minimal `cargo check --no-default-features --target
  wasm32-unknown-unknown` for `ofdb-kv --features indexeddb,sync` and
  `ofdb-sql --features indexeddb,sql,sync`: passed without fixture feature unification.
- Changed-crate `cargo hack test -p ofdb-sync-runtime --feature-powerset
  --all-targets`: **10 passed**, one feature combination.
- Workspace and isolated fixture `cargo fmt --all -- --check`; `git diff --check`.
- Explicitly merged integration tip before completion: `git merge --no-edit
  browser-indexeddb` reported **Already up to date**, integration still `0bae223`.

#### Failed/incomplete broad gates and intermediate attempts

- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`:
  **failed** at `ofdb-btree --no-default-features --features sharded` with eight
  existing missing std/async_stream/in-memory exports and ToString errors. Reproduced
  the same eight errors using `cargo test -p ofdb-btree --no-default-features
  --features sharded --all-targets` on untouched integration/base `0bae223`.
  No BTree source was changed. Remaining feature combinations are not claimed passing.
- `just crap`: **timed out at 120 seconds** compiling coverage for BTree Redb,
  package 5/32. No complete coverage or CRAP score exists.
- An initial full Chrome invocation passed all **87** browser tests but hit its
  240-second command limit at the subsequent doc-test phase. The final direct
  library-only full run completed successfully as listed above.
- An initial full Firefox Window invocation reported a browser-suite failure;
  its truncated captured tail did not retain the assertion detail. Repeated all
  86 tests except the long retry successfully, then reran the complete **87-test**
  suite successfully. No unrelated production change was made to mask that failure.
- One direct Firefox attempt supplied an absent WebDriver path and failed before
  launching tests; subsequent runs used wasm-pack's actual cached driver. The first
  attempted wasm-pack name filter was rejected as a Cargo build argument; focused
  runs thereafter used direct Cargo/wasm-bindgen-test-runner filtering.
- Clippy found a test RefCell borrow held over a snapshot await and a borrow whose
  lexical scope included a later await; both were shortened before final checks.
- No Safari/WebKit, live network peer, actual quota exhaustion, power-loss guarantee,
  automatic cross-tab wakeup, full release-feature matrix or completed complexity
  gate is claimed. Ticket-09 behavior is complete with the focused evidence above;
  broad workspace gates remain explicitly incomplete.
