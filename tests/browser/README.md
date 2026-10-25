# Browser-local execution harness

This isolated Rust consumer exercises public in-memory SQL/KV clients and durable
IndexedDB KV storage in real browsers without a Tokio runtime. Its own workspace prevents native facade
**development** dependencies (Redb, servers, native networking and temporary
files) from entering browser builds. SQL persistence remains subsequent-ticket
work. With `sync`, KV tests exercise checked broad/selected sessions and native
protocol fixtures.

## Run

From the repository root, with `wasm32-unknown-unknown`, `wasm-pack`, Chrome and
Firefox installed:

```sh
wasm-pack test --headless --chrome tests/browser
wasm-pack test --headless --chrome tests/browser --features worker,sync
wasm-pack test --headless --firefox tests/browser
wasm-pack test --headless --firefox tests/browser --features worker,sync
```

The default context is Window; `worker` selects Dedicated Worker. Tests assert the
actual context. `sync` enables KV protocol and committed-subscription tests alongside
the facade sync/staging compile checks. Set `WASM_BINDGEN_TEST_TIMEOUT=120` for the
full suite, including notification-retention overflow scenarios.
`wasm-pack` obtains matching wasm-bindgen and WebDriver tools if absent (network
access required). `--chromedriver PATH` or `--geckodriver PATH` can select existing
drivers; also supply `--chrome` or `--firefox` explicitly.

For a nonstandard browser installation, set `WASM_BINDGEN_TEST_WEBDRIVER_JSON` to
an absolute path to a local capabilities JSON file. For example, use
`goog:chromeOptions.binary` and `moz:firefoxOptions.binary` for the browser
executables. Keep machine-specific configuration in ignored
`webdriver.local.json`, not in the shared fixture. Sandboxed/container Chrome may
need `goog:chromeOptions.args: ["--no-sandbox"]`; do not use that outside a suitably
isolated test environment.

Compile all smoke tests without launching a browser:

```sh
cargo check --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown
cargo check --manifest-path tests/browser/Cargo.toml --tests --target wasm32-unknown-unknown --features worker,sync
cargo tree --manifest-path tests/browser/Cargo.toml --target wasm32-unknown-unknown --edges normal --features sync
```

Check that the dependency tree contains no Redb, native server/protocol, tonic,
Iroh or tempfile packages. Tokio's synchronization primitives are used by change
notifications; no Tokio runtime is created in Wasm tests. Browser deadlines and
continuous retry timers use the same platform timer capability.

Native thread-safety and spawned query/transaction/sync regressions:

```sh
RUST_MIN_STACK=16777216 cargo test --manifest-path tests/browser/Cargo.toml --test native --features sync
```

## Stack budget

The fixture's optimized dev profile and `.cargo/config.toml` select a **16 MiB
Wasm linear stack**, matching the repository's bounded-stack native SQL regression
harness. `wasm-pack` runs Cargo inside this fixture, so its target configuration
is applied. Cargo invoked from another directory does not discover a manifest's
`.cargo` configuration automatically; for a linked Wasm test build from the root,
add `--config tests/browser/.cargo/config.toml` to Cargo.

This is a real resource requirement, not a claim that SQL works with Rust's
small default Wasm stack: the default-stack SQL smoke test trapped with an
out-of-bounds access, and native SQL tests overflowed the default thread stack.
Browser consumers must configure an adequate linear stack (for example
`-C link-arg=-zstack-size=16777216`) and budget that memory. Reducing SQL async
construction stack usage is separate work; these tests do not establish a minimum
stack size or a release-build memory ceiling.

## Execution contract

`ofdb-platform::{MaybeSend, MaybeSync, BoxFuture}` preserves native `Send`/`Sync`
bounds and boxed futures. Only `wasm32-unknown-unknown` relaxes them to local work.
No unsafe assertions make JavaScript handles transferable. Kernel and BTree tests
use `Rc` handles, local values/callbacks, streams and a timer await to keep this
contract exercised beyond a facade-only compile.

Browser wall time uses `Date.now`; UUIDs enable UUID's JS support and Automerge
enables its browser randomness feature on that target. Runtime tests compare the
clock and UUID timestamp with JS wall time, check distinct UUIDs and actor IDs,
and check expired and future KV values. Browser sleep/timeout use platform timers,
including fractional-millisecond rounding and chunking waits longer than the
signed 32-bit timer limit. Native deadlines retain Tokio's behavior.

## IndexedDB persistence checks

`src/indexeddb.rs` exercises the byte BTree, database-wide snapshot/staging API,
public KV facade, and native history API. It checks binary comparison using real
`indexedDB.cmp`, typed values (including integer limits and exact float bits),
Unicode/NUL/literal-prefix scans, expiry, generations, and stale history after
persisted Tombstones. Revision conflicts are tested with independent handles and
with two simultaneously staged writers in separate Wasm worker instances.

The fixture also exports **test-only** `restart_probe` and `race_begin` Wasm
functions. `indexeddb_helpers.js` loads the generated `/wasm-bindgen-test` module
in fresh module workers and same-origin iframe pages; this URL is supplied by the
wasm-bindgen-test runner, not a production application requirement. A writer is
terminated/removed after commit; a fresh instance reads identical generation IDs,
heads and native history. The iframe restart check is Window-only. Worker runs
still destroy/recreate separate nested workers. These are real execution-context
restarts, not just reopening Rust handles; they do not simulate a browser-process
crash, OS restart or power loss.

Fault injection is confined to the fixture: abort after a native write request
succeeds, and throw `QuotaExceededError` after another request has been queued.
Both must roll back records, table metadata and revision together. Access denial,
incompatible metadata/version, versionchange closure, cancellation after issued
commit, independent completion invalidation, rollback/drop, read-only and net
byte no-op behavior are covered. Actual disk quota exhaustion is not claimed.

The measurement test loads 100 and 1,000 records with 1 KiB values and logs elapsed
snapshot time and raw key+value bytes. For visible samples, run a built test
module with `wasm-bindgen-test-runner MODULE snapshot_materialization_measurement
--nocapture` and the same WebDriver environment. No timing threshold is asserted.
Snapshots read the entire database, reconstruct ordered maps and retain **two
Rust-owned copies**, in addition to transient IndexedDB/JS data. This is not a
selective-I/O or bounded-memory implementation.

## KV checked sync acceptance

With `sync`, `src/kv_sync.rs` tests broad/selected caller-owned local transports,
offline nested merges, native deltas and missing-dependency snapshot recovery,
generations/Tombstones, expiry history, independent acknowledgements, concurrent
commits, overflow/gaps, replay silence, rejection and cancellation. The test-only
`kv_sync_helpers.js` observes an actual issued incoming write so the session can
be dropped before completion; committed state and subscription fan-out must survive.
Abort/quota faults reuse the existing injected native-API failures, not disk exhaustion.

`src/kv_native_fixture.json` contains native-generated current-protocol initial
snapshot and subsequent incremental-change transcripts, expected native response
bytes, and committed snapshots. Browser responders must match both transcripts
and retained history exactly. The native consumer verifies the same committed
fixture without regenerating it:

```sh
cargo test --manifest-path tests/browser/Cargo.toml --test kv_protocol --features sync
# Explicitly regenerate only when intentionally updating the fixture:
OFDB_REGENERATE_KV_FIXTURE=1 cargo test --manifest-path tests/browser/Cargo.toml --test kv_protocol --features sync
```

These are logical native/browser interoperability checks without bundled network
transport. Local subscriptions do not monitor other tabs/opens or remote-only
writes: external-write gaps require broad recovery. Selected logical inventories
do not avoid the IndexedDB backend's whole-database physical snapshots.
