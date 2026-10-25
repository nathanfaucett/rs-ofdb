# Browser-local execution harness

This isolated Rust consumer exercises the public in-memory SQL/KV clients in real
browsers without a Tokio runtime. Its own workspace prevents native facade
**development** dependencies (Redb, servers, native networking and temporary
files) from entering browser builds. It is the harness for subsequent IndexedDB
tickets; it does not implement or test persistence yet.

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
actual context. `sync` also compiles the facade sync/staging APIs; this ticket does
not claim browser/native sync interoperability or IndexedDB validation.
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
