# Browser-local execution harness

This isolated Rust consumer exercises public in-memory SQL/KV clients and durable
IndexedDB SQL/KV storage in real browsers without a Tokio runtime. Its own workspace prevents native facade
**development** dependencies (Redb, servers, native networking and temporary
files) from entering browser builds. With `sync`, SQL and KV tests exercise checked
broad/selected sessions and native protocol fixtures; SQL also exercises scoped
history/candidate validation.

## Release matrix automation

Use `just browser-check`, `just browser-accept`, `just browser-measure`,
`just browser-native`, and `just browser-final` from the repository root.
`just browser-orchestration-test` checks orchestration through these public recipes. The [reproduction guide](../../docs/browser-validation.md)
details prerequisites, driver selection, bounds, logs/JSON, measurements and known
broad-gate failures. The dedicated [browser workflow](../../.github/workflows/browser.yml)
runs the four full acceptance cells independently, not a native workspace Wasm
all-features build. The [public guide](../../docs/browser.md) documents supported
feature sets and user-facing quickstarts.

## Run

From the repository root, with `wasm32-unknown-unknown`, `wasm-pack`, Chrome and
Firefox installed:

```sh
WASM_BINDGEN_TEST_TIMEOUT=240 wasm-pack test --headless --chrome tests/browser --features sync --lib
WASM_BINDGEN_TEST_TIMEOUT=240 wasm-pack test --headless --chrome tests/browser --features worker,sync --lib
WASM_BINDGEN_TEST_TIMEOUT=240 wasm-pack test --headless --firefox tests/browser --features sync --lib
WASM_BINDGEN_TEST_TIMEOUT=240 wasm-pack test --headless --firefox tests/browser --features worker,sync --lib
```

The default context is Window; `worker` selects Dedicated Worker. Tests assert the
actual context. `sync` enables SQL/KV protocol and committed-subscription tests alongside
the facade sync/staging compile checks. Set `WASM_BINDGEN_TEST_TIMEOUT=240` for the
full sync suite. The continuous runner test uses real browser delays from 100 ms
through two 30-second capped retries (about 112 seconds on its own), not a fake
clock. This also accommodates notification-retention overflow scenarios.
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
snapshot time, raw key+value bytes and Wasm linear capacity before/after snapshot.
Capacity is not live or peak heap and does not measure transient JS/IDB memory.
`just browser-measure all all target/browser-validation/measure 600 PATH` reproduces all four
browser/context samples with explicit drivers; see the reproduction guide. For visible samples, run a built test
module with `wasm-bindgen-test-runner MODULE snapshot_materialization_measurement
--nocapture` and the same WebDriver environment. No timing threshold is asserted.
Snapshots read the entire database, reconstruct ordered maps and retain **two
Rust-owned copies**, in addition to transient IndexedDB/JS data. This is not a
selective-I/O or bounded-memory implementation.

### SQL persistence

`src/sql_indexeddb.rs` tests typed DDL/insert/select separately from SQL-text
execution, client ownership, catalog/index reopen, failed-batch atomicity, stable
transactions across timer awaits, stale commits, rollback/drop/aborted transactions,
logical table drop/recreation, SQL/KV/name isolation and injected native failures.
The Kernel seam additionally checks binary ranges, missing tables and stale
read-only commits. `sql_indexeddb_helpers.js` restarts fresh worker/iframe Wasm
instances through the test-only `sql_restart_probe` export; persisted indexed reads,
unique constraints and UUID Tombstones must survive. These are execution-context
restarts, not whole-browser/OS crash or power-loss tests. Checked SQL sync acceptance is described below.

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

## SQL transaction and reconciliation semantics

`src/sql_semantics.rs` adds public-facade transaction/aborted-state, defaults,
natural/composite key, unique constraint, index, multi-table failure and separate
worker commit-race checks. With `sync`, it builds concurrent causal-history
fixtures through public sync, then checks losing values, partial/invalid explicit
Resolution, stale replay, permanent unique-key losers and drop/recreation. Fresh
workers and (in Window mode) iframe pages compare complete exported native history
before/after context destruction, including catalog, indexes and Tombstones.
These fixture-setup sync calls do not replace ticket 08's checked-sync acceptance.
Run both Window `--features sync` and Dedicated Worker `--features worker,sync`
in Chrome and Firefox using the commands above.

## Storage lifecycle

`src/storage_lifecycle.rs` adds SQL/KV facade acceptance for incompatible physical
schemas and kind markers without reset, value-free actionable access errors,
client clones after Database drop, independent namespace isolation, native
abort/injected-quota atomicity, and revision exhaustion at the byte-storage seam.
Its separate JS helper uses actual peer workers for version-change closure and
an uncooperative connection blocking deletion. The latter queues a facade open
without an open blocked event: it must fail on the documented five-second timer,
and releasing the peer must not let the late request create an empty database.
The same tests run under both Window and Dedicated Worker runners. Injected
quota failures are not evidence of real storage exhaustion; see the
[storage policy](../../crates/btree-indexeddb/README.md#lifecycle-and-browser-failures).

## Browser query deadlines

`src/deadlines.rs` exercises both public IndexedDB clients without a Tokio runtime:
all query entry points time out on pending snapshots, cloned clients keep independent
settings, transaction creation is bounded, and transaction operations get fresh
budgets after caller awaits. It covers typed SQL, SQL-text and generic translation,
including CPU work before translation's first yield, pending translation inside an
explicit transaction, and one budget shared by translation and commit observation.
Platform edge checks cover zero/ready work, sub-millisecond waits and delays beyond
the browser's signed 32-bit timer range.

`deadline_helpers.js` is fixture-only fault injection. It temporarily holds the
adapter's native completion callback, while a separate native event listener
observes the real IndexedDB transaction completing. Pending readonly snapshots let
tests time out or cancel **before** persistence. For issued commits, the existing
`afterNextWrite` helper cancels while a real native write is queued; held completion
observation forces public query/transaction timeouts without depending on disk speed.
After the caller exits, the fixture releases observation and independently reopens
SQL/KV to verify the complete persisted write. With `sync`, KV also verifies no early
publication and an eventual committed wakeup without polling the original caller.
With `sync`, SQL likewise verifies no early publication and eventual committed
wakeups after query/transaction timeout or cancellation. These tests do not simulate quota
exhaustion or require changes to the persistence adapter's lifecycle API.

`Client::with_deadline(Duration)` has no default deadline; clones copy settings
independently. Each query, transaction creation and transaction operation gets its
own budget, excluding connection setup and caller idle time. SQL translation and
execution share one budget. Browser timers are cooperative: background throttling
or synchronous computation can delay delivery, and already-ready work may win.
Timeout/cancellation does **not** prove rollback, undo completed writes, or guarantee
hard interruption. Explicitly roll back or drop uncommitted staging when abandoning
it; once native commit has been issued, independently observed completion may still
persist it. Native configured deadlines continue to use Tokio time.

## SQL checked sync acceptance

With `sync`, `src/sql_sync.rs` uses the public Database → Client → Transaction
and existing sync APIs with caller-owned in-process transports, including local
`Rc` callback state. It covers checked broad bootstrap, independent selected work,
later-commit retention, scoped directions/history/candidate policies, natural and
composite keys, offline conflicts and explicit Resolution, Tombstones, catalog
drop/recreation, native dependency recovery, replay silence, bounded session/stage
failure, malformed catalog rejection, authorization revocation and cancellation.
A denied later data apply leaves the earlier complete catalog intact; retry
converges. Native abort and injected quota errors leave no partial catalog/index
state. These are injected failures, not actual disk-quota exhaustion.

The existing fixture-only `afterNextWrite` helper observes an issued incoming
native data write so its caller can be abandoned before completion. Local and
incoming committed state and independent subscription fan-out must survive.
`deadlines.rs` additionally holds native completion observation while SQL query
and explicit transaction callers time out/cancel; publication must not occur early.
Overflow/lag require All. An independent open deliberately does not wake a local
subscription; an explicit broad pass recovers its writes. No automatic cross-tab,
remote-only wakeup, durable replay or selective physical I/O is claimed.

`src/sql_native_fixture.json` contains native-generated current-v7 bootstrap and
subsequent incremental/deletion/catalog transcripts, exact expected native replies,
and complete retained history with deletion metadata. Both native and browser
responders verify this committed fixture, without regenerating expected results:

```sh
RUST_MIN_STACK=16777216 cargo test --manifest-path tests/browser/Cargo.toml --features sync --test sql_protocol
# Explicit regeneration only when intentionally updating the fixture:
RUST_MIN_STACK=16777216 OFDB_REGENERATE_SQL_FIXTURE=1 cargo test --manifest-path tests/browser/Cargo.toml --features sync --test sql_protocol
```

Run Window `--features sync` and Dedicated Worker `--features worker,sync` in both
Chrome and Firefox. Native stage-directory APIs/types and tempfile remain absent
from the browser target even with `ofdb-sql/native-stage` explicitly enabled.
Continuous browser runner coverage is described below, separately from these one-shot tests.

## Continuous synchronization

With `sync`, `src/continuous.rs` exercises the public `Database::sync_changes()` →
`ChangeSubscription::run` seam in both contexts. It covers initial reconciliation,
idle silence, coalescing, commits after selected-work capture, independent consumers,
real exponential retry delays and reset, local `Rc` callback futures, caller drop,
last-backend closure during callbacks/retries and merged SQL/KV broad sessions.
The merged test exchanges existing checked SQL and KV protocols over caller-owned
in-memory channels and verifies peer state and settled replay, not bundled networking.
Native regressions also spawn the same runner to verify `Send`, retry and shutdown.

The caller owns and must poll the runner; constructing it starts no task. Merge
subscriptions only for broad sessions, never for selective store mapping. Callbacks
that retain Database/Client handles also keep their backends alive. Cancellation
stops polling transport work, but cannot undo earlier completed persistence.
Independent opens, other tabs and remote-only writes do not wake this runner;
applications supply remote wakeups or explicit broad recovery. There is no automatic
cross-tab coordination, transport/peer/authorization selection or durable replay.
