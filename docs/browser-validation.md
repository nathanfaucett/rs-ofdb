# Browser release validation

The public [browser guide](browser.md) defines the supported features and limits.
This document records **local real-browser evidence**, not a claim that the new
GitHub Actions workflow has already run remotely or that every native workspace
feature combination passes.

## Reproduce

From a source checkout, install:

- Rust **1.99.1**, `rustfmt`, `clippy`, and `wasm32-unknown-unknown`.
- Python 3 (standard library only; validated with Python 3.12).
- `wasm-pack` **0.15.0** and real Chrome/Chromium and Firefox installations.
- Matching ChromeDriver and GeckoDriver. `wasm-pack` can download drivers/tooling,
  but Chrome must have a matching driver; pass explicit driver paths if needed.
- For measurements, `wasm-bindgen-test-runner` matching the fixture lock's
  `wasm-bindgen` **0.2.128**. `cargo install wasm-bindgen-cli --version 0.2.128 --locked`
  installs it, or pass `--runner` pointing to wasm-pack's matching cached runner.
- Native regression prerequisites: `protoc`; final gates additionally require
  `cargo-hack`, `cargo-llvm-cov`, LLVM tools, `cargo-crap` and `just`.

Example setup and complete commands (run suites sequentially to avoid Cargo lock
contention and browser startup starvation):

```sh
rustup toolchain install 1.99.1 --profile minimal --component rustfmt,clippy --target wasm32-unknown-unknown
rustup default 1.99.1
cargo install wasm-pack --version 0.15.0 --locked
python3 -m unittest discover -s scripts -p test_browser_matrix.py
python3 scripts/browser_matrix.py check --timeout 600 --output target/browser-validation/check
python3 scripts/browser_matrix.py browser --timeout 600 --output target/browser-validation/browser
python3 scripts/browser_matrix.py measure --timeout 600 --output target/browser-validation/measure
python3 scripts/browser_matrix.py native --timeout 600 --output target/browser-validation/native
python3 scripts/browser_matrix.py final --timeout 240 --output target/browser-validation/final
```

For exact browser selection, use `--browser chrome|firefox` and
`--context window|worker`; default `all` runs all four cells. Use
`--chromedriver PATH`, `--geckodriver PATH` or the corresponding `CHROMEDRIVER` /
`GECKODRIVER` environment variables for explicit installations. `measure` requires
explicit drivers or drivers on PATH, plus `--runner PATH` or the matching runner
on PATH. Window and Worker measurements are separate fresh test instances.

For nonstandard browser binaries set `WASM_BINDGEN_TEST_WEBDRIVER_JSON` to an
absolute local capabilities JSON file; `tests/browser/webdriver.local.json` is
ignored. The keys are `goog:chromeOptions.binary` and `moz:firefoxOptions.binary`.
Only an isolated sandbox that needs it should add Chrome's `--no-sandbox` argument.
Do not commit machine paths/capabilities or use that option for production browsing.

The script resolves its repository from its own path; commands execute there even
if called elsewhere. Defaults are two Cargo build jobs, 16 MiB native thread stack,
and a 240-second browser test budget; explicit environment settings override these.
The fixture config supplies a 16 MiB Wasm stack and optimized dev profile. A full
sync suite includes real capped retry waits totaling about 112 seconds; do not
replace it with a fake clock and call that browser acceptance.

Each subprocess has its own `--timeout` bound, including compilation, with process
group termination on timeout. Checks continue after failures to collect remaining
evidence, but **any failure/unavailable tool/timeout exits nonzero**. Each output
directory holds exact command logs and `results.json` with status, exit code and
elapsed time. Use a different output directory to retain multiple invocations;
the same directory overwrites that invocation's files. Missing tools do not count
as a pass. The final suite runs formatting, the requested feature powerset, then
`just crap` independently even if the powerset fails.

### What is automated

- Seven standalone package-specific facade feature checks, without fixture
  in-memory/translator feature unification; normal dependency graphs for each.
- SQL and KV native remote-only graph isolation. Browser graphs reject native
  Redb/backends, filesystem/temp resources, native client/server/protocol, Iroh,
  Tonic, socket and filesystem implementation crates. Tokio synchronization
  primitives are allowed; tests create no Tokio runtime in Wasm.
- The combined fixture's normal graph with `sync,ofdb-sql/native-stage`, proving
  that native staging selection does not pull file staging into this target.
- Browser-target Clippy in both contexts; compilation of **all four Rust code
  blocks** in `docs/browser.md` as an isolated Wasm consumer under ignored
  `target/browser-quickstarts`. Its resolver is seeded from the fixture lock.
- The full public-interface acceptance suite with `sync` in both browsers/contexts,
  using `wasm-pack test ... --lib`; this deliberately avoids an irrelevant later
  native/doc-test phase. Runtime assertions distinguish Window from Worker.
- Reproducible snapshot diagnostics with `cargo test` and the matching runner,
  `--nocapture`, and explicit fixture target config (Cargo does not discover a
  manifest's `.cargo` config from arbitrary working directories).
- Native fixture consumption and Send/Sync/spawn ownership/runner regressions,
  KV memory/Redb ownership and sync, SQL Redb/SQL/constraints/checked sync, runtime
  scheduling, focused Clippy, rustfmt and final broad gates.

Graph guards are package-name checks, not proof that every possible future native
API is unreachable: keep them current with feature changes and retain real browser
compilation/runtime tests. Negative automation tests prove forbidden dependencies
fail even after a zero-exit tree command; other tests cover missing tools,
nonzero exits, bounds and command/feature selection.

### CI

[`.github/workflows/browser.yml`](../.github/workflows/browser.yml) runs independent
feature checks, four browser acceptance jobs and native regressions on
Ubuntu 24.04. It pins Rust, wasm-pack, tested browser versions and full action
commit SHAs; Chrome setup installs its matching driver. Firefox's driver is
obtained by wasm-pack. All jobs have bounds, fail independently, and upload raw
logs/JSON even after failure. No job silently skips a browser, converts a failure
to success or substitutes Node/mocked IndexedDB. Artifacts are retained 14 days.
Measurements are a separate reproducible local suite, not an asserted CI latency
threshold. Existing native CI still owns its broader feature/coverage gates.

Events are pull requests, pushes to main/integration, and manual dispatch; token
permission is read-only contents. Matrix values enter shell commands via environment
variables. No secrets, deployment or write token is needed. Design follows GitHub's
[workflow syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax)
and [secure use](https://docs.github.com/en/actions/reference/security/secure-use)
guidance (least privilege, immutable actions and no privileged fork checkout).

## Local acceptance evidence — 2026-10-25

Clean ticket branch `browser-indexeddb-10` started at
`f1d079740b9bf4fe338fc4e81eb7c1b3aab9500b`, the integration tip containing tickets
01–09. Ticket 10 changes automation/docs and the diagnostic test only, not runtime
implementation. The worktree was relocated under `.scratch/worktrees/browser-10`
for editor access. Browser tools reused the ignored ticket-01 capabilities and
matching drivers; no binaries or local browser paths were committed to automation.

Host: Linux x86_64; rustc **1.99.1 (58cf8f9c3 2026-10-13)**; wasm-pack **0.15.0**;
fixture wasm-bindgen **0.2.128**; optimized dev fixture with **16 MiB Wasm stack**.
Chrome **156.0.8078.47**, Firefox **158.0.1** (Flatpak binaries selected explicitly).

| Browser/context          | Full suite with sync | Command wall time including build/startup |
| ------------------------ | -------------------: | ----------------------------------------: |
| Chrome Window            |        **87 passed** |                  346.37 s (initial build) |
| Firefox Window           |        **87 passed** |                                  192.91 s |
| Chrome Dedicated Worker  |        **84 passed** |                  251.37 s (context build) |
| Firefox Dedicated Worker |        **84 passed** |                                  188.70 s |

The three iframe/page-only tests do not run inside the Worker configuration;
Worker mode still destroys/recreates independent nested workers. These tests cover
persistence/context restart, typed and text SQL, explicit transaction lifetime,
revision races, constraints, conflicts/Resolution, lifecycle/errors, deadlines,
checked SQL/KV native-wire fixtures, invalidation and actual continuous retry and
cancellation. Restart means fresh iframe/worker Wasm instance, **not** whole browser,
OS or power-loss recovery. Abort/quota/access fault injection uses real native
IndexedDB requests but is not evidence of physical quota exhaustion or eviction.

Focused final validation also passed:

- Seven standalone Wasm feature checks and all associated graphs; combined
  native-stage graph and both native remote-only graphs.
- Window/Worker browser Clippy, native facade/runtime Clippy; both rustfmt checks.
- Compiled public quickstart blocks, six Python automation tests and actionlint
  **1.7.7** on the new workflow. No remote Actions execution is claimed.
- Native fixture protocols/ownership: **5 passed** (3 ownership/runner + SQL/KV
  fixture consumers); KV regressions: **19 passed**; SQL regressions: **70 passed**;
  shared runtime: **10 passed**. Commands are exactly those in the script's native
  suite, using `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2`.
- Four focused snapshot measurements: **1 test passed per browser/context**.

### Snapshot samples

One sample per size/context, in a fresh filtered measurement instance; not an
average, percentile, throughput claim or release-build benchmark. The test seeds
one logical table with 4-byte keys and 1 KiB byte values, commits it, then measures
`Database::snapshot()` using browser `Date.now()`. This excludes Wasm module load,
open/metadata validation and seeding. Small timings have millisecond clock precision.

| Browser/context | 100 records / 102,800 raw bytes | 1,000 records / 1,028,000 raw bytes |
| --------------- | ------------------------------: | ----------------------------------: |
| Chrome Window   |                            4 ms |                               35 ms |
| Chrome Worker   |                            4 ms |                               34 ms |
| Firefox Window  |                            3 ms |                               19 ms |
| Firefox Worker  |                            3 ms |                               20 ms |

In every filtered sample, measured Wasm linear capacity before → after snapshot was:

- 100 records: **17,891,328 → 18,022,400 bytes** (growth 131,072 bytes).
- 1,000 records: **18,939,904 → 19,988,480 bytes** (growth 1,048,576 bytes).

These are **allocator/linear-memory capacity**, not live allocations, RSS, peak
heap or incremental retained memory. Capacity includes the reserved stack and
allocator high-water usage, may be reused, and does not shrink between the two
sizes. It excludes JS/IndexedDB allocations. Capacity growth being less than two
payload copies does not contradict retained base/staging copies: already-allocated
capacity is reused. The measured record counts/lengths imply **205,600 / 2,056,000
raw payload bytes** in two copies before map/allocation/metadata overhead. That is
a lower-bound payload calculation, **not** a measured peak-heap value.

The backend materializes every record/history/logical table for every transaction;
ordered reconstruction adds O(N log N) CPU. These small byte-storage diagnostics
are not end-to-end SQL/Automerge workload benchmarks or application cold-start
measurements. No safe maximum database size, selective physical read performance,
minimum stack or release-memory ceiling has been established. Profile application
history sizes and concurrent transactions separately.

## Failed and incomplete final gates

- `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 cargo hack test --feature-powerset --all-targets`
  **failed (exit 1 from cargo-hack, underlying Cargo 101)** after 39.01 seconds at
  `ofdb-btree --no-default-features --features sharded`. Eight existing errors:
  unavailable `async_stream` / `std`, missing in-memory exports and missing
  `ToString` imports. This is the previously base-reproduced failure
  recorded in tickets 01, 06, 08 and 09. Ticket 10 changes no BTree implementation.
  Remaining combinations were not completed; **no full-powerset pass**.
- `CARGO_BUILD_JOBS=2 just crap` **timed out at 240 seconds**, collecting coverage
  for `ofdb-value`, package **6/32**, compiling Automerge/value dependencies.
  Process-group termination reported recipe line 40 signal 15. No complete
  coverage result or CRAP score; a partial `/tmp/lcov.info` is not a workspace score.
- Prior ticket 08 documents a pre-existing native `sync_stack` SIGABRT on its
  explicitly configured 2 MiB Tokio worker. Ticket 10's focused native suite does
  not include that low-stack test and does not claim to fix or revalidate it.
- WebKit is out of scope, not an incomplete validation gate. There is no older
  browser/minimum version, default-stack SQL, actual disk quota exhaustion,
  private-mode eviction, browser/OS crash, power loss or live network peer evidence.

These broader gates are not waived or marked green. Ticket 10 delivers the
requested reproducible validation and accurate support documentation; it does not
make the unrelated full native matrix or complexity gate release-ready.

## P2 cold-read review fix — 2026-10-25

Branch `browser-indexeddb-review`, based on integration `3fbd651`, fixes only the
cold SELECT finding. The shared SQL executor now classifies SELECT-only batches
before initializing catalogs, and does not initialize catalogs for reads. The
IndexedDB Kernel already reads missing catalogs as empty. No open-time commit or
storage-format change was introduced; intentional empty logical-table DDL still
persists. Both ordinary and explicit facade transactions use this path.

Three new public-interface browser tests verify cold-read revision/subscription
silence, stale cold reader commit after independent DDL, and ordinary/explicit
empty-table/index reopen plus checked sync replay silence. Both cold-read tests
were observed failing against the original executor and passing with the fix.
The first full matrix failed only the new empty-DDL test's expected index list,
which omitted implicit primary-key indexes; the corrected focused test and all
four complete reruns passed. Initial failures are not counted as acceptance.

Reproduction uses the commands and prerequisites above, with existing ignored
local WebDriver capabilities and matching ticket-01 ChromeDriver. Build artifacts
were reused from the ticket-10 worktree via `CARGO_TARGET_DIR`; no source edits
were made there. From the review worktree:

```sh
python3 scripts/browser_matrix.py browser --timeout 600 --chromedriver /home/nathan/code/rust/aicacia/ofdb-browser-01/tests/browser/target/chromedriver-linux64/chromedriver --output target/review-validation/browser-rerun
python3 scripts/browser_matrix.py native --timeout 600 --output target/review-validation/native
python3 scripts/browser_matrix.py check --timeout 600 --output target/review-validation/check
python3 scripts/browser_matrix.py final --timeout 240 --output target/review-validation/final
```

The browser command additionally set `WASM_BINDGEN_TEST_WEBDRIVER_JSON` to the
existing ticket-01 `tests/browser/webdriver.local.json`; it used the ticket-10
`tests/browser/target` build directory. The other suites used ticket-10 `target`.
The script supplied two build jobs, 16 MiB native stack and 240-second browser
budget; fixture Wasm stack remains 16 MiB. Exact commands, logs, statuses and
elapsed times are in the ignored output directories above.

| Browser/context          | Complete suite with sync | Wall time including build/startup |
| ------------------------ | -----------------------: | --------------------------------: |
| Chrome Window            |            **90 passed** |                          217.05 s |
| Chrome Dedicated Worker  |            **87 passed** |                          211.12 s |
| Firefox Window           |            **90 passed** |                          168.11 s |
| Firefox Dedicated Worker |            **87 passed** |                          165.91 s |

Also passed: all seven standalone Wasm feature checks and graph guards, native
remote-only and combined native-stage graph guards, Window/Worker Clippy, public
quickstart compilation; native SQL **70**, KV **19**, protocol/ownership **5**, and
runtime **10** tests; native Clippy; workspace/fixture formatting and diff checks.

Final broad gates were attempted independently, not waived:

- `cargo hack test --feature-powerset --all-targets` failed after **19.22 s** on
  `ofdb-btree --no-default-features --features sharded`, the same eight previously
  documented missing `std`/`async_stream`/in-memory-export/`ToString` errors.
  Remaining feature combinations did not complete.
- `just crap` timed out after **240.01 s**, during coverage dependency compilation
  for `ofdb-kv` (package 7/32); no complete CRAP score.

No new measurement, whole-browser crash/power-loss, physical quota or
cross-tab-notification claim is made. This fixes the P2 read-only transaction
contract, not unrelated broader native feature/coverage failures.

## Native cold-read regression correction — 2026-10-25

The first review fix at `90a47d7` missed backend differences: in-memory reads of
absent catalogs returned Storage errors, while Redb transaction reads implicitly
created physical tables. The earlier native tests passed but did not cover cold
catalog reads; they were insufficient regression coverage, not proof of parity.

The follow-up keeps catalog initialization out of SELECT-only batches and fixes
the backend read contract. In-memory missing point/full/range reads return empty;
untouched commits neither advance revision nor overwrite newer state. Redb
transactional reads check for an existing table before opening it, without table
creation or invalidation. Explicit DDL/write creation paths remain unchanged.
SQL still validates user table existence and reports Rejected `Table not found`,
rather than treating a missing SQL table as an empty user table.

Five new native facade tests cover cold catalog reads on both native backends,
missing-user-table errors, cold explicit SELECT followed by committed DDL,
in-memory ordinary/stale-reader revision isolation, and subscription silence
plus an empty physical Redb table list after reads. Two Kernel tests also cover
missing point/full/range reads. Behavioral failures were observed before fixing
in-memory cold reads, revision-changing read commits, and Redb cold-read signals.

Passed before the browser rerun:

```sh
cargo test -p ofdb-sql-engine -p ofdb-sql-engine-automerge -p ofdb-sql-engine-redb -p ofdb-btree-redb --features ofdb-sql-engine/in-memory,ofdb-sql-engine/std --all-targets
cargo clippy -p ofdb-sql-engine -p ofdb-sql-engine-automerge -p ofdb-sql-engine-redb -p ofdb-btree-redb --features ofdb-sql-engine/in-memory,ofdb-sql-engine/std --all-targets -- -D warnings
cargo check -p ofdb-sql-engine --no-default-features --features in-memory
python3 scripts/browser_matrix.py native --timeout 600 --output target/review-followup/native
```

Native regressions: **75 SQL**, **19 KV**, **5 protocol/ownership**, **10 runtime**
tests passed, plus native facade/runtime Clippy. The backend test and native
commands used two build jobs, 16 MiB native stack, and the same ticket-10 native
build directory as the preceding review validation. Clippy and the no-default
engine check also reused that build directory.

Then reran the `browser`, `check`, and `final` script suites with the same driver,
capabilities, target directories and timeouts as the preceding review section,
with output under `target/review-followup/{browser,check,final}`. All seven Wasm
feature checks, graph guards, Window/Worker Clippy, public quickstarts and
formatting passed. Full browser acceptance, including the original read-only
revision/subscription/conflict and empty-DDL replay regressions:

| Browser/context          | Complete suite with sync | Wall time including build/startup |
| ------------------------ | -----------------------: | --------------------------------: |
| Chrome Window            |            **90 passed** |                          245.64 s |
| Chrome Dedicated Worker  |            **87 passed** |                          235.13 s |
| Firefox Window           |            **90 passed** |                          195.63 s |
| Firefox Dedicated Worker |            **87 passed** |                          172.82 s |

Final broad gates remain incomplete: powerset failed after **1.97 s** at the
previously documented eight `ofdb-btree --no-default-features --features sharded`
errors; CRAP timed out after **240 s**, compiling coverage for `ofdb-kv-client`
(package 11/32). No full powerset pass or complete CRAP score. No new
performance, crash/power-loss, quota or cross-tab-notification claims.
