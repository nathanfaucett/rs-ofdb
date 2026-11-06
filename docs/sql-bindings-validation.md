# SQL browser binding qualification

## Scope and risk

Ticket 01, app-owned data. Work is limited to the SQL TS/WASM binding, its build,
package, and tests. No app, service, filesystem, or sync implementation is changed.
The public contract is in [`packages/sql/README.md`](../packages/sql/README.md).

Priority: foundational. Complexity: one reused owner value codec and a small
owned-handle lifecycle. Risk: high for precision, transaction atomicity, storage
failure, and an interrupted commit. Validation uses the user-approved seams:
the public TS SQL wrapper with generated WASM, and browser transactions/reopen.
No database or generated-binding mock is used.

## Source review versus measured failures

Source review found `withDeadline`/`cloneClient` names that differed from generated
`with_deadline`/`clone_client`, a JS-number declaration for `u64`, guessed
base64/text values, integer conversion through JSON numbers, missing explicit
handle disposal, and a browser bundle that could relocate the WASM URL.

Measured red/green sequence in a real Chromium browser:

1. The original wrapper failed with `this.wasm.cloneClient is not a function`.
   Importing generated declarations, using the generated names, and validating
   positive `u64` bigint deadlines made the public signature test pass.
2. Tagged UUID/text/blob/integer parameters failed with `Column value has the
wrong type`. Reusing the existing public owner
   `ofdb_kv::{parse_value, encode_value}` codec made the lossless round-trip pass.
   No second Rust lossless codec was introduced.
3. The cleanup test failed with `db.close is not a function`. Owned handle
   disposal was implemented. The same test then exposed an out-of-bounds WASM
   trap in transaction execution with the default stack. Rebuilding with the
   owner's tested 16 MiB stack made it pass. The package build now fixes that
   setting; the ignored per-crate Cargo profile/target settings were removed.
4. The first browser bundle fetched `browser/ofdb_wasm_bg.wasm` and returned 404.
   Keeping the generated module external to that bundle preserves its adjacent
   WASM and adapter paths. The browser bundle then passed the suite.
5. The installed-consumer declaration check exposed the generated
   `Symbol.dispose` library requirement. It now checks with
   `ES2022,DOM,ESNext.Disposable`, without `skipLibCheck`. The requirement is
   documented. Negative type assertions check primitive values and number-based
   deadlines are not accepted.

## Measured browser evidence — 2026-11-02

Linux x86_64, Rust **1.99.1**, wasm-pack **0.13.1**, Cargo-locked wasm-bindgen
**0.2.130**, Node **26.5.0**, pnpm **12.4.1**, TypeScript **5.9.3**, Rolldown
**1.2.16**, Vite **8.3.5**, Playwright **1.49.0**. The browser is the existing
Playwright cache build `chromium-1248`: **Chrome for Testing 156.0.8078.4**.
`BROWSER_PATH` selects that executable. The attempt to install Playwright 1.49's
browser exceeded a 120-second budget; it is not used as evidence.

Commands from the repository root, with an existing shared `CARGO_TARGET_DIR`
selected for compilation and `BROWSER_PATH` selected for browser tests:

```sh
pnpm install --frozen-lockfile
pnpm --filter @ofsdk/sql run build:all
pnpm --filter @ofsdk/sql run check
pnpm --filter @ofsdk/sql run lint
pnpm --filter @ofsdk/sql test
pnpm --filter @ofsdk/sql run test:package
cargo test --locked -p ofdb-kv --lib value_codec
cargo fmt --all -- --check
cargo fmt --manifest-path tests/browser/Cargo.toml --all -- --check
```

Results:

- Actual release WASM build: passed; fixed **16 MiB stack**, no wasm-opt step.
- TS declaration/check and source lint: passed. Generated signatures are imported,
  not duplicated. Installed tarball declarations also passed strict compilation.
- Prebuilt browser export: **15 tests passed**.
- Isolated consumer installed offline from the tarball, then Vite production build
  and the same real-browser suite: **15 tests passed**. Window and dedicated
  module Worker both load generated WASM and the real IndexedDB adapter. No Node
  polyfill or native import is needed. The consumer uses only installed package
  files, not TS source aliases or another checkout.
- Existing owner lossless codec tests: **2 passed**.
- Rust formatting and `git diff --check`: passed.

The 15 browser tests cover:

- Generated clone/deadline signatures; invalid deadlines.
- Base64-looking text, empty text/blob, binary ciphertext, UUID, signed i64 limits,
  a value above JS safe integer range, negative-zero float bits, bool, null,
  collision-free JSON object keys, nested signed/unsigned numbers, float bits,
  empty arrays/objects, positional and named parameters, and both execute aliases.
- Rejected untagged values, undefined, NaN, JS bigint/number cells, integer overflow,
  noncanonical/malformed base64, malformed UUID/float/tag payloads and extra fields;
  rejection produces no partial write.
- Atomic multi-call transaction commit/rollback, pending-write invisibility,
  stale writer rejection, and consumed handles after success/failure.
- Storage denial (`SecurityError`), incompatible-storage failed open with a
  preserved sentinel, and successful retry after denial.
- Injected `QuotaExceededError` after native writes have started, and native abort
  after the first successful records request. Reopen shows no partial commit.
  These reuse the owner's existing browser fault helpers, not mock storage.
- Page restart before commit (no writes) and after native commit with its
  completion callback withheld (all writes). Reopen resolves the lost outcome.
- In-flight close, concurrent transaction calls, independent client ownership,
  and ten open/close/delete cycles without forced garbage collection. Native
  deletion is not blocked by retained connections after all owned handles close.
  Every browser test also deletes its SQL databases at teardown and fails if a
  retained connection blocks deletion, including after quota/abort/failed open.
- Real module-worker commit followed by page restart, and retry after a failed
  WASM fetch in the same realm.

## Artifact delivery and reproducibility

Build output:

- `packages/sql/artifacts/ofdb-sql-0.1.0.tgz` (about 1.6 MiB compressed).
- `packages/sql/artifacts/SHA256SUMS` for copying and offline verification.
- `packages/sql/pkg/ofdb_wasm_bg.wasm` (6,817,290 bytes).
- `packages/sql/artifacts/consumer/dist/`: measured Vite production consumer.

The source and tests are committed; build outputs are ignored. Copy the tarball
and checksum into the consuming checkout (or an immutable artifact store). Its
installation needs no sibling source path, npm runtime dependencies, or Rust.
The package contains licenses, README, ESM, browser bundle, declarations, WASM,
and generated IndexedDB adapter snippets.

Consecutive pinned `build:all` runs produced identical WASM and generated JS.
Repeated packing of the final source produced identical tarballs in this worktree:

```text
f4431b41e36a8c440833f0ef2f1f66a7be805de91b8c8111b91083c443c5e5c6  pkg/ofdb_wasm_bg.wasm
cad2568dc446280dcd5cf72a1105061a1b1e9c5e59a6d869b6fd46f2c3bc620d  pkg/ofdb_wasm.js
6890d08568c7a0425d7dd6d3f3d3f8ab33013a32c131a7f0290ab8b976c879b0  artifacts/ofdb-sql-0.1.0.tgz
```

This measures repeatability on this toolchain/host. Byte-for-byte identity across
other operating systems or toolchains is not established. The build script
checks Rust/wasm-pack versions, uses Cargo/pnpm locks and a fixed stack, and bounds
compilation. It does not depend on this host's checkout layout.

## Unmet repository gates and limits

These are not passing gates and must not be presented as such:

- `RUST_MIN_STACK=16777216 CARGO_BUILD_JOBS=2 cargo hack test --feature-powerset --all-targets`
  failed in existing `ofdb-btree` sharded no-default-feature tests: unresolved
  imports and missing `ToString`, before the binding suite. Cargo exited 101;
  cargo-hack exited 1. This change does not fix that unrelated feature matrix.
- `CARGO_BUILD_JOBS=2 timeout --kill-after=5 90 just crap` timed out during coverage
  dependency compilation, exit 124. No CRAP score was obtained.
- `cargo clippy --locked -p ofdb-wasm --target wasm32-unknown-unknown -- -D warnings`
  failed in existing `crates/platform/src/time.rs` at `manual_is_multiple_of` and
  `result_unit_err`, exit 101. The actual WASM build passes but still reports the
  existing deprecated base64 functions used by the unchanged KV WASM conversion.
- The updated Rust `tests/browser/src/wasm_api.rs` fixture was formatted and aligned
  with the tagged SQL contract, but its separate Rust browser runner was not run.
  The measured acceptance above is the requested generated-WASM/public-TS seam.

No claim is made for actual disk filling, OS process crash/power loss during an
IndexedDB write, heap/task-count profiling, other browser engines, browser minimum
versions, cross-host reproducible bytes, registry publication, app adoption,
sync/subscriptions, or the existing KV JS conversion. Quota and interrupted-commit
coverage have the controlled injection/restart scope stated above. All SQL handle
cleanup and connection-release assertions passed; they are not a general heap
leak or background-task profiler.

## Typed SQL responses — 2026-11-06

Tickets 01 and 02 add matching `Client.select` and `Transaction.select` methods.
Ticket 02 starts from integration commit `3194a76`. Both methods use the internal
`parseRows` implementation and their existing `execute` path. No Rust, WASM,
transport, storage, or runtime dependency changes were needed.

Measured TDD sequence: the first public transaction typed-read test failed with
`tx.select is not a function`. Adding the shared-path method made it pass.
Existing mapping, tagged-value, and constructor-boundary transport tests now run
against both Query Handles. Additional real-browser tests cover pending writes,
parameters, empty results, independent ownership, parser failure followed by
explicit rollback/commit/close, post-execution batch rejection, busy operations,
consumed handles, and close during an in-flight typed read.

Validation used Node 26.5.0, TypeScript 5.9.3, Playwright 1.49.0, and the existing
Chromium 156 cache build. The existing Ticket 01 WASM artifact was reused; no new
WASM compilation was needed. An ignored local CommonJS Playwright config used
the existing HTTP test server because the Portless setup had timed out. An ESM
local config also timed out before test discovery. Neither local config nor any
infrastructure override is part of the change.

Results:

- `pnpm --filter @ofsdk/sql run check` and `run build`: passed.
- `node browser-tests/package.mjs`: passed for each supported entry point.
  Offline tarball installation, strict positive/negative declaration checks for
  both entries, and Vite production builds passed. Consumer assertions cover
  both Query Handles, parser return inference, readonly inputs, asynchronous
  parser rejection, argument order, and tagged parameters.
- All **21 typed-read tests passed** through each packed entry:
  `@ofsdk/sql` and `@ofsdk/sql/browser`.
- Full direct browser suite and full packed suites for both entries: each had
  **44 passes and 2 failures**. The same two reported baseline failures remain
  in `migration-validation.test.ts`: invalid input reports an undefined version
  instead of 0; history validation reports version 2 instead of 1. No typed-read
  or transaction test failed.
- `pnpm --filter @ofsdk/sql run lint`: the same five existing
  `noShadowRestrictedNames` errors for the package's `Error` name. No lint fixes
  were included.
- `timeout --kill-after=5 120 cargo hack test --feature-powerset --all-targets`:
  failed in existing `ofdb-btree` sharded no-default-feature tests with unresolved
  imports and missing `ToString`; cargo-hack exit 1. The matrix did not complete.
- `timeout --kill-after=5 120 just crap`: timed out, exit 124, during coverage
  compilation for crate 6 of 33 (`ofdb-value`). No final CRAP score was produced.

These results do not qualify a fresh WASM build, another browser engine, or app
adoption. Ticket status was not changed.
