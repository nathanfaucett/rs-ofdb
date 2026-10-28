# 01: Ship a runnable persistent KV example

**What to build:** A developer can build and serve a minimal browser page through just, save and retrieve a KV value using the public Rust facade, and reload the page to observe persistent IndexedDB data. Include documentation, accessible operation states, and a real served-page smoke check as part of the working example.

**Blocked by:** None (can start immediately).

**Status:** complete

- [x] One minimal page provides labeled native controls to enter a KV Key and text value, save, and retrieve the committed value or a clear missing-key state.
- [x] Database construction and Query Operations stay in Rust using Database to create the Client. Use only required browser features, stable example-specific IndexedDB storage, and no silent memory fallback or destructive initialization.
- [x] Controls are keyboard-accessible and guarded during initialization and relevant pending operations. Loading, committed success, and actionable errors are visible and accessible; success is shown only after the operation completes.
- [x] User-provided content is rendered as text, never executable markup. All required handwritten JavaScript, including browser helpers, has meaningful JSDoc.
- [x] Documented just commands build and serve the actual Wasm application from a clean checkout. Explain prerequisites, the local URL, server shutdown, origin-scoped storage, and browser storage limitations. Do not require Python, a frontend framework, a public JS SDK, or a new application-level runner.
- [x] A bounded smoke check through just drives the actual served page in Chromium and Firefox: save a distinctive value, await commit success, reload the same origin/context, retrieve, and verify the value. Fresh storage shows a missing state, and markup-like input displays safely.
- [x] Automated checks use isolated browser storage, report failures nonzero with useful diagnostics, and clean up browser/server processes on failure or timeout. Reuse existing browser tooling where practical rather than introducing a generic automation framework; WebKit is not required.
- [x] Check the example's independent browser build and dependency isolation, Rust formatting and Clippy, handwritten JavaScript syntax/JSDoc, basic keyboard use, and visible error handling through a controlled failure where feasible without production-only injection APIs.
- [x] Link the example from public browser documentation and keep instructions complete for KV use without waiting for SQL or the separate just migration.

## Comments

### Implementation and validation — 2026-10-28

- Branch `implement/browser-examples-kv`, based on verified integration commit
  `2718cffc93efbff751a790de0b194b5b00bfd923`. Isolated worktree at
  `.scratch/browser-examples/worktree-kv` in the original checkout.
- Complexity/risk assessment: a small example; the important risks are accidental
  volatile storage, premature write success, unsafe rendering, and leaked browser
  processes. Keep KV independent so ticket 02 can extend this same application.
- Confirmed test seam is the actual served page, as recorded in the spec. TDD red:
  the real Chrome smoke failed against the missing page before implementation;
  green: the complete served KV flow passes in both required engines.
- Added `examples/browser`, independent Wasm and native-tool Cargo workspaces with
  lockfiles, and `browser-example-build`, `browser-example-serve`, and
  `browser-example-smoke` just recipes. Reused WebDriver and the existing cached
  wasm-pack geckodriver; no Python/Node/Playwright prerequisites. Browsers and a
  matching ChromeDriver were downloaded locally for this environment only.
- **Passed:** `just browser-example-smoke`, Chromium engine **156.0.8078.73** and
  Firefox **159.0**. Fresh driver profiles; missing key; native Tab/Tab/Enter save;
  commit success; reload in the same session/origin; startup fresh read and explicit
  retrieve; markup-like/SQL-punctuation text remains text; browser-test-only
  IndexedDB-open denial yields visible recovery guidance and disabled controls.
- **Passed:** independent Wasm build; dependency-tree isolation (no SQL, sync,
  remote, Redb, native networking or helper dependencies). The facade transitively
  uses Tokio's **sync-only** feature, not a Tokio browser runtime.
- **Passed:** workspace/example/helper rustfmt; example Wasm Clippy and helper
  all-targets Clippy with `-D warnings`; workspace all-targets Clippy with
  `-D warnings`; `git diff --check`. Native helper `cargo test` passes but has
  zero unit tests; behavioral coverage is the served-page smoke, not that command.
- **Passed:** both handwritten scripts parse in both engines, with an additional
  parse-only check of the app source. JSDoc, labels, native focus outlines,
  disabled-state guards and accessible text statuses were reviewed. Keyboard
  coverage used actual WebDriver key events, not an interactive manual browser
  session; no screen-reader or visual accessibility audit is claimed.
- **Passed expected failure:** temporarily withholding the built Wasm file made
  the served-page check exit 1 after its bounded UI wait, with the page's
  actionable startup error in the diagnostic. The asset was restored. Process
  inspection after both passing and failing runs found no test browser/driver
  processes left running.
- **Broad validation blocker:** `cargo hack test --feature-powerset --all-targets`
  fails in unchanged `ofdb-btree` with `--no-default-features --features sharded`:
  missing `std`, `async_stream`, and `InMemoryBTreeShardFactory` imports/features.
  This ticket changes no library code and does not repair that existing failure.
- **Not completed:** `just crap` timed out during instrumented workspace dependency
  compilation (first at 120 seconds, final attempt at 240 seconds). No CRAP score
  or coverage result is claimed. No remote CI, WebKit, real quota exhaustion, or
  crash durability validation is claimed.
- Local detailed logs are retained under the worktree's ignored
  `.scratch/browser-tools/` (`smoke.log`, `expected-failure.log`,
  `feature-powerset.log`, `crap-final.log`, `workspace-clippy.log`).
