# 01: Ship a runnable persistent KV example

**What to build:** A developer can build and serve a minimal browser page through just, save and retrieve a KV value using the public Rust facade, and reload the page to observe persistent IndexedDB data. Include documentation, accessible operation states, and a real served-page smoke check as part of the working example.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] One minimal page provides labeled native controls to enter a KV Key and text value, save, and retrieve the committed value or a clear missing-key state.
- [ ] Database construction and Query Operations stay in Rust using Database to create the Client. Use only required browser features, stable example-specific IndexedDB storage, and no silent memory fallback or destructive initialization.
- [ ] Controls are keyboard-accessible and guarded during initialization and relevant pending operations. Loading, committed success, and actionable errors are visible and accessible; success is shown only after the operation completes.
- [ ] User-provided content is rendered as text, never executable markup. All required handwritten JavaScript, including browser helpers, has meaningful JSDoc.
- [ ] Documented just commands build and serve the actual Wasm application from a clean checkout. Explain prerequisites, the local URL, server shutdown, origin-scoped storage, and browser storage limitations. Do not require Python, a frontend framework, a public JS SDK, or a new application-level runner.
- [ ] A bounded smoke check through just drives the actual served page in Chromium and Firefox: save a distinctive value, await commit success, reload the same origin/context, retrieve, and verify the value. Fresh storage shows a missing state, and markup-like input displays safely.
- [ ] Automated checks use isolated browser storage, report failures nonzero with useful diagnostics, and clean up browser/server processes on failure or timeout. Reuse existing browser tooling where practical rather than introducing a generic automation framework; WebKit is not required.
- [ ] Check the example's independent browser build and dependency isolation, Rust formatting and Clippy, handwritten JavaScript syntax/JSDoc, basic keyboard use, and visible error handling through a controlled failure where feasible without production-only injection APIs.
- [ ] Link the example from public browser documentation and keep instructions complete for KV use without waiting for SQL or the separate just migration.
