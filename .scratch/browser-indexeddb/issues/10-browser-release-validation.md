# 10: Validate and document the supported browser feature matrix

**What to build:** Browser SQL and KV support ships with reproducible real-browser validation, public usage documentation and explicit capability and performance limits alongside preserved native support.

**Blocked by:** 05 — Make browser storage failures and connection lifecycle actionable; 09 — Run continuous browser synchronization with cancellation and retry.

**Status:** ready-for-agent

- [ ] Automate package-specific Wasm checks for KV indexeddb, SQL indexeddb typed statements, optional SQL translation and supported browser sync combinations. Do not require native CLI/server workspace all-features builds under Wasm.
- [ ] Run real-browser acceptance tests in headless Chromium and Firefox, including representative Window and Dedicated Worker usage. Node-only or mocked IndexedDB tests are not acceptance evidence.
- [ ] Validate representative persistence and transaction-lifetime scenarios in WebKit before claiming WebKit support; otherwise document the unvalidated status explicitly.
- [ ] Check supported normal dependency graphs exclude Redb, native filesystem resources, native temporary staging and native TCP/Unix/gRPC transport, while preserving remote-only native isolation contracts.
- [ ] Run formatting, focused Clippy, browser checks/tests, native regressions, cargo hack test --feature-powerset --all-targets and just crap as final implementation validation. Record exact unavailable tools, timeouts and pre-existing failures rather than claiming a pass.
- [ ] Publish browser quickstarts for asynchronous construction, existing Client queries and explicit transactions for both facades, plus caller-owned sync and runner lifetime guidance.
- [ ] Document origin scoping, quota, eviction, private-mode limits, blocked/version-change policy, error behavior and timeout-versus-rollback semantics.
- [ ] Record measured snapshot materialization memory/startup costs and the initial O(database-size) transaction snapshot ceiling; do not advertise selective physical reads without evidence.
- [ ] Document feature selection, supported browser baseline and exclusions: no JavaScript SDK, automatic cross-tab observation, native retirement emulation or bundled browser transport.
- [ ] Remove obsolete or misleading Wasm declarations encountered within this feature's scope rather than preserving compatibility glue; do not perform unrelated cleanup.
