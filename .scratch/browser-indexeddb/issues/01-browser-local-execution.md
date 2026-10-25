# 01: Make embedded storage safe for browser-local execution

**What to build:** Run existing in-memory SQL and KV queries in a single-threaded browser Wasm environment without a Tokio runtime, while preserving native thread-safety guarantees. This is the initial cross-cutting portability prefactor for the browser IndexedDB specification.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Use a consistent target-aware strategy for storage traits, SQL Kernel, RowCodec, Row Reconciler, stream types, boxed transaction callbacks and sync staging. Browser-local JavaScript handles and futures must not require unsafe Send/Sync assertions.
- [ ] Replace affected declarations and call sites coherently; do not retain temporary compatibility interfaces or duplicate browser engines.
- [ ] Native builds retain existing Send/Sync contracts and support for spawned query and sync work.
- [ ] Public in-memory SQL and KV Query Handles execute representative reads, writes and transactions in real browser tests without Tokio.
- [ ] Verify browser wall-clock timestamps, KV expiry, UUID randomness and Automerge actor generation at runtime, with necessary feature propagation.
- [ ] Add focused Wasm compile checks and a real-browser smoke-test harness usable by subsequent tickets in Window and Dedicated Worker contexts.
- [ ] Supported browser checks do not acquire native-only development dependencies; target-gate those dependencies or isolate browser consumer tests as needed.
- [ ] Run focused native regressions and record exact validation results. Compilation alone is not proof of browser clock or randomness support.
