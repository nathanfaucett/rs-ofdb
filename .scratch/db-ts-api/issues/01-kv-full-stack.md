# 01: KV full stack — WASM Database/Client/Transaction + TS mirror

**What to build:** The KV store exposes a Database → Client → Transaction pattern matching the Rust facade. `KvDatabase.open(name)` returns a Database; `.client()` returns a Query Handle with `get`, `set` (with optional expiry), `delete`, `scan`, `scanPrefix`, `scanAll`, `transaction()`, `withDeadline(ms)`, `clone()`. Transactions support the same ops plus `commit()`/`rollback()`. The TS package mirrors this exactly.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] WASM crate exposes `KvDatabase`, `KvClient`, `KvTransaction` with all KV operations
- [ ] `wasm_bindgen_test` in `tests/browser/` covers open → client → get/set/delete → scan → transaction → commit/rollback → persistence across reopen
- [ ] TS `packages/db/src/` mirrors the pattern with thin modules, generic `get<T>`, typed errors
- [ ] `tsc --noEmit` passes, example still compiles
