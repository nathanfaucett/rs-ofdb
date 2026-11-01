# 02: SQL full stack — WASM Database/Client/Transaction + TS mirror

**What to build:** The SQL store exposes a Database → Client → Transaction pattern matching the Rust facade. `SqlDatabase.open(name)` returns a Database; `.client()` returns a Query Handle with `execute`, `executeSql`, `transaction()`, `withDeadline(ms)`, `clone()`. Transactions support `execute`/`executeSql` plus `commit()`/`rollback()`. `SqlResult` includes `columns` and `rows`. The TS package mirrors this exactly.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] WASM crate exposes `SqlDatabase`, `SqlClient`, `SqlTransaction` with execute/executeSql
- [ ] `wasm_bindgen_test` in `tests/browser/` covers open → client → execute → transaction → commit/rollback → persistence across reopen
- [ ] TS `packages/db/src/` mirrors the pattern with `SqlResult` type and typed errors
- [ ] `tsc --noEmit` passes, example still compiles
