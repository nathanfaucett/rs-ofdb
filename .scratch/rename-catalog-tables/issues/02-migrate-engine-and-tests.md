# 02: Migrate Engine and Tests

**What to build:** Update the SQL engine's execution logic and the entire test suite to use the new human-readable constants and the name-based `index_storage` function.

**Blocked by:** 01: Expand Schema Definitions

**Status:** ready-for-agent

- [ ] `executor.rs` uses the new constants and the name-based storage function.
- [ ] All database tests pass using the new human-readable names.
