# 01: Expand Schema Definitions

**What to build:** Add the new, human-readable constants and a new version of `index_storage` that accepts a name instead of a UUID, alongside the existing `__engine_` versions.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] `schema.rs` contains both the old `__engine_` constants and the new human-readable ones.
- [ ] `schema.rs` contains both the UUID-based `index_storage` and a new name-based version.
