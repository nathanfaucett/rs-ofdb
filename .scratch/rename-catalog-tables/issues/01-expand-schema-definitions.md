# 01: Expand Schema Definitions

**What to build:** Add the new, human-readable constants and a new version of `index_storage` that accepts a name instead of a UUID, alongside the existing `__engine_` versions.

**Blocked by:** None (can start immediately).

Status: done

- [x] `schema.rs` contains both the old `__engine_` constants and the new human-readable ones.
- [x] `schema.rs` contains both the UUID-based `index_storage` and a new name-based version.

## Comments

### Completion evidence

- Implemented in `6b5fc5052c16b847e995ebc04e801e0490f0abd1` (`Implement 01: Expand Schema Definitions`), verified with `git show`: added the four human-readable `_NAMED` constants and `index_storage_named(name: String)` alongside the original definitions.
- Confirmed this commit is an ancestor of ticket 03 and integration `rename-catalog-tables`. The checklist records the completed expansion stage; ticket 02 migrated callers and ticket 03 removes the transitional definitions.
