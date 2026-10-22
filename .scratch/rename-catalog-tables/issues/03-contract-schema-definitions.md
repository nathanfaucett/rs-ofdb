# 03: Contract Schema Definitions

**What to build:** Remove the obsolete `__engine_` constants and the UUID-based `index_storage` function to clean up the module.

**Blocked by:** 02: Migrate Engine and Tests

Status: done

- [x] `schema.rs` is cleaned up and only contains the new, human-readable definitions.

## Comments

### Implementation

- Started from a clean `rename-catalog-tables-03` worktree at `5b9554d`, identical to integration `rename-catalog-tables`. Verified both integration and ticket 01 (`6b5fc50`) are ancestors before implementation. No edits in the integration worktree.
- Complexity: low; risk: namespace regression and index-storage ownership; priority: remove obsolete naming without weakening the four real catalog reservations or table/index collision protection.
- Deleted the unused UUID-derived storage helper and four transitional `_NAMED` aliases. Renamed the active helper and all callers to `index_storage(name: &str) -> &str`, avoiding allocations except where the index list must own a storage name.
- Removed both obsolete `__engine_index_` prefix guards. Only `tables`, `table_fields`, `indices`, and `index_fields` are catalog reservations; existing cross-kind storage collision protection remains.
- Searched the whole repo, including hidden issue documents and docs, for callers and obsolete names. Updated current design claims about catalog names, table identity, index storage, and cleanup. Historical spec/ticket descriptions remain historical evidence, not compatibility code.
- Marked ticket 01 done with evidence from its original implementation commit.

### TDD and focused validation

Agreed seam: public SQL via the facade. Added table and index tests in separate red/green slices: both initially failed with `Catalog table names are reserved` on `__engine_index_user`, then passed after removing their respective guards. Tests also exercise the four former catalog names as ordinary user names, table CRUD/drop, index catalog queries/drop, and unique-index enforcement. Corrected the index fixture to isolate each name rather than reuse a tombstoned row UUID. Existing SQL tests continue to reserve all four actual catalogs and protect shared storage.

Passed (commands use this worktree Cargo manifest when invoked externally):

- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sql,redb,in-memory,sync --test local_sql --test cluster_sync` — 17 + 10 tests.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test kernel --test scoped_session --test session` — 50 + 6 + 11 tests.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --test catalog` — 22 tests.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sql,redb,in-memory,native-stage --test scoped_sync` — 25 tests.
- `cargo check -p ofdb-sql-engine --no-default-features`.
- `cargo clippy -p ofdb-sql-engine --no-default-features` — no warnings.
- Rustfmt on changed engine files and added SQL test blocks; `git diff --check`.

Full feature powerset and `just crap` intentionally deferred to the coordinator. No new dependencies, compatibility layer, or automatic migration; old databases require fresh storage/manual migration per spec.
