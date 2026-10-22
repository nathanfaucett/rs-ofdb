# 02: Migrate Engine and Tests

**What to build:** Update the SQL engine's execution logic and the entire test suite to use the new human-readable constants and the name-based `index_storage` function.

**Blocked by:** 01: Expand Schema Definitions

Status: done

- [x] `executor.rs` uses the new constants and the name-based storage function.
- [x] All database tests pass using the new human-readable names.

## Comments

### Implementation

- Verified a clean `rename-catalog-tables-02` worktree with HEAD and integration branch `rename-catalog-tables` both at ticket 01 base `6b5fc50` before implementation.
- Complexity: moderate; risk: high where derived index records and logical rows share storage names; priority: protect storage before switching names. No new dependencies or persisted-format compatibility layer.
- Switched the original exported catalog constants to human-readable values, migrating all dependent engine, codec, sync, and test callers together. Added built-in catalog SQL schemas so the renamed catalogs are directly queryable.
- Migrated index creation, lookup, updates/rebuilds, drops, and schema reconciliation to `index_storage_named`. Cleanup of losing/deleted definitions preserves storage used by an active same-name index, including recreation on a different table.
- Reserved all four catalog names and rejected table/index storage collisions before mutation and before reconciliation cleanup. Cross-kind historical name claims remain reserved: reusing a dropped table name as an index (or vice versa) would collide with retained logical-row/tombstone history.
- Updated literal catalog names and prefix-based catalog detection throughout tests. Retained transitional `_NAMED` aliases and the unused UUID `index_storage` helper for ticket 03; that ticket should keep the now-human-readable original exported constants and remove the duplicate aliases/obsolete helper.

### TDD and validation

Public SQL execution is the agreed behavior seam. Observed red then green for catalog querying (`Table not found`), table/index namespace collisions (DDL incorrectly accepted), replicated namespace preflight (without the guard, sync failed incidentally with `Invalid document` rather than a namespace rejection), and recreated-index cleanup (without the ownership guard, a duplicate unique value was accepted after dropping the old owner).

Passed focused commands from this worktree (use `cargo ... --manifest-path /home/nathan/code/rust/aicacia/ofdb-rename-catalog-02/Cargo.toml` when invoking outside it):

- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sql,redb,in-memory,sync --test local_sql --test cluster_sync` — 15 + 10 tests.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test kernel --test scoped_session --test session` — 50 + 6 + 11 tests.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --test catalog` — 22 tests.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features sql,redb,in-memory,native-stage --test scoped_sync` — 25 tests.
- `cargo check -p ofdb-sql-engine --no-default-features`.
- `cargo clippy -p ofdb-sql-engine --no-default-features` — only the five transitional dead-code warnings assigned to ticket 03.
- Rustfmt on changed engine code and new test blocks; `git diff --check`.

Default-stack runs overflowed in SQL tests, including existing explicit-transaction/table-recreation tests; focused passing runs use a 16 MiB test-thread stack. An initial combined validation invocation hit its 200-second limit during compilation; its remaining checks were subsequently run separately and passed. Full feature powerset and `just crap` are intentionally deferred to the coordinator as requested. Existing databases require fresh storage/manual migration per spec.
