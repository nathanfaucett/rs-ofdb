# 02: Engine Implementation (Primary Key Indexing)

**What to build:** Update the `create_table` logic in the engine to use the new flexible primary key validation. If any primary key column is not of type `UUID`, the engine will automatically create a unique index that maps the primary key value(s) to the internal row `UUID`, enabling lookups by the user-defined key.

**Blocked by:** 01: Schema Expansion (Add flexible validation)

**Status:** done

- [x] Use `validate_primary_key` in `create_table` (already wired by ticket 01).
- [x] Distinguish a single UUID primary key from natural and composite primary keys, including UUID-only composites.
- [x] Automatically create a unique index over all user primary-key columns, mapping to the existing internal row UUID payload. Support configurable names with default `pk_<table_name>`.
- [x] Verify implementation with integration tests.

## Comments

### 2026-10-23 — Implementation and validation

- Risk assessed as moderate: identity/index mismatches can corrupt lookup and mutation consistency. Storage/replication identity remains an immutable UUID; single UUID primary keys retain their UUIDv5/v7 identity contract, while natural/composite inserts generate independent UUIDv7 identities.
- Index maintenance now receives the actual storage row UUID rather than extracting it from user columns. Rebuild and incoming-state maintenance preserve this identity. The UUID remains the existing index-record payload; it is **not** added to the unique column tuple.
- Natural/composite primary keys reject NULL/missing values and duplicates. Existing SQL predicate execution, updates, deletes, secondary indexes, rebuilds, and primary-key upserts work with flexible values. SQL primary-key columns remain immutable, as before. The existing SQL executor scans predicates; no new query optimizer was introduced.
- Configure names through SQL `CONSTRAINT <name> PRIMARY KEY (...)` (or named inline primary keys), or typed `CreateTableWithIndexes` with a unique index covering exactly the primary-key columns. Index order may differ from table column order. Without an explicit matching index, natural/composite tables get `pk_<table>`.
- Prevent dropping primary-key enforcement indexes and reject index-name collisions atomically. Preserve additional column-level UNIQUE constraints on composite-key tables.
- Updated `GLOSSARY.md` Row Identity and documented the typed index-naming contract. Ticket 03 remains untouched.
- TDD: observed failing integration tests for integer insertion, named composite creation, removing primary-key enforcement, index-name collisions, additional column uniqueness, and reordered composite conflict targets before the corresponding fixes.
- Passed final focused validation: `cargo test -p ofdb-sql-engine-automerge --test flexible_primary_keys` (9 tests); `cargo test -p ofdb-sql-engine -p ofdb-sql-translator -p ofdb-sql-query --lib` (17 tests); `cargo check -p ofdb-sql-engine --no-default-features`; `cargo fmt --all -- --check`; `git diff --check`.
- `cargo test -p ofdb-sql-engine-automerge --tests` passed during implementation, including existing kernel, catalog, conflict, and sync suites. A final broader rerun hit its 120-second limit while recompiling `kernel(test)`, before test execution; final focused tests passed afterward.
- Focused `cargo clippy -p ofdb-sql-engine -p ofdb-sql-engine-automerge -p ofdb-sql-translator -p ofdb-sql-query --all-targets` passed with one existing dependency warning at `crates/btree-redb/src/btree.rs:95` (`needless_question_mark`). The strict `-- -D warnings` attempt is blocked by that unrelated warning; it was not changed.
- Full workspace feature matrix and `just crap` deliberately deferred to the coordinator, as requested.
