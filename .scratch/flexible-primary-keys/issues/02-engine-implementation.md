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

### 2026-10-23 — All four review findings fixed

- Single implementer worktree: `.scratch/worktrees/flexible-primary-keys-review-fixes`, branch `flexible-primary-keys-review-fixes`, based on integration `flexible-primary-keys` at `6608df3`. Read the project rules, glossary, spec, and tickets; invoked the TDD skill. Agreed task seams: sync sessions, public replicated-state application, public conflict APIs/transaction reads, and SQL DDL.
- Risk/complexity assessed before implementation: moderate, with replicated schema/index integrity the highest-risk path. Reused existing matching-index detection and lookup; no dependencies, storage-format changes, compatibility wrappers, or unrelated B-tree fixes.
- **P1 sync catalog validation:** removed the UUID-type requirement while retaining exact field ownership, missing-field/key rejection, supported non-null key types, and index-column completeness. `flexible_primary_keys_bootstrap_and_incremental_sync` first failed with `table numbers has no UUID primary key`, then passed for integer, text, and non-UUID composite bootstrap plus incremental updates. The incremental pass asserts a `Changes` frame; replicated duplicate composite inserts remain rejected. Existing catalog rejection tests still pass, with the obsolete text-key rejection changed to null-type rejection.
- **P1 replicated index-name collision:** schema reconciliation now requires every surviving natural/composite table to have a matching unique index after name winners are selected, before commit. Invalid batches are rejected/rolled back rather than committed without enforcement. `replicated_primary_index_collision_rolls_back_the_entire_batch` reproduced the successful invalid apply before the fix; now checks complete exported-state equality on rollback in both directions, preserves the unrelated winning index, and verifies duplicate rejection on the preserved composite-key receiver.
- **P2 conflict APIs:** the shared key resolver uses a matching configured unique index for natural/composite keys and the direct UUID path only for single UUID keys. Keys contain primary-key values in table-column order, reordered internally for configured index order. `conflict_apis_resolve_user_primary_keys_including_transaction_reads` first failed with `Primary key must be a UUID`; now covers integer, configured text, reordered non-UUID and mixed UUID composites, and single UUID keys through `row_conflicts`, `row_conflicts_batch`, `row_conflict_values`, `resolve_row`, and both existing transaction inspection methods. Tests check actual concurrent values, resolution, missing keys, duplicate/input batch order, and malformed keys; callers never need the internal identity. Key-type validation now follows schema validation, so malformed-catalog errors precede malformed-key errors; the existing error-order regression was updated explicitly. Public engine method docs record tuple order.
- **P2 DROP INDEX:** only the final matching unique index is protected. `only_the_last_matching_primary_key_index_is_protected` failed on `DROP INDEX extra_id` before the fix; now covers dropping either redundant/default index, preserving an alternate enforcer, rejecting the final drop, and continued duplicate rejection.
- **PASS final focused suites:** `cargo test -p ofdb-sql-engine-automerge -p ofdb-sql-sync -p ofdb-sql-engine --tests --lib` — 139 tests, including 12 flexible-primary-key tests, 50 kernel tests, 12 session tests, and 22 sync catalog tests.
- **PASS final lint/format:** `cargo clippy -p ofdb-sql-engine -p ofdb-sql-engine-automerge -p ofdb-sql-sync --all-targets --no-deps -- -D warnings`; `cargo fmt --all -- --check`; `git diff --check`.
- **Known validation limitations:** did not rerun the workspace feature powerset or `just crap`, as requested. Coordinator validation already found an unchanged B-tree sharded/no_std feature failure and complexity runs timing out at 180/300 seconds. Those unrelated failures remain unchanged; focused validation does not claim a green workspace matrix.
