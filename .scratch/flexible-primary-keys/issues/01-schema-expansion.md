# 01: Schema Expansion (Rename and relax validation)

**What to build:** Rename `validate_uuid_primary_key` to `validate_primary_key` on `TableSchema`. The new validator enforces that at least one primary key column exists (allowing composite keys) and that all primary key columns use supported types, without requiring `UUID`. This allows the engine to transition away from the strict UUID requirement.

**Blocked by:** None (can start immediately).

**Status:** completed

- [x] Rename `validate_uuid_primary_key` to `validate_primary_key` in `sql-schema`.
- [x] Update `sql-schema` tests to verify the new validator allows non-UUID and composite primary keys.
- [x] Update `executor.rs` to call `validate_primary_key` instead of `validate_uuid_primary_key`.


## Comments

### 2026-10-23 — Implementation completed

- Renamed `TableSchema::validate_uuid_primary_key` to `validate_primary_key` directly; no legacy wrapper or code references remain. Updated the executor caller.
- Accepts all eight supported non-null `ValueType` variants and composite primary keys, including multiple UUID components. Rejects missing primary keys and any null-type primary key component; unrelated null-type columns remain allowed. Null defaults remain allowed (they are not column nullability declarations).
- TDD seam: the ticket-specified public `TableSchema` validator. Observed red → green for non-UUID types, composites, and null-type rejection; added missing-key and non-key-null regression coverage.
- Risk/complexity: low, localized schema validation and one caller, with no new dependencies. Schema acceptance intentionally precedes engine indexing; no indexing or row-identity changes were implemented (ticket 02).
- PASS: `cargo test -p ofdb-sql-schema` (5 unit tests); `cargo test -p ofdb-sql-engine --lib` (3 unit tests).
- PASS: `cargo test -p ofdb-sql-schema --all-targets` both with default features and `--no-default-features` (5 tests each).
- Feature matrix: `cargo hack test --feature-powerset --all-targets -p ofdb-sql-schema -p ofdb-sql-engine` passed configurations 1–16 of 18, then timed out at 120 seconds compiling configuration 17. The initial no_std test import failure was fixed before this run. Direct follow-up engine tests with `--all-targets --no-default-features --features default,in-memory,wasm` and `--features std,in-memory,wasm` both PASS (5 tests each). The full cargo-hack invocation is not recorded as passed.
- PASS: `cargo clippy -p ofdb-sql-schema -p ofdb-sql-engine --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`.
- `just crap`: timed out after 120 seconds during coverage dependency compilation; no complexity result available.
- Branch `flexible-primary-keys-schema` started exactly at integration branch `flexible-primary-keys` tip `59ac5a5`; integration tip will be merged before handoff.
