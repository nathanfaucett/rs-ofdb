# 01: Schema Expansion (Rename and relax validation)

**What to build:** Rename `validate_uuid_primary_key` to `validate_primary_key` on `TableSchema`. The new validator enforces that at least one primary key column exists (allowing composite keys) and that all primary key columns use supported types, without requiring `UUID`. This allows the engine to transition away from the strict UUID requirement.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Rename `validate_uuid_primary_key` to `validate_primary_key` in `sql-schema`.
- [ ] Update `sql-schema` tests to verify the new validator allows non-UUID and composite primary keys.
- [ ] Update `executor.rs` to call `validate_primary_key` instead of `validate_uuid_primary_key`.
