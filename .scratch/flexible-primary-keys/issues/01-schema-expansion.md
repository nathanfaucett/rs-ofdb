# 01: Schema Expansion (Add flexible validation)

**What to build:** Add `validate_primary_key` to `TableSchema` which validates that exactly one primary key exists, without enforcing that its type must be `UUID`. This allows the engine to transition away from the strict UUID requirement.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Implement `TableSchema::validate_primary_key` in `sql-schema`.
- [ ] Update `sql-schema` tests to verify the new validator.
- [ ] Ensure `validate_uuid_primary_key` still exists and passes current tests.
