# 03: Schema Cleanup (Remove UUID restriction)

**What to build:** Remove the legacy `validate_uuid_primary_key` method from `TableSchema` and clean up the `sql-schema` crate, as the new `validate_primary_key` is now the standard.

**Blocked by:** 02: Engine Implementation (Primary Key Indexing)

**Status:** ready-for-agent

- [ ] Remove `validate_uuid_primary_key` from `TableSchema`.
- [ ] Cleanup any remaining legacy references in `sql-schema`.
