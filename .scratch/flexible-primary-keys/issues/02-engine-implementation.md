# 02: Engine Implementation (Primary Key Indexing)

**What to build:** Update the `create_table` logic in the engine to use the new flexible primary key validation. If any primary key column is not of type `UUID`, the engine will automatically create a unique index that maps the primary key value(s) to the internal row `UUID`, enabling lookups by the user-defined key.

**Blocked by:** 01: Schema Expansion (Add flexible validation)

**Status:** ready-for-agent

- [ ] Update `create_table` in `sql-engine` to use `validate_primary_key`.
- [ ] Implement logic to detect if any primary key column is a non-UUID type.
- [ ] Implement automatic creation of a unique index spanning all primary key columns plus the internal row `UUID` for non-UUID primary keys.
- [ ] Verify implementation with integration tests.
