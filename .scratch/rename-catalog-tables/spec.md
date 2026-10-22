## Problem Statement

The current database engine uses internal catalog tables that are prefixed with `__engine_` (e.g., `__engine_tables`, `__engine_indices`). This makes them difficult to query directly as regular tables and deviates from the design principle that internal catalog structures should behave like any other table. Furthermore, index storage is currently derived from a UUID, which prevents indices from being treated as named entities in the same way as tables.

## Solution

The internal catalog tables will be renamed to simple, human-readable names, and the mechanism for determining index storage will be simplified so that an index's storage name is its actual name. This ensures that the database schema and its internal structures are consistent and easily queryable.

## User Stories

1. As a database user, I want to query the `tables` table directly so that I can see all available tables without using engine-specifc prefixes.
2. As a database user, I want to query the `indices` table so that I can inspect index definitions easily.
3. As a database user, I want the system to treat indices as first-class named entities, just like tables.
4. As a developer, I want the engine's internal state to be more intuitive to inspect via SQL.

## Implementation Decisions

- **Schema Rename**: The following constants in the schema module will be updated to use simpler storage names:
    - `ENGINE_TABLES_STORAGE` $\rightarrow$ `"tables"`
    - `ENGINE_TABLE_FIELDS_STORAGE` $\rightarrow$ `"table_fields"`
    - `ENGINE_INDICES_STORAGE` $\rightarrow$ `"indices"`
    - `ENGINE_INDEX_FIELDS_STORAGE` $\rightarrow$ `"index_fields"`
- **Index Storage Refactor**: The `index_storage` function will be modified. Instead of generating a name based on a UUID, it will now use the provided index name directly.
- **Namespace Protection**: The `create_table` and `create_index` operations will be updated to reserve the new, shorter catalog names (`tables`, `table_fields`, etc.) to prevent users from creating tables or indices that shadow the internal catalog.
- **Test Suite Update**: All existing tests that verify the presence or behavior of the `__engine_` prefixed tables will be updated to reflect the new naming convention.

## Testing Decisions

- **Integration Testing**: We will update existing integration tests in `crates/sql/tests/` that currently assert the existence of `__engine_` tables.
- **Automated Validation**: We will rely on the existing test suite, ensuring that the change doesn't break core synchronization and schema reconciliation logic.
- **Seam**: The primary testing seam will be the SQL execution path in `sql-engine/src/executor.rs`, ensuring that `CREATE TABLE` and `CREATE INDEX` correctly interact with the updated schema logic.

## Out of Scope

- Changing the underlying data model or storage engine.
- Implementing a new migration system for existing databases (this change assumes a fresh start or manual migration).

## Further Notes

This change aligns the internal database mechanics with the high-level design goal of treating the catalog as a collection of ordinary, queryable tables.
