# Map: Rename Catalog Tables

## Notes
The goal is to rename the internal catalog tables to human-readable names and simplify the index storage mechanism so that indices are treated as first-class named entities, consistent with how tables are handled.

## Decisions-so-far
- **Catalog Table Renaming**: The storage names for internal tables will be changed from `__engine_...` to:
    - `tables`
    - `table_fields`
    - `indices`
    - `index_fields`
- **Index Storage Simplification**: The `index_storage` function will be refactored to accept an index name instead of a UUID, using the name directly for storage.
- **Namespace Protection**: Implementation of checks in `create_table` and `create_index` to ensure these new names are reserved and cannot be shadowed by user-created entities.

## Fog
- Finalizing the exact implementation of `index_storage` refactoring (expanding before contracting).
- Verifying full coverage of the test suite against the new naming convention.
