## Problem Statement

Currently, the database engine enforces a hard restriction that every table must have a single primary key column of type `UUID`. This limits the ability to use natural or user-defined primary keys (like integers or strings) and makes composite primary keys difficult to define without additional mapping.

## Solution

The engine will be updated to support flexible primary keys. A table can now have a primary key of any supported type. If the primary key is not a `UUID`, the engine will automatically create a unique index that maps the user-provided primary key value(s) to the internal row `UUID`. This allows for natural, user-defined, and composite primary keys while maintaining the internal `UUID`-based row identification system.

## User Stories

1. As a developer, I want to define a table with an integer primary key, so that I can use natural identifiers instead of generating UUIDs.
2. as a developer, I want to define a table with a string primary key, so that I can use unique names or codes as identifiers.
3. As a developer, I want to define a table with a composite primary key consisting of multiple columns, so that I can model complex relationships naturally.
4. As a system, I want to automatically maintain a unique index for non-UUID primary keys, so that lookups by the primary key remain efficient.

## Implementation Decisions

- **Schema Validation**: The `TableSchema` validation logic will be updated to rename `validate_uuid_primary_key` to `validate_primary_key`. The requirement that the primary key must be of type `UUID` will be removed, while still enforcing that exactly one primary key exists.
- **Index Creation**: The `create_table` operation will be modified. If the detected primary key type is not `UUID`, the engine will create an implicit unique index. This index will contain the primary key columns and the internal row `UUID`.
- **Lookup Mechanism**: The engine will leverage its existing indexing system to perform lookups via the newly created primary key index.

## Testing Decisions

- Tests will verify that tables can be created with various primary key types (Integer, Text, etc.).
- Tests will ensure that primary key lookups work correctly for these non-UUID types.
- Tests will verify that unique constraint violations are correctly caught for the new primary key indexes.
- Tests will use existing integration testing patterns within the `sql-engine` crate.

## Out of Scope

- Changing the internal representation of rows (they remain `UUID`-based).
- Adding new primary key types beyond those already supported by the `Value` type.

## Further Notes

This change aligns with the existing indexing architecture where all indexes are mappings from values to row `UUID`s.
