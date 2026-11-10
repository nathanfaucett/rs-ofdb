# Issue 08: SQL — Emit ChangeEvent per row on SqlEngine commit

## Description

Modify `SqlEngine` to emit `ChangeEvent` via `ChangeSignal::committed_with_events` for each changed row on commit. Each row is an independent Automerge document.

## Type

task

## Priority

Critical

## Status

ready-for-agent
## Blocked by

- **02-core-changesignal-committed-events**: ChangeSignal must support event broadcast
- **01-core-changeevent-storagekey**: ChangeEvent and StorageKey types

## Acceptance Criteria

- `SqlEngine` commit collects changed rows across all tables
- For each changed row: extracts incremental blob via `SyncRowCodec::export_change` (or direct document access)
- Calls `ChangeSignal::committed_with_events([ChangeEvent { key: StorageKey { table, key: row_uuid_bytes }, blob, generation }])`
- `generation` from row's change hash or sequence
- Catalog tables (tables, fields, indices, index_fields) also emit events
- Existing invalidation path still works for old subscribers
- Unit test: insert row → commit → verify ChangeEvent emitted with correct table, row, blob

## Seams

- `sql-engine-automerge/src/lib.rs` — `AutomergeEngine::commit()` or equivalent
- `sql-engine/src/engine.rs` — commit path
- `sql-sync/src/codec.rs` — `SyncRowCodec::export_change` (source of blob)

## Notes

- `StorageKey::table = table_name` (e.g., "users", "posts")
- `StorageKey::key = row_uuid.as_bytes()`
- Blob = `change.raw_bytes()` from row's Automerge document
- Catalog tables (storage tables) also emit — needed for schema sync
- Must handle multi-table transactions (single commit → multiple events)

## Comments

## Answer
