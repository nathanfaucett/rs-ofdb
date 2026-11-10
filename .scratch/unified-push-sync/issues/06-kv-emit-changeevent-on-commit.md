# Issue 06: KV — Emit ChangeEvent on KvStore commit

## Description

Modify `KvStore`/`KvTransaction` to emit `ChangeEvent` via `ChangeSignal::committed_with_events` on successful commit. Each changed document produces one event with its incremental Automerge blob.

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

- `KvTransaction::commit()` collects changed documents
- For each changed document: extracts incremental blob via `document.save_incremental()`
- Calls `ChangeSignal::committed_with_events([ChangeEvent { key: StorageKey { table: "", key: doc_id }, blob, generation }])`
- `generation` from document heads hash (or monotonically increasing counter)
- Existing `committed_invalidations()` still works for old subscribers
- Unit test: set key → commit → verify ChangeEvent emitted with correct blob

## Seams

- `kv-store/src/store.rs` — `KvTransaction::commit()`
- `kv/src/database.rs` — `Database::commit()` (if different path)
- `btree` — `ChangeSignal` available on backend

## Notes

- `StorageKey::table = ""` for KV (distinguishes from SQL tables)
- `StorageKey::key = document_id` (Vec<u8>)
- Blob = `AutoCommit::save_incremental()` bytes
- Only emit for documents that actually changed (heads differ)
- Must not emit for read-only transactions

## Comments

## Answer
