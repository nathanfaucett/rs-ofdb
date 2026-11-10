# Issue 01: Core — ChangeEvent and StorageKey types in btree/changes.rs

## Description

Add the core event types that unify KV and SQL sync: `ChangeEvent` (committed change with incremental Automerge blob) and `StorageKey` (unified key for both KV documents and SQL rows).

## Type

task

## Priority

Critical

## Status

ready-for-agent
## Blocked by

(None — can start immediately)

## Acceptance Criteria

- `ChangeEvent { key: StorageKey, blob: Vec<u8>, generation: u64 }` added to `btree/src/changes.rs`
- `StorageKey { table: String, key: Vec<u8> }` added with `table=""` for KV, table name for SQL
- Both types derive `Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize`
- `StorageKey` implements `From<DocumentId>` for KV and `From<(String, Uuid)>` for SQL
- Unit tests: round-trip serialization, ordering by generation then key

## Seams

- `btree/src/changes.rs` — new public types
- No existing callers affected (new types only)

## Notes

- This is a pure additive change — no existing code breaks
- `generation` provides total ordering per key for push protocol
- `blob` = `change.raw_bytes()` from Automerge (the incremental binary blob)
- Keep `StorageInvalidation` for backwards compat during transition

## Comments

## Answer
