# Issue 11: Cleanup — Remove old kv-sync, sql-sync, sync-runtime

## Description

Delete the old pull-based sync crates and their dependencies now that KV and SQL both use `push-sync`.

## Type

task

## Priority

Medium

## Status

ready-for-agent
## Blocked by

- **07-kv-replace-kvsync-with-pushsync**: KV fully migrated
- **09-sql-replace-sqlsync-with-pushsync**: SQL fully migrated

## Acceptance Criteria

- Delete `ofdb/crates/kv-sync/` directory
- Delete `ofdb/crates/sql-sync/` directory
- Delete `ofdb/crates/sync-runtime/` directory
- Remove `kv-sync`, `sql-sync`, `sync-runtime` from workspace `Cargo.toml`
- Remove `feature = "sync"` gates that enabled old crates
- Update `kv/src/lib.rs` re-exports to use `push-sync` types
- Update `sql/src/lib.rs` re-exports to use `push-sync` types
- All tests pass (KV sync tests, SQL sync tests)
- No references to old types (`KvInventory`, `KvChange`, `SyncRowInventory`, `SyncIncrementalChange`, etc.)

## Seams

- Workspace `Cargo.toml` — remove crates
- `kv/src/lib.rs` — re-exports
- `sql/src/lib.rs` — re-exports
- `storage-server` / `storage-service` — update imports

## Notes

- This is a wide refactor — blast radius across callers
- Do in batches: 1) workspace + kv re-exports, 2) sql re-exports, 3) storage-server, 4) delete dirs
- Each batch must keep CI green
- Old protocol types can be kept temporarily behind `#[deprecated]` if needed for transition

## Comments

## Answer
