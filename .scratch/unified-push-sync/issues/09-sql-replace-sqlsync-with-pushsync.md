# Issue 09: SQL — Replace sql-sync with push-sync

## Description

Replace the `sql-sync` crate with a thin wrapper around `push-sync`. The new `sql-push-sync` (or updated `sql-sync`) re-exports `push_synchronize` with SQL-specific apply logic.

## Type

task

## Priority

Critical

## Status

ready-for-agent
## Blocked by

- **05-crate-pushsynchronize-impl**: push_synchronize() must exist
- **08-sql-emit-changeevent-per-row**: SqlEngine emits ChangeEvent per row

## Acceptance Criteria

- New crate or updated `sql-sync` depending on `push-sync`
- `push_synchronize` called with SQL-specific `apply_inbound`:
  - `ChangeEvent` → `SyncRowCodec::import_change` / `import_snapshot`
  - Handles `SnapshotRequest` by calling `export_state` + `export_metadata`
- Catalog tables synced first (order 0-3), then data tables (order 4)
- Re-exports `PushConfig`, `PushSyncTransport`, `PushError` as `SessionConfig`, `SyncTransport`, `SyncError`
- `ChangeSubscription` from `sync-runtime` wrapped to use `events()` stream
- All existing SQL sync tests pass with new implementation

## Seams

- `sql-sync/src/lib.rs` — new public API
- `sql-sync/src/apply.rs` — SQL apply_inbound implementation
- `sql/src/lib.rs` — re-exports

## Notes

- `apply_inbound` for SQL:
  ```rust
  |event| {
      let (table, row) = decode_storage_key(&event.key);
      if event.is_snapshot() { 
          codec.import_snapshot(tx, table, row, event.blob) 
      } else { 
          codec.import_change(tx, table, row, event.blob) 
      }
  }
  ```
- Catalog table ordering preserved via `catalog_order(table)` in outbound
- `SyncSelection` replaced by filtering at application layer
- `SyncStage` / staging removed — push protocol handles recovery inline

## Comments

## Answer
