# Issue 07: KV — Replace kv-sync with push-sync

## Description

Replace the `kv-sync` crate with a thin wrapper around `push-sync`. The new `kv-push-sync` (or updated `kv-sync`) re-exports `push_synchronize` with KV-specific apply logic.

## Type

task

## Priority

Critical

## Status

ready-for-agent
## Blocked by

- **05-crate-pushsynchronize-impl**: push_synchronize() must exist
- **06-kv-emit-changeevent-on-commit**: KvStore emits ChangeEvent on commit

## Acceptance Criteria

- New crate or updated `kv-sync` depending on `push-sync`
- `push_synchronize` called with KV-specific `apply_inbound`:
  - `ChangeEvent` → `KvTransaction::import_changes` / `import_snapshot`
  - Handles `SnapshotRequest` by calling `export_snapshot`
- Re-exports `PushConfig`, `PushSyncTransport`, `PushError` as `SyncConfig`, `SyncTransport`, `SyncError`
- `ChangeSubscription` from `sync-runtime` wrapped to use `events()` stream
- Existing `synchronize()` signature preserved as wrapper (or new `push_synchronize()` exported)
- All existing KV sync tests pass with new implementation

## Seams

- `kv-sync/src/lib.rs` — new public API
- `kv-sync/src/apply.rs` — KV apply_inbound implementation
- `kv/src/lib.rs` — re-exports

## Notes

- `apply_inbound` for KV:
  ```rust
  |event| {
      if event.blob.is_snapshot() { tx.import_snapshot(...) }
      else { tx.import_changes(&event.key.key, &[NativeChange::from_bytes(event.blob)]) }
  }
  ```
- `sync-runtime::ChangeSubscription` updated to use `btree::ChangeSubscription::events()`
- Can deprecate old `kv-sync` protocol types (`KvInventory`, `KvChange`, etc.)

## Comments

## Answer
