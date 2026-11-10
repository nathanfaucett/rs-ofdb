# Issue 02: Core — ChangeSignal::committed_with_events

## Description

Add `ChangeSignal::committed_with_events(events)` that broadcasts `ChangeEvent` (including the incremental blob) via the watch channel. This replaces the invalidation-only `committed_with` for new callers.

## Type

task

## Priority

Critical

## Status

ready-for-agent
## Blocked by

- **01-core-changeevent-storagekey**: ChangeEvent and StorageKey types must exist

## Acceptance Criteria

- `ChangeSignal::committed_with_events(events: impl IntoIterator<Item = ChangeEvent>)` added
- Internally broadcasts via existing `watch::Sender<History>` with extended payload
- `History` struct extended to store `Vec<ChangeEvent>` per revision (or new parallel channel)
- `committed_with(invalidations)` kept as deprecated wrapper for transition
- Unit test: call `committed_with_events` → subscriber receives events with correct blobs

## Seams

- `btree/src/changes.rs` — `ChangeSignal` impl, `History` struct
- Storage backends (KvStore, SqlEngine) will call this on commit

## Notes

- Must not break existing `ChangeSubscription` consumers yet
- Consider dual-channel approach: one for invalidations (old), one for events (new)
- Or extend `History.batches` to `(u64, Vec<ChangeEvent>)`

## Comments

## Answer
