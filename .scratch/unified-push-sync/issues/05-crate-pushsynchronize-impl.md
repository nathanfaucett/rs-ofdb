# Issue 05: Crate — push_synchronize() implementation

## Description

Implement the core `push_synchronize()` function that runs the push-based sync protocol over a `PushSyncTransport`. Handles outbound event streaming, inbound application, snapshot fallback, and acknowledgment flow.

## Type

task

## Priority

Critical

## Status

ready-for-agent
## Blocked by

- **04-crate-pushsync-protocol-transport**: Protocol messages and transport trait must exist
- **03-core-changesubscription-events-stream**: Outbound event stream source

## Acceptance Criteria

- `push_synchronize<B, T, F>(store, transport, config, apply_inbound)` implemented
- **Outbound**: reads `ChangeEvent` stream from storage, sends `PushMessage::ChangeEvent`
- **Inbound**: receives messages, applies `ChangeEvent` via `apply_inbound` callback
- **Snapshot fallback**: on missing deps, sends `SnapshotRequest`, receives `Snapshot`, applies
- **Ack flow**: receiver sends `Ack` per key; sender tracks unacked, resends on timeout
- **Hello/manifest**: exchanges digest, skips sync if equal
- **Done**: both sides send `Done` when caught up
- Returns `PushResult { sent_changes, received_changes, sent_snapshots, received_snapshots }`
- Integration test: two in-memory stores, write on one → verify push delivery to other

## Seams

- `push-sync/src/session.rs` — main sync loop
- `push-sync/src/outbound.rs` — outbound event handling
- `push-sync/src/inbound.rs` — inbound message handling

## Notes

- Single function replaces both `kv-sync::synchronize` and `sql-sync::synchronize`
- `apply_inbound` callback abstracts KV vs SQL apply logic
- Backpressure via `max_pending_acks` config
- Retry with exponential backoff on transport errors

## Comments

## Answer
