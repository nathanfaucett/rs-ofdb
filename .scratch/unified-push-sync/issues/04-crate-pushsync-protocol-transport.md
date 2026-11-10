# Issue 04: Crate — push-sync protocol and PushSyncTransport trait

## Description

Create new `push-sync` crate with unified push-based sync protocol messages and transport trait. This replaces both `kv-sync` and `sql-sync` protocols.

## Type

task

## Priority

Critical

## Status

ready-for-agent
## Blocked by

- **01-core-changeevent-storagekey**: ChangeEvent and StorageKey types (re-exported)

## Acceptance Criteria

- New crate `ofdb/crates/push-sync` with `Cargo.toml`
- Protocol messages:
  - `PushMessage::Hello { protocol_version, manifest_digest }`
  - `PushMessage::ChangeEvent { key: StorageKey, blob: Vec<u8>, generation: u64 }`
  - `PushMessage::SnapshotRequest { key: StorageKey }`
  - `PushMessage::Snapshot { key: StorageKey, blob: Vec<u8> }`
  - `PushMessage::Ack { key: StorageKey, generation: u64 }`
  - `PushMessage::Done`
- `PushSyncTransport` trait: `async fn send(&mut self, PushMessage) -> Result<(), E>` / `async fn receive(&mut self) -> Result<PushMessage, E>`
- `PushConfig` with `max_frame_bytes`, `max_pending_acks`, `snapshot_fallback_enabled`
- `PushError<E>` enum covering transport, protocol, apply errors
- Unit tests: message round-trip serialization, frame size limits

## Seams

- `push-sync/src/protocol.rs` — messages
- `push-sync/src/transport.rs` — trait
- `push-sync/src/lib.rs` — public API

## Notes

- No inventory, no diff, no selection phase
- `manifest_digest` in Hello enables fast "nothing changed" detection
- `Ack` per key enables flow control and exactly-once semantics
- Snapshot fallback triggered by receiver on missing deps

## Comments

## Answer
