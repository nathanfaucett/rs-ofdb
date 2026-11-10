# Issue 10: Transport — Iroh PushSyncTransport impl

## Description

Implement `PushSyncTransport` for Iroh transport (used by native mesh sync). Wraps existing iroh byte stream with push-sync protocol framing.

## Type

task

## Priority

High

## Status

ready-for-agent
## Blocked by

- **04-crate-pushsync-protocol-transport**: PushSyncTransport trait defined

## Acceptance Criteria

- `IrohPushTransport` implements `PushSyncTransport`
- Uses existing `iroh::Endpoint` connection + `Accept` loop
- Frames `PushMessage` with length prefix (postcard serialization)
- Handles bidirectional streaming: concurrent send/receive
- Backpressure: respects `max_pending_acks` via window
- Reconnection: on disconnect, re-establishes and re-sends unacked events
- Unit test: two in-memory iroh endpoints, send ChangeEvent → verify receipt

## Seams

- `push-sync/src/iroh_transport.rs` (new) or `sql-sync/src/iroh_transport.rs` (adapt)
- `ofnet` / `iroh-chain` integration for peer discovery

## Notes

- Existing `IrohTransport` in `sql-sync` implements old `SyncTransport` (request/response)
- New impl uses bidirectional stream, not request/response
- Can reuse iroh connection management from `ofnet`
- Frame format: `len: u32 (BE) + postcard_bytes`

## Comments

## Answer
