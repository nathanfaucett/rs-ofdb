# Issue 03: Core — ChangeSubscription::events() stream

## Description

Add `ChangeSubscription::events()` returning a `Stream<Item = ChangeEvent>` that yields committed changes in order. Replaces `select()` + `acknowledge()` + `changed()` pattern.

## Type

task

## Priority

Critical

## Status

ready-for-agent
## Blocked by

- **01-core-changeevent-storagekey**: ChangeEvent type must exist
- **02-core-changesignal-committed-events**: ChangeSignal must broadcast events

## Acceptance Criteria

- `ChangeSubscription::events(&mut self) -> impl Stream<Item = ChangeEvent> + '_` added
- Stream yields each `ChangeEvent` exactly once per subscription (no dup, no loss)
- Events delivered in generation order per key
- `select()`, `acknowledge()`, `changed()` marked `#[deprecated]` with migration note
- `consume()` and `closed()` kept for internal use
- Unit test: subscribe → commit via ChangeSignal → stream yields ChangeEvent with blob

## Seams

- `btree/src/changes.rs` — `ChangeSubscription` impl
- `sync-runtime` crate will wrap this for `run()` loop

## Notes

- Implementation: track per-source cursor (generation), poll watch channel, yield new events
- Must handle merged subscriptions (multiple sources)
- No acknowledgment needed — stream is fire-and-forget per subscription
- If lagging behind > HISTORY_COMMITS, yield `ChangeEvent::All` sentinel or error

## Comments

## Answer
