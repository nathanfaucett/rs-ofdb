# Spec: Unified Push-Based Sync for KV and SQL

## Context

Currently, both `kv-sync` and `sql-sync` use **pull-based synchronization** with inventory comparison:
1. Peers exchange inventories (lists of known change hashes)
2. Sender computes diff locally
3. Sender fetches missing changes from storage
4. Sender transmits changes; receiver applies

This requires **round-trips** and **explicit `synchronize()` calls** even though the storage layer already emits `ChangeSignal` events on commit.

The storage layer (`btree/src/changes.rs`) has:
- `ChangeSignal::committed_with(invalidations)` — broadcasts on commit via watch channel
- `ChangeSubscription::select()` — returns `ChangeWork` with invalidations
- `ChangeSubscription::changed()` — async wakeup on new commits

But the sync protocol ignores this push capability and re-implements pull-based diff.

**Goal:** Unify KV and SQL sync under a single **push-based** model where:
- Storage emits `ChangeEvent { key, blob }` on commit (includes the incremental Automerge blob)
- Consumers receive changes as a stream — no inventory, no diff, no round-trip
- Single protocol, single transport trait, single subscription type

---

## Current State

### KV Sync (`kv-sync`)
- Protocol: `Message::Inventory` → `Message::Changes` / `Message::Snapshots`
- Transport: `SyncTransport { send, receive }`
- Subscription: `ChangeSubscription` (from `sync-runtime`, wraps btree's)

### SQL Sync (`sql-sync`)
- Protocol: `SyncMessage::Inventory` → `SyncMessage::Changes` / `SyncMessage::State`
- Transport: `SyncTransport { send, receive }`
- Subscription: `ChangeSubscription` (same, from `sync-runtime`)

### Common: `btree/src/changes.rs`
- `ChangeSignal` — backend-owned, `committed_with(StorageInvalidation)`
- `ChangeSubscription` — consumer, `select()` → `ChangeWork { invalidations }`
- `StorageInvalidation::Key { table, key }` — no blob, just identity

---

## Proposed Design

### 1. Core Event Type (in `btree/src/changes.rs`)

```rust
/// A committed change with its incremental Automerge blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeEvent {
    /// Logical key: table (SQL) or empty (KV), plus row/document key
    pub key: StorageKey,
    /// Raw Automerge change bytes (change.raw_bytes())
    pub blob: Vec<u8>,
    /// Optional: generation/version for ordering
    pub generation: u64,
}

/// Unified key for both KV (table="") and SQL (table="users")
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageKey {
    pub table: String,   // "" for KV, table name for SQL
    pub key: Vec<u8>,    // Document ID or row UUID bytes
}
```

### 2. Updated `ChangeSignal` (in `btree/src/changes.rs`)

```rust
impl ChangeSignal {
    /// Called by storage backend on successful commit.
    /// Includes the incremental blob so consumers don't need to re-fetch.
    pub fn committed_with_events(&self, events: impl IntoIterator<Item = ChangeEvent>) {
        // Broadcast via watch channel
    }
    
    // DEPRECATE: committed_with(invalidations) — kept for transition
    pub fn committed_with(&self, invalidations: impl IntoIterator<Item = StorageInvalidation>) { ... }
}
```

### 3. Push-Based `ChangeSubscription` (in `btree/src/changes.rs`)

```rust
impl ChangeSubscription {
    /// Returns a stream of ChangeEvent. No select/acknowledge needed.
    /// Each event is delivered exactly once per subscription.
    pub fn events(&mut self) -> impl Stream<Item = ChangeEvent> + '_ { ... }
    
    // DEPRECATE: select(), acknowledge(), changed()
}
```

### 4. Unified Push Sync Protocol (new crate: `push-sync`)

```
┌─────────────────────────────────────────────────────────────────┐
│  PushSyncProtocol                                               │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Message::Hello { protocol_version, manifest_digest }     │  │
│  │ Message::ChangeEvent { key, blob, generation }           │  │
│  │ Message::SnapshotRequest { key }                         │  │
│  │ Message::Snapshot { key, blob }                          │  │
│  │ Message::Ack { generation }                              │  │
│  │ Message::Done                                             │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

- **No inventory, no diff** — sender pushes `ChangeEvent` directly
- **Snapshot fallback** — if receiver missing deps, requests snapshot
- **Ordered delivery** — `generation` ensures ordering per key
- **Ack-based** — receiver acknowledges highest generation per key

### 5. Unified Transport Trait (in `push-sync`)

```rust
pub trait PushSyncTransport {
    type Error;
    
    /// Send a push sync message
    async fn send(&mut self, msg: PushMessage) -> Result<(), Self::Error>;
    
    /// Receive next push sync message
    async fn receive(&mut self) -> Result<PushMessage, Self::Error>;
}
```

### 6. Unified Sync Function (in `push-sync`)

```rust
/// Run push-based synchronization.
/// 
/// - `outbound`: stream of ChangeEvent from local storage (ChangeSubscription::events())
/// - `inbound`: applies received ChangeEvent to local storage
/// - `transport`: bidirectional push transport
/// - Returns when both sides send Done
pub async fn push_synchronize<B, T, F>(
    store: &Store<B>,
    transport: &mut T,
    config: PushConfig,
    mut apply_inbound: F,  // applies ChangeEvent to store
) -> Result<PushResult, PushError<T::Error>>
where
    B: Backend,
    T: PushSyncTransport,
    F: FnMut(ChangeEvent) -> Result<(), ApplyError>,
```

### 7. KV Integration (`kv-sync`)

- Replace `kv-sync` protocol with `push-sync`
- `KvStore` emits `ChangeEvent` on `commit()` via `ChangeSignal`
- `push_synchronize` replaces `synchronize()`
- `ChangeSubscription::events()` replaces `select()` + `synchronize()`

### 8. SQL Integration (`sql-sync`)

- Replace `sql-sync` protocol with `push-sync`
- `SqlEngine` emits `ChangeEvent` per row on commit
- `SyncRowCodec::export_change` becomes unnecessary — blob already in event
- `push_synchronize` replaces `synchronize_with_stage_factory`

---

## Seams (Testing Points)

| Seam | Current | Proposed | Test Strategy |
|------|---------|----------|---------------|
| **Storage commit** | `ChangeSignal::committed_with(invalidations)` | `ChangeSignal::committed_with_events(events)` | Unit test: commit → verify event contains correct blob |
| **Subscription** | `ChangeSubscription::select()` → `ChangeWork` | `ChangeSubscription::events()` → `Stream<ChangeEvent>` | Unit test: subscribe → commit → verify stream yields event |
| **Sync protocol** | `kv-sync` / `sql-sync` (pull) | `push-sync` (push) | Integration test: two nodes, write → verify push delivery |
| **Transport** | `SyncTransport { send, receive }` (request/response) | `PushSyncTransport { send, receive }` (bidirectional stream) | Mock transport: verify message sequence |
| **KV apply** | `import_changes` / `import_snapshot` | `apply_change_event` (direct apply) | Unit test: apply ChangeEvent → verify document state |
| **SQL apply** | `merge_snapshot` / `merge_change` | `apply_change_event` per row | Unit test: apply ChangeEvent → verify row state |

**Highest seam**: `push_synchronize()` — single entry point for both KV and SQL.

---

## Migration Plan (Breaking Changes — No Backwards Compat)

### Phase 1: Core Events (btree)
1. Add `ChangeEvent`, `StorageKey` to `btree/src/changes.rs`
2. Add `ChangeSignal::committed_with_events()`
3. Add `ChangeSubscription::events()` stream
4. Deprecate `select()`, `acknowledge()`, `changed()`

### Phase 2: Push Sync Crate
1. Create `ofdb/crates/push-sync` with:
   - Protocol messages
   - `PushSyncTransport` trait
   - `push_synchronize()` function
   - Inbound/outbound handlers

### Phase 3: KV Migration
1. Update `KvStore::commit()` to emit `ChangeEvent` via `ChangeSignal`
2. Replace `kv-sync` with `push-sync` in `kv-sync` crate (or new `kv-push-sync`)
3. Update `kv` crate re-exports

### Phase 4: SQL Migration
1. Update `SqlEngine` commit to emit `ChangeEvent` per row
2. Replace `sql-sync` with `push-sync` in `sql-sync` crate (or new `sql-push-sync`)
3. Update `sql` crate re-exports

### Phase 5: Cleanup
1. Remove old `kv-sync`, `sql-sync` crates
2. Remove `sync-runtime` (merged into `push-sync`)
3. Update all callers

---

## Acceptance Criteria

1. **Single protocol** — `push-sync` crate used by both KV and SQL
2. **No inventory round-trip** — write on node A → change appears on node B in one RTT
3. **Unified subscription** — `ChangeSubscription::events()` works for both KV and SQL
4. **Snapshot fallback** — if receiver missing deps, automatically requests snapshot
5. **Ordered per key** — changes for same key delivered in generation order
6. **Backpressure** — transport respects flow control (configurable window)
7. **Tests pass** — existing sync tests adapted to push model pass

---

## Non-Goals

- Conflict resolution beyond Automerge CRDT merge (handled at apply)
- Partial/selective sync (all keys sync; filtering at application layer)
- Persistent transport sessions (handled by transport impl, e.g., iroh)
- Auth/authorization (handled at transport layer)

---

## Dependencies

- `btree` crate: `ChangeEvent`, `ChangeSignal`, `ChangeSubscription` changes
- New `push-sync` crate
- `kv` crate: integrate push sync
- `sql` crate: integrate push sync
- Transports: `iroh` transport impl for `PushSyncTransport`

---

## Proposed Tickets

| # | Title | Type | Priority | Blocked By |
|---|-------|------|----------|------------|
| 01 | Core: ChangeEvent and StorageKey in btree/changes.rs | task | Critical | — |
| 02 | Core: ChangeSignal::committed_with_events | task | Critical | 01 |
| 03 | Core: ChangeSubscription::events() stream | task | Critical | 01 |
| 04 | Crate: push-sync protocol and transport trait | task | Critical | 01 |
| 05 | Crate: push_synchronize() implementation | task | Critical | 04 |
| 06 | KV: Emit ChangeEvent on KvStore commit | task | Critical | 02 |
| 07 | KV: Replace kv-sync with push-sync | task | Critical | 05, 06 |
| 08 | SQL: Emit ChangeEvent per row on SqlEngine commit | task | Critical | 02 |
| 09 | SQL: Replace sql-sync with push-sync | task | Critical | 05, 08 |
| 10 | Transport: Iroh PushSyncTransport impl | task | High | 04 |
| 11 | Cleanup: Remove old kv-sync, sql-sync, sync-runtime | task | Medium | 07, 09 |

---

Status: ready-for-agent
