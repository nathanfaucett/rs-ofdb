# 04 — Integrate selective SQL and incremental KV reconciliation

Status: Done

Depends on: 02, 03

## Purpose

Use changed identities to avoid whole-store work while retaining existing reconciliation correctness.

## Work

- Complete one SQL end-to-end slice: successful commit, changed-identity selection, peer comparison, missing-history export and validated atomic apply.
- Expand catalog invalidation to complete required dependency sets and table-wide invalidation where needed. Preserve tombstones, drop/recreation, uniqueness side effects and derived Index Records.
- Include metadata in selection and transfer when it changes alone or alongside document history. Do not fall into the current shape's possible trap of choosing deltas and omitting changed metadata; characterize actual behavior before replacing it.
- Initialize new/reconnected peers with broad reconciliation. Use broader fallback for unknown/gapped work and snapshot recovery for unavailable dependencies/history.
- Make remote-origin delivery explicit: a local subscription cannot observe remote-only commits. Choose a live receive/remote-wakeup mechanism or a documented caller-driven reconciliation contract. Do not silently promise realtime delivery on an idle peer without one.
- Retain pre-apply authorization, scoped history/candidate validation, staging cleanup and existing complete-row/catalog atomicity. Do not use local mutation payloads to bypass network validation.
- Add incremental KV exchange using actual document change identities; preserve generation comparison, stale-generation rejection, tombstone-wins behavior and concurrent snapshot merge. Keep KV and SQL protocols separate.
- Ensure incoming replication fans out to other peers and repeated known state settles without endless no-op commits/echo. Version any wire change explicitly and remove superseded paths instead of supporting old versions.

## Acceptance

Three-peer tests demonstrate bidirectional delivery under the chosen transport contract and fan-out. Offline concurrent changes converge after reconnect; snapshot recovery retains local concurrent work. Tests cover combined metadata/deltas, schema changes and KV generation replacement. An established small data update avoids whole-database document reconstruction, except explicitly documented conservative fallback.

## Validation

Run focused SQL/KV session, facade and lifecycle integration tests. Use deterministic work counters, not latency thresholds, to prove selective discovery.

## Comments

Implemented on integration branch `sync-change-detection` via SQL commit `3a81acc` and KV commit `d1e76c0`, merged at `a93fd12`. Detailed interfaces, TDD evidence, focused validation and limits are in [SQL integration](../sql-integration.md) and [KV integration](../kv-integration.md).

Both stores negotiate the union of selected work, preserve validated atomic apply and metadata, and use broad fallback for new/reconnected peers, gaps and catalog invalidation. SQL protocol is 7; KV protocol is 3. Caller-driven remote wakeups/reconciliation are explicit: local notifications alone do not observe remote-only commits. The formerly ignored SQL combined document/deletion regression now passes normally. SQL's deterministic established one-row fixture records two summaries with no snapshot exports, row-ID scans, separate inventories or digest reloads. KV's 101-key fixture exchanges only selected-key inventory and no snapshots.

Focused suites and facade multi-peer/denial/retry/concurrent-commit tests passed as recorded in the handoffs. Large SQL payload/cancellation tests exceeded bounded focused runs and remain issue05 validation work, not claimed passes. No workspace feature-powerset or CRAP success is claimed here.
