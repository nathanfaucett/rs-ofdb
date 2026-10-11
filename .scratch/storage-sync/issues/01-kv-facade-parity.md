# 01 — Verify KV facade parity for production consumers

Status: ready-for-agent
Type: task
Work status: resolved
Owner: ofdb
Blocked by: None
Priority: High
Complexity / risk: Low / medium

## Scope and deliverable

Prove existing `ofdb-kv` interfaces replace direct `KvStore<RedbByteBTree>` construction. Use `crates/kv/src/{database,client,transaction}.rs` and facade integration tests. The consumer uses table `kv`, not default `ofdb-kv`. Reuse APIs; add one only for a demonstrated gap. No caller timestamps/backend types.

## Acceptance

- Open a supported existing `kv` table with `Database::open_with_table`, read/write/reopen through `Database::client`, without format conversion or legacy wrappers.
- Invalid/unsupported existing table handling stays explicit; do not silently create a replacement table.
- Explicit `Client::transaction` commit/rollback preserves typed values, scans/ranges/prefix ordering, deletion, generations/tombstones and absolute expiry behavior.
- Facade-only consumer sync fixture needs no direct store/sync dependency. Internal backend fixture setup is allowed; the tested consumer path uses the facade.
- Focused tests with relevant redb/in-memory/sync combinations pass; record commands/results. Authorization support is independently ticket 04.

## Exclusions and integration

No of edits, query redesign, quotas/timing policy or format migration. [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) consumes parity evidence for `of/crates/storage-service/src/database_runtime.rs`; external migration does not block local verification.

## Evidence

Commit: b144761 (storage-sync-owners). Existing APIs reused; no production changes required.

- `cargo test -p ofdb-kv --features redb,in-memory,sync --test embedded_redb_client --test embedded_client --test facade_sync`: 7 passed. Initial cold build hit 120s bound; rerun exposed a test spelling error (Boolean versus Bool), corrected; final run passed.
- `cargo test -p ofdb-kv --features redb --test embedded_redb_client`: 3 passed.
- `cargo test -p ofdb-kv --features in-memory --test embedded_client`: 3 passed.
- Existing named `kv` table read/write/reopen; missing/default and incompatible typed tables explicitly rejected; typed transactions, rollback, ordered range/prefix, deletion and expiry proven by embedded_redb_client. Facade-only sync fixture imports only ofdb_kv and transport channel support; offline delete/update tombstone and recreated-generation convergence plus expiry proven by facade_sync.
- This is a parity proof against existing behavior, not a new implementation red/green claim. Consumer migration remains external ticket of 02.

## Comments

- Claimed by storage-sync-owners on 2026-10-07 at base 3c4bf50. Public seams agreed by requested scope: ofdb-kv Database/Client/Transaction and facade sync. Complexity low, persisted-table compatibility risk medium; reuse existing APIs.

- Construction, transactions and sync already exist; this ticket is not a mandate to add them again.

- Historical planning audit (superseded by resolved acceptance above): canonical triage is ready-for-agent; work was open, with blockers recorded separately and no implementation yet claimed.
