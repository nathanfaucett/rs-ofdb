# 02 — Separate document capabilities from protocol

Status: Planned

Depends on: 01

## Purpose

Remove the Row Reconciler's need to depend on SQL wire/session types without making the sync crate a mere transport wrapper.

## Work

- Review `crates/sql-sync/src/codec.rs`, `crates/sql-engine-automerge/src/codec.rs` and engine Row Reconciler interfaces.
- Move the smallest history/state capability interface and required semantic types to the engine/Row Reconciler side. Keep `SyncMessage`, transport frames and session orchestration in `sql-sync`.
- Obtain document heads/history summary plus metadata from a consistent transaction snapshot, without save/load solely for digest discovery.
- Define actual Automerge change IDs independently from physical `DocumentChangeKey` hash labels. Cover multi-change payloads and dependency order.
- Evaluate exporting missing changes from document history contained in snapshots, retaining full-state fallback when necessary. Do not mandate physical incremental retention.
- Rewrite call sites and delete superseded interfaces; do not add compatibility adapters or a new sync-core crate.
- Preserve no_std/alloc behavior for engine interfaces and feature isolation for remote-only facades. Keep implementation logic out of lib.rs/mod.rs.

## Acceptance

The Automerge Row Reconciler no longer needs protocol-owned state-unit types for its semantic capabilities. Dependency direction is explicit and tested. Existing SQL validation and session behavior remain intact; tests establish equivalent digests/history interpretation and snapshot fallback.

## Validation

Run focused codec/session tests, affected no-default-features checks and relevant facade feature checks. Record any intentional interface or protocol break; do not assume a wire change is necessary.

## Comments
