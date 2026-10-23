# 03 — Publish commit-scoped logical invalidation

Status: Planned

Depends on: 01

## Purpose

Let reconciliation select changed logical state without treating storage records as a durable feed.

## Work

- Extend existing transaction bookkeeping where sound. Select a single tested publication strategy that cannot lose invalidation after a successful commit, including cancellation around commit completion.
- Cover SQL Logical Rows, catalog/dependency invalidation, metadata-only changes, indirect tombstones/cleanup and KV Key/Generation changes. Ignore derived index bytes as independent replicated identities.
- Publish after successful commit; discard on rollback/failure. Use conservative table/catalog or broader invalidation where exact row identity collection is not reliable.
- Retain generic storage wakeups where useful. Keep SQL/KV semantics out of generic B-tree interfaces.
- Define subscription initialization, selected-work watermark, success acknowledgement and retry behavior. Never consume writes that arrive during reconciliation accidentally.
- Give independent peers independent progress. Bound memory and represent overflow or unknown changes as an explicit broader-reconciliation requirement; no durable cursor promise.
- Cover incoming replication for fan-out, including metadata and generation replacement. Preserve existing caller-controlled runner lifetime and cancellation.

## Acceptance

Deterministic tests cover rollback, failed commit, multi-write commits, cancellation near commit, commits during reconciliation, two consumers at different progress, new subscriptions, overflow/gaps and incoming replication. A missed identity causes conservative reconciliation, never silent loss. Acknowledgement of one peer does not clear another peer's work.

## Validation

Run targeted backend, engine/KV and subscription tests as applicable. Record which backends support selective notifications and the explicit behavior for unsupported backends.

## Comments
