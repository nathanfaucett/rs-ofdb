# 07: Synchronize browser KV replicas using the existing protocol

**What to build:** An IndexedDB-backed browser KV replica exchanges state and native history through caller-supplied transport and exposes committed work for selective reconciliation with existing native peers.

**Blocked by:** 02 — Persist KV queries and explicit transactions in IndexedDB.

**Status:** ready-for-agent

- [ ] Support broad and selected checked Sync Sessions through the public facade with browser-local futures and caller-owned transport and authorization.
- [ ] Preserve the current KV protocol version and payload formats, native history validation, snapshot recovery, latest-generation selection and Tombstone semantics.
- [ ] Integrate Committed Change Subscriptions for shared clients and incoming replication, publishing only after successful IndexedDB transaction completion, including when a commit caller stops polling.
- [ ] Preserve independent selected-work acknowledgement, broad initial work, overflow/gap fallback and retention of commits arriving during reconciliation.
- [ ] Read-only commits, rollback, failed commit, identical bytes and known replay remain silent. History changes may notify even when visible values do not change; elapsed expiry does not notify.
- [ ] Browser facade tests cover nested-value merges, offline concurrent edits, generation replacement, stale imports, Tombstones, expiry history and replay silence.
- [ ] Rejected, malformed or canceled incoming batches persist no partial writes; earlier committed batches remain valid progress and a fresh session can converge.
- [ ] Verify browser/native logical interoperability using payload exchange or fixtures produced by the current native implementation, without adding bundled network transports.
- [ ] Document that local subscriptions do not automatically observe another tab or remote-only writes, and that external-write gaps require broad recovery. Do not claim bounded physical reads for a full-snapshot backend.
