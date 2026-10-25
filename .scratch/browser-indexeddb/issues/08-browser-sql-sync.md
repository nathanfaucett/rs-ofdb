# 08: Synchronize browser SQL replicas using the existing protocol

**What to build:** An IndexedDB-backed browser SQL replica reconciles schema, Logical Rows, conflicts and deletion metadata through caller-supplied transport using existing bounded memory staging and checked apply semantics.

**Blocked by:** 04 — Preserve full SQL transaction and reconciliation semantics.

**Status:** ready-for-agent

- [ ] Support checked broad, selected and scoped Sync Sessions through browser-compatible public facade interfaces without changing the current SQL protocol version or logical payload formats.
- [ ] Reuse existing Row Reconciler capabilities, schema reconciliation, history validation, checked apply and bounded memory staging. Native stage-directory interfaces remain native-only.
- [ ] Preserve atomic complete catalog apply, Logical Row and Tombstone state, derived Index Record maintenance and per-batch caller authorization checks.
- [ ] Integrate Committed Change Subscriptions for shared clients, conflict Resolution and incoming replication, publishing after successful persistence even if the caller stopped polling.
- [ ] Preserve independent selected-work acknowledgement, later-commit retention, initial broad selection and overflow/gap fallback. Failed, rolled-back and net byte no-op transactions remain silent.
- [ ] Real-browser tests cover bootstrap, concurrent offline edits, conflict inspection/Resolution after sync, cancellation, denied apply, recovery, catalog lifecycle and replay silence.
- [ ] Malformed or rejected incoming batches cannot partially persist catalog, row or index state; prior completed batches survive interrupted sessions and retry converges.
- [ ] Verify browser/native logical interoperability using current native payloads or generated fixtures, including deletion metadata and native history, without adding bundled network transports.
- [ ] Document external-write and remote-only wakeup limits and avoid claiming selective physical I/O from the initial full-snapshot backend.
