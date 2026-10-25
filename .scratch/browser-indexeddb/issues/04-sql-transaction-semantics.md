# 04: Preserve full SQL transaction and reconciliation semantics

**What to build:** Browser SQL callers use explicit multi-call transactions, relational constraints, conflict inspection and Resolution with the same persistent domain guarantees as existing embedded backends.

**Blocked by:** 03 — Run persistent SQL statements through the browser Query Handle.

**Status:** ready-for-agent

- [ ] Public explicit transactions preserve stable snapshots and read-your-writes across unrelated browser awaits; stale writers fail atomically across independent handles.
- [ ] SQL execution errors preserve existing aborted-transaction rules. Commit of an aborted transaction cannot persist earlier writes; rollback and dropped staging discard pending state.
- [ ] Verify defaults, natural and composite primary keys, unique constraints and derived Index Record maintenance through browser facade tests.
- [ ] Verify table/index drop and recreation preserve namespace and identity rules and cannot restore stale rows.
- [ ] Use persisted concurrent-history fixtures to verify conflict inspection, losing values and explicit Resolution through public Query Handles and transaction interfaces.
- [ ] Verify schema, row history, Tombstones and derived indexes remain consistent after reopen and actual browser-context restart.
- [ ] Exercise multi-table failure and concurrent commit scenarios to prove catalog and index state cannot be partially persisted.
- [ ] Reuse existing SQL acceptance scenarios where possible without porting filesystem or multi-thread test-harness assumptions into browser tests.
