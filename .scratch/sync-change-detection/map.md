# Sync change detection work map

Status: Implementation complete; validation blocked

[Specification](spec.md)

| Order | Issue                                                                               | Depends on | State / deliverable                                             |
| ----- | ----------------------------------------------------------------------------------- | ---------- | --------------------------------------------------------------- |
| 01    | [Characterize correctness and discovery cost](issues/01-characterize-invariants.md) | None       | Done: evidence and mutation-path inventory                      |
| 02    | [Separate document capabilities from protocol](issues/02-document-capabilities.md)  | 01         | Done: engine-owned RowReconciler; protocol stays sync-owned     |
| 03    | [Publish commit-scoped logical invalidation](issues/03-committed-identities.md)     | 01         | Implemented: independent bounded consumers and broad fallback   |
| 04    | [Integrate selective SQL and incremental KV](issues/04-selective-reconciliation.md) | 02, 03     | Done: SQL7/KV3, selected union and validated apply              |
| 05    | [Validate, simplify and document](issues/05-validation-and-cleanup.md)              | 04         | Documentation complete; validation blocked (evidence in ticket) |

02/03 landed with disjoint ownership; 04 retained separate SQL/KV semantics and protocols without crate consolidation or compatibility glue. [Current design](../../docs/design.md) replaces planning descriptions. [SQL](sql-integration.md) and [KV](kv-integration.md) handoffs retain implementation/TDD evidence. Ticket 05 records actual final command results: implementation-complete does not resolve blocked validation gates.
