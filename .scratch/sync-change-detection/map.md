# Sync change detection work map

Status: Proposed

[Specification](spec.md)

| Order | Issue | Depends on | Deliverable |
| --- | --- | --- | --- |
| 01 | [Characterize correctness and discovery cost](issues/01-characterize-invariants.md) | None | Executable evidence and mutation-path inventory |
| 02 | [Separate document capabilities from protocol](issues/02-document-capabilities.md) | 01 | Engine-owned reconciliation interface, protocol stays sync-owned |
| 03 | [Publish commit-scoped logical invalidation](issues/03-committed-identities.md) | 01 | Tested independent-consumer change detection with fallback |
| 04 | [Integrate selective SQL and incremental KV](issues/04-selective-reconciliation.md) | 02, 03 | End-to-end live and offline reconciliation |
| 05 | [Validate, simplify and document](issues/05-validation-and-cleanup.md) | 04 | Failure/concurrency coverage, measured improvement, final checks |

02 and 03 are conceptually independent after 01; parallelize edits only after establishing disjoint file ownership. Within 04, complete a SQL slice before choosing how much KV document machinery to share.

No separate prototype is scheduled. Issue 01 is the evidence gate. Use a bounded experiment only for a concrete unresolved question, such as whether Automerge-native per-document sync materially simplifies current inventory exchange. No speculative crate consolidation or new shared protocol is required.
