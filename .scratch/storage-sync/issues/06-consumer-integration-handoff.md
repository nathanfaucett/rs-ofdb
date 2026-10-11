# 06 — Record facade-only consumer integration handoff

Status: ready-for-agent
Type: task
Work status: open
Owner: ofdb documentation/evidence; of owns consumer implementation
Blocked by: External of 02 / of 03 integration and authorization evidence; local 05 is resolved
External dependencies: [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) and [of 03 — execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) integration evidence
Priority: High
Complexity / risk: Low / medium

## Scope

Record stable public contract and externally supplied integration evidence. This is an ofdb documentation ticket, not permission to edit of/offs/ofnet.

## External coordination

- [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md): replace direct Redb/B-tree/KvStore construction in `of/crates/storage-service/src/database_runtime.rs` with facade handles/named-table selection and Client queries/transactions.
- Migrate `of/crates/storage-server/src/database_protocol.rs` to public facade sync/native staging and real caller hooks bound to authenticated peer/resource.
- Delete obsolete `of/crates/storage-server/src/sync_stage.rs` after generic coverage moves to ofdb. Keep generic persistence/sync tests transport-neutral in ofdb and real Iroh authentication/domain authorization integration tests in of atop ofnet; remove direct backend/store/sync dependencies used only by migrated consumers, not unrelated dependencies blindly.
- Preserve offline writes/reconciliation without new global security/freshness/timing/budget policy.
- of 02 requires local 01–05 plus [offs 01](../../../../offs/.scratch/filesystem-sync/issues/01-remote-operation-admission.md) and [offs 02](../../../../offs/.scratch/filesystem-sync/issues/02-staged-content-write-integrity.md). of 02 and [offs 03 — Iroh extraction/offline reconnect](../../../../offs/.scratch/filesystem-sync/issues/03-iroh-extraction-offline-reconnect.md) co-deliver extraction with reusable mesh endpoint/allowlist/routing/pairing networking in ofnet/iroh-chain and domain trust/authorization plus protocol transport adapters in of atop ofnet, leaving ofdb with no Iroh dependence. This refines existing ownership, not a new networking requirement/framework/task or reciprocal completion gate; routing does not promise transparent mesh stream relay. This ticket follows local 05 and of 02/03 evidence; it does not gate of 02 or offs 03.

## Acceptance

- 05 has local evidence; public usage/check responsibility is documented as implemented, not only planned.
- Record delivery/test evidence on [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) and [of 03 — execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md); both are required, not just tracker existence.
- Production database consumers use only ofdb-kv/ofdb-sql interfaces: no direct backend construction/lower-level sync/concrete staging for this responsibility.
- Real Iroh authentication and domain authorization integration in of, through protocol transport adapters built atop ofnet, proves authenticated peer/resource binding, denial before next apply, permitted completion of already-authorized work, valid partial progress and cleanup across separate SQL/KV phases. Endpoint allowlists enforce connection admission, not resource permission or a substitute for per-apply authorization.
- Generic persistence/sync tests reside in ofdb, remain transport-neutral, and need no Iroh dependence; real authentication/domain integration resides in of. Record unresolved external blockers; local tests alone do not establish migration completion.

## Comments

- Both sibling maps and concrete handoff tickets have been read. Implementation evidence remains pending. Local tickets 01–05 have no external completion gate; 06 is downstream of of 02/03 evidence.

- Tracker audit: canonical triage is ready-for-agent; work remains open. Blockers are recorded separately and no implementation is claimed.

- 2026-10-11 documentation checkpoint: owner tickets 01–05 and 07–15 are integrated into local main and their tracker records are persisted selectively. Consumer evidence has not been reassessed here; sibling tracker consolidation does not satisfy this ticket's acceptance. Work status remains **open**, with no consumer closure or new test qualification.
