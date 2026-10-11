# Storage sync ownership and public-facade migration

Status: Owner-local delivery implemented and integrated; consumer acceptance remains open (06).
Scope: ofdb contract and documentation checkpoint; no new implementation or validation.

## Current checkpoint — 2026-10-11

Local `main` contains the reviewed owner delivery through `8b94799cfadeefd85cd297b4199330776d1ae0bd`. Tickets 01–05 and 07–15 are resolved only within their recorded owner contracts; [06](issues/06-consumer-integration-handoff.md) remains open pending consumer integration and authorization evidence. See [the current map and cleanup receipt](map.md). This checkpoint does not infer transport extraction, domain authentication, bootstrap readiness or consumer completion from owner APIs or sibling tracker consolidation.

The starting-point inspection and original requirements below are historical context, not a claim about current consumer source files. Follow-ups add public conflict inspection, causal incremental repair, fixed-schema scopes, full historical-value and transaction-consistent candidate validation, UUIDv5 user identities, directional scoped sessions and managed native retirement. Exact acceptance and limits are recorded in their tickets and the linked owner reports; directional scoped negotiation requires both peers to update, and native retirement retains its Unix/exclusive-namespace/permanent-fence limits.

## Decision and ownership

`ofdb-kv` and `ofdb-sql` are the exclusive production database interfaces for `of` consumers. `ofdb` owns SQL/KV persistence, CRDT reconciliation, transport-neutral sync protocol execution, staging, batching, atomic apply, and cleanup, with no Iroh dependence. `ofnet`/`iroh-chain` owns reusable networking: mesh endpoints, endpoint allowlist enforcement, routing, and pairing. `of` owns domain trust/authorization, authenticated peer/resource binding, and protocol transport adapters built atop ofnet; it selects resources/peers and schedules explicit sessions. Filesystem replication remains an `offs` concern.

Endpoint allowlists govern connection admission, not resource permission; an admitted connection does not replace of's domain authorization or ofdb's caller pre-apply checks. This refines ownership within the existing coordinated extraction, not a new networking requirement, framework, ticket, or dependency gate. Routing does not imply transparent mesh stream relay.

All nodes can write locally while offline and later reconcile through CRDT semantics. This boundary does not promise globally synchronized authorization, instantaneous revocation, or globally consistent security decisions during partitions.

## Verified starting point (historical inspection)

- `of/crates/storage-service/src/database_runtime.rs` constructs/caches `KvStore<RedbByteBTree>`, opens Redb directly, uses table `kv`, and supplies timestamps.
- `of/crates/storage-server/src/database_protocol.rs` calls lower-level `ofdb_kv_sync::synchronize` in both roles and SQL `Database::synchronize_with_stage_factory`.
- `of/crates/storage-server/src/sync_stage.rs` implements concrete file staging and generic replay/rollback/cleanup tests, with IdP IDs for temporary names and caller-owned byte limits. Move the mechanism and generic tests, not the domain dependency or custom policy.
- KV already provides `Database::open_with_table`, `Database::client`, `Client::transaction`, `Database::synchronize`, and snapshot export/import. Reuse these; no replacement query abstraction is needed.
- SQL already provides memory-backed `Database::synchronize` and custom `synchronize_with_stage_factory`. The observed native production path requires a caller factory; SQL sync generally does not. Add optional ofdb-owned native file staging exposed through the facade.
- `crates/sql-sync` is `no_std` with `alloc`, with an optional std Iroh adapter at the verified starting point, not the target ownership. Under the existing coordinated extraction, Iroh protocol transport adapters belong in of atop ofnet, leaving ofdb without Iroh dependence. Embedded facade features already require std. Keep generic core functionality free of native filesystem/runtime requirements.
- SQL validates catalog/recovery completeness and groups complete rows into apply batches. KV commits received snapshot batches atomically. A session is not one transaction.

Source anchors: `crates/kv/src/{database,client,transaction,lib}.rs`, `crates/sql/src/{database,api}.rs`, `crates/sql-sync/src/{session,staging,lib}.rs`, `crates/kv-sync/src/session.rs`, and their manifests.

## Required contract

### Public interfaces

Production database consumers construct/control storage through the facades and use `Client` for ordinary queries and explicit transactions. No direct backend/store construction, caller timestamps, or lower-level sync orchestration remains for this responsibility. Caller-selected native staging location/configuration is permitted; implementing record storage/replay/cleanup in of is not. Memory staging remains available. Existing generic stage interfaces may remain useful extension/test seams; no legacy wrapper is required.

### Authorization before each apply

Expose the smallest suitable generic caller pre-apply hook through public sync. Prefer an existing seam or function/closure if sufficient; no new trait is mandated and no shared SQL/KV policy framework is required. ofdb contains no IdP/Management resource, token, grant, revocation, or freshness model. Caller-bound peer/resource context stays in of; ofdb owns check placement, not the decision.

An apply operation is one atomic mutation scope selected by ofdb: a complete SQL catalog dependency batch, complete-row data batch, recovery/retry apply, or KV snapshot batch. Frames are not transaction or authorization boundaries.

Check authorization before **each** apply operation begins, including staged replay, recovery snapshots, and retries. A session/hello check, frame-receipt check, or single check before replaying a whole stage is insufficient. Do not split rows or catalog dependency sets to increase check frequency.

Denial/check failure prevents that operation from mutating storage, stops later applies, returns an actionable error, and releases staging resources. Earlier valid committed operations remain valid partial progress. An already-authorized operation may finish atomically even if the decision changes while it runs; no mid-transaction interruption or commit-time reauthorization is required.

Ordinary local queries/transactions remain caller-authorized before the relevant facade operation. Direct public state/snapshot apply entry points must document their authorization responsibility: use the checked production sync path, or require the caller to check before each direct atomic apply. Production code must not use an unchecked shortcut to bypass the boundary; this does not require domain policy on every query handle.

### Integrity, cancellation, and cleanup

- Retain protocol/version, payload, identity, digest, schema, and dependency validation. Authorization never substitutes for structural validation.
- SQL commits catalog/schema, CRDT rows, tombstones, and derived indexes within the existing engine transaction scope. Complete row changes/state and catalog dependency batches must not commit piecemeal across frames.
- Missing dependencies use existing recovery; missing advertised recovery state must not report success. Recovery and retried changes pass the same pre-apply check and validation.
- KV preserves same-generation Automerge merging, greatest-generation selection, and tombstones. Malformed snapshots roll back the current whole batch.
- Disconnect, cancellation, replay failure, or denial releases unfinished stages/uncommitted work. Preserve completed atomic operations: an error does not imply session-wide rollback. Incomplete rows/catalog/recovery must not report completion.
- Native stages replay append order, reject invalid/truncated records and invalid lifecycle use, close handles, and remove temporary files on success/error/denial/cancellation/drop. Cleanup failure is observable without masking a primary error. Drop is not a process-crash/power-loss cleanup guarantee.

### Exclusions

No custom byte quotas, timeouts, deadlines, freshness windows, or new budget policy. Preserve necessary existing protocol/framing/configuration validation; do not copy caller hard-coded stage limits into new policy knobs. Existing query deadlines and KV absolute expiry are not redesigned. Tests can cancel deterministically without production timeout policy.

No background replication, global write leader, global security consistency promise, domain policy engine, implicit persisted-data migration, or backwards-compatibility glue. The original task excluded wire redesign; follow-up 14 subsequently introduced the explicitly documented scoped direction handshake revision, not an unscoped wire change or legacy adapter. Sequential SQL/KV phases are not a distributed atomic commit. This checkpoint edits no code and no other workspace.

## Machine-local validation scope

Current acceptance for all tickets 01–15 is limited to what can be verified on this Ubuntu Linux x86_64 host. Each ticket's machine-local note applies this policy to its existing contract; it changes validation scope, not implementation requirements, ownership, dependencies, resolved status or historical evidence.

- Use bounded, focused tests/checks of the relevant public facade, package, named suite or test filter, with only necessary supported features. Normally bound each command to 120–300 seconds and record exact revision, command, features, bound and outcome; a timeout is incomplete, not a pass or permission for an automatic larger sweep. Relevant no-default/no_std compilation and dependency-graph checks run for the host, not foreign-target qualification.
- Multi-node, offline/partition, reconnect, protocol, authentication and restart scenarios may use isolated databases/processes/instances on this same host, separate persistence/stage paths and loopback or transport-neutral channels as appropriate. Physical machines are not required. Retain both roles, divergent writes, hostile payloads, recovery/retry, denial, cancellation, integrity, atomic rollback, valid partial progress, native reopen and targeted cleanup wherever locally feasible.
- Generic fake hooks and injected faults qualify only the exercised owner boundary. They do not qualify production transport, real authenticated peer/resource binding or consumer domain authorization. Real consumer integration evidence remains required from its owner and can be generated by isolated same-host instances through the real adapters. Public API and SQL/KV/of/ofnet/offs ownership remain unchanged.
- Distinguish passed, failed, timed out, unrun and explicitly unsupported cases. An available but unrun test is not unsupported; an unsupported API/platform is not a pass. Missing implementation or a failing feasible correctness/security/integrity test remains a real blocker. Do not replace native persistence or real integration requirements with mocks or weaken the contract to excuse a gap.
- Foreign OS/architectures, distributed physical deployment, exhaustive crash/power-loss qualification and full workspace/all-target/all-feature/powerset/CRAP sweeps are follow-up qualification, not ticket closure gates. Full `cargo hack test --feature-powerset --all-targets` and `just crap` are not authorized. Preserve their exact historical successes, failures, timeouts and coverage limits without extending them to later revisions. Explicitly unsupported capabilities (for example remote conflict operations and unmanaged retirement) remain unsupported, not deferred supported promises.
- This checkpoint is documentation-only: no code, build, test, download, cleanup, commit, branch or cross-repository write. Validate document coverage, policy consistency, relative links and unchanged status/dependency/evidence text with file tools; claim no new runtime qualification. Resolved tickets stay resolved within their recorded contracts; open tickets need their actual evidence before closure.

## Test ownership and acceptance

Generic tests in ofdb are transport-neutral and use fake allow/deny/error hooks, changing decisions between operations, both roles, replay/recovery/retry, rollback, cancellation cleanup, valid partial progress, and offline divergent writes/convergence; they do not depend on Iroh. Real Iroh authentication and domain authorization integration tests remain in of, using its protocol transport adapters built atop ofnet and covering authenticated peer/resource/Management/IdP binding. Connection-admission allowlists do not establish resource authorization.

Ticket acceptance is in [map.md](map.md). Prove facade-only use, including supported existing `kv` table opening and transactions without caller timestamps. Native staging must be optional and usable without a caller factory implementation.

The original qualification plan required focused checks followed by `cargo hack test --feature-powerset --all-targets`, `just crap`, formatting and Clippy. Ticket 05 records their historical completion and coverage limitations; later follow-up timeouts are not passes or qualification of the current revision. Current acceptance follows the [machine-local validation scope](#machine-local-validation-scope), with relevant package/suite feature checks and targeted formatting/Clippy instead of blanket final commands. No execution or new qualification is authorized for this documentation checkpoint.

## Integration references and blockers

- [of map](../../../of/.scratch/offline-first-nodes/map.md): [of 02 — public persistence and sync integration](../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) consumes local tickets 01–05 plus [offs 01 — operation admission](../../../offs/.scratch/filesystem-sync/issues/01-remote-operation-admission.md) and [offs 02 — staged-content integrity](../../../offs/.scratch/filesystem-sync/issues/02-staged-content-write-integrity.md). These owner prerequisites supply public APIs and generic acceptance, not real domain authorization implementation in ofdb.
- [offs map](../../../offs/.scratch/filesystem-sync/map.md): [offs 03 — Iroh extraction/offline reconnect](../../../offs/.scratch/filesystem-sync/issues/03-iroh-extraction-offline-reconnect.md) and of 02 co-deliver extraction and consumer updates after the owner prerequisites, with reusable mesh endpoint/allowlist/routing/pairing networking in ofnet/iroh-chain and domain trust/authorization plus protocol transport adapters in of atop ofnet. This is one coordinated delivery, not reciprocal completion dependencies.
- [of 03 — execution-boundary authorization](../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) follows of 01/02 and consumes the owner hooks for real local authorization integration.
- Local tickets 01–05 have no external completion gate. Local 06 follows local 05 and evidence from of 02 **and** of 03; it records integration acceptance and does **not** gate of 02 or offs 03.
- The sibling maps still contain coarse owner-map dependencies and inspection-era placeholders. The concrete dependency slices above follow the confirmed coordination order; no sibling file is edited here and no implementation completion is inferred from tracker existence.

## Complexity, risk, priority

High priority: public boundary, pre-apply authorization, cleanup preservation. KV facade reuse is low complexity; native stage lifecycle is medium complexity/risk; SQL complete batches and recovery checks are highest risk. Prefer existing seams and deterministic tests over new layers.
