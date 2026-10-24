# Commit-scoped change detection and incremental reconciliation

Status: Implementation complete; validation blocked (see ticket 05)

## Decision

Implemented through tickets 01–04; ticket 05 reconciles current documentation and records final validation evidence and blockers. No separate prototype or new shared sync-core crate was needed.

Keep SQL and KV Sync Sessions separate. Realtime and offline reconciliation use the same validated apply semantics: transfer missing Automerge history when possible, and snapshots plus metadata when necessary. Improve selection of work before changing crate topology.

Current behavior is documented in [design](../../docs/design.md), [goal](../../docs/goal.md), and the root [glossary](../../GLOSSARY.md). Engine-owned `RowReconciler` capabilities replace protocol-owned semantics; SQL wire 7 and KV wire 3 are intentionally incompatible with earlier versions. See [SQL integration](sql-integration.md), [KV integration](kv-integration.md) and [ticket 05 evidence](issues/05-validation-and-cleanup.md). Implementation completion is not a claim that the full validation gates passed.

## Original problem and source evidence

This is the pre-implementation baseline retained for comparison, not a description of current ownership/discovery. Tickets 02–04 replaced the production dependency, summary/discovery and invalidation paths below.

- `crates/btree/src/changes.rs` provides coalesced committed wakeups, not changed identities or durable replay. `crates/sync-runtime/src/subscription.rs` schedules reconciliation over those wakeups.
- `crates/sql-sync/src/session.rs::sync_manifest_limited` enumerates rows and exports document state to calculate manifests. With `AutomergeRowCodec`, digest calculation then loads that state to hash its heads. Incremental transmission therefore does not imply incremental discovery cost.
- `crates/sql-engine-automerge/Cargo.toml` depends on `sql-sync`; its Row Reconciler implements a protocol-owned `SyncRowCodec` and uses protocol-owned types.
- `crates/btree-automerge/src/util.rs::tx_update` keys stored incrementals by a digest of heads, whereas SQL `merge_row` uses a change hash. Persisted records are not a uniform stream of individual Automerge changes.
- `crates/btree-automerge/src/compaction.rs::run_compaction` and SQL `merge_state` replace prior document records with snapshots. Physical records are not a durable commit log.
- SQL deletion and table-drop metadata is separate from Automerge document history. Document heads alone cannot detect all replicated state changes.
- `crates/kv-store/src/store.rs::import_snapshot` already merges divergent same-generation histories. KV needs more efficient discovery/exchange, not replacement of its merge semantics.
- SQL apply coordinates catalog dependencies, validation, authorization checks and derived Index Records. Those responsibilities cannot be bypassed by generic document replication.

## Goals

1. Avoid whole-database document reconstruction for ordinary small live updates after initial reconciliation.
2. Publish changed logical identities only for successful commits, covering local writes and incoming replication.
3. Preserve metadata-only updates, tombstones, table recreation, KV generation selection, conflict resolution and derived index correctness.
4. Separate Row Reconciler history/state capabilities from Sync Session protocol types.
5. Retain full reconciliation as the correctness fallback for restart, notification gaps, overflow, reconnect and missing history/dependencies.
6. Keep transport, peer selection, domain authorization decisions and explicit runner lifetime caller-owned.

## Non-goals

- A durable ordered event feed, exactly-once delivery, checkpoint/envelope replication, or persisted peer frontiers.
- A unified SQL/KV wire protocol or a new shared sync-core crate.
- Moving catalog validation or engine mutation semantics into `btree-automerge`.
- Equating a physical storage key, a document head digest, and an Automerge change hash.
- Replicating derived Index Records as canonical state.
- Automatic background tasks or replacement of caller-owned network admission.
- Compatibility wrappers for superseded interfaces or formats.

## Implemented ownership

| Concern                                                                | Owner                                            |
| ---------------------------------------------------------------------- | ------------------------------------------------ |
| Successful commit and generic committed notification                   | Storage transaction machinery                    |
| Changed Logical Row/catalog identities and dependency invalidation     | SQL engine with its Row Reconciler               |
| Changed KV Key/Generation and generation invalidation                  | KV Store                                         |
| Document reconstruction, heads, change dependencies and history export | Automerge implementation                         |
| Peer comparison, framing, staging, recovery and retry                  | Store-specific sync modules and existing runtime |
| Atomic apply and derived Index Records                                 | SQL engine; KV Store for KV state                |

Transaction-owned physical mutation bookkeeping captures successful commits, including indirect tombstones/schema cleanup and incoming replication. SQL/KV map it to logical invalidation; neither requested `changes` nor `MutationSummary` alone is treated as a complete inventory. No parallel observer framework or persisted peer frontier was added.

The minimum useful notification contains identities/invalidation, not retained payloads. A conservative table/catalog invalidation is acceptable where exact row collection is unsafe or expensive. Unsupported/untracked mutation paths must request broader reconciliation rather than silently omit work.

## Required correctness contract

### Commit and subscription lifecycle

- Aborted, failed and rolled-back transactions publish no successful logical change batch.
- A committed mutation is discoverable before subsequent admission can hide it; cancellation after persistence succeeds must not lose its invalidation.
- In-flight reconciliation cannot acknowledge later commits accidentally. Define a revision/watermark or equivalent handoff for batch selection and completion.
- Each peer/consumer advances independently. One session consuming work must not clear another peer's pending work. A newly connected peer starts with broad reconciliation, not only changes since subscription.
- Bound retained in-memory state. An explicit gap/full-reconciliation marker replaces unbounded buffering; it is not a durable history promise.
- A reconnect, restart, new peer, gap or overflow uses a broader comparison before resuming selective work.
- Incoming replication participates in detection for fan-out. Known/no-op history must settle without infinite echo.
- Local commit subscriptions do not detect writes committed only on a remote peer. Define remote wakeups, a live receive path or caller-driven reconnect/reconciliation explicitly; do not claim local notifications alone provide bidirectional realtime delivery.

### Document and metadata reconciliation

- Read a document's history summary and metadata from a consistent transaction snapshot. Avoid exporting and reloading a full snapshot solely to obtain a digest.
- Compare actual Automerge change identities/dependencies, not stored incremental-key labels. Preserve dependency order even when storage keys are hash-sorted or payloads contain several changes.
- Missing changes should be exportable from reconstructed document history where supported. Absence of a standalone incremental record is not proof that Automerge history was deleted by storage compaction.
- Send relevant metadata even when document deltas are available. Metadata-only edits, or edits where both metadata and document heads changed, must not be dropped by delta selection.
- Snapshots merge histories; they do not discard concurrent local work. Missing dependencies recover without accepting incomplete final state.
- Session completion is not whole-session atomicity or a globally frozen database snapshot. Preserve existing complete-row/catalog apply scopes and valid earlier committed progress.

### Store-specific behavior

- SQL catalog updates expand to the complete dependency set required by the existing validated apply path. A drop/recreation may invalidate an entire table.
- The Engine remains the only apply path for engine-managed Logical Rows; retain authorization checks before atomic apply, candidate/history validation and atomic derived-index maintenance.
- KV retains latest-generation selection, stale-generation rejection, tombstone-wins reconciliation and same-generation merge. A generation replacement invalidates selection for its KV Key.
- KV Expiry is read-time visibility, not a committed mutation. Preserve its existing semantics without promising timer-generated sync events.

## Implementation sequence and outcome

See [map.md](map.md) and individual issues for the completed sequence and remaining validation gates. SQL and KV reuse native document history without unifying store semantics.

1. Characterize commit, history and metadata invariants; measure current work.
2. Put document history/state capabilities on the engine/Row Reconciler side of the seam; retain protocol messages in SQL sync.
3. Add commit-scoped logical identity detection with independent consumer progress and explicit broader-reconciliation fallback.
4. Integrate selective SQL reconciliation and incremental KV exchange through validated apply paths; explicitly define remote-change delivery.
5. Exercise failure/concurrency scenarios, measure actual work reduction, remove superseded code and update current documentation.

## Validation and decision gates

The initial tests must establish baseline behavior rather than assume every existing path is correct. Record any reproducible existing defect separately and repair defects necessary to meet this contract with regression coverage.

Focused tests cover rollback/failed commit, multi-write transactions, commit during export/apply, cancellation at commit, two independent peers, new peer/reconnect, gap/overflow, incoming-change fan-out, no-op echo suppression, metadata-only and combined metadata/document changes, dependency recovery, multi-change payloads, snapshot rewriting/compaction, SQL catalog/drop/recreation/unique-key side effects, KV concurrent nested edits/delete/recreate/stale generations and expiry.

Use deterministic counters or test instrumentation to compare document reconstruction, full snapshot export and history enumeration for an established peer receiving one-row updates. Avoid brittle timing assertions. Broader scans remain permitted for explicit fallback and catalog dependency invalidation.

Before implementation, record complexity/risk/priority: high priority for correctness, high risk around commit publication and missed metadata, medium-to-high implementation complexity. Run focused checks during implementation. Final validation after implementation: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo hack test --feature-powerset --workspace --all-targets`, and `just crap`. Report unavailable tools or unrelated failures rather than claiming validation passed.

## Retained implementation limits

- Invalidation is bounded in process: 128 retained commits, each at most 128 physical invalidations / 16 KiB of identity bytes. Oversized commits, lag/gaps, new/cloned subscriptions and reconnect/restart require All. Selected work can combine retained commits; retention limits do not bound all transaction/session memory.
- Select before reads and acknowledge only success. Every store/peer has independent unmerged subscription lifetime; later commits and other peers remain pending. Tokens are not durable cursors or authorization scopes.
- SQL exact-row work avoids unrelated row enumeration; table invalidation permits a table scan and catalog/dependency/unknown invalidation is broad. Metadata mismatches use complete state plus metadata. Restricted SQL policies use scoped validated APIs, not selected APIs as admission.
- KV selected work re-reads latest generations and enumerates full selected document history. Broad fallback retains whole-store inventory; configured per-document/frame limits can reject oversized work. SQL likewise has frame/apply/session budgets; neither protocol chunks a single oversized document.
- Local signals cover shared wrappers only, not external writes or remote-only commits. Callers must arrange remote wakeups/receive delivery or explicit two-sided reconciliation. SQL memory supports subscriptions; KV memory remains caller-driven broad sync. No automatic bidirectional realtime or durable replay promise is introduced.

## Design guardrails

- If exact row tracking would duplicate SQL mutation logic, use conservative table invalidation first.
- If notification ownership cannot provide reliable per-peer progress, keep broad reconciliation until a tested contract exists.
- If a protocol change is required, version the affected store's protocol explicitly; do not add legacy glue.
- If document-native sync would replace substantial inventory machinery, compare it in a bounded experiment before adopting it. Do not add another protocol layer merely for symmetry.
