# 01 — Public native SQL/KV whole-resource retirement

Status: resolved — managed native owner contract independently reviewed, corrected and qualified for authorized integration

## Request and scope

Read-only consumer handoff:
`of/.scratch/offline-first-nodes/evidence/05-completion/owner-api-audit.md` and
`of/docs/public-persistence.md#managed-copy-retirement-public-owner-handoff`.

Own isolated branch/worktree: `native-resource-retirement`,
`ofdb/.scratch/worktrees/native-resource-retirement`.
Base: `storage-sync-integration`, `4cd65c17c08733c7eb74ef65850380c2ba74f8c6`.
No `of`/`offs` edits. Original implementation did not have merge authorization;
subsequent recovery request authorizes reviewed integration into storage-sync-integration.

Assessment before implementation: high-priority concrete consumer capability gap;
medium/high lifecycle risk (native writer exclusion, retained transactions,
suspended sync/stages, exact-file cleanup and durable retry). The owner needs no
transport, authorization, catalog, or replication policy. Tests are at the requested
public SQL/KV Database/Client/transaction/sync seams. TDD and ponytail skills loaded.

## Concrete answer

Both public native Database types now provide `retire_and_remove(&self).await` and
`Database::remove_retired(path)`, returning `std::io::Result<()>` with `redb` enabled.
See [the exact owner contract and limits](../../../docs/native-resource-retirement.md).

One shared native owner gate revokes issued Database/Client clones, transaction
operations (including SQL user translation), and every facade sync entry point.
Admitted local operations finish, idle transactions release/rollback, and sync
waits abort without network progress. Writer admission lives at the native backend
and is asynchronous; it does not hold a native transaction across network waits.
The durable marker records resource identity and owned native stages. Owned handles
release before stage cleanup and exact database-file removal; failures propagate
and recorded cleanup can resume in another process. No key/table deletes,
Tombstones, catalog writes, remote erasure, or containing-directory sweep.

### Explicit consumer limits / review decisions

- Unix-only qualification; caller owns the resource pathname and marker namespace.
  SQL and named `kv` resources must be dedicated files, independently retired.
- Permanent marker means no same-path recreation/ungate API in this follow-up.
  Review suitability for future resource reselection; no consumer marker unlinking.
- Native built-in hosting attempts issue unmanaged backend handles, so retirement
  refuses those owners with `Unsupported` before claiming revocation. Remote/raw
  independently issued backend handles are not covered. Host revocation needs a
  separate contract if the consumer needs it.
- Owner tracking covers native stage-directory variants. Custom stage-factory
  cleanup remains caller-owned. No crash-before-marker orphan-stage sweep.
- Futures must be polled/dropped to drain. Failed marker persistence cannot promise
  a durable request; caller pending intent must survive and retry independently.
- Abrupt process-exit recovery is tested, not power loss or every syscall boundary.

## TDD evidence

Observed RED before implementation: missing KV APIs; missing SQL APIs. Additional
RED: stage `Drop` deletion failure incorrectly permitted successful resource
removal; later, retired SQL transactions still invoked user translation. Both
regressions are active and GREEN with the owner changes. No ignored tests, softened
integrity checks, fallback, or fake success.

A 60-second combined run exposed a gate-order deadlock after moving writer
admission to native transactions: a new operation waited behind retirement's writer
before checking retirement. The early shared-state denial fixes it; the admitted
operation/cancellation regression now passes. A separate 120-second grouped
regression run timed out compiling its final backend check; the remaining focused
check was rerun and passed. SQL sync futures require the recorded 16 MiB test stack;
the initial ordinary-stack fixture overflowed, not a passing validation.

## Final focused validation

All commands use only this worktree and the initially empty isolated
`CARGO_TARGET_DIR=target/native-retirement`. SQL sync tests use
`RUST_MIN_STACK=16777216`. Exact commands/output are under `../evidence/`.

- SQL retirement: 8 harness tests pass (7 behavioral cases plus the child fixture
  no-op in ordinary runs). Includes live Database/Client clones and transactions;
  denial before custom translation; direct sync-state denial; real native stages
  during pending pre-apply; local work during pending sync; stage cleanup failure
  with native-handle release, retry/reopen denial; admitted-write drain and canceled
  retirement; abrupt owner-process exit before destructors followed by exact native
  persistence/stage cleanup from a fresh process; unrelated native reopen; native
  hosting refusal.
- KV retirement: 5 pass. Includes named `kv`, all query operations through retained
  client clones, live transactions, asynchronous admitted queued-work drain behind
  an idle transaction, checked/unchecked pending sync cancellation plus local work,
  changed-resource identity refusal with unrelated replacement retained, repeat
  removal/reopen denial, neighbor retention/reopen, and native hosting refusal.
- Existing KV native named-table/client/transaction regressions: 3 pass.
- Existing SQL native client lifetime/persistence regression: 1 pass.
- Changed native multi-table adapter transaction regression: 1 pass.
- Changed SQL native kernel isolation/reopen regression: 1 pass.
- Focused facade Clippy and native/memory/remote/server feature compile check pass.
  The existing SQL client regression has feature-dependent unused-import warnings;
  its file was not edited. No powerset, CRAP, all-target/workspace sweeps run.
- Touched Rust formatting and `git diff --check` are recorded separately.

## Independent review and integration

Historical pre-review handoff: no independent review or integration authorization
had been obtained at `2b424b5`. Requested review scope:
shared admission races/async writer ordering, idle-transaction rollback, abort/drop
stage lifetime, marker fsync/publication/identity validation, exact stage/persistence
cleanup and native lock probe, failure/cancellation/restart idempotence, and the
hosting/permanent-fence/custom-stage limits against the consumer handoff. The recovery request now directs reviewed integration after fixes and focused
qualification. This does not complete of05 or the entire offline-first implementation.

## Comments

2026-10-09: Generic owner implementation and focused native evidence retained on the
isolated branch. Consumer policy/completion remains in `of`; no consumer edits.

2026-10-09 independent recovery review: reviewed the entire `4cd65c1..2b424b5`
change from correctness/QA, lifecycle/design, architecture and security/data
perspectives. Global change-review instructions loaded directly (tool registration
unavailable); no subagent tools available. Confirmed a blocker via deterministic
public KV paused-opener regression: marker precheck raced retirement, then redb
construction recreated a live retired resource. RED and GREEN are retained in
`../../native-resource-retirement-review/evidence/`. Shared native namespace
serialization now covers both facade constructors, marker publication and restart
cleanup; removed obsolete standalone precheck exports. No cross-process/alternate
path coordinator claim. Added explicit SQL active-transaction-operation drain/no
unlink/later commit denial evidence. All managed facade routes trace to the gate.

Independent focused qualification after correction: SQL retirement 9/9 (one child
fixture is a no-op in the ordinary harness); KV retirement 6/6; existing SQL native
client 1/1, KV native named-table/client/transaction 3/3, btree multi-table commit
1/1, SQL kernel isolation/reopen 1/1. Strict facade library Clippy, touched-file
format and diff checks pass. Full commands, logs, review rationale and retained
limits in `../../native-resource-retirement-review/review.md`. No powerset, CRAP,
workspace sweep or consumer tests. Owner worktree retained; only the review-owned
isolated target is to be cleaned after exact-path verification. Local root owner
storage-sync ticket 15/map will record actual integration head and cleanup. The
hosting refusal, permanent fence/reselection gap, caller-owned custom stages,
Unix/exclusive-namespace scope, and power-loss/pre-marker-stage limits remain
explicit; owner acceptance is not of05 consumer acceptance.
