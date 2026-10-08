# Scoped SQL candidate validation — owner follow-up 13

## Concrete consumer need

Read-only review of original `of/docs/public-persistence.md` bootstrap handoff (125–155), original offline-first-nodes spec and latest ticket03 marker3 comments identified the scalar-observation limitation: `(table, column, value)` cannot correlate an actual complete candidate and current related local rows. This owner change supplies observations at existing scoped sync transactions. Purpose-bound bootstrap admission, trust/readiness, original actor/action/target/provenance and current outbound transport authorization remain in of. No ledger/auth fields, domain policy, generic domain trait, transport framework or consumer edit is included.

Claimed13 in the verified original ignored storage-sync tracker/map before implementation, at independently integrated `98df448df74fdca1628abe41859793971eee044c`. Owner branch and integration matched; merge into owner was Already up to date. Complexity moderate; risk high because speculative state, rollback and disclosure are security-relevant. Ticket13 remains **in-progress / pending independent review**. Ticket12 is not changed by this follow-up.

## Exact public facade

Existing signatures and structural/historical validation contracts are unchanged. Two opt-in methods are added to `ofdb_sql::Database`:

```rust
pub async fn synchronize_tables_candidate_validated_checked<T, V, C, Q, U>(
    &self,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    tables: &[&str],
    validate: V,
    validate_candidate: C,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>

pub async fn synchronize_tables_with_stage_directory_candidate_validated_checked<T, V, C, Q, U>(
    &self,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    directory: impl AsRef<std::path::Path>,
    tables: &[&str],
    validate: V,
    validate_candidate: C,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
```

Bounds retain `T: SyncTransport`, displayable transport errors, `V: Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync`, and `Q: FnMut() -> U + Send` where `U: Future<Output = Result<(), String>> + Send`. The additional bound is:

```rust
C: Fn(&SyncCandidateView) -> Result<(), String> + Send + Sync
```

The callback can borrow non-static caller data. Memory staging uses existing `sync`; native staging uses existing `native-stage`. No new feature or dependency. Public evidence types re-exported by ofdb-sql:

- `SyncValidationDirection::{Incoming, Outgoing}`.
- `SyncCandidateView { direction, candidates: Vec<SyncKey>, tables: Vec<SyncTableView> }`.
- `SyncKey::Row { table: String, row: Uuid }` identifies each candidate logical row (deduplicated, user tables only).
- `SyncTableView { name: String, columns: Vec<String>, rows: Vec<SyncRowView> }` supplies selected-table column order and related rows.
- `SyncRowView { id: Uuid, row: Row, deleted: bool, conflicts: Vec<(String, Vec<Value>)> }` supplies actual complete logical row, tombstone state and every currently concurrent value under its column name. Empty `conflicts` is a successful native observation, not an ignored inspection error.

These are read-only borrowed observations during callback invocation, not database/backend/transaction handles. There is no async query callback or network wait under a write transaction. Selected-table evidence is owner-materialized from one transaction, including rows not changed by this batch. Excluded tables and other contexts are not supplied or exported; if a domain needs related evidence, its selected tables must include it. Missing related rows remain missing rather than being synthesized from scalar observations.

## Transaction semantics and limits

Incoming: the common data-row apply path merges **all rows in the actual atomic batch**, then collects row/tombstone/conflict/related-row evidence from that same still-open owner transaction. A synchronous callback validates that materialized prospective view **before commit**. Snapshot, delta, snapshot-only fallback and dependency recovery route through the same gate, including per-row dependency retries. Validation failure rolls back all mutations in that batch; earlier committed catalog/data progress and independent local changes remain. Strict codec hash membership, dependency order, catalogs/indices, historical schema/value checks and existing temporary stage cleanup remain intact.

`Engine::mutate_tables_validated(tables, operation, validate)` is the minimal generic implementation helper: operation returns transaction-derived output and mutation summary; `validate: FnOnce(&O) -> EngineResult<()>` runs synchronously before schema/index reconciliation and commit. Existing `mutate_tables` uses the same implementation with an allowing validator. This accommodates borrowed callbacks without requiring them to be static, exposing a raw transaction to consumers, or reading the database independently after commit. The public facade never exposes the generic engine/kernel/codec types through this new seam.

Outgoing: initial/preflight state exports, actual incremental exports and fresh fallback/recovery exports materialize complete source candidates plus related rows in the **same read transaction as the export**. Validation then runs over that immutable view before those exports are sent. It does not reconstruct a row from historical scalar bags. Old historical-value inspection still checks all assignments/defaults, including overwritten/deleted/losing-conflict values, independently of candidate decisions.

Important non-guarantees:

- A view describes one owner transaction, not a coherent session-wide export or source multi-row atomic transaction. Protocol frame boundaries are not commit boundaries. Large batches can split at existing byte limits, and related evidence can be incomplete across those commits.
- Incoming snapshot and delta stages can be separate atomic operations. Consumer validation must handle pending state, retries and missing related rows rather than invent source transaction ordering.
- An allowing callback permits persistence only. Pending/incomplete rows or conflicts do **not** acquire owner/domain trust automatically. Domain trust/readiness decisions remain consumer-owned and must be checked on durable state.
- The callback may observe a prospective batch that later fails index/commit validation or is retried. It must be read-only/idempotent and must not publish external trust or commit side effects. Do not block, reenter this database or perform network IO in it.
- This is complete current row/conflict evidence, **not** correlated original historical mutation tuples, signed mutation authority, or an authenticated actor ledger. Transport peer, CRDT actor/hash and UUID are not original domain authority.
- Validation occurs at export/apply, not every subsequent socket send. Async current-policy/outbound revocation enforcement and independent context admission remain consumer concerns. No freshness/deadline/lease guarantee is added.
- Evidence materialization scans/copies all selected logical rows and their current conflicts per observed transaction. It is opt-in and costs O(selected row/value evidence) per batch/export; it does not offer lazy queries or a new generic quota. Old methods do not materialize this evidence.
- Callback errors can be sent in Abort; return non-sensitive constants, never confidential values. Abort is best effort and does not wait for peer progress. Discard the transport after failure; canceled send can partially write a frame. Stages are cleaned, but earlier valid commits are not session-wide rolled back.

## TDD and focused evidence

User-authorized seams: public Database scoped memory/native-stage sync, Client conflict reads and native reopen; existing public owner codec/session recovery seam for hostile-wire coverage. All commands ran in the owner worktree only. `E` below is the exact literal prefix:

`CARGO_TARGET_DIR=target CARGO_INCREMENTAL=0 RUST_MIN_STACK=16777216`

Build/test commands had tool1200s bounds unless noted. No timeout in this follow-up.

1. Initial interface RED: `E cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync candidate_correlation -- --nocapture`: exit101, missing public evidence types/methods.
2. Behavioral RED after facade wiring but before transactional gate: exact same command exit101, **0 passed / 1 failed**, `invalid complete candidate must not commit: ()`. Invalid two-row correlation committed despite individually valid scalar values.
3. GREEN: same command exit0, **1 passed** in both roles. Two candidate rows merge before validation despite separate frames; rejection rolls both back, stages empty, native reopen confirms absence; corrected retry persists matching rows and survives reopen.
4. Outgoing RED: `E cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync candidate_outgoing -- --nocapture`: exit101, **0 passed / 1 failed**, invalid actual source correlation returned successful SyncResult. After same-read export evidence/gate, `E cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync candidate_ -- --nocapture` exit0, **2 passed**. Outgoing denial in both roles sends only scope negotiation+Abort, not history exchange; source remains intact, receiver has no imported row.
5. Additional public qualification: a local related-row UPDATE after staging/before actual incoming delta transaction is observed as a concurrent conflict; callback sees both `next`/`revoked` values, denies merged candidate, native reopen retains the prior reference and independently committed local revocation. Independent same-key creation+UPDATE before first exchange exercises validated full-state fallback, preserves pending rows/conflicts, native reopen and tombstone evidence. Later-byte-batch rejection retains the first committed batch after native reopen and preserves source rows.
6. `E cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts`: exit0, **23 passed** (18 scoped +5 conflicts). Includes the five new candidate tests and prior strict history, stable identity, resolution/reopen, tombstone and pending-Abort cleanup regressions.
7. `E cargo test -p ofdb-sql-engine-automerge --test scoped_session --test session`: exit0, **14 passed** (4 scoped-wire +10 session). Forced dependency recovery in both roles rejects actual prospective import and source mutation after preflight before recovery export, retains prior receiver row, while existing historical scalar/hostile-wire/causal/replay/byte-limit cases remain passing. Recovery forcing runs on owner memory engine/stages; native candidate apply/fallback/reopen/cleanup are qualified through the public facade, not a separately forced native-recovery transport fixture.
8. Five sequential repeats each of `E cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync candidate_ -- --nocapture --test-threads=1` and `E cargo test -p ofdb-sql-engine-automerge --test scoped_session candidate_recovery -- --nocapture`: **10/10 commands passed**,25 public+5 owner recovery test executions; each subprocess90s, combined tool300s bounds.
9. With exact `CARGO_TARGET_DIR=target CARGO_INCREMENTAL=0` prefix: `cargo check -p ofdb-sql-sync --no-default-features`; `cargo check -p ofdb-sql --no-default-features --features in-memory,sync,sql`; `cargo check -p ofdb-sql --no-default-features --features redb,native-stage,sql`: all exit0. Strict `cargo clippy -p ofdb-sql-sync --lib --no-default-features -- -D warnings` and `cargo clippy -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync -- -D warnings`: exit0. `cargo fmt --all -- --check`, `git diff --check`: exit0.

Failed implementation/harness attempts retained honestly: capturing the borrowed validator in the higher-ranked async engine closure caused a lifetime error; replaced by transaction-derived output plus synchronous validated mutation helper, not static/global state. One recovery scope argument needed `as_deref`. An attempted test decoder used postcard absent from SQL dev dependencies; no dependency added, preflight-only scope/Abort frame-count assertion used instead. The first later-batch fixture compressed to one batch and failed its explicit candidate-count assertion (4 pass/1 fail); increased deterministic non-sensitive fixture text from780k to950k to cross the actual existing byte boundary, exact targeted rerun passed. Strict facade Clippy first failed only the new private callback type complexity; a private lifetime-aware type alias fixed it, rerun passed. No integrity, conflict or policy assertion was weakened.

No full CRAP, cargo-hack, feature sweep, workspace lint/test, of test, bootstrap/domain/platform acceptance or authenticated original-actor qualification is claimed. Original dirty design/goal docs and other workspaces are untouched; no owner back-merge into integration. This report is owner-local evidence for independent review, not completion of consumer03/04 or downstream06.

## Final integration and isolated cleanup

Before-commit integration merge remains Already up to date at98df448; prior98df448 ancestry verified, original dirty design/goal unchanged. `CARGO_TARGET_DIR=target cargo metadata --offline --no-deps --format-version=1` verified exact `/home/nathan/code/rust/aicacia/ofdb/.worktrees/storage-sync/target`, non-symlink and not equal/samefile to original/shared target. `cargo clean --offline --target-dir /home/nathan/code/rust/aicacia/ofdb/.worktrees/storage-sync/target` exit0, removed3627 files/2.8GiB; target asserted absent afterward. Cleanup subprocess120s/tool180s bound, no timeout. No root/shared/other-agent cleanup.
