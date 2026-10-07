# Public SQL conflict operations — owner follow-up 07

2026-10-07. Owner `storage-sync-owners`, base/integration `38696d981e88bcfe20a475f49eb514f498f29b75`. Original ignored tracker: `.scratch/storage-sync/issues/07-public-sql-conflict-api.md`. This supplies the native conflict-aware API missing from consumer of02; ticket 06 is downstream handoff evidence, not a gate. No consumer code is changed or migration declared complete.

## Consumer-derived minimum contract

Reviewed `of/.worktrees/public-persistence/docs/public-persistence.md` and original `of/crates/db/src/native.rs`, `idp-model/src/replica.rs`, `management-service/src/replica/selection_policy.rs`, and IdP refresh-token/signer repositories. Those consumers require conflict names, every concurrent value, explicit field resolution, and snapshot-local checks alongside transaction reads/mutations. Current projected SQL rows alone are not a substitute for conflict inspection.

`Database` remains construction/control. Obtain the query handle with `Database::client()`; the following methods are public on `ofdb_sql::Client`:

```rust
pub async fn row_conflicts(&self, table: &str, key: &Row)
    -> Result<Vec<String>, QueryError>;
pub async fn row_conflict_values(&self, table: &str, key: &Row)
    -> Result<Vec<(String, Vec<Value>)>, QueryError>;
pub async fn resolve_row(&self, table: &str, key: &Row, values: Vec<(String, Value)>)
    -> Result<(), QueryError>;
```

`ofdb_sql::Transaction`, created by `Client::transaction()`, exposes the same two inspection signatures, using its existing transaction snapshot, including its own uncommitted catalog/row writes. It does not open a second transaction. Aborted transactions return `Rejected`, not misleading empty conflict results. Commit/rollback consume the public transaction handle, so closed handles cannot be reused.

Results contain only existing public `Row`, `Value`, owned strings/collections, and `QueryError`; no kernel/codec/Redb/backend type escapes. Existing engine inspection/resolution and error conversion are reused; only engine transaction inspection entry points were missing. No dependencies, feature definitions, policy types, transport messages, or unrelated APIs were changed. Existing backend reexports are not removed in this follow-up.

Embedded native and in-memory clients support the methods independently of the `sync`/SQL-text features. Sync/SQL-text are needed only by the regression fixture to create and merge histories. Remote clients/transactions return `QueryErrorKind::Unsupported`, including remote-only builds: they never pretend an unsupported inspection returned clear state. Existing configured client/transaction deadline handling is reused; no new timing policy is introduced. Default facade and engine core no_std checks remain independent of native/runtime features.

## Security behavior retained, not moved into storage

The facade delegates to the same engine operations used by the original consumers. Concurrent false/true, NULL/selected UUID, distinct revocation timestamps, and competing owners remain observable with exact column identities. Resolving a subset leaves other conflicts unresolved. Empty, unknown, duplicate, nonconflicted/primary-key assignments, invalid keys and missing rows retain existing rejection; unsuccessful resolution does not change the row or clear conflicts. Tombstoned rows cannot be resurrected by resolution.

An empty conflict list is not proof that a row exists or that an operation is authorized. IdP remains responsible for fail-closed security decisions, administrator identity, monotonic revoked_at/consumed_at restrictions and audit persistence. Management still decides when NULL/false is a restrictive convergence and rejects ownership/admin ambiguity. The facade does not automatically select security values, authorize resolution, insert audit records, or substitute last-writer-wins projected writes. This does not enable mixed private IdP/Management history exchange; the consumer's allowlisted-history gap remains separate.

## TDD and focused evidence

Public seams tested: `Database::open/in_memory`, `Database::client`, Client conflict/query methods, Client-created Transaction, public SQL sync/state transfer. Tests import only `ofdb_sql` plus existing runtime/test support, not engine/kernel/codec/Redb constructors.

- Red: `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,sync,sql --test conflicts native_conflicts` failed with 12 missing Client-method errors. Minimal Client delegation made that test green (1 passed).
- Next red: same command with filter `transaction_conflict` failed with 6 missing Transaction-method errors. Minimal active-engine-transaction inspection and facade delegation made it green (1 passed).
- Subsequent native validation expanded malformed-key/missing-row/table and atomic rejection cases. Native resolution/reopen now reconciles complete state through public `export_sync_state` / `apply_sync_state`, matching the original consumer conflict tests. This test transfer uses trusted locally generated fixtures; direct apply still requires caller authorization.
- During feature checks, remote-only unused-argument and memory-test unused-import warnings introduced by this follow-up were corrected; final standalone checks and targeted all-feature Clippy pass without these warnings.

Focused chain bounded at 300s, every command completed successfully:

| Exact command                                                                                                                                                                      | Outcome                                                                                                                                                                      |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cargo check -p ofdb-sql --no-default-features`                                                                                                                                    | Passed                                                                                                                                                                       |
| `cargo check -p ofdb-sql --no-default-features --features remote`                                                                                                                  | Passed without introduced warnings                                                                                                                                           |
| `cargo check -p ofdb-sql-engine --no-default-features`                                                                                                                             | Passed                                                                                                                                                                       |
| `cargo check -p ofdb-sql-sync --no-default-features`                                                                                                                               | Passed                                                                                                                                                                       |
| `cargo check -p ofdb-sql --no-default-features --features redb`                                                                                                                    | Passed, no sync/SQL-text required                                                                                                                                            |
| `cargo check -p ofdb-sql --no-default-features --features in-memory`                                                                                                               | Passed, no sync/SQL-text required                                                                                                                                            |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test conflicts --test client --test facade_sync --test cluster_sync --test local_sql` | 3 conflict + 5 client + 7 facade sync + 9 cluster + 11 local SQL tests passed (35 total); rerun successfully in 120s-bounded chain, log `target/storage-sync-07-focused.log` |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features remote,server,redb,in-memory,sql --test remote`                                                                         | 2 passed, including all five unsupported conflict calls on a real remote client/transaction; rollback remains usable                                                         |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features in-memory,sync,sql --test conflicts`                                                                                    | 1 passed independently without native storage or Tokio runtime                                                                                                               |
| `cargo clippy -p ofdb-sql -p ofdb-sql-engine --all-targets --all-features`                                                                                                         | Passed                                                                                                                                                                       |
| `cargo fmt --all -- --check`, `git diff --check`                                                                                                                                   | Passed                                                                                                                                                                       |

Additional 300s-bounded chain completed: `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-engine --feature-powerset --all-targets` all **12/12** combinations passed; `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --all-features --doc` **3** compile-fail architecture tests passed; `cargo clippy --workspace --all-targets --all-features` passed with the same two unchanged KV CLI useless-conversion warnings at `p2p.rs:268/:332`.

Normal graph `cargo tree -p ofdb-sql --no-default-features -e normal --prefix none --features remote` was checked by package names: 85 packages, no SQL engine/engine-automerge/engine-redb/sync, Redb, Automerge, Iroh or tempfile. No remote-only dependency expansion.

## Failures, limits and broad outcomes

### Observed incremental-session gap (not repaired or claimed passing)

After explicit conflict resolution and native reopen, an initial version of the facade test's final **incremental sync session** failed with `RemoteAbort("Error: Incremental change hash does not match key (data change batch)")`. It reproduced with `RUST_BACKTRACE=1 RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,sync,sql --test conflicts native_conflicts -- --nocapture`; the backtrace points to the final exchange after resolution/reopen, not initial divergence.

That validation occurs in the unchanged `sql-engine-automerge/src/codec.rs::apply_change` path. This follow-up does not change the incremental codec, sync session, or engine's pre-existing nontransactional resolve/inspection implementation. No clean-baseline execution of this new scenario was performed, so an independently baseline-reproduced classification is not claimed. The finding is retained here rather than weakening the integrity check or broadening this facade ticket into a sync repair. A reconstructed session-variant fixture is retained at ignored `target/storage-sync-07-incremental-repro.rs` for further investigation (not part of the passing test suite). **No incremental-session-after-resolution success is claimed.** Persisted resolution and whole-state reconciliation are independently demonstrated through the same state seam used by the original consumer tests. This gap must be investigated before claiming that incremental scenario is qualified.

### Final broad runs (sequential, each `timeout_ms: 1200000`)

- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`: **1200s timeout compiling combination 309/581** on SQL (`default,in-memory,native-stage,redb,remote,sql`); combinations 1–308 completed, no observed failing test/compiler error. Output `target/storage-sync-07-powerset.log`. Not a complete powerset pass; earlier 05's 581/581 pass was at its recorded code revision, not evidence for these later changes.
- `just crap`: **completed, exit 0, 5m2.140s**, all 28 per-package coverage invocations plus analyzer. Output `target/storage-sync-07-crap.log`. **199/1,045 functions above threshold 30**, with the unchanged warning that only **2/96 files match final LCOV**. The baseline shared output path leaves only final-package coverage (documented in 05); not valid whole-workspace coverage or an all-green complexity claim. No failing enforced gate observed and no unrelated complexity/recipe changes made.

Ticket 07's owner-local API acceptance is demonstrated by the red/green/native/reopen/transaction/memory/remote/feature evidence above. Full broad feature qualification of this follow-up is incomplete, and the observed incremental-session gap remains explicit. These are not concealed by prior revision passes or by whole-state fixture success. Historical default-stack SQL limits remain; successful SQL tests use the stated stack setting. No consumer migration, domain-history exchange, real domain authorization, or external of/offs acceptance is claimed. Original dirty design/goal docs and other repositories are untouched.
