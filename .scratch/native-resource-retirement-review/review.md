# Independent native managed-resource retirement review

Date: 2026-10-09. Reviewed full `4cd65c17c08733c7eb74ef65850380c2ba74f8c6..2b424b5c0cadbad296f0046a38fd4c5dc6d64867`, then the corrective diff. Original root `storage-sync-integration` and owner `native-resource-retirement` were clean; stash list empty. Read consumer audit `of/.scratch/offline-first-nodes/evidence/05-completion/owner-api-audit.md` and `of/docs/public-persistence.md` managed-copy contract without writing to either of or offs.

Global change-review skill read directly after tool lookup failed. Correctness/QA, design/lifecycle, architecture and security/data perspectives reviewed directly: no subagent tool is available. TDD and ponytail loaded. Risk: high-priority, high-risk native lifetime/unlink boundary; fix is confined to shared namespace admission and two public regression tests.

## Blocker found and corrected

At original `crates/kv/src/database.rs:80-90` and `crates/sql/src/database.rs:258-261`, retirement-marker checking was separate from native construction. A concurrent opener could pass the check, pause, then recreate a usable database after retirement successfully removed it. Deterministic public KV `open_with_table` regression pauses table conversion with channels; retirement succeeds before opening resumes. On original code it fails with `a concurrent opener must not bypass the retirement fence` (exit 101). See `evidence/kv-open-race-red.log`.

The shared native `open_unretired` holds one process-local namespace mutex across marker check and redb open/create. Durable marker publication and `remove_retired` lock the same mutex. Both SQL and KV constructors use it, with caller parameter conversion before admission; the obsolete standalone public marker-precheck exports were removed. No backend/namespace lock is held across an async drain. This prevents managed same-process constructors racing publication/cleanup; it is not a cross-process namespace coordinator. Successful green regression leaves the path absent. See `evidence/kv-open-race-green.log`.

## Reviewed invariants

- SQL Database clones and embedded Client clones retain the same NativeResource Arc. KV clone_shared and embedded Client cloning likewise share it. Every embedded query routes through ResourceGuard admission. SQL translator execution for ordinary operations is inside the admitted engine call; explicit transaction translation acquires its transaction guard before invoking user code.
- Admission checks retirement before waiting for the backend read lock and again after acquiring it. The second successful check is the admission boundary. Retirement marks state under the same state mutex before waiting for exclusive backend ownership; new queries/transactions/sync calls deny even while drain waits.
- Explicit native transactions are registered while transaction construction still owns backend admission. Retirement erases idle slots to free native writer admission; active operations retain both backend read admission and transaction slot lock and therefore cannot be erased/unlinked while running. Exclusive backend acquisition guarantees active slots are no longer locked before final erasure. A subsequent commit is a new operation and denies; this does not promise an idle transaction may commit after retirement.
- New SQL regression pauses translation within an already-admitted explicit transaction operation. Retirement is pending, the persistence file remains, fresh transactions deny, and remove_retired refuses the live native owner. The admitted operation finishes, retirement then removes the file, and later commit denies. No detached native writer survives successful managed retirement.
- Async native writer admission eliminates executor-blocking begin_write contention among shared backend clones. Retirement releases idle transactions before waiting for admitted queued operations. Sync sessions use weak abort registrations; retirement aborts transport/preapply waits, and their future drop releases guards/stages. Executors must poll or drop those futures to drain.
- Checked/unchecked, stage-factory/directory, table-scoped/validated/candidate sync routes converge on session gating; sync manifests/exports/applies and KV snapshot export/import acquire normal admission. Runtime configuration/client construction is not persistence access. Hosting is explicitly excluded via sticky hosted state: retirement returns Unsupported before fencing, rather than claiming escaped server backend clones are revoked.
- Marker publication uses tempfile persist_noclobber, file fsync and parent-directory fsync. It records native Unix identity and exact tracked stage identities/absolute paths before drain. Stage tracking is retired before recording, so a racing late stage creation denies and its temporary file drops. Closed stages may remain in the durable record; absent files are idempotently accepted.
- Backend release precedes targeted stage/persistence cleanup. remove_retired probes the redb native lock before stage removal; live admitted owners refuse cleanup. Recorded identities are checked before unlink; changed file/symlink/non-file refuses, unrelated contents/directories are retained. The marker remains permanently. Cleanup failure never reports successful physical removal.
- Cancellation after durable publication retains the gate; retry or sequential fresh-process remove_retired resumes recorded cleanup. The abrupt-child test exits without destructors after fence publication and with live stages, then a fresh process verifies targeted cleanup and unrelated native reopen. No domain catalog/Tombstone/remote-peer mutations or consumer filesystem unlink are added.

No remaining blocker found within the explicitly managed, dedicated-file, Unix, exclusive-namespace owner contract after correction. Generic capability acceptance is not consumer of05 completion.

## Independent focused validation

All cargo commands ran in existing owner worktree with **initially empty** `CARGO_TARGET_DIR=target/owner-retirement-review`, `CARGO_PROFILE_DEV_DEBUG=0`, and `--config 'profile.dev.package."*".opt-level=0'` (dependencies unoptimized for the cold focused build). Final qualification also sets `RUST_MIN_STACK=16777216`. Logs include exact argument lists and exits in `evidence/`.

| Command arguments after cargo test/config | Result | Evidence |
| --- | --- | --- |
| `-p ofdb-sql --features redb,sql,native-stage,server --test retirement` | 9 passed, no failures/ignores; 8 behavioral tests plus ordinary child-fixture no-op | `sql-retirement.log` |
| `-p ofdb-kv --features redb,sync,server --test retirement` | 6 passed | `kv-retirement.log` |
| `-p ofdb-sql --features redb,sql --test client redb_client_survives_database_drop_and_persists_data` | 1 passed | `sql-existing-native.log` |
| `-p ofdb-kv --features redb --test embedded_redb_client` | 3 passed | `kv-existing-native.log` |
| `-p ofdb-btree-redb --lib database_transaction_commits_multiple_tables_once` | 1 passed | `btree-native-admission.log` |
| `-p ofdb-sql-engine-redb --lib logical_names_are_isolated_and_persisted` | 1 passed | `sql-kernel-native-admission.log` |

Strict `cargo clippy` on SQL (`redb,sql,native-stage,server`) and KV (`redb,sync,server`) libraries with `-- -D warnings` passes. `rustfmt --check --edition 2024` on the seven touched Rust files and `git diff --check` pass. Existing SQL client fixture has feature-dependent unused imports; it was not changed. No hack, CRAP, feature powerset, workspace sweep or consumer tests run. Earlier owner's broader facade feature check is historical evidence, not independently repeated here.

## Exact public consumer handoff and limits

With `redb` enabled, both **ofdb_sql::Database** and **ofdb_kv::Database** provide:

```rust
pub async fn retire_and_remove(&self) -> std::io::Result<()>;
pub fn remove_retired(path: impl AsRef<std::path::Path>) -> std::io::Result<()>;
```

SQL `Database::open(path)` and named KV `Database::open_with_table(path, "kv")` must target separate dedicated native files. Removal is whole-file, not just the named table; unrelated tables must not share that file. Of coordinates independently retryable SQL/KV phases and durable pending intent. Issued managed DB/Client/transaction operations deny after the gate; existing admitted local operations drain, active sync waits cancel, idle transactions roll back. IO errors propagate; ordinary operations use existing storage errors, sync uses PreApply.

- Unix only, exclusive resource pathname/marker namespace. No hard-link/alternate-path or concurrently active cross-process namespace-owner coordination, external namespace replacement defense, or untrusted-marker parsing/security contract. Process-local open/publication/cleanup IO is globally serialized; normal queries and async drain are not.
- Permanent `.ofdb-retired` fence: no same-path recreation/reselection/ungate API. Consumers must not unlink the marker. This is an explicit future lifecycle gap, not a reason to falsify removal success.
- Sticky built-in native hosting attempt makes retire_and_remove Unsupported, even after hosting ends. Remote/server/raw separately constructed backends are not covered. Close all such owners and reopen a fresh managed Database before retirement; host revocation itself is not delivered.
- Owner-created native stage-directory variants are tracked. Custom stage-factory cleanup is caller-owned. No pre-marker-crash orphan-stage recovery/sweep is promised.
- Futures must continue polling or be dropped for drain. Failed durable marker publication leaves in-process retirement gated but cannot reconstruct unpersisted intent after restart; caller pending policy must retry.
- Tested abrupt process exit is after durable marker publication, not power loss or exhaustive filesystem failure injection. SQL abrupt-exit staging test exercises shared native restart mechanism; no independent KV abrupt-child test was added.
- Not every facade sync variant has an independent behavioral retirement test; all were traced through shared gated branches. Tests exercise active SQL transaction operations, not artificially suspended native synchronous commits.

## Integration disposition

User explicitly authorizes hooks-disabled integration only after review/correction. Commit corrective code/tests/report in existing owner branch, then fast-forward clean storage-sync-integration with hooks disabled. Record actual resulting full hash and local map/ticket in root `.scratch/storage-sync/issues/15-native-resource-retirement.md`; preserve stashes/clean tracked state. Keep existing worktree until integrated owner consumer evidence is obtained. Verify and clean only review-owned `target/owner-retirement-review`; original owner `target/native-retirement` and root/shared targets remain untouched.
