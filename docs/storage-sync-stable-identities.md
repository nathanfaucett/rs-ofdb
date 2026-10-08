# Stable SQL row identities — owner follow-up 12

## Consumer blocker and scope

Original of03 Comments35 records that Management `selection_policy.rs::resource_selection_id` hashes Device/owner/Application/kind/resource into `Uuid::new_v5(&Uuid::NAMESPACE_URL, ...)`, then inserts/looks up that exact key through public SQL. Reviewed the original consumer generation, insert-versus-update/retry, conflict reads and UPDATE-based deselection. The previous owner1480a09 rejected the real key as UUIDv7-only. No of file, fake domain UUIDv7 key, domain policy or consumer execution is part of this change. Owner12 was claimed in the verified original ignored tracker before implementation, after integration1480a09 was merged into the owner (Already up to date).

The old glossary's all-UUIDv7 statement conflicted with this demonstrated stable user identity requirement. The updated Row Identity definition distinguishes user identity from catalog generation.

## Exact public contract

No new SQL facade method, schema option, feature or dependency. Existing `ofdb_sql::Database`/`Client`/`Transaction` APIs accept explicit `Value::Uuid(id)` primary keys or SQL `CAST('...' AS UUID)` with:

- **User rows:** RFC-variant UUIDv5 or UUIDv7 only. Nil/max, unsupported versions (including v4), invalid variants, malformed UUID text and non-UUID primary-key Values remain rejected on mutation.
- **Generated user IDs:** unchanged UUIDv7 generation through the existing omitted-key typed insert path. No v5 generation or timestamp fiction added. SQL positional column-count rules are unchanged.
- **Catalog/schema rows:** UUIDv7 only on local mutations and incoming manifest/state/delta addresses. Catalog generation/recreation ordering remains unchanged; forged v5 catalog addresses reject before mutation.
- **Identity:** exact immutable table+UUID bytes; no rewriting, sanitizing or rehashing user identity. Two independently created rows with the same table and UUID merge one logical history. Explicit conflict inspection/resolution and transaction semantics remain available at that exact v5 key.
- **Ordering/security:** v5 carries no creation time and proves neither namespace/name derivation nor authorization/provenance. Owner validates version/variant, not a caller's derivation inputs. Unique-key conflict ownership retains deterministic largest raw UUID ordering, not chronological priority. Both peers/hosts need the updated owner for v5 use; an older v7-only peer can reject rather than silently translate it.
- **Deletion/retry:** tombstones still prevent exact-key resurrection, including stale replay and concurrent update. Reselection before deletion uses UPDATE on the same key. Stable identity does not promise delete-and-reinsert; callers own that domain lifecycle.

The engine's existing validator is now table-aware and shared with sync: `ofdb_sql_engine::validate_row_id(table: &str, id: &Uuid) -> EngineResult<()>`. This is an owner implementation helper, not a consumer dependency requirement. Local query execution, single/multi-row mutation and sync manifest/state/delta address validation use it; the duplicate sync UUIDv7 validator is deleted. Generation routines, wire format/version and apply-time incremental hash validation are unchanged.

## Reconciliation edges exposed by the stable-key TDD fixture

Independent same-key creation revealed two previously unqualified edges needed for the requested semantics:

1. Automerge can resolve identical concurrent `\0engine_columns` layouts using a Delete operation. Strict history inspection rejected this legitimate owner-generated operation after explicit resolution/reopen. A diagnostic red identified exactly `Delete of historical scoped row layout`. The inspector now permits that known user-row layout deletion; every historical layout Put still must match the local fixed schema and consume its entire encoding. Catalog layout deletion remains rejected. A new seventh malformed-history variant puts a layout with hidden trailing bytes and then deletes it: still rejects. No deleted history is pruned or declared sanitized.
2. Reverse retry sync failed `Incremental change hash does not match key`. Temporary diagnostic established **already in snapshot: true**: inventories omitted changes already held inside full snapshots and therefore requested/replayed a known change without a separate delta record. Inventories now advertise **all known history hashes in causal order**, including snapshot-contained history. Snapshot-only IDs can return None from delta export; existing snapshot fallback/dependency recovery handles that. Existing byte budgets include the additional IDs. The apply-change hash guard is untouched; temporary instrumentation was removed. Tests distinguish known history from separately exportable deltas and still enforce matching payload hash, malformed-batch rollback and a forced causal hash-order inversion across frames.

Concurrent assignments can remain conflicts even when decoded values are equal (for example identical id/resource_key values from independent insertions). Existing conflict inspection deliberately remains unchanged. Resolving `selected` alone does not erase unrelated assignments. This fixture proves partial resolution and same-key UPDATE retry, not a new guarantee that all identical concurrent operations disappear. Of must qualify its own fail-closed duplicate/conflict policy on real partition scenarios; no domain conflict rule was weakened to make that pass.

## TDD and focused evidence

Authorized seams: public Client/Database/native sync/conflict operations and existing engine/codec/session boundaries.

RED exact command:

`RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync uuidv5_same_key -- --nocapture`

Failed at the real explicit v5 insert with `QueryError(Rejected, "Invalid query: Primary key must be a UUIDv7")`. Later red/green fixture stages isolated the layout Delete and snapshot-inventory issues above. Temporary assumptions that a partial resolution cleared all unrelated concurrent assignments, and that SQL positional inserts could omit a column, were corrected to the actual existing public contracts; no production behavior was relaxed for those assumptions.

Initial green linking failed with linker signal7/Bus error while the filesystem was100% full (owner target299GB). Direct cache removal was blocked by tool safeguards; `cargo clean -p ofdb-sql` succeeded in the owner worktree only. Logs and other workspaces were preserved. Subsequent commands used `CARGO_INCREMENTAL=0` to limit regenerable build artifacts. One focused compilation hit a300s tool timeout, then a bounded1200s retry completed and supplied the diagnostic red; timeout was not counted as a pass.

Final tests prove:

- Native v5 insert/reopen; independent native/memory partition insertion at the same key; one converged logical row, retained true/false conflict values, explicit partial restrictive resolution, native reopen, incremental transfer and reverse same-key UPDATE retry.
- Whole-resource v5 bootstrap plus scoped delete/update partition race; stale whole-state replay leaves rows absent; exact-key reinsertion rejects; native tombstone reopen/repeat sync.
- Invalid explicit identities/types/text and primary-key mutation reject; omitted-key typed insert still generates RFC v7.
- Exhaustive version/variant matrix: versions0–15 and four variants, accepting only RFC v5/v7 user IDs and RFC v7 catalog IDs; nil/max reject. Sync negative cases include nil, unsupported v4, invalid variant and v5 in all four catalog tables, with target manifest unchanged.
- Prior historical-value disclosure/denial/recovery tests, malformed history (now seven variants), native pending-Abort cleanup, causal ordering, no-op UPDATE and conflict regressions remain active.

| Exact final command                                                                                                                                                                                                       | Outcome                                                                      |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `RUST_MIN_STACK=16777216 CARGO_INCREMENTAL=0 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts --test client --test facade_sync --test cluster_sync --test local_sql` | **49 passed**:12 scoped+5 conflicts+5 client+7 facade+9 cluster+11 local SQL |
| `RUST_MIN_STACK=16777216 CARGO_INCREMENTAL=0 cargo test -p ofdb-sql-engine-automerge --all-targets`                                                                                                                       | **69 passed**:1 unit+49 kernel+6 realtime+3 scoped wire+10 session           |
| `RUST_MIN_STACK=16777216 CARGO_INCREMENTAL=0 cargo test -p ofdb-sql-sync --all-targets --all-features`                                                                                                                    | **32 passed**                                                                |
| `CARGO_INCREMENTAL=0 cargo test -p ofdb-sql-engine --lib`                                                                                                                                                                 | **3 passed**, including identity matrix                                      |
| `RUST_MIN_STACK=16777216 CARGO_INCREMENTAL=0 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts`                                                                       | **10/10 repeats,170 tests passed**                                           |
| `RUST_MIN_STACK=16777216 CARGO_INCREMENTAL=0 cargo test -p ofdb-sql-engine-automerge --test session incremental_session_preserves_dependency_order_not_hash_order`                                                        | **10/10 repeats**, forced actual missing-delta hash inversion                |
| `RUST_MIN_STACK=16777216 CARGO_INCREMENTAL=0 cargo test -p ofdb-sql-engine-automerge --test scoped_session`                                                                                                               | **10/10 repeats,30 tests passed**,21 hostile/late-value wire paths per suite |
| `CARGO_INCREMENTAL=0 cargo check -p ofdb-sql-engine --no-default-features`; same for sql-sync; SQL no-default checks bare, remote, in-memory+sync+sql, redb+native-stage+sql                                              | All passed                                                                   |
| `CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets --all-features`; `cargo fmt --all -- --check`; `git diff --check`                                                                                             | Passed; two inherited KV CLI conversion warnings only                        |

All repeat subprocesses were sequential, each bounded90s, tool bound300s;30 runs/210 test executions passed. Focused rebuild commands used tool1200s bounds. Logs under ignored `target/storage-sync-12-{facade,codec,sync,engine,repeats}.log`. Earlier all-target runs failed only tests assuming inventory meant stored deltas: assertions now include known snapshot history while still asserting two exact independently exportable mutation deltas and their hash integrity. Final full focused runs above are green.

## Bounded broad qualification (incomplete workspace coverage)

Commands ran sequentially with process290s (`timeout --kill-after=5s 290s`) and tool300s bounds:

- `RUST_MIN_STACK=16777216 CARGO_INCREMENTAL=0 timeout --kill-after=5s 290s cargo hack test --feature-powerset --all-targets`: **exit124**, compiling40/581 on KV (`default,in-memory,redb,server,sync`);1–39 completed, no observed compiler/test failure. Log `target/storage-sync-12-powerset.log`. NOT full581 pass.
- `CARGO_INCREMENTAL=0 timeout --kill-after=5s 290s just crap`: **exit124**, compiling coverage target12/28 (KV server CLI); analyzer never ran. Log `target/storage-sync-12-crap.log`. No current complexity/coverage score or enforced-gate result claimed; no previous revision's result reused.
- `RUST_MIN_STACK=16777216 CARGO_INCREMENTAL=0 timeout --kill-after=5s 290s cargo hack test -p ofdb-sql-engine -p ofdb-sql-sync -p ofdb-sql-engine-automerge --feature-powerset --all-targets`: **exit0,17/17 complete** (engine12, Automerge1, sync4). Log `target/storage-sync-12-owner-powerset.log`. This does not substitute for the SQL facade/workspace powerset.

Final before-commit integration merge was Already up to date at1480a09; the exact public scoped/conflict command passed17 tests again after facade rustdoc updates, and format/diff checks passed.

Owner-local12 identity/reconciliation acceptance is demonstrated; full workspace powerset/CRAP and actual of03/of05 selection/authorization qualification remain explicitly incomplete. No other workspace was edited or tested; original dirty design/goal docs remain untouched. All prior owner commits are retained. Integration is merged into the owner before committing/reporting, never the reverse.
