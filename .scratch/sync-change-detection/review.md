# Independent-review fixes

Status: Both confirmed P1 findings fixed; focused validation passed; workspace validation remains blocked

## Scope and assessment

- Worktree `.scratch/worktrees/review-fixes`, branch `sync-change-detection-review-fixes`, clean base `a34c361` on integration `sync-change-detection`. No other worktree was modified. Loaded `tdd` and `ponytail`; read the specification, tickets, glossary, issue conventions and SQL/KV integration evidence.
- Approved public seams: SQL `apply_incremental_changes_for`, engine history/value inspection and committed subscriptions; KV sessions over `SyncTransport`, plus real store/facade queries. No private-method tests or production probe hooks.
- Priority high (data loss and untrusted resource exhaustion); implementation complexity low-to-medium; correctness risk high around dependency retention, atomic validation and peer-controlled allocation. Reused native history and existing frame/error/config patterns, with no dependencies, compatibility glue, arbitrary policy framework or unrelated lint/feature fixes.

## P1: overlapping SQL bundles overwrite retained history

Reproduced independently through public apply: bootstrap S, create dependent A -> B -> C, apply `[A,B]` advertised as B, then `[B,C]` advertised as B. Before the fix, `overlapping_bundles_preserve_full_history_and_current_values` failed at the assertion that exported destination history has no missing dependencies. The second bundle overwrote the physical B record that alone retained A. An initial fixture compile attempt required marking its loaded `AutoCommit` mutable; the subsequent executable failure is the red evidence, not that compiler error.

`AutomergeRowCodec::apply_change` now identifies previously unknown native changes against the existing document, validates the complete candidate exactly as before, then persists each new change under its own native hash. It never replaces a known change's physical record with a differently bundled payload. Bundled incoming payloads and advertised-hash membership remain supported. Complete dependency, decoded-row, stale-definition and full/scoped engine validation remain intact; all writes still occur inside the existing atomic engine transaction.

The regression asserts full native inventory, no missing dependencies, visible current values (`Katherine`, `Paris`), stable replay manifest/history and **no committed invalidation on replay**. The existing multi-change regression also passes; its obsolete physical-bundle assertion wording now describes native history rather than assuming one stored record.

Red/green:

```text
cargo test -p ofdb-sql-engine-automerge --test change_detection overlapping_bundles -- --nocapture
  Before: exit 101, missing-dependency assertion failed after overlapping apply.
cargo test -p ofdb-sql-engine-automerge --test change_detection
  After: all 8 passed, including overlap, malformed/incomplete history,
  metadata, snapshot rewriting, rollback and selective-work counters.
```

## P1: unbounded multi-frame KV inventory retention

Reproduced independently with a finite scripted `SyncTransport` session, valid v3 Hello/Selection, and individually valid inventory frames. Before the receive fix, the aggregate-limit and duplicate-ID tests both returned `Transport(())` after reading past `End`, instead of rejecting the offending inventory. The aggregate test's first red case exercised the entry limit; its byte-limit case is also covered in the completed green run. Config fields were present for the red run, but no receive guards existed yet.

`receive_inventory` now checks cumulative encoded inventory-frame bytes and entry count **before appending a batch**, using saturating arithmetic. A bounded ID set rejects duplicates within or across frames before the batch is retained. Exceeding a budget returns the existing `FrameTooLarge`; duplicates return `InvalidFrame`.

Minimal explicit config additions, per received directional inventory:

- `max_inventory_bytes`: default **64 MiB**, counting encoded inventory frames, including empty inventory frames.
- `max_inventory_entries`: default **65,536** documents.

This bounds peer-derived inventory/maps/key lists in both session directions. One incoming frame can still be decoded before the cumulative check, bounded independently by `max_frame_bytes`/`max_snapshots_per_batch`. Selection is one frame, its union consists of two bounded selections, and recovery retains only one bounded Changes frame until a bounded Snapshot frame arrives; neither has the multi-frame accumulation defect. Locally constructed whole-store discovery is not claimed to have a new allocation cap. Existing config callers already use defaults/struct update syntax, so no caller rewrites or wire-version change were needed. Explicit exhaustive downstream config literals must add the fields or use `..Config::default()`.

Red/green:

```text
cargo test -p ofdb-kv-sync --test inventory_limits -- --nocapture
  Before: 2 failed (aggregate entry limit and duplicate IDs), 1 passed.
  After: all 3 passed, covering byte/entry budgets before End, duplicates
  within/across frames, and 1,024 documents exactly at both aggregate limits.
```

`large_valid_inventory_syncs_values_across_many_frames` additionally transfers **257 actual keys**, with 16 documents per frame and a 257-entry aggregate limit, completing at least 17 inventory frames and checking every destination value. Existing selected nested edits, native dependency recovery, incomplete recovery rejection, complete-batch rollback, tombstone/recreation, no-op replay, restart and facade fan-out tests remain green.

## Validation after fixes

Build commands used command-scoped `CARGO_TARGET_DIR=/home/nathan/code/rust/aicacia/ofdb/target`, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_BUILD_JOBS=2`. No environment/config files or unrelated artifacts were changed. Disk check showed 298 GiB available. Long calls were bounded at 120–300 seconds; final workspace gates at 180/240 seconds. The first combined KV store/sync + facade call reached its 240-second bound during facade compilation **after store/sync passed**; a separate 240-second facade retry passed. No timeout is reported as a test pass.

Passed:

```text
cargo test -p ofdb-sql-engine-automerge -p ofdb-sql-sync --all-targets -- --skip oversized
cargo test -p ofdb-sql-sync --test catalog oversized_data_row_is_rejected_before_apply
cargo test -p ofdb-sql --features sync,sql,redb,in-memory,native-stage --test scoped_sync --test facade_sync --test commit_invalidation --test changes --test sync_stack
cargo test -p ofdb-kv-store -p ofdb-kv-sync --all-targets
cargo test -p ofdb-kv --features sync,in-memory,redb,server --all-targets
cargo clippy -p ofdb-sql-engine-automerge -p ofdb-sql-sync -p ofdb-sql -p ofdb-kv-store -p ofdb-kv-sync -p ofdb-kv --all-targets --features ofdb-sql/sync,ofdb-sql/sql,ofdb-sql/redb,ofdb-sql/in-memory,ofdb-sql/native-stage,ofdb-kv/sync,ofdb-kv/in-memory,ofdb-kv/redb,ofdb-kv/server -- -D warnings -A clippy::needless_question_mark
cargo fmt --all -- --check
git diff --check
```

The SQL facade scoped suite completed **25/25**, including large-payload cancellation/reopen, history/candidate denial, conflicts and restricted scopes. The engine/sync run retains the existing `oversized` filter; the SQL-sync oversized test was run separately and passed. The two previously unfinished engine oversized tests were not rerun here and remain an unverified gate, not inferred defects.

Exact final workspace gates rerun after the fixes:

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Failed (101), same pre-existing `clippy::needless_question_mark` at `crates/btree-redb/src/btree.rs:95`. |
| `cargo hack test --feature-powerset --workspace --all-targets` | Failed (1; underlying Cargo 101), same `ofdb-btree --no-default-features --features sharded` configuration: unavailable std/async_stream/in-memory shard factory and missing ToString. |
| `just crap` | Not repeated, as requested: ticket05's unchanged 600-second instrumented-build timeout still leaves no workspace CRAP score. |

The prior diagnostic-only workspace Clippy allowance run also exposed unrelated KV-server CLI `useless_conversion`; it was not fixed or claimed cleared by the affected-package Clippy pass. Full validation remains blocked independently of these two repaired findings.

Integration merge `GIT_EDITOR=true git merge --no-edit sync-change-detection` returned **Already up to date**, integration tip `a34c361`. Only these review fixes/tests and their documentation/evidence are included in the review-fixes commit.
