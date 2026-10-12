# Qualified native catalog integration — instrumentation retired, main unmerged

2026-10-12. User authorized a dedicated original-root integration branch, removal of temporary native profiling only, scoped issue16 assessment and final focused owner checks. No main merge, push, provider/target cleanup, cross-repository writes or broader spec closure.

## Exact refs / source / preservation

- Original root was clean on `diagnostics/native-idp-stage-timing`, **3ec105590794331d62f67f33ccc4a7f56e623351**. Created **`integration/native-catalog-reuse`** directly from that exact tip and left the diagnostic ref unchanged. No reset/rebase/cherry-pick/drop of historical source/evidence.
- Main remains **8a848b2749dafb073d1491c7969c079e6be5bae5**. Integration is prepared locally, not merged into main.
- Stable cleaned implementation tested: **72aae34b75f0fd65f4feadfb49e13ae948b41f6c**, cleanup commit parent3ec1055. Later integration documentation commit changes only receipts/map/issue16, not runtime source.
- Accepted correction source retained: **cce094c** per-scan current layout, **89e85d3** same-operation raw field handoff, **46fe260** selected table-ID reuse, **2bbb158** raw table handoff preserving codec winner/parser rules, **defd321** nonjoin SELECT raw-catalog handoff/all-row decoding/semantic coverage.
- User/consumer receipts report independent approval of these corrections, including final defd321 and semantic/sync-target coverage. This session does not claim a new independently performed cleanup review. New timer removal was inspected and verified with focused checks below; final main integration review remains separately authorized.
- Existing tests, source history and every historical raw log/argv/status/failed attempt/receipt are retained on both diagnostic ancestry and qualified integration. The diagnostic branch still contains timer source/dependencies. No ticket08/spec/consumer source edited here.

## Cleanup precisely bounded

Removed native admission/begin/body/completion timers and timing fields; schema/conflict stage scopes/counters; codec catalog/document timing aggregation; private sql-engine stage_timing module and its declaration. Removed only the three direct log declarations and three package dependency-list edges from owner Cargo.lock. No package/version removed: log already exists elsewhere transitively.

`Cargo.lock`, all three affected Cargo.toml files and `crates/btree-redb/src/database.rs` now match main **exactly**. Native admission guard/begin_write/commit/abort semantics therefore match pre-instrumentation main; no admission/check/deadline shortcut. Codec conflict selection still uses the same raw snapshot and fallback helper, now directly without timing-result plumbing. All measured corrections, largestUUID/malformed/default/tombstone/error rules and decoding every SELECT row before predicates remain.

Production search for `of::idp_stage_timing|StageTiming|CatalogTiming|stage_timing|elapsed_us` across affected btree-redb/sql-engine/sql-engine-automerge src trees finds no matches. Performance regression tests retain their ordinary Instant/elapsed assertions; they are correctness/performance tests requested to preserve, not production instrumentation. Historical receipts contain original timer descriptions/commands, deliberately not erased.

CRP: cleanup low complexity, medium risk from accidental semantic deletion/graph/source provenance; priority finish qualified generic delivery without observability code. Same-operation/native snapshot invariant and low-level RowCodec migration remain explicit. No cache across operations, index framework, permission/conflict weakening, persistence/provider change or bounds increase.

## External consumer qualification / review provenance

Read accepted consumer receipt commit **a361acaca8bc3e85f24ffa81790547a7ae1117ed**, [08-reviewed-select-correction-rerun.md](../../../../of/.worktrees/ticket08-full-nodes/.scratch/offline-first-nodes/evidence/08-reviewed-select-correction-rerun.md).

Actual passing run source: consumer **be22adc86e1b1a375f65b0d544fde08f54a434ed**, original owner **3ec105590794331d62f67f33ccc4a7f56e623351**. Four serial unchanged-feature/no-stage-logger tests, no source change or retry in the sequence:

| Run | Exact existing slice | Test result |
| --- | --- | --- |
|30|restrictions/retained sockets|1pass/0fail,265.88s,exit0|
|31|offline join/partition/reopen|1pass/0fail,223.51s,exit0|
|32|grants/consumption/revocation|1pass/0fail,247.39s,exit0|
|33|one explicit restrictions repeat|1pass/0fail,267.68s,exit0|

Both disconnected journeys, authenticated reconciliation, learned grant denials, retained-operation denial/recovery/readback/cleanup and ordinary pair teardown reached according to exact existing assertions. Maximum completed server middleware HTTP interval6.988472556s (offline filesystem201); not client slack or a universal SLA. Prior genuine restrictions26 failure at earlier e15424f remains retained and is not relabeled or matrix-deferred. Four improved-margin passes qualify this representative machine-local correction, not all loads/platforms/providers or guaranteed10s behavior.

**Cleanup provenance limit:** those composed passes precede timer retirement. This session runs only the requested focused owner gates at cleanup72aae34; no fresh composed consumer result on cleaned owner code is claimed. Original dependency links still resolve original owner root.

Read-only current consumer observation: clean **d9e40eb4e903f8e073e3a8c35fd879b6e1ad3806**, `realpath ../ofdb/crates/sql` resolves `/home/nathan/code/rust/aicacia/ofdb/crates/sql`; its Cargo.lock already has no log edge on ofdb-btree-redb/sql-engine/sql-engine-automerge. The owner agent did not edit that lockfile or infer this later checkout is the earlier passing run source. Parent can inspect its cleanup before final consumer validation.

## Actual final focused checks — all cleanup source72aae34

All Rust commands: exclusive user slot, serial systemd **MemoryMax4G/MemorySwapMax0/RuntimeMaxSec900**, terminal910000ms, offline/locked/jobs1/shared `/home/nathan/code/rust/aicacia/of/target`, standard optimized+debuginfo profiles, RUST_MIN_STACK67108864, unset RUSTFLAGS/CARGO_ENCODED_RUSTFLAGS. No cap/profile/target change, Cargo Hack or CRAP. No resource/compile/test failure in this cleanup sequence. Reported service256–512K peaks are implausible, not actual RSS/headroom evidence; swap0B accounting does not prove host-wide zero swap.

| Raw basename | Exact selection / actual outcome |
| --- | --- |
|16-qualified-native01|ofdb-sql features redb,sql,sync, tests scan_layout/conflicts: compile1m07s; **3 conflicts passed0.39s +6 scan_layout passed46.74s**,exit0|
|16-qualified-semantic01|ofdb-sql-engine-automerge test catalog_snapshot: compile23.21s; **3passed/0failed0.01s**,exit0|
|16-qualified-sync-catalog01|ofdb-sql-sync test catalog (actual modified TestCodec support compiled): compile48.27s; **18passed/0failed2.95s**,exit0|
|16-qualified-nostd01|engine/sync no-default-feature libs check:2.50s,exit0|
|16-qualified-clippy01|btree-redb/engine/codec/sync libraries, strict-Dwarnings:5.15s,exit0|
|16-qualified-facade-clippy01|selected native facade scan_layout/conflicts targets, strict-Dwarnings:5.57s,exit0|
|16-qualified-semantic-clippy01|catalog_snapshot target, strict-Dwarnings:4.03s,exit0|
|16-qualified-sync-clippy01|sync catalog target including support, strict-Dwarnings:12.80s,exit0|
|16-qualified-format|all11 changed/affected Rust files, rustfmt --check edition2024 skip_children=true:exit0|

**30 tests passed total**. Cleanup performance samples:512 repeated predicate SELECTs8.042253061s;512 raw-table conflict checks15.043351993s;512 field-heavy checks7.300528844s;512 many-table checks8.615215512s;512-row SELECT42.020454ms. All original test budgets unchanged; not extrapolated to a consumer deadline guarantee.

Exact literal commands: [16-qualified-commands.txt](16-qualified-commands.txt) and individual `*-command.json`. Complete merged raw output plus wrapper `.status` retained. [16-qualified-integrity.txt](16-qualified-integrity.txt) records SHA256 and all0 statuses, checked against files. Python subprocess capture mirrors full output and propagates systemd-run exit status, no pipeline masking. Changed-file format, git whitespace checks and cleanup commit rustfmt hook passed. No full workspace/feature/powerset/deployment qualification inferred.

## Exact main delta / tracker disposition

Relative to main8a848b2, runtime/test Rust delta is **10 files,735insertions/50deletions**:

Production (6):
- crates/sql-engine-automerge/src/codec.rs
- crates/sql-engine/src/codec.rs
- crates/sql-engine/src/executor.rs
- crates/sql-engine/src/index.rs
- crates/sql-engine/src/lib.rs
- crates/sql-engine/src/schema.rs

Tests (4):
- crates/sql-engine-automerge/tests/catalog_snapshot.rs
- crates/sql-engine-automerge/tests/kernel.rs
- crates/sql-sync/tests/support/mod.rs
- crates/sql/tests/scan_layout.rs

Everything else in the final main delta is owner Markdown/map/issue16 or preserved/generated evidence (raw logs, exact argv/status/command/hash manifests). **No Cargo.lock/Cargo.toml/B-tree implementation/timing-module delta versus main.** Full committed path/status inventory is [16-qualified-main-delta.txt](16-qualified-main-delta.txt); it includes itself and all retained evidence, not a hidden runtime change.

Issue16 now retains canonical triage `Status: ready-for-agent` and sets **Work status: resolved**, with scoped Answer and map row. The measured generic question was answered, reviewed same-operation corrections have test-first/public/semantic/consumer evidence, and temporary observability is retired. Historical OOM/timeouts and source limitations remain. This is not owner full-spec, consumer08, deployment/global-single-use or arbitrary mutable-kernel closure.

## Remaining action / caveats

Low-level external RowCodec implementations/callers must supply new catalog inputs/optional scan catalog; public facade is unchanged. Native/in-memory stable snapshots tested, not arbitrary interior-mutable custom kernels/all malformed storage failures. Consumer four-run proof is inherited at instrumented-but-unlogged3ec1055; cleanup72aae34 is separately owner-qualified. Parent final review of **3ec1055..72aae34** timer-only cleanup plus complete **main8a848b2..integration/native-catalog-reuse** correction/test/docs delta should precede an explicitly authorized main merge. No fresh independent cleanup-review claim, consumer run, merge or push here. Preserve diagnostic3ec1055 while integration review remains pending. Build slot released with final report.
