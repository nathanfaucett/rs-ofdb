# Raw same-operation table snapshot for codec conflict layout — RED/GREEN

2026-10-12. Owner-only narrow correction. No consumer edit/build/rerun, main merge, ticket/spec closure or independent source-review claim.

## Source / existing evidence

Initial owner clean `45b6ea759e268e74cc5611a8cfbfa1e9ca7949a2`, diagnostics/native-idp-stage-timing; latest accepted table identity correction46fe260 and earlier approved corrections retained. Final source/test commit **2bbb158bc1f2196bd62a06be7250f46d05dd170e**, parent45b6ea7;7 files90insertions/14deletions. Source/test commit separate from diagnostic receipts; no added logger/timer/dependency/manifest/profile/provider changes.

Read consumer clean **377066b4452487e76a2e00208d51757cc6f8d3fc**, receipt08-reviewed-table-correction-rerun.md and raw24. Verified raw SHA256 `9f7d3f23ed663f1a4127de6df9582a28b72cfa1446f224b849a72ac3f4d39186`. Recomputed final-create interval:1369 catalog reconstruction records7.309823s,531 engine table-catalog lookups0.822186s,365 field-schema catalogs7.457629s,198 conflict-schema records4.243780s,198 codec records0.351661s,189 conflict-column-discovery records0.305880s;330 admissions4.208823s/max1.632112s. All nested/interleaved/background/failure wall time, not exclusive B CPU/request cost.

Inherited actual progress: restrictions21 PASS345.39s/grants23 PASS294.10s at821be10/45b6ea7; offline22/24 still fail B filesystem POST10s. Raw24 initial authorization7.839503s, mutation0.011995s, response introspection2.518443s already10.369941s before response local policy. Initial token signer/trust/private-key bucket1.997057s includes waits, not isolated crypto/native work. Current cleanup-admission timers1–4us do not reproduce run19's0.818s wait. No cause transferred from previous run or no-effect claim from timeout.

## Minimal supported correction and correctness boundary

CRP: low-to-medium complexity from extending existing raw snapshot handoff; medium semantic/API risk at full-catalog provenance, winner selection, malformed filtering and snapshot lifetime; high priority to remove measured duplicate native reconstruction. No index framework/cache layer or authorization shortcut.

- Engine initial table lookup still scans/validates every live table row before key validation, selects its largest matching UUID, and reports the same logical lookup/malformed-row errors. A shared reader optionally observes each live raw `(UUID, Row)` entry. Ordinary engine lookups use no-op observation and do not retain an extra vector.
- `row_conflicts` and `resolve_row` retain that full live raw table snapshot, alongside their existing raw field snapshot, only for the current operation.
- Codec `columns` receives those raw entries instead of reconstructing the table catalog a second time. **The codec's existing largest-UUID fold and parsing rules remain unchanged**: it checks first-value `as_text`, selects the largest matching identity, filters fields by selected UUID, preserves position/name ordering/defaults and falls back only under the existing empty-layout rules. This does not substitute engine normalized columns or its selected UUID for codec selection. Engine stricter validation still completes before document inspection. Raw entries preserve other tables and values needed by codec rules.
- Tombstoned table rows are skipped before observation, as both old engine/codec paths skipped them. Same-operation fields and table IDs/rows share the current native transaction snapshot. No catalog snapshot retained on the codec/database/client or across operations. Catalog changes in a later operation remain visible, including within one explicit transaction.
- Existing point get_row/document_columns, value conflict inspection/candidates, non-conflict scan/mutation paths still call columns with no supplied snapshot and retain their old catalog behavior. Built-in fixed catalog layouts still return before consuming the snapshot. Direct built-in tests supply empty raw table/field slices.
- Low-level required API update: `RowCodec::conflicted_columns` additionally takes `table_rows: &[(Uuid, Row)]`; caller supplies every live table entry plus the existing live field rows from the same operation. All repository implementations/callers updated; custom external implementations/callers must adapt. Public SQL facade unchanged. No default/compatibility adapter.
- Source-preserved logical malformed/default/fallback rules are not a new exhaustive malformed-store or arbitrary interior-mutable-kernel qualification. Native snapshots tested; a removed second physical read cannot reproduce a failure exclusive to that duplicate I/O. Independent review should check full snapshot provenance and custom-kernel contract.

## Test-first focused execution

Public approved seam: persistent native Query Handle row_conflicts/SELECT. New fixture256 UUID-only tables,512 public inspections on a real target row, clear-result assertion on every call. Fixed18s machine-local budget chosen before RED and never raised; existing15s/12s/2s regression budgets and consumer10s/360s/120s bounds unchanged. No cross-platform SLA.

| Run | Actual result |
| --- | --- |
| red01 | Baseline45b6ea7 plus newly authored regression, no owner correction yet. Compile8.13s; **0pass/1fail/4filtered,25.59s,exit101**.512 calls20.771092078s exceed18s. |
| green01 | Snapshot correction. Compile54.63s; **3 conflicts passed0.39s +5 scan_layout passed37.49s**,exit0. New sample15.331331586s; existing128-table sample7.568310204s, large-field conflict sample7.213252050s, SELECT54.469775ms. |
| boundary01 | `ofdb-sql-engine-automerge --test kernel conflict`: compile50.09s, **3pass/0fail/47filtered,0.34s**,exit0. Internal catalog conflict/resolution, reordered compatible schema conflict/resolution, replicated unique winner/deleted winner. |
| nostd01 | Engine/sync no-default-feature libs,1.69s,exit0. |
| clippy01 | Strict library Clippy failed type-complexity for private tuple parameter,exit101. Retained; no lint suppression. |
| clippy02 | Same strict engine/codec/sync libraries,4.81s,exit0 after private `CatalogRows` type alias. Alias only changes spelling, not representation/behavior. |
| facade-clippy01 | Strict selected public scan_layout/conflicts test targets,4.31s,exit0 on final alias source. |

Public cases cover conflicts/values/resolution, missing/invalid keys/tables, aborted snapshots/rollback, current defaults/catalog evolution in one explicit transaction, tombstones, reopen/drop-recreate and convergence. Boundary tests cover actual competing/reordered schemas and built-in internal conflict behavior. No mock/private timer-count assertion. Public/kernel runtime tests executed before the final private alias; final library/test Clippy compiled the alias form, with no fabricated post-alias runtime execution.

Every invocation launched serially under user-exclusive slot, **systemd4G/swap0/runtime900s**, terminal910000ms, offline/locked/jobs1/original shared of/target, standard profiles, RUST_MIN_STACK67108864 and unset RUSTFLAGS/CARGO_ENCODED_RUSTFLAGS. Preflight available17GiB, host swap406MiB used; no detected cargo/rustc preflight/follow-up. Clippy02 briefly awaited a target lock, holder unverified, so independently proven whole-session exclusivity is not claimed. Reported service peaks256K are implausible, not actual RSS/headroom evidence. No OOM/resource failure/retry/cap increase.

Complete exact commands: [16-raw-table-commands.txt](16-raw-table-commands.txt), plus individual `16-raw-table-<run>-command.json`. Complete merged raw `.log` and `.status` files for all7 runs are preserved, including RED/Clippy failure. [16-raw-table-integrity.txt](16-raw-table-integrity.txt) records raw SHA256/status. Python wrapper mirrors full output and exits with systemd-run status, no pipeline masking. Changed-file rustfmt/whitespace and source commit rustfmt hook passed. No powerset/CRAP/workspace/consumer sweep.

## Point reads / primary-key SELECT: examined, not blindly optimized

Current `sql-engine/src/executor.rs::select_rows` obtains schema, resolves the table, scans/decodes all rows, then applies predicate. Current codec `get_row -> document_columns -> columns` independently discovers current catalog; scan_rows already reuses one layout per scan from cce094c. SQL primary-key equality has no demonstrated direct-fetch path here.

Replacing SELECT scan with get_row would stop decoding nonmatching rows and could suppress malformed-row/catalog errors. Reusing schema's normalized columns would also lose codec's different field selection/fallback rules. Raw24 has catalog/body buckets but no query shape, table/request correlation or per-SELECT row count proving a particular primary-key scan dominates. No primary-key/index shortcut, broader RowCodec query framework, or point-read schema shortcut is justified by these logs alone.

## Interpretation / next action

This same-host public sample improved20.771s ->15.331s (~26.2%), proving the particular duplicate-table reconstruction reduction. It is **not a predicted offline pass**: raw24's conflict-codec window was only0.351661s while required engine field-schema work was4.243780s; aggregates cannot be assigned exclusively to B. Other current schema work/admission still occurs on separate authorization operations and cannot be cached across them.

Independent review of **2bbb158 versus45b6ea7**: full live raw table capture, codec's unchanged largestUUID/malformed/fallback logic, snapshot lifetime and low-level API migration. Then parent may rerun existing exact offline/B-create diagnostic against verified owner source and retain10s. If remaining costs require SELECT/point-read changes, obtain query-specific evidence and regressions for nonmatching malformed rows and competing schemas before extending interfaces. No further generic rewrite or new consumer run initiated here; no source-level permission/deadline workaround. Build slot released with final report.
