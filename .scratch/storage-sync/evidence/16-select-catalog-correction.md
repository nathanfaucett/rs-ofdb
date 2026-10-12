# SELECT operation-local raw catalog handoff — RED/GREEN and reviewer gaps

2026-10-12. Owner-only correction. No consumer build/edit/rerun, main merge, push, ticket/spec closure or reliability guarantee. Source/test commit **defd3213a0a0c4293199994064f628d9227ef9b7**, parent **e15424f3d29c0ee50ae31f84396fb4541d3465dc** on diagnostics/native-idp-stage-timing. Receipts/raw output committed separately. Preserved and completed the partial untracked `catalog_snapshot.rs`; it was not discarded.

## Existing local reliability issue remains real

Read consumer be22adc receipt `08-reviewed-raw-table-correction-rerun.md`; its actual run source was consumer377066b/ownere15424f. Offline25 PASS286.57s, grants27 PASS293.42s and restrictions29 PASS346.52s do **not erase restrictions26 FAIL193.73s at the same source**. B filesystem create still timed out10s in26; server401 after panic/cancellation does not establish a pre-timeout permission rejection or no effect. Successful B creates9.684547s/offline25 and9.342915s/diagnostic28 leave small margins. This is a genuine local reliability blocker, not matrix/deployment deferral.

Existing diagnostic28 whole enabled interval:32324 catalog reconstruction records133.129482s;5976 conflict-schema records84.526540s;5795 conflict-column-discovery records0.302142s;9874 admissions86.515307s/max5.492437s. These include two hosts, background work/reconciliation/shutdown and overlap; not exclusive B-create CPU/time or identified admission holders. Conflict codec duplicate catalogs are already removed. `resolve_row` does repeat get/encode/apply work, but successful clear-row authorization uses `row_conflicts`; no measurement identifies resolution/encoding as the failing request's cause. Separate authorization operations must still read current state; no cross-operation reuse is justified.

## Supported smallest additional repetition

Current non-join `select_rows` validates/materializes schema, then scan_rows independently rediscovers the same current table/field catalogs to decode rows. Accepted cce094c already shares layout *within* one scan; this patch shares complete raw catalogs from schema discovery with that scan in the same native operation. Public tests independently demonstrate that particular duplicate cost; no request-level attribution or guaranteed fix of26 is claimed.

CRP: low-to-medium complexity from one optional borrowed catalog input and existing helpers; medium semantic/interface risk at snapshot provenance, malformed/competing definitions and error order; high priority for measured duplicate reconstruction. No new module framework/index/cache/dependency/clock/permission/deadline changes.

- Non-join SELECT captures every live raw table entry via existing `lookup_table_id_with_rows`, and every live raw field entry via existing `table_schema_with_fields`, retaining all engine validation before projection/order/data processing.
- `RowCatalog` names the same borrowed raw table/field tuple already used privately by codec columns. It is defined in `codec.rs` and re-exported from thin lib.rs. This is ephemeral operation data, not a stored cache or normalized schema shortcut.
- `RowCodec::scan_rows` now accepts `Option<RowCatalog>`; the SELECT scan receives Some. All other callers/adapters were mechanically updated with None, retaining their current reconstruction behavior. Joins, index rebuilds, matching mutation scans, point get_row/encode and conflict values are not generalized by this correction.
- Codec scan still lazily discovers layout on the first live document, skips tombstones as before, reconstructs/decodes **every live row before predicate filtering**, and retains largestUUID/name/field/default/stored-layout fallback logic by consuming raw entries. It does not substitute normalized ColumnSchemas, fetch only a matching primary key, push down limits or omit malformed/nonmatching data.
- The redundant post-schema table-name lookup is replaced by the same owned table-name string allocation without a second read; initial full table validation already occurred. Validation/data/error ordering is preserved for a stable native snapshot. Physical failures exclusive to a removed duplicate read cannot be replayed, and arbitrary interior-mutable custom kernels changing state during an operation remain unqualified.
- No public SQL facade change. Required low-level interface migration: external RowCodec scan_rows implementations/callers must accept the extra optional same-operation catalog; None means reconstruct current layout as before. No default/compatibility wrapper or old-version adapter.

Production/mechanical diff is small:8 existing files54insertions/17deletions including30 public performance-test lines. The separate preserved semantic test file adds263 lines across3 real public-interface fixtures, no mocks. Source commit9 files317insertions/17deletions; no manifest/lock/provider/consumer changes or added diagnostic instrumentation.

## Test-first results and retained failure

User-confirmed public seam: persistent native Query Handle SELECT/row_conflicts; raw codec/catalog and sync support seams explicitly authorized. New SELECT regression performs512 real primary-key-predicate queries over one row with200 extra catalog fields and verifies exact `[1]` every time. Fixed12s machine-local budget set before RED and never increased; all existing owner regression and consumer HTTP10s/pair360s/probe120s limits unchanged. This is not a platform SLA.

| Execution / raw basename | Exact focused selection / actual result |
| --- | --- |
| `16-select-catalog-red01` | Baseline e15424f plus new SELECT test, no production patch. ofdb-sql features redb,sql,sync, test scan_layout, exact `native_select_reuses_current_catalog_within_fixed_budget`. Compile18.42s; **0passed/1failed/5filtered,14.97s,exit101**.512 SELECTs14.858945230s fail12s. |
| `16-select-catalog-green01` | Compile FAILED E0505 before test launch: scan borrowed select.from.table while later result construction moved it. Retained status101, not runtime evidence. Fixed with the same independent owned table string allocation the old lookup produced, no extra catalog read or semantics change. |
| `16-select-catalog-green02` | ofdb-sql features redb,sql,sync, `--test scan_layout --test conflicts`, nocapture/test-threads1. Compile54.67s; **3 conflicts passed0.37s +6 scan_layout passed48.86s**,exit0. New512 SELECTs **8.502561848s** (~42.8% decrease); existing SELECT512-row scan40.487886ms. All prior budgets retained. |
| `16-catalog-semantic01/02` | Initial partial semantic fixture development at baseline;1 then2 tests passed,7.79s/4.29s compile. Characterization of reviewed code, not a fabricated semantic RED/GREEN cycle. |
| `16-catalog-semantic03` | Final completed fixture, ofdb-sql-engine-automerge test catalog_snapshot. Compile18.53s; **3passed/0failed,0.01s**,exit0. |
| `16-sync-support01` | Baseline support target verified: ofdb-sql-sync `--test catalog manifest_matches_exported_state_digests -- --exact`. Compile1m55s; **1passed/17filtered**,exit0. |
| `16-sync-support02` | Same focused target recompiled against new scan_rows interface,37.20s; **1passed/17filtered**,exit0. This compiles the actual `catalog.rs -> mod support -> TestCodec` harness, not merely its library. |
| `16-select-catalog-nostd01` | Engine/sync no-default-feature libs check,1.75s,exit0. |
| `16-select-catalog-clippy01` | Strict engine/codec/sync libraries,2.70s,exit0. |
| `16-catalog-semantic-clippy01` | Strict new semantic test target,25.13s,exit0. |
| `16-select-catalog-facade-clippy01` | Strict selected facade scan_layout/conflicts targets,4.36s,exit0. |

Final production patch executed in green02, semantic03 and sync-support02; subsequent strict checks/formatting introduced no semantic production changes before source commit. No fictitious RED commit; baseline/new test and working patch were executed before later commit.

### Semantic coverage (public results, not private counters)

1. Actual concurrent Automerge changes produce column1 conflict. Empty current catalog preserves stored positional fallback. Full raw table fixtures are persisted, include a malformed unrelated integer name, and deliberately list higher matching UUID before lower; codec still skips its non-text unrelated entry and selects largest UUID, not iteration order. The loser column label would hide the conflict if chosen. Current winner adds literal `fresh-default`; get_row and supplied-catalog scan produce it. Tombstoning the malformed definition permits stricter Engine row_conflicts afterward and returns literal column name1.
2. Engine sees a persisted malformed unrelated table and invalid key simultaneously; returns literal `Invalid table name`, proving full catalog validation before key handling.
3. Public raw change store deliberately injects a non-byte cell in a different/nonmatching document. Public predicate SELECT still raises literal `Logical row column is not encoded bytes` rather than returning only the matching good row. This guards against suppressing decoded-row errors via a primary-key shortcut.

Semantic fixtures use the real in-memory kernel/public codec, Engine and raw change-store interfaces; they do not claim new durable/native-provider qualification. Existing persistent native public tests cover defaults/DDL changes within one explicit transaction, reopen/drop-recreate, tombstones, conflict values/resolution and rollback. No arbitrary mutable-kernel/whole-corruption matrix claimed.

## Bounds and exact artifact provenance

All launches serial by this agent under exclusive slot: systemd **4G/swap0/runtime900s**, terminal910000ms, offline/locked/jobs1/original shared of/target, standard optimized+debuginfo profiles, RUST_MIN_STACK67108864, unset RUSTFLAGS/CARGO_ENCODED_RUSTFLAGS. Preflight14–15GiB available across resumed partial task, host swap421–422MiB used; no other detected cargo/rustc at preflight/follow-up. No resource-cap increase, cache/target cleanup, provider changes or concurrent agent build. Reported256–512K service memory peaks are implausible, not actual RSS/headroom evidence.

All12 literal invocations: [16-select-catalog-commands.txt](16-select-catalog-commands.txt), plus original individual `*-command.json`. Full raw `.log`/wrapper `.status` preserved for semantic01–03, sync-support01–02, SELECT RED/failed compile/GREEN/no_std/library Clippy/facade Clippy and semantic Clippy. [Integrity manifest](16-select-catalog-integrity.txt) records SHA256/status; failures remain101 and all actual passes0. Wrapper captures merged stdout/stderr, mirrors output live and exits with systemd-run status, no pipeline masking.

Changed-file rustfmt/whitespace and source commit rustfmt hook passed. No consumer or whole-owner powerset/CRAP/workspace sweep; this is focused evidence, not final full-spec gates.

## Exact next action / unresolved reliability

Independent source review of **defd321 versus e15424f**, especially complete same-operation raw catalog provenance, scan lifetime/low-level migration, strict malformed/default/winner behavior and continued all-row decoding. After approval, parent verifies original owner dependency root/revision and reruns the **unchanged restrictions filter that genuinely failed26**, followed by relevant unchanged offline/grants filters at the same source, retaining10s/360s/4G900 bounds and any failures. Diagnostic-feature pass/retry does not erase an unchanged-feature failure; sufficient margin/repeatability remains an actual local qualification decision, not matrix deferral.

No consumer reliability claim is made from the owner benchmark. If failures remain, correlate request/store-specific required fresh schema operations/admission and current-owner authorization rather than caching checks or inflating deadlines. No justified resolution/get_row/encode shortcut was selected from the present timings, and no further broad framework/owner rewrite started. Source/semantic/support gaps addressed; build slot released with final report.
