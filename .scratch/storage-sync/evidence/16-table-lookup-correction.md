# Same-operation table identity reuse — residual catalog correction

2026-10-12. Narrow owner source/test work only. No consumer build/edit, main merge, ticket/spec closure or independent-review claim. Exact source/test commit **46fe26028d81b4f188131560f56d72dddcd154da**, parent **1ae1ba26d6610e547013c777ccb21a5515e1ae31**, on diagnostics/native-idp-stage-timing. Evidence committed separately.

## Read-only consumer evidence

Read clean consumer821be10 receipt `08-reviewed-field-correction-rerun.md` and verified raw19 SHA256 `3f61152f3c2352e78946ed51f3c758d720c0cf37e39cff5bc9adb912f59c94a1`. Recomputed its interval from the last filesystem POST marker:1933 catalog reconstructions7.562160s,198 conflict-schema records4.643144s,198 codec records0.336346s,365 field-schema catalog records7.141134s,1095 table-catalog lookups1.578123s,331 admissions2.162318s/max1.018761s. Nested/interleaved/background/failure work, not exclusive request CPU.

Inherited actual consumer result: grants17 PASS305.44s at1ae1ba2; restrictions now get past B restriction PUT204 but fail B filesystem POST10s. Run19 initial authorization6.707333s plus mutation0.009848s plus response introspection3.634124s already10.351305s before response local-owner completion. Initial local policy1.627140s includes cleanup admission0.817840s. Filesystem mutation completion is not POST201/durable acceptance. Generic owner work is part of remaining successful authorization cost; consumer cleanup serialization is a separate measured contributor. No new causal attribution or consumer pass follows from this owner test.

## Supported minimal correction / scope

CRP: low complexity (private table-ID handoff and duplicate-read deletion), medium snapshot/error-ordering risk, high priority for measured repeated generic validation work. No new cache/index layer/API adapter/dependency/profile/deadline.

- `schema::table_schema` previously performed `lookup_table_name`, then `table_schema_for -> columns -> read_columns -> lookup_table_id` repeated the identical complete table-catalog scan in one operation. Remove the first call; `columns` still fully resolves/validates the table before any field processing.
- `row_conflicts` still validates the entire table catalog **before** key validation. It retains the selected UUID instead of discarding it, passing that UUID to the private `table_schema_with_fields/read_columns` path. `resolve_row` does the same after its existing empty-assignment validation, retaining table-before-key ordering. This removes two redundant engine table scans in these paths.
- Codec's independent table lookup, all raw field-catalog reading/validation/filtering/ordering/defaults, conflict/document/fallback checks, prior operation-local raw fields handoff and prior per-scan layout correction remain unchanged. No new RowCodec/public facade signature change.
- All reuse is a local value derived from the current operation's native transaction snapshot. Subsequent operations still read current catalog, including DDL inside one explicit transaction. No reuse across trust/authorization calls or transactions.
- Logical missing-table/malformed-table validation still uses the same complete lookup before field processing. A removed second physical scan cannot reproduce a transient error exclusive to that duplicate read; arbitrary interior-mutable kernels changing the catalog within the operation remain a source-review limitation, not tested native behavior.

Source files: `crates/sql-engine/src/schema.rs`, `crates/sql-engine/src/executor.rs`; public regression added in `crates/sql/tests/scan_layout.rs`.3 files51insertions/11deletions, including36 test lines. Existing diagnostic labels/timers unchanged; source commit separable from diagnostics/evidence.

## Actual test-first results and retained failures

Public agreed seam: persistent native Query Handle row_conflicts/SELECT. New fixture128 independent UUID-only tables,512 conflict inspections on an existing target row; each must return an actual clear result. Fixed15s machine-local budget was set before RED and never raised. Earlier12s/2s regression budgets and all consumer10s/360s bounds unchanged. This15s regression threshold is not a cross-platform SLA.

| Run | Actual result |
| --- | --- |
| red01 | Baseline1ae1ba2 plus new test, before source changes. Compile6.58s; **0pass/1fail/3filtered,18.25s,exit101**.512 calls16.845904219s, fails15s. |
| green01 (FAILED attempt) | Only preliminary duplicate-call deletion.512 calls15.270589408s; **0pass/1fail/3filtered,16.91s,exit101**. Failure retained; no threshold increase. |
| green02 / correctness | Complete initial-ID handoff plus duplicate-call deletion. Compile53.25s; **3 conflicts passed0.42s +4 scan_layout passed21.27s**,exit0. New512-call sample10.867073807s; existing large-field conflict7.868953222s, SELECT57.110248ms. |
| nostd01 (FAILED) | New resolver string used `.to_string()` without alloc trait in no_std; E0599,exit101. Fixed with already-imported `String::from(table_name)`, no added dependency/import. |
| nostd02 | Engine/sync no-default-features libs check,1.04s,exit0. |
| clippy01 | Engine/sync libs, strict-Dwarnings,5.76s,exit0. |
| facade-clippy01 | Selected public scan_layout/conflicts tests with native/sql/sync features, strict-Dwarnings,2.95s,exit0. |

Public correctness covers catalog changes/defaults within one explicit transaction, tombstones, commit/reopen/drop-recreate, missing/invalid table/key, aborted transactions, conflict values and resolution, convergence and rollback. green02 executed before the final no_std string-constructor spelling change; final source after `String::from` compiled in nostd02 and strict Clippy (including selected test binaries). Constructors produce the same owned string; no further public execution is fabricated for that final spelling change.

All invocations were launched serially by this agent under user-exclusive slot, systemd4G/swap0/runtime900s/terminal910000ms, offline/locked/jobs1, standard profiles/shared `of/target`, RUST_MIN_STACK67108864, unset RUSTFLAGS. Preflight16GiB available, host swap434MiB used; no detected cargo/rustc at preflight/follow-up. Both green logs show transient build-directory lock waits; holder unknown, so no independently proven whole-session exclusivity. Collected memory peaks256–512K are implausible, not RSS/headroom evidence. No OOM/resource failure claimed.

Exact complete commands: [16-table-lookup-commands.txt](16-table-lookup-commands.txt). Each run also retains machine-readable `16-table-lookup-<run>-command.json`, full merged raw `.log`, wrapper `.status`. [Integrity manifest](16-table-lookup-integrity.txt) records SHA256/status, including both failed test attempts and failed/fixed no_std boundary. Wrapper mirrors full output and returns systemd-run status without pipeline masking. Formatting/whitespace and source-commit rustfmt hook passed. No full powerset/CRAP/workspace sweep in this narrowly scoped investigation.

## Interpretation and next action

The new same-host sample improved16.846s ->10.867s (~35.5%), consistent with removal of repeated table reconstruction. It demonstrates the specific owner redundancy, **not that B filesystem creation now meets10s**. The run19 aggregation cannot assign a saved lookup cost exclusively to this request. Required live field-schema/conflict validation still happens each operation; sharing those results across independent authorizations would violate the no-cross-operation constraint. Consumer response authorization/current-owner cleanup still must complete; no permission, cleanup or post-mutation check may be skipped.

Independent review of46fe260 versus1ae1ba2 should verify initial table UUID handoff, native snapshot lifetime and error ordering. Then parent can run its existing focused B filesystem/request diagnostic against verified owner source, retaining10s. If that still exceeds the deadline, use request-specific measurements to separate required generic current-schema work from cleanup admission/current-policy and repeated complete authorizations; no further owner rewrite or gratuitous full-pair run is justified by this receipt alone. No further owner code change proposed without new evidence. Build slot released with final report.
