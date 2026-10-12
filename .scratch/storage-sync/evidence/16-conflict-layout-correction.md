# Same-operation field-catalog reuse for conflict inspection — RED/GREEN

2026-10-12. User authorized narrow owner correction at public SELECT/row_conflicts seams from measured consumer run13/14 work. No consumer edit/build/rerun, main merge, ticket/spec closure or independent-review claim in this session.

## Evidence read and source provenance

Read consumer clean `26ad55d`/source `0dce5e0` receipt `08-consumer-diagnosis.md`, stage-summary JSON for13/14 and raw14 around the29.960290s completed admission. Consumer12 endpoint connect1.746ms and14 both owner-session-start markers distinguish earlier endpoint suspicion; actual security session progress still exceeds360s. Consumer13 reports conflict columns4.036619s versus document reconstruction0.035867s, separately validated engine schema4.622949s. These are nested/interleaved wall totals, not isolated CPU or exclusive B request time. Consumer row_conflicts count=0 is an unset timer counter, not a conflict count.

Initial clean owner root on `diagnostics/native-idp-stage-timing`, **`a6f23119257b591023b17da550bfc2ba7460231b`**. Existing accepted scan correction cce094c and diagnostic instrumentation b1d957d retained. Final correction/test commit **`89e85d39e1f8ea700ddb1239d02a1f1c327b108a`**, parent a6f2311. Exactly7 files/110insertions/16deletions; no manifest/dependency/lock/provider changes. This source/test commit is separate from evidence and adds no timing/logging code. Existing diagnostics remain separately owned/history-preserved, not main-integrated.

## Complexity/risk/priority and correction

Complexity low-to-medium (one operation-local catalog handoff, shared schema-read loop, required codec parameter); semantic risk medium from raw catalog validation, filtering/order/default/fallback and low-level API boundary; priority high from measured duplicate field-catalog reconstruction. Public SQL facade unchanged.

- Engine schema discovery still validates every live field row in the same order and performs the same table checks. A shared `read_columns` loop optionally observes raw rows; ordinary schema queries use a no-op observer, so do not allocate a retained snapshot.
- `table_schema_with_fields` retains **all** live raw field-catalog rows, not just the table-selected/normalized `ColumnSchema`s, only for the current operation. Both engine calls of `conflicted_columns` (`row_conflicts`, `resolve_row`) pass this snapshot.
- Codec retains its own table-catalog lookup, raw field parsing, filtering by table ID, position/name ordering, defaults, built-in fixed layouts and stored-layout/document-key fallback. Only the duplicate field-catalog reconstruction is replaced by iteration over the supplied same-operation rows. In particular engine filters by table ID **and table name**, whereas codec historically filters by ID; passing only normalized schema columns would change malformed-catalog behavior. Raw rows avoid that unsafe shortcut.
- No permission/conflict/default/type check removed; absent/invalid keys, missing tables and aborted transactions still follow engine validation before document inspection. Missing documents still return no conflicts only after schema validation. Tombstoned field rows are excluded in both original paths. Logical fallback/error behavior is source-preserved; no new public fixture deliberately manufactures every malformed storage/I/O failure. A removed second physical scan cannot reproduce a transient failure exclusive to that duplicate scan, and reuse requires a stable snapshot across the operation.
- Required low-level API change: `RowCodec::conflicted_columns` now takes `table_fields: &[Row]`, the live field-catalog snapshot. All repository implementations/callers were updated; built-in-catalog direct tests supply an empty slice because the codec's fixed built-in layout does not consume it. No compatibility/default method or old-version adapter. External custom implementations/callers must adapt. Public `Client::row_conflicts`, SQL and resolution signatures are unchanged.
- Current fields are not reused between operations, queries or transactions. No transaction/global cache or stored-layout shortcut. Existing `row_conflict_values`/candidate value-introspection path is intentionally unchanged; no claim that all conflict inspection is optimized.

## Actual TDD and validation

User-confirmed public seam: Query Handle SELECT/row_conflicts on persistent native stores. New performance test invokes512 public conflict inspections on one existing row with200 extra catalog fields and verifies a clear result every time. Fixed12s machine-local budget was set before RED and never changed; this is a regression threshold, not a universal SLA or a change to consumer10s/360s deadlines.

| Run | Exact focused selection | Actual result |
| --- | --- | --- |
| RED | `ofdb-sql --features redb,sql,sync --test scan_layout native_conflict_checks_with_large_catalog_keep_fixed_budget -- --exact --nocapture --test-threads=1` | Baseline a6f2311 plus new test only; compile27.74s,0passed/1failed/2filtered,16.46s,exit101.512 checks **16.342331790s**, fail fixed12s. |
| GREEN | same literal selection | Codec/schema handoff patch; compile22.40s,1passed/0failed/2filtered,8.12s,exit0.512 checks **7.488326755s**. |
| Correctness | `ofdb-sql --features redb,sql,sync --test scan_layout --test conflicts -- --nocapture --test-threads=1` | Compile35.20s, **3 conflicts passed0.38s +3 scan_layout passed8.41s**,exit0. Repeated conflict checks7.333984786s; SELECT54.270190ms. |
| Codec boundary | `ofdb-sql-engine-automerge --test kernel conflict -- --nocapture --test-threads=1` | Compile5m07s, **3passed/0failed/47filtered,0.38s**,exit0. Internal catalog conflict/resolution, reordered compatible schema conflicts/resolution, replicated unique conflict/deleted winner. |
| no_std boundary | `cargo check -p ofdb-sql-engine -p ofdb-sql-sync --no-default-features --lib` | exit0,56.61s. |
| Library Clippy | `ofdb-sql-engine`, `ofdb-sql-engine-automerge`, `ofdb-sql-sync`, `--lib -- -D warnings` | exit0,5.10s. |
| Facade-test Clippy | `ofdb-sql --features redb,sql,sync --test scan_layout --test conflicts -- -D warnings` | exit0,11.15s. |

All invoked serially under user-exclusive slot with systemd **4G/swap0/runtime900s**, offline/locked/jobs1/shared `of/target`, ordinary profiles, terminal910000ms. Preflight available memory16GiB, host swap401MiB used; no detected cargo/rustc build before/after. No observer logger installed by these focused public tests. Reported collected service memory peaks256–292K are implausible, not real RSS evidence.

RED source was executed before codec changes; GREEN/correctness/boundary/Clippy executed final working-tree patch later committed as89e85d3. No fictitious RED commit. Existing public tests cover current defaults/catalog changes within the same transaction, commit/reopen/drop-recreate, missing/invalid key/table and aborted-state errors, local conflict values, partial/full resolution, tombstone nonresurrection, convergence and rollback. Native snapshot semantics verified; arbitrary interior-mutable custom kernel behavior is not independently proven.

Complete exact invocations: [commands](16-conflict-layout-commands.txt), plus each adjacent `*-command.json`. Full raw logs/status: `16-conflict-layout-{red01,green01,correctness01,boundary01,nostd01,clippy01,facade-clippy01}.log` and `.status`. [Integrity/status manifest](16-conflict-layout-integrity.txt) stores raw SHA256 and observed statuses; wrapper captures merged stdout/stderr and exits with systemd-run return code, no pipeline masking. Raw logs/argv/status and this receipt are preserved in a separate evidence commit.

Changed-file `rustfmt --check --edition 2024 --config skip_children=true` on all7 files, `git diff --check`, and commit-hook rustfmt passed. Owner justfile has **no boundary-check recipe**; boundary qualification above is the actual codec/no_std checks, not an invented just result. No powerset/CRAP/workspace sweep in this narrowly authorized focused investigation; final full qualification remains pending.

## Admission29.960290s — no unsupported holder attribution

Raw14 line88274 completed admission29.960290s after an anonymous61.711ms body_rollback/6us rollback. That neighboring completion does not identify the waited-on database or all holders over the wait. Both sessions/background/current authorization interleave anonymous records; cancelled/dropped/pre-logger holders may be unrecorded. A wait can include queueing behind multiple transactions; this is a possibility, not a recovered causal timeline.

Source `btree-redb/src/database.rs:27–45,81–106` holds admission for body/completion. `sql-sync/src/session.rs:640–685` holds a read transaction across per-table row-ID/state/metadata export. This is a relevant generic holder candidate, but not sufficient evidence that it caused the29.960s wait, the whole stalled session or all authorization cost. No admission/session-lifetime patch or deadlock claim is justified here; raw IDs/correlation would be needed for precise attribution.

## Concrete next action

Independent review of **89e85d3 versus a6f2311**, especially full raw-field preservation/filter differences, same-operation snapshot contract, error ordering and low-level RowCodec migration. Then parent reruns its exact cli-enabled cold-B restriction/grants diagnostics against verified original owner root to measure remaining conflict schema/codec/admission and owner-session progress. If admission/session still dominates, add safe diagnostic transaction/store correlation without SQL/identity/secret values before choosing a holder correction. No deadline, authorization, cache, acceptance or main changes. Rust build slot released with final report.
