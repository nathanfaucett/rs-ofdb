# Snapshot-local Automerge scan layout correction — focused RED/GREEN

2026-10-12. User authorized owner source/tests at public SELECT/row_conflicts seams and a narrow correction for measured per-row current-layout cost, not cross-operation caching. Independent review is next; no main integration, consumer requalification or ticket closure.

## Source, complexity, risk and scope

- Clean initial owner `73086e2` on `diagnostics/native-idp-stage-timing`; same reviewed instrumentation `b1d957d`, owner main `8a848b2` retained through merge `66ec216`. Consumer dependencies still resolve this original root. No consumer builds/source/harness changes in this correction session.
- Correction source/test commit: **`cce094cfad997425d608f30689a50ccf700ca79c`**, parent `73086e2`. Exactly two files: `crates/sql-engine-automerge/src/codec.rs`, `crates/sql/tests/scan_layout.rs`. Existing diagnostic timers/dependency edges are untouched by the correction diff; this commit is separable from instrumentation and receipts. No branch creation, main merge, reset, push, provider change or dependency addition.
- Complexity low: two scan-local layout variables and one shared borrow-or-own document fallback helper. Risk medium: current catalog selection, defaults, document fallback, tombstones and conflict mappings must remain valid; scan results share one borrowed current layout only while the same transaction read operation is alive. Priority high from measured repeated layout reconstruction, not a claim that this fixes every consumer timeout.
- User-confirmed TDD seams: public native SQL Query Handle SELECT and row_conflicts. No internal logging-count assertion, mock codec/kernel or catalog side-channel test.

## Minimal correction

`scan_rows` and noncatalog `scan_row_states` discover current columns once, lazily at the first eligible document, into stream-local `Option<Vec<Column>>`. Every document still reconstructs its state and checks deletion as before. `scan_rows` still skips tombstones before any layout discovery; state scans still include deleted states. Catalog state scanning itself is unchanged.

Shared `document_columns_for` borrows nonempty current columns; only an empty current layout invokes the original stored-layout / document-key fallback, separately for each document. Existing point reads/conflict inspection still discover current columns for each operation and retain the same fallback. No stored-layout shortcut over current catalog, permission/conflict omission, global/transaction/cross-operation cache, prewarming, deadline increase or persistence-policy change.

The native facade's read guard is held across each query, so the source transaction cannot be mutated while the scan is alive. New operations discover a fresh layout even within one explicit transaction after DDL. The regression test explicitly checks this case.

## RED → GREEN and correctness

Raw output files below are complete merged stdout/stderr, captured by a Python subprocess wrapper that mirrored output live and exited with systemd-run's status. Commands printed in tool output are recorded literally below; raw logs begin with the transient-unit announcement. Every build was launched serially by this investigation, with shared `/home/nathan/code/rust/aicacia/of/target`, standard profiles, offline/locked/jobs1,4G/zero swap/service900s/terminal910000ms. Before RED, available memory17GiB and no detected cargo/rustc build; parent reserved the exclusive slot. GREEN did log a brief build-directory lock wait; the holder was not identified and no cargo/rustc remained at follow-up inspection. Do not claim independently proven whole-session exclusivity or infer the holder was the parent.

| Run | Raw log | Result |
| --- | --- | --- |
| RED | `16-scan-layout-red01.log` | Baseline owner73086e2 plus newly authored public performance test; no codec correction yet. Compile9m33s; one test failed8.02s, exit101. SELECT512 rows with200 extra catalog fields took **7.087856604s**, failing fixed2s budget. Results' cardinality/endpoints were correct before budget assertion. |
| GREEN | `16-scan-layout-green01.log` | Same performance test with minimal codec patch; compile22.18s,1passed/0failed,1.09s,exit0. SELECT **54.888714ms**, unchanged2s budget. |
| Correctness | `16-scan-layout-correctness01.log` | Corrected codec plus final regression tests (now cce094c). Compile50.62s; existing conflicts **3passed/0failed,0.39s**; scan_layout **2passed/0failed,1.18s**;exit0. Repeated SELECT **62.957581ms**. |
| Facade Clippy | `16-scan-layout-clippy01.log` | Same final source, focused tests/features,exit0,1m30s; no warning records. |
| Codec Clippy | `16-scan-layout-codec-clippy01.log` | Same final source, codec lib directly linted,exit0,16.61s; no warning records. |

RED/GREEN source was executed before its later commit: RED consisted of clean baseline plus the first performance test; GREEN added only the codec correction; correctness added the second regression test. Commit cce094c identifies final validated source, not the nonexistent earlier RED commit.

New correctness test checks public SELECT before and after ADD COLUMN/default plus row deletion in the **same explicit transaction**, retained row_conflicts result, commit/reopen persistence, DROP/recreate empty state and new-schema default issuance. Existing native conflicts tests exercise actual concurrent values, explicit partial/full resolution, reopen/convergence, tombstone nonresurrection, local transaction snapshots, rollback/aborted-state rejection and unchanged-history sync. Document fallback extraction preserves source behavior; no new public fixture specifically manufactures a missing/malformed catalog fallback, and that case is a review consideration rather than separately claimed new test coverage.

The2s performance assertion is a machine-local regression budget with ~32–36x observed GREEN headroom, not a cross-platform SLA or a changed consumer/request deadline. 7.088s versus54.9ms is one same-host RED/GREEN observation, not a universal speedup claim. The inherited consumer PUT/B-refresh outcomes remain unqualified until independently reviewed owner source is exercised there.

### Exact commands

```sh
systemd-run --user --wait --pipe --collect --unit=ofdb-scan-layout-red-01 --property=MemoryMax=4G --property=MemorySwapMax=0 --property=RuntimeMaxSec=900 --working-directory=/home/nathan/code/rust/aicacia/ofdb env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS RUST_MIN_STACK=67108864 CARGO_BUILD_JOBS=1 cargo test --offline --locked --jobs=1 --target-dir /home/nathan/code/rust/aicacia/of/target -p ofdb-sql --features redb,sql,sync --test scan_layout native_select_with_large_catalog_keeps_scan_within_budget -- --exact --nocapture --test-threads=1
systemd-run --user --wait --pipe --collect --unit=ofdb-scan-layout-green-01 --property=MemoryMax=4G --property=MemorySwapMax=0 --property=RuntimeMaxSec=900 --working-directory=/home/nathan/code/rust/aicacia/ofdb env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS RUST_MIN_STACK=67108864 CARGO_BUILD_JOBS=1 cargo test --offline --locked --jobs=1 --target-dir /home/nathan/code/rust/aicacia/of/target -p ofdb-sql --features redb,sql,sync --test scan_layout native_select_with_large_catalog_keeps_scan_within_budget -- --exact --nocapture --test-threads=1
systemd-run --user --wait --pipe --collect --unit=ofdb-scan-layout-correctness-01 --property=MemoryMax=4G --property=MemorySwapMax=0 --property=RuntimeMaxSec=900 --working-directory=/home/nathan/code/rust/aicacia/ofdb env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS RUST_MIN_STACK=67108864 CARGO_BUILD_JOBS=1 cargo test --offline --locked --jobs=1 --target-dir /home/nathan/code/rust/aicacia/of/target -p ofdb-sql --features redb,sql,sync --test scan_layout --test conflicts -- --nocapture --test-threads=1
systemd-run --user --wait --pipe --collect --unit=ofdb-scan-layout-clippy-01 --property=MemoryMax=4G --property=MemorySwapMax=0 --property=RuntimeMaxSec=900 --working-directory=/home/nathan/code/rust/aicacia/ofdb env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS RUST_MIN_STACK=67108864 CARGO_BUILD_JOBS=1 cargo clippy --offline --locked --jobs=1 --target-dir /home/nathan/code/rust/aicacia/of/target -p ofdb-sql --features redb,sql,sync --test scan_layout --test conflicts
systemd-run --user --wait --pipe --collect --unit=ofdb-scan-layout-codec-clippy-01 --property=MemoryMax=4G --property=MemorySwapMax=0 --property=RuntimeMaxSec=900 --working-directory=/home/nathan/code/rust/aicacia/ofdb env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS RUST_MIN_STACK=67108864 CARGO_BUILD_JOBS=1 cargo clippy --offline --locked --jobs=1 --target-dir /home/nathan/code/rust/aicacia/of/target -p ofdb-sql-engine-automerge --lib
rustfmt --check --edition 2024 --config skip_children=true crates/sql-engine-automerge/src/codec.rs crates/sql/tests/scan_layout.rs
git --no-pager diff --check
```

Formatting/whitespace checks passed; commit hook rustfmt passed. MemoryPeak256.0K in these collected service logs is implausibly low, not actual RSS/headroom evidence. No powerset/CRAP/workspace sweeps: this user-authorized investigation is narrowly focused, and independent review/full qualification remain pending.

## Next action

Independent review of **cce094c versus73086e2** (not the entire diagnostic branch). Review scan snapshot lifetime, lazy empty/deleted behavior, per-document empty-layout fallback, and preservation of conflict/schema rules. Then parent authorizes/executes the identical consumer probe with original root correction source and records provenance. No owner main merge or ticket/spec closure; leave temporary diagnostics intact until measurement/review finishes. Build slot is released after this receipt.
