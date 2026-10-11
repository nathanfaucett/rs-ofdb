# 09 — Fixed-schema table-scoped checked SQL sessions

Status: ready-for-agent
Work status: resolved
Owner: storage-sync-owners
Priority: High
Complexity / risk: High / high (history confidentiality, catalog references)
Blocked by: None; 06 downstream

## Contract

Consumer b39d3fad, original of02 comments35–55 and public-persistence.md require existing checked sessions scoped by tables slice. Public Database::synchronize_tables_checked(transport, config, role, tables, check) and synchronize_tables_with_stage_directory_checked(transport, config, role, directory, tables, check). Existing zero-argument async check, transports and owner stages only. IdP eleven public metadata/grant tables and Management six RBAC/selection tables explicitly listed in of02; all local_* plus email/phone/audit tables excluded. No of edits.

## Acceptance

- Reject empty/duplicate/invalid scope, require locally predeclared schemas. Both directions constrain manifest, catalog/index, incremental/state and recovery; peer cannot widen scope.
- Validate full historical column identities and encoding, including unknown overwritten/deleted properties and malformed history; reject schema widening, never prune CRDT bytes/project rows.
- Preserve selected full conflicts, causal history, resolution and data tombstones; atomic catalog/index dependencies and existing per-apply cancellation checks/cleanup in both roles. Document committed-progress/failure transport disposal limits honestly.
- Public facade red/green/native stages/reopen/adversarial regressions, no_std/features/lints and bounded full qualification outcomes. Minimal owner implementation, no generic auth framework/domain policy/dependency expansion.

## Comments

- Claimed 2026-10-07 at25b9251 after reading current original consumer comments/docs, old original mixed schema and actual b39d3fad separated worktree schema. Public Database session seams authorized by user. TDD loaded; implementation confined to ofdb owner worktree/original tracker. Integration25b9251 ancestor; owner initially clean.
- Review P1 follow-up:09's resolved acceptance is structural schema/type/history scope ONLY, not domain value confidentiality. Current original of02 Comments35–37 require a caller seam over every historical decoded value; tracked/claimed separately in [11](11-historical-value-validation.md). Do not use09's SQL-type checks as closed-JWK/grant acceptance. No domain policy belongs in owner.
- Final implementation/report commit `2fbac37d26f8dc65b072481cd243a509c7460b43`. Owner worktree clean; integration `25b92516df6ef884d9cdf723da65c64568acdbd0` merged before and after commit (Already up to date), ancestry and preserved10 commit verified. No owner merge into integration; rustfmt commit hook passed. Pre-existing GC warning left untouched.
- Owner-local acceptance resolved. Both requested public Database methods use existing zero-argument async check/stages and shared session pipeline. Caller allowlist restricts outgoing/incoming catalog/index/state/incremental/recovery. Predeclared local column/index definitions remain authoritative. Full historical operations reject unknown overwritten/deleted keys, malformed/trailing encodings and widened schemas; no projected import, hash relaxation, new dependencies/features/auth traits/domain policy. Fixed-schema sessions explicitly reject unsupported DDL/drop histories rather than claiming migration safety.
- TDD: public scoped facade initially failed on missing methods; strict history trailing-value regression failed `historical variant 3 must reject` before full-consumption decoding. Native/memory/reopen tests demonstrate selected catalog/index/data, retained excluded secrets/indexes, conflicts/resolutions/tombstones, multiple tables and reverse roles. Both-role pending-Abort denial returns with empty native stages. Seven post-validation wire scenarios cover state/index/incremental/recovery attacks and valid dependency recovery. Earlier legitimate commits may remain on later failure; discard failed transports.
- Exact final focused commands/results: `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts --test client --test facade_sync --test cluster_sync --test local_sql` (43 passed); `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --all-targets` (67 passed); `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --all-targets --all-features` (32 passed). Public scoped/conflict suites repeated10/10 (110 tests); owner scoped wire suite repeated10/10 (seven paths per run). Final resume reran public scoped/conflict11 tests, kernel history1 test and exact `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test scoped_session`1 test: all passed. A combined `--test scoped_session --test kernel scoped` filtered the wire test out; the exact unfiltered rerun above supplies evidence, not that filtered invocation.
- No-default SQL/remote/sync/native-stage and engine/sync checks passed; memory-only conflicts passed. `cargo clippy --workspace --all-targets --all-features` passed with two unchanged KV CLI warnings. `cargo fmt --all -- --check` and `git diff --check` passed. Full exact commands and limitations in tracked `docs/storage-sync-table-scope.md`.
- Final broad commands after strict decode fix ran sequentially with1200s bounds: `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets` timed out compiling306/581 (1–305 completed, no observed failure), NOT full pass. `just crap` completed exit0,28 coverage commands,207/1061 above30, existing2/97 LCOV match/95 unmatched limitation. No enforced complexity gate observed; no aggregate coverage claim. Logs `target/storage-sync-09-final-{powerset,crap}.log`. Local ticket criteria demonstrated; full powerset qualification remains incomplete and consumer eleven/six-table integration/domain policy is downstream06, not claimed here.
