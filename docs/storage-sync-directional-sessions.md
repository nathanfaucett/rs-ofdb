# Direction-constrained scoped SQL sessions (owner 14)

Implementation and focused local evidence; **pending independent review, not resolved**. This is the generic owner handoff for of04 bootstrap retry, not authenticated bootstrap/readiness acceptance. Consumer pointer: read-only `of/.worktrees/equal-node-setup/docs/public-persistence.md` receive-only handoff and `node_bootstrap_owner_tests.rs`, consumer revision `14bb87c1f1cdb0d6fa29794abe92f2c97d933a23`. Implementation starts from independently integrated `4dde323cf0d3082767fe829aeb5534e466fa77bf`.

## Public contract

`ofdb_sql` (feature `sync`) re-exports:

```rust
pub enum SyncDirection {
    Bidirectional, // default
    SendOnly,
    ReceiveOnly,
}

pub struct SessionConfig {
    pub direction: SyncDirection,
    pub max_units_per_frame: usize,
    pub max_session_bytes: usize,
}
```

Use existing methods with unchanged signatures, including:

```text
Database::synchronize_tables_candidate_validated_checked(
    transport, config, role, tables, validate, validate_candidate, check)
Database::synchronize_tables_with_stage_directory_candidate_validated_checked(
    transport, config, role, directory, tables, validate, validate_candidate, check)
```

Historical callback remains `Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync`; candidate callback remains `Fn(&SyncCandidateView) -> Result<(), String> + Send + Sync`; pre-apply callback remains `FnMut() -> U`, `U: Future<Output = Result<(), String>> + Send`. Structural-only and historical-value-only scoped methods consume the same option through the shared pipeline. Native staging still requires `native-stage`. No new dependency, feature, trait, transport or domain policy.

Set source `direction: SyncDirection::SendOnly` and receiver `ReceiveOnly`, usually with `..SessionConfig::default()`. Role is independent turn ordering. Non-bidirectional **unscoped** session calls reject `InvalidConfiguration` before transport progress. Existing exhaustive `SessionConfig` literals must supply the new field or use the default.

### Negotiation and execution

The scoped pre-Hello handshake is now postcard `(sorted_table_names, SyncDirection)`, rather than table names alone. Both scoped peers, including default bidirectional peers, must update. Trailing bytes and incompatible pairs reject before history exchange; only Bidirectional/Bidirectional, SendOnly/ReceiveOnly and ReceiveOnly/SendOnly complement. Unscoped Hello/version semantics are unchanged. There is no legacy compatibility adapter.

ReceiveOnly skips outgoing full-history/candidate preflight, outbound history construction and recovery snapshot export. It sends **no State/Changes frames**, including catalog/index rows and recovery. It still sends genuine scoped manifests/digests and causal known-change inventories, Hello, snapshot requests and Done/control frames. These metadata are not empty/faked and are not confidential: row/catalog addresses, digests and change IDs may be disclosed. Manifest digest calculation can internally encode stored state/metadata; this is not history payload publication. Scope creation still validates local schema/defaults.

SendOnly rejects every reverse State/Changes message before history validation, staging or apply, even forged catalog/index frames, malformed data and recovery frames. ReceiveOnly rejects any nonempty reverse snapshot request before exporting a recovery snapshot. A peer's declared direction is not trusted as enforcement. Controls and genuine inventories still allow source forward deltas and dependency recovery.

Permitted forward state/delta/fallback/recovery retain fixed-schema and complete historical-value checks, actual candidate validation, pre-apply authorization/cancellation, hash validation, causal order and native atomic apply/stage cleanup. Candidate validation is synchronous and transaction-consistent; no independent database reads or network wait under the write transaction. Direction suppresses publication rather than using callback denial as a filter. Receiver-local histories/conflicts/tombstones remain local; this deliberately does not promise two-way convergence. A learned receiver tombstone does not resurrect when the unaware source changes its live row.

Sessions are not globally atomic. Later denial rolls back its batch; earlier valid commits and local mutations remain. Discard failed transports; non-waiting Abort/cleanup is unchanged. Direction is not authentication, domain trust, freshness or acknowledgment of bootstrap completeness.

## Focused TDD and validation

Every cargo command below ran in `ofdb/.worktrees/storage-sync` with:

```sh
CARGO_TARGET_DIR=target CARGO_INCREMENTAL=0 RUST_MIN_STACK=16777216
```

Tool runtime bound: 1,200,000ms per invocation. No observed runtime timeout, no CRAP, no cargo-hack/powerset or workspace sweep; no of edits/tests.

### RED and intermediate attempts

- `cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync directional_native_reopen -- --nocapture --test-threads=1`: first compilation failed because the test helper moved non-Copy `SessionConfig`; fixed with `.clone()`.
- Same exact command with default bidirectional sessions before the direction implementation: exit101, 0 passed/1 failed, runtime0.15s (compile43.11s). Exact reopen retry failed with `remote sync aborted: Error: receiver must not publish candidates`. This is the actual retained receiver outgoing-preflight blocker, not an integrity or schema failure.
- After implementation, `cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync directional_ -- --nocapture --test-threads=1`: exit0, initial two tests passed (6.30s).
- `cargo test -p ofdb-sql-engine-automerge --test scoped_session directional_ -- --nocapture --test-threads=1`: exit0, two protocol tests passed (0.67s), then extended with explicit retained-inventory/exact-retry and index attacks.

### Final focused evidence

| Exact command after the environment prefix | Observed result |
| --- | --- |
| `cargo test -p ofdb-sql-engine-automerge --test scoped_session --test session -- --test-threads=1` | exit0; 6 scoped +10 session tests; 2.09s/1.05s |
| `cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts -- --test-threads=1` | exit0; 23 scoped +5 conflict tests; 13.73s/0.52s |
| `cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync directional_ -- --test-threads=1` | exit0; 4 tests, repeated after final index assertion, 6.81s; previous repeat7.11s |
| `cargo test -p ofdb-sql-engine-automerge --test scoped_session directional_ -- --test-threads=1` | exit0; 2 tests; final repeat0.75s |
| `cargo check -p ofdb-sql --no-default-features` | exit0 |
| `cargo check -p ofdb-sql-sync --no-default-features` | exit0 |
| `cargo check -p ofdb-sql --no-default-features --features sync` | exit0 |
| `cargo clippy -p ofdb-sql --features redb,in-memory,native-stage,sql --lib --test scoped_sync --test conflicts -- -D warnings` | exit0, including final test edits |
| `cargo clippy -p ofdb-sql-sync --no-default-features --lib -p ofdb-sql-engine-automerge --test scoped_session --test session -- -D warnings` | exit0 |
| `cargo fmt --all -- --check`; `git --no-pager diff --check` | exit0 |

New public facade tests run both receiver turn roles:

- Native empty initial import, completed reopen exact retry, later source update/new row, unrelated receiver selected/excluded mutations containing locally retained denied-for-export values. Zero reverse sent/received units/changes; source manifest unchanged. Forward unique index constraint remains enforced.
- Two 950k payload rows force separate apply batches: deny pre-apply after exactly one native record committed, clean stages, reopen, exact retry imports the other without duplicates. Earlier progress remains.
- Independent same-key native conflicts import without reverse source convergence; named source/receiver conflicts survive reopen and repeated retry; source stays conflict-free and unchanged.
- Directional unscoped calls reject without transport progress.

New protocol tests run both source turn roles:

- No receiver-generated State/Changes frames, including catalog/index/recovery. Completed retry advertises real retained application change IDs and resends no application snapshots/deltas.
- Fabricated reverse application/catalog/index State, nonempty malformed Changes, recovery State and source-requested receiver snapshot all reject with the direction-specific error, not incidental hash/schema rejection; source manifest unchanged.
- Four incompatible direction pairs explicitly reject on both peers.
- Forced forward missing-prerequisite recovery succeeds; forward full-state overwritten/deleted/losing-conflict values, incremental payload, recovery values, late export/fallback/recovery contamination and actual candidate recovery import/export denial still reach the generic validation callbacks. Denied incoming candidates remain absent and earlier valid row progress remains.

Existing public candidate correlation/rollback/concurrent-local/schema-reorder, pending-Abort native cleanup, scoped historical/scope/index, UUID identity/causal/conflict/tombstone and native conflict resolution/no-op regressions also passed above. These tests are owner evidence, not consumer bootstrap/authentication qualification.

## Handoff / remaining acceptance

Original ignored tracker: `.scratch/storage-sync/issues/14-directional-scoped-sessions.md` and map. Ticket14 remains in-progress pending independent review. Integration is merged **into the owner only** before commit/report; all prior owner commits retained. Exact final hash and post-commit verified isolated target cleanup evidence are recorded in the original tracker/map. Original dirty design/goal documentation and other workspaces remain untouched.
