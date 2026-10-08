# Fixed-schema checked SQL table sessions — owner follow-ups 09/10/11

Owner `storage-sync-owners`, `.worktrees/storage-sync`, starting at preserved `25b92516df6ef884d9cdf723da65c64568acdbd0`. Both original ignored tickets were claimed before implementation. No consumer repository was edited or executed. Original dirty `docs/design.md` / `docs/goal.md` remain untouched. Integration is merged into this owner branch before committing/reporting, never the other direction.

## Consumer-derived contract

Reviewed original of02 current comments35–55, consumer `b39d3fad`'s public-persistence.md, original mixed IdP migration and the consumer worktree's separated migration, actual `public_security_tables`, `PUBLIC_POLICY_TABLES`, and Client verifier update transaction. The concrete scopes are:

- IdP: users, credentials (metadata only), applications, clients, keys, devices, idp_replica_members, idp_replica_signers, oauth2_user_consents, oauth2_authorization_codes, oauth2_refresh_tokens.
- Management: roles, permissions, role_permissions, application_user_roles, device_selection_policies, device_resource_selections.
- Consumer excludes user_emails, user_phone_numbers, idp_conflict_resolutions and all local_* tables, including local_client_credentials, local_credential_secrets, local_device_enrollments. The owner does not hardcode those domain names or a local_* naming policy: the caller's slice defines the allowlist. Do not include local-only or secret-bearing schemas in that slice.

Separating verifiers into local tables is essential. These sessions do not sanitize a mixed-history file or validate domain content (e.g. public JWK semantics, Installation/actor/trust, revocation policy). That stays with of. Consumers must initialize their secret-free public schemas locally on both peers, use the exact reviewed scope, admit the transport themselves, and supply the existing per-apply policy/cancellation check. This report is owner-local acceptance, not completed consumer migration or of03 authorization proof; 06 cannot gate these owner deliverables.

## Public facade API

Available on `ofdb_sql::Database`, with the existing types and callback:

```rust
#[cfg(feature = "sync")]
pub async fn synchronize_tables_checked<T, Q, U>(
    &self,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    tables: &[&str],
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send;

#[cfg(feature = "native-stage")]
pub async fn synchronize_tables_with_stage_directory_checked<T, Q, U>(
    &self,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    directory: impl AsRef<std::path::Path>,
    tables: &[&str],
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send;
```

Memory and native embedded backends use the same implementation. Both peers must call a scoped method: a bounded scope-name preamble requires exact matching sorted table sets, rejects trailing bytes and offers no unscoped downgrade. Scope order in the caller's slice is irrelevant; empty/duplicate/unknown/internal-catalog scopes and invalid configurations reject. Methods use existing MemorySyncStageFactory/FileSyncStageFactory and the same protocol/session/apply/recovery implementation. No new auth trait, concrete transport, dependency, feature, timer, quota, deadline, freshness loop or projected-row import is added. Existing whole-resource operations and Client/Transaction APIs remain available and unscoped.

## Enforcement and limits

- `sql-sync/scope.rs` binds caller-selected tables to local predeclared columns (names/types/defaults/primary key), selected index names/uniqueness/column order, and selected catalog identity/reference graph. Outgoing manifest/state enumeration skips excluded keys **before** codec export. Inventories and incremental selection derive only from those selected units. Every actual incremental payload and requested recovery state is checked again before transmission, including source changes between initial export and later inventory/export calls.
- The receiver validates incoming manifest/inventory addresses, complete state and delta history before staging/apply, catalog/table/column/index identities and references, recovery request scope and requested recovery history. Matching table names cannot adopt a peer's wider schema or unknown index. Locally captured table/index authority is rechecked after the caller's check and immediately before each atomic incoming apply.
- Automerge implements the existing SyncRowCodec's new complete-history inspection capability. Other codecs default to explicit Unsupported instead of claiming safe projection. The inspector walks historical operations, not just current visible keys: root/map-only properties, allowed identities, encoded value types, immutable historical primary key, exact stored column layouts/defaults, schema references, and permitted metadata. It rejects unknown overwritten/deleted properties, nested/sequence/unsupported operations, arbitrary change metadata and uninspectable queued/bundled history. Values, layouts and metadata must consume their entire encoded byte slices: valid prefixes with trailing hidden bytes reject.
- Fixed schema means catalog history must have immutable attribute values. Compatible losing-definition tombstones remain transferable; table-drop/recreation events, observed-drop histories, changed/dropped index or column definitions and tainted older layouts can reject explicitly. This is **not** a DDL evolution/migration/sanitization API. Existing full-resource DDL behavior is unchanged. A caller cannot declare an old file safe merely by deleting or dropping its secret column.
- Complete selected histories remain opaque Automerge changes/snapshots; no property pruning, last-writer-wins reinsertion or actor/hash rewrite. Existing apply_change integrity verification is unchanged. Causal inventory order, concurrent values, explicit resolutions, data tombstones, complete catalog/index transactions and dependency recovery are retained.
- The pre-apply callback authorizes/cancels **incoming atomic apply operations**, not arbitrary outgoing reads, peer admission, or in-flight interruption of an already authorized atomic operation. Earlier committed operations remain on later failure. These sessions are not whole-session atomic. Source preflight/history validation and strict receiving failures attempt Abort without waiting for peer progress where engine abort delivery applies. Native stages release on local failure/drop. **Discard the transport after any failure**: a canceled send can have partially written a frame. Caller callbacks must report current decisions; no owner timing policy is invented.

## TDD and focused evidence

Authorized seams: public Database/Client/Transaction and existing owner Engine/SyncRowCodec/session transport boundaries.

1. `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync` first failed with two missing public scoped-method errors, then passed after implementation.
2. Native/memory selected history test covers selected catalog/index/row transfer, excluded local secret retention, native reopen and empty stage directory. Multi-table fixture covers two selected tables/indexes, scope-order independence, reverse sync and retained excluded local index usability. Invalid/mismatched scopes and widened/overwritten secret schema fixtures reject with receiver state unchanged.
3. Public scoped conflict test performs sequential and divergent offline updates, transfers full concurrent values across one-unit frames, resolves explicitly, reopens native storage, synchronizes resolution deltas and transfers a tombstone in the reverse role.
4. Native denial/cancellation test makes Abort send permanently pending after the callback denies. Both roles return PreApply, preserve denied target state, and leave no stage files. It uses no timers or peer-progress assumption.
5. Owner wire test mutates frames **after sender validation**: hidden/deleted unknown property in state, out-of-scope data state, invalid index reference, out-of-scope incremental address, unauthorized recovery request, and a hidden/deleted property in a requested recovery snapshot all reject. Cases that fail before apply retain the complete target manifest; recovery failures may retain earlier legitimate catalog progress, but do not import local-secret or poisoned recovered rows. A missing initial row dependency independently recovers selected full history successfully. These assertions exercise actual wire paths, not only the codec helper.
6. Owner historical inspection test rejects deleted unknown keys, overwritten malformed known values, unknown-key deletion deltas, trailing value bytes, trailing layout bytes and trailing metadata. Review found postcard::from_bytes ignores unused bytes: added trailing-value fixture observed a real red (`historical variant 3 must reject`), then strict scoped decoding made it green. Ordinary query decoding was not changed. The initial SQL-text DROP COLUMN fixture failed because the translator supports ADD COLUMN only; it was replaced with supported overwritten-history evidence plus a real deleted-property owner fixture, not reported as a passing SQL DROP test.

Final focused runs completed within300s; SQL tests use documented `RUST_MIN_STACK=16777216`:

| Exact command                                                                                                                                                                                         | Outcome                                                                                                           |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts --test client --test facade_sync --test cluster_sync --test local_sql` | **43 passed**:6 scoped+5 conflict/no-op+5 client+7 facade+9 cluster+11 local SQL                                  |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --all-targets`                                                                                                                       | **67 passed**:1 unit+49 kernel+6 realtime+1 hostile-scoped session+10 session                                     |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --all-targets --all-features`                                                                                                                    | **32 passed**:9 unit+18 catalog+1 transport+4 native-stage                                                        |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts`                                                                       | Final **10/10 repeated runs,110 tests passed**                                                                    |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test scoped_session`                                                                                                               | Final **10/10 repeated runs**, each exercises all seven wire scenarios                                            |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features in-memory,sync,sql --test conflicts`                                                                                                       | 1 passed independently, without native storage/Tokio runtime                                                      |
| `cargo check -p ofdb-sql --no-default-features`; same with `--features remote`, `--features sync`, `--features native-stage`                                                                          | All passed; bare sync/native-stage retain existing unused from_engine warning when no embedded backend is enabled |
| `cargo check -p ofdb-sql-engine --no-default-features`; `cargo check -p ofdb-sql-sync --no-default-features`                                                                                          | Passed                                                                                                            |
| `cargo clippy --workspace --all-targets --all-features`                                                                                                                                               | Passed; two unchanged KV CLI useless-conversion warnings                                                          |
| `cargo fmt --all -- --check`; `git diff --check`                                                                                                                                                      | Passed                                                                                                            |

Repeat harness is sequential, each subprocess bounded60s and total tool bound300s. Logs under ignored target/storage-sync-09-{facade,codec,sync,repeats}.log.

After interruption, integration merge was Already up to date at25b9251. Final resume reran the public scoped/conflict command (11 passed), `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test scoped_session --test kernel scoped` (history1 passed; wire test filtered out), then the exact unfiltered `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test scoped_session` (1 passed, seven wire paths). Formatting and diff whitespace checks passed again. The filtered invocation is not counted as wire qualification.

## No-op follow-up 10 — separately committed

Commit **`c368f16641d0039ac3d32edd1e271d997df3f5e9`**. Consumer Client update keeps the same revision and local verifier during no-rotation, correctly reaching a genuine unchanged UPDATE. Owner regression `unchanged_updates_commit_without_new_history_and_remain_syncable` first failed with `QueryError(Storage, "Incremental payload has no change")` using:

`RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,sync,sql --test conflicts unchanged_updates -- --nocapture`.

Minimal merge_row fix returns the decoded existing row only when no newly applied change exists **and the incremental payload is genuinely empty**. Nonempty invalid deltas still error. No timestamp fiction, ignored exception, new change record, hash relaxation or domain workaround. Public test proves direct and transaction no-op commit preserve exact exported state/history, native reopen retains it, then a real update and incremental sync remain usable. Full conflict suite and67 owner tests pass; final repeated conflict suite includes this test. The consumer's own no-rotation test must be rerun by its owner; it was not run or modified here.

## Bounded broad outcomes — incomplete full qualification

Final commands after the strict-decoding fix ran sequentially, each tool `timeout_ms:1200000`:

- `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`: **1200s timeout while compiling306/581**, SQL features `native-stage,redb,remote,sql`. Combinations1–305 completed, no observed test/compiler failure; unchanged client-test unused-import warning appears in the interrupted combination. Log target/storage-sync-09-final-powerset.log. **Not a full581-combination pass.**
- `just crap`: **completed, exit0**, all28 coverage commands/analyzer. **207/1061 functions exceed30**; only **2/97 analyzed files match final LCOV**,95 unmatched. Log target/storage-sync-09-final-crap.log. This existing aggregation problem is not corrected by the new API and is not aggregate coverage qualification. Complexity risk is recorded; no enforced failing gate observed or unrelated refactor made.

Earlier pre-strict-fix broad run also timed out1200s during305/581 (1–304 completed); earlier CRAP exit0 reported207/1060 and2/97. Those exploratory outcomes do not supersede the final measurements. No old05/08 revision pass is reused as qualification of09/10.

Owner-local09/10 acceptance is demonstrated, with incomplete broad powerset/coverage qualification explicitly retained. No production IdP/Management adapter wiring, complete consumer eleven/six-table integration, domain policy acceptance, old mixed-history migration, or registry publication is claimed.

## Follow-up 11 — caller-owned historical value validation

Claimed before implementation at preserved/integrated `2fbac37d26f8dc65b072481cd243a509c7460b43`. Original of02 review Comments35–37 identifies a P1: structurally valid Text can contain forbidden domain content even when overwritten, deleted or losing a conflict.09 only qualified structural scope/types, not domain value confidentiality. The consumer request is linked in original `.scratch/storage-sync/issues/11-historical-value-validation.md`; no of file or consumer test was edited/executed. Of must supply its closed field/JWK/grant rules and qualify its real marker3 transport; this owner change contains no such policy.

### Additive public APIs

```rust
#[cfg(feature = "sync")]
pub async fn synchronize_tables_validated_checked<T, V, Q, U>(
    &self,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    tables: &[&str],
    validate: V,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
    V: Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync,
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send;

#[cfg(feature = "native-stage")]
pub async fn synchronize_tables_with_stage_directory_validated_checked<T, V, Q, U>(
    &self,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    directory: impl AsRef<std::path::Path>,
    tables: &[&str],
    validate: V,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
    V: Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync,
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send;
```

These are `ofdb_sql::Database` methods using public `ofdb_sql::Value` only. Call `validate(table_name, column_name, decoded_value)` for every historical data-column Put and stored column-layout default, including overwritten/deleted/resolved and active losing-conflict values. Predeclared schema defaults and selected catalog column defaults are validated under the actual selected table/column identity, not internal catalog field names. The closure can borrow caller state; no 'static bound. Implicit Null defaults are observations too. Calls can repeat; their ordering/count is not a contract. Malformed/unknown/unsupported histories continue to reject before apply. Validation returns observations from the existing complete-history traversal (`SyncRowCodec::inspect_history`); no parallel codec, projected import, pruning or rehashing. `InspectedHistory` is only a tuple alias for this owner codec result.

All existing scoped state/delta/recovery validations use this callback, both directions/roles. Initial selected state is validated before Hello/history disclosure; actual later exported deltas and requested recovery snapshots are rechecked against their full history, catching source contamination after preflight. Incoming state/deltas/recovery validate before staging/mutation. Existing async `check` remains separate and runs at incoming atomic apply. No new trait, dependency, feature, timer, domain policy or freshness guarantee. Existing non-validated scoped APIs remain structural-only, now explicitly documented: their internal permissive callback is not domain confidentiality acceptance.

Errors returned by `validate` are `SyncError::Engine` with the callback reason; they may be sent as Abort, so callers must return non-sensitive reasons. `check` denial remains distinct `PreApply`. Callback validation is not a mutable domain-policy freshness/revocation guarantee. Retain prior failure rules: no whole-session atomicity, earlier legitimate commits can remain, stages release locally without peer-progress dependency, discard failed transports. A terminal sending responder may finish before learning the receiver's rejection; no end-to-end success acknowledgment guarantee was added. An initial test incorrectly required both peers always fail, then was corrected to require the receiving callback's exact denial and state assertions, not an incidental sender status.

### 11 focused/TDD evidence

- RED: `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync historical_value_denial -- --nocapture` failed on both missing validated public methods. GREEN native source/reopen regression denies generic forbidden Text after overwrite, row deletion and conflict resolution, in both roles, leaves source/peer state unchanged, and observes no forbidden bytes in any emitted frame. Positive public validated native/memory transfer preserves scope/history and native reopen.
- Native receiving regression uses a structurally permissive peer, validates full-state and incremental histories in both roles, and makes Abort send permanently pending after value denial. Exact state stays unchanged across native reopen; all created stage files disappear without timers.
- Public default regression rejects a forbidden Text schema default before any transport I/O and proves a borrowed (non-static) validator works.
- Owner hostile wire regression changes frames after sender validation: overwritten/deleted known Text, an active losing conflict whose visible value is asserted safe, a real encoded incremental delta with matching ID/hash, and requested recovery state. Each of five scenarios runs both roles and requires the receiver's actual callback reason, not an unrelated integrity error. Rejected imports remain absent; recovery retains earlier authorized catalog commits and a valid local row.
- Separate owner late-source mutation test changes persisted history after preflight at Inventory/RequestSnapshots. Both roles reject before disclosing actual incremental/recovery content; frame assertions prohibit forbidden bytes. Four paths per run. Existing seven structural hostile paths and six malformed/unknown/trailing encoding kernel fixtures remain active/passing.

| Exact final command                                                                                                                                                                                                                        | Outcome                                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts --test client --test facade_sync --test cluster_sync --test local_sql`                                      | **46 passed**:9 scoped+5 conflicts+5 client+7 facade+9 cluster+11 local SQL                                                          |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --all-targets`                                                                                                                                                            | **69 passed**:1 unit+49 kernel+6 realtime+3 scoped-wire+10 session                                                                   |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --all-targets --all-features`                                                                                                                                                         | **32 passed**                                                                                                                        |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features redb,in-memory,native-stage,sql --test scoped_sync --test conflicts`                                                                                                            | **10/10 repeats,140 tests passed**                                                                                                   |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-engine-automerge --test scoped_session`                                                                                                                                                    | **10/10 repeats,30 tests passed**, each suite exercises21 wire paths                                                                 |
| `cargo check -p ofdb-sql --no-default-features`; same with `--features remote`, `--features sync`, `--features native-stage`; `cargo check -p ofdb-sql-engine --no-default-features`; `cargo check -p ofdb-sql-sync --no-default-features` | All passed; bare sync/native-stage retain inherited unused from_engine warning                                                       |
| `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features in-memory,sync,sql --test conflicts`                                                                                                                                            | 1 passed, no native runtime                                                                                                          |
| `cargo clippy --workspace --all-targets --all-features`; `cargo fmt --all -- --check`; `git diff --check`                                                                                                                                  | Passed; two inherited KV CLI conversion warnings only. Initial new tuple type-complexity warning removed with InspectedHistory alias |

Repeat subprocesses were sequential, individually bounded60s, total tool300s. Logs `target/storage-sync-11-{facade,codec,sync,repeats}.log`. After final documentation and integration merge (Already up to date at2fbac37d), the exact public scoped/conflict command passed14 tests again and the unfiltered owner scoped_session command passed3 tests again; formatting/diff checks passed.

### 11 bounded broad outcomes (not full qualification)

Commands ran sequentially with tool `timeout_ms:300000` and process limit290s (`timeout --kill-after=5s 290s`), so timeout completion is recorded as exit124:

- `RUST_MIN_STACK=16777216 timeout --kill-after=5s 290s cargo hack test --feature-powerset --all-targets`: **exit124**, compiling261/581 (`default,in-memory,native-stage,sql` on SQL),1–260 completed, no observed test/compiler failure. Log `target/storage-sync-11-powerset.log`. NOT full581 pass.
- `RUST_MIN_STACK=16777216 timeout --kill-after=5s 290s cargo hack test -p ofdb-sql-sync -p ofdb-sql-engine-automerge --feature-powerset --all-targets`: **exit0,5/5 combinations complete** (Automerge owner1; sync no features, iroh+native-stage, iroh, native-stage). Log `target/storage-sync-11-owner-powerset.log`. Does not substitute for SQL facade/workspace powerset.
- `timeout --kill-after=5s 290s just crap`: **exit124**, compiling coverage target28/28, analyzer never ran. Cached retry, same exact command, also **exit124**, compiling target27/28. Logs `target/storage-sync-11-crap.log` and `target/storage-sync-11-crap-retry.log`. No new complexity/aggregate coverage score or gate result is claimed;09's historical scores are not this revision's qualification.

11 owner-local callback/API/history/cleanup acceptance is demonstrated. Full workspace powerset and CRAP remain incomplete, explicitly recorded rather than silently passed. Consumer closed-value rules, real marker3 malicious JWK/grant fixtures and final of02 acceptance remain its owner's responsibility;06 remains downstream.
