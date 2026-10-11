# 04 — Check generic authorization before every KV apply

Status: ready-for-agent
Type: task
Work status: resolved
Owner: ofdb
Blocked by: None
Priority: High
Complexity / risk: Medium / high

## Machine-local validation scope

Apply the [common Ubuntu Linux x86_64 policy](../spec.md#machine-local-validation-scope). Use bounded focused KV facade/snapshot-batch tests and relevant host feature checks. Isolated same-host stores cover both roles, buffered later batches, allow/deny/error, pending-Abort and cancellation, malformed-batch rollback, valid prior commits and fresh-session retry with merge/generation/tombstone semantics intact. Fake hooks qualify the owner boundary, not real consumer authentication/domain authorization. This scope note preserves resolved status and historical evidence; no tests run in this docs-only checkpoint.

## Scope and deliverable

Expose the smallest suitable generic caller check through ofdb-kv sync. Start with `crates/kv-sync/src/session.rs` received batches/transactions and `crates/kv/src/database.rs`. Prefer an existing seam or function/closure; no mandated new trait. Consumer uses public facade, not lower-level imports.

## Acceptance

- Both roles check before every atomic incoming snapshot batch, including already-buffered later batches; hello/session/receive-only checks are insufficient.
- Transport-neutral fake-hook tests cover allow, first-batch deny/error, allow-then-deny, and changes after authorization; no Iroh dependence.
- Denial/error mutates none of the denied batch, stops later applies, preserves valid earlier commits; already-authorized work may finish atomically.
- Malformed snapshots roll back the whole current batch. Preserve Automerge merge, greatest-generation selection, tombstones and value/expiry behavior.
- Disconnect/cancellation commits no partial current batch; subsequent explicit sync can converge from valid partial progress.
- Document direct public snapshot-import authorization responsibility; no production bypass. Hook errors remain actionable without message parsing as policy contract.
- Focused sync/facade tests pass with recorded commands/results and facade-only consumer dependencies.

## Exclusions and integration

No wire redesign, distributed SQL/KV transaction, quotas/timing/freshness policy or domain imports. [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) replaces lower-level calls in `of/crates/storage-server/src/database_protocol.rs` and [of 03 — execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) retains real Iroh authentication and domain authorization integration tests in of through protocol transport adapters built atop ofnet. Reusable mesh endpoint/allowlist/routing/pairing networking belongs to ofnet/iroh-chain; connection admission does not grant resource permission.

## Evidence

Commit: 2d391a5. Public ofdb_kv::Database::synchronize_checked(transport, role, config, check), async FnMut() -> Result<(), String>; SyncError::PreApply is re-exported by facade. Check immediately precedes the whole incoming batch transaction, before any import. Denial sends existing Abort best-effort, stops later applies, preserves earlier commits. Direct import responsibility documented. No wire changes/domain imports/policy knobs.

- Red: `cargo test -p ofdb-kv --features sync,in-memory --test facade_sync` failed on missing checked method and public SyncError.
- Green: same command: 4 passed. Both roles: all allow, first deny/error, allow-then-deny with already-buffered batches, persisted query-visible prior progress and subsequent explicit sync convergence. Decision flips after first authorization but authorized batch finishes; deterministic pending second hook cancellation drops owner and preserves only prior batch. Initial cancellation test polled before handshake progress (0 calls); replaced with oneshot readiness, final pass.
- `cargo test -p ofdb-kv-sync --lib`: 8 passed, including malformed received-batch rollback, dropped transport/malformed frames and interrupted later batch progress.
- `cargo test -p ofdb-kv --features sync,in-memory,redb --test embedded_redb_client --test embedded_client --test facade_sync`: 10 passed.
- Facade-only test imports only ofdb_kv plus runtime support, no store/sync types or caller timestamps. Expiry, tombstones and recreated generations retain parity from 01. Malformed direct import rolls back both rows; underlying received-batch rollback unchanged and tested.

### Merger P1 correction / requalification

Fix commit: `f506052dceb1ee0e20f984364de158f912a06a15`. Reopened/claimed at a6e5f6a: awaited best-effort Abort could block local denial indefinitely. Existing Abort send is now polled once with core polling and dropped if pending, returning original PreApply error without peer progress. Checked facade documentation corrected (no longer claims unchecked); failed transport must be discarded, Abort delivery not guaranteed. No timers, dependencies, tasks, policy or wire changes.

- Red/green (120s each): `cargo test -p ofdb-kv --features sync,in-memory --test facade_sync pending_abort` first failed deterministically at `local failure must not await Abort send`, then passed 1 test after fix. Both roles, stop at batch 1/2, caller denial/cancellation detail preserved; exactly one pending send poll, denied batch absent, prior batch committed, fresh transport retry converges.
- `cargo test -p ofdb-kv --features sync,in-memory,redb --test facade_sync --test embedded_client --test embedded_redb_client`: 5 + 3 + 3 passed in 300s && chain.
- `cargo test -p ofdb-kv-sync --all-targets`: 8 unit + 1 restart passed in same chain, preserving malformed-batch rollback and interrupted-session behavior.
- `cargo check -p ofdb-kv-sync --no-default-features` followed by `RUST_MIN_STACK=16777216 cargo hack test -p ofdb-sql-sync -p ofdb-kv-sync --feature-powerset --all-targets`: passed all 5 combinations (KV 1/SQL 4) within 300s.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path test-fixtures/storage-sync-consumer/Cargo.toml --offline --target-dir target`: 1 persisted separate SQL/KV scope test passed in focused chain.
- `cargo fmt --all -- --check` and `git diff --check`: passed. `cargo clippy --workspace --all-targets --all-features` completed within 300s with two unchanged KV CLI warnings.

Existing pending-hook/drop cancellation and change-after-authorization tests also passed. Owner-local acceptance restored; resolved. 05 full workspace powerset/CRAP gaps remain unclosed. No integration merge or other workspace edits.

Evidence report commit/final owner HEAD: `1cfdfc0ff5a99497323661e04a48cda702ada2e2`. Worktree clean, a6e5f6a preserved as ancestor. Inspected integration tip 3c4bf50 already an ancestor; no merge performed.

## Comments

- Reopened/claimed for merger P1 at a6e5f6a on 2026-10-07: awaited best-effort Abort can block local PreApply failure. Add public facade pending-send regression and fix misleading synchronize_checked documentation. No timers or integration merges.

- Claimed by storage-sync-owners on 2026-10-07 after SQL implementation 8dddf07. Medium complexity/high atomicity risk: place async caller closure once before each snapshot transaction; preserve current batch rollback and prior commits. Tests through ofdb-kv facade.

- Incoming KV batches already commit separately; preserve the boundary rather than making a session atomic.

- Historical planning audit (superseded by resolved acceptance above): canonical triage is ready-for-agent; work was open, with blockers recorded separately and no implementation yet claimed.
