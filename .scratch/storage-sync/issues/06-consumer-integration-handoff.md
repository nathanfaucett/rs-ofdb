# 06 — Record facade-only consumer integration handoff

Status: ready-for-agent
Type: task
Work status: resolved
Owner: ofdb documentation/evidence; of owns consumer implementation
Blocked by: None; local 05 and final of 02 / of 03 evidence accepted
External dependencies: [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) and [of 03 — execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) integration evidence
Priority: High
Complexity / risk: Low / medium

## Machine-local validation scope

Apply the [common Ubuntu Linux x86_64 policy](../spec.md#machine-local-validation-scope). Accept externally supplied bounded, focused consumer evidence from isolated same-host instances using real Iroh/ofnet adapters and real domain authorization: authenticated peer/resource binding, per-apply denial, already-authorized completion, partial progress, SQL/KV phase separation and cleanup. Physical nodes/foreign OS/full sweeps are not gates. Facade migration, obsolete concrete-stage removal and real authorization implementation/evidence are still required; owner mocks or tracker consolidation cannot substitute. The final Answer accepts the exact of 02/of 03 producer evidence under this policy; this docs-only closure runs no consumer tests and claims no new runtime evidence.

## Scope

Record stable public contract and externally supplied integration evidence. This is an ofdb documentation ticket, not permission to edit of/offs/ofnet.

## External coordination

- [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md): replace direct Redb/B-tree/KvStore construction in `of/crates/storage-service/src/database_runtime.rs` with facade handles/named-table selection and Client queries/transactions.
- Migrate `of/crates/storage-server/src/database_protocol.rs` to public facade sync/native staging and real caller hooks bound to authenticated peer/resource.
- Delete obsolete `of/crates/storage-server/src/sync_stage.rs` after generic coverage moves to ofdb. Keep generic persistence/sync tests transport-neutral in ofdb and real Iroh authentication/domain authorization integration tests in of atop ofnet; remove direct backend/store/sync dependencies used only by migrated consumers, not unrelated dependencies blindly.
- Preserve offline writes/reconciliation without new global security/freshness/timing/budget policy.
- of 02 requires local 01–05 plus [offs 01](../../../../offs/.scratch/filesystem-sync/issues/01-remote-operation-admission.md) and [offs 02](../../../../offs/.scratch/filesystem-sync/issues/02-staged-content-write-integrity.md). of 02 and [offs 03 — Iroh extraction/offline reconnect](../../../../offs/.scratch/filesystem-sync/issues/03-iroh-extraction-offline-reconnect.md) co-deliver extraction with reusable mesh endpoint/allowlist/routing/pairing networking in ofnet/iroh-chain and domain trust/authorization plus protocol transport adapters in of atop ofnet, leaving ofdb with no Iroh dependence. This refines existing ownership, not a new networking requirement/framework/task or reciprocal completion gate; routing does not promise transparent mesh stream relay. This ticket follows local 05 and of 02/03 evidence; it does not gate of 02 or offs 03.

## Acceptance

- 05 has local evidence; public usage/check responsibility is documented as implemented, not only planned.
- Record delivery/test evidence on [of 02 — public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) and [of 03 — execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md); both are required, not just tracker existence.
- Production database consumers use only ofdb-kv/ofdb-sql interfaces: no direct backend construction/lower-level sync/concrete staging for this responsibility.
- Bounded focused real Iroh authentication and domain authorization integration in of, through protocol transport adapters built atop ofnet on isolated same-host instances, proves authenticated peer/resource binding, denial before next apply, permitted completion of already-authorized work, valid partial progress and cleanup across separate SQL/KV phases. Endpoint allowlists enforce connection admission, not resource permission or a substitute for per-apply authorization; fake hooks alone do not satisfy this consumer gate.
- Generic persistence/sync tests reside in ofdb, remain transport-neutral, and need no Iroh dependence; real authentication/domain integration resides in of. Record unresolved external blockers; local tests alone do not establish migration completion.

## Comments

- Both sibling maps and concrete handoff tickets have been read. Implementation evidence remains pending. Local tickets 01–05 have no external completion gate; 06 is downstream of of 02/03 evidence.

- Tracker audit: canonical triage is ready-for-agent; work remains open. Blockers are recorded separately and no implementation is claimed.

- 2026-10-11 documentation checkpoint: owner tickets 01–05 and 07–15 are integrated into local main and their tracker records are persisted selectively. Consumer evidence has not been reassessed here; sibling tracker consolidation does not satisfy this ticket's acceptance. Work status remains **open**, with no consumer closure or new test qualification.

## Answer

2026-10-12 — **Resolved by authorized documentation-only acceptance of existing final of02 and of03 Answers and producer receipts.** No implementation, build, test, sweep or new runtime qualification. This supersedes the historical open disposition above; acceptance is revision-specific Ubuntu Linux x86_64 evidence, not a current-head rerun or full Node/platform acceptance.

### Acceptance crosswalk

| Requirement                                                    | Accepted source, revision and evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Local 05 and implemented public contract                       | [05](05-local-regression-qualification.md) is resolved; its exact generic facade/staging/recovery/atomicity/cleanup commands and historical broad-run limits remain authoritative. Owner qualification is transport-neutral, not real consumer authorization.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Facade-only production migration and stage deletion            | [of02 final Answer and independent review comment](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md): consumer `a095bba5cc7f94a7e25eca0d2c9f6fe09c1ad899`, original ofdb `1480a09d1f7efa29261e6bf2dc2125ab844fae7b`, offs `516b6926136c348ba3e7a183e4145b8fde7c1fd5`. Public SQL/KV Database/Client/Transaction and named `kv` table replace raw construction/lower-level orchestration; obsolete consumer `sync_stage.rs` is deleted. Boundary/source/metadata audits and original-root regressions support the migration, without source overrides. Generic stages/codecs remain owner-owned; domain adapters remain in of atop ofnet.                                                                                    |
| Real authenticated peer/resource binding and current authority | [of03 final Answer](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) accepts execution `9e0d7e52b04e0790db0945a16849e4ad7adebff6`, parent `8ddf6a307c116301099073f58f952acf3ab78020`. [Execution completion receipt](../../../../of/.scratch/offline-first-nodes/evidence/03-completion/README.md) records original owners ofdb `4cd65c17c08733c7eb74ef65850380c2ba74f8c6`, offs `9a25f8150f6467c92e33a4343aac3034af6ba038`, ofnet `85632a965a07ab4f3ee1e7654cb762c061afe8d2`. Actual Connection remote identity, independent native trust, exact kind/namespace/ID, both Device selections/restrictions and local failure are exercised on production SQL/KV adapters. Admission allowlisting does not grant resource authority. |
| Denial before next apply, valid progress, retry and cleanup    | The same execution receipt records both roles, established-connection SQL/KV receive/apply and queued owner-payload sends. Public KV scan observes exactly the first committed `a` batch before learned denial; later batch is absent, source/prior progress survive and restored authorization permits retry/reconnect. SQL and KV are separate atomic phases, not a distributed/session-wide rollback. Owner03/04/05 establish complete-operation rollback, native-stage cleanup and already-authorized atomic completion; real consumer admitted FS/security work finishes before next-operation denial. Do not invent a separately rerun SQL/KV mid-transaction revocation test.                                                                                        |
| Independent review and bounded evidence ownership              | [Completion review receipt](../../../../of/.scratch/offline-first-nodes/evidence/completion-review/README.md) reviewed execution9e0d7e5 and setupf2f2b90 on detached combined `162e76873845a16189ff9ca68b49216630107aa5`, not integration. It independently passed namespace, production FS parent cancellation and public-bootstrap/offline retry; larger SQL/KV/outbound collections were audited from producer results, not independently rerun. of03 final Answer subsequently records execution-only fast-forward; the historical setup merge blocker is not a blocker for this handoff.                                                                                                                                                                               |

### Exact inherited commands and results (not executed for this closure)

of02 independent review ran in original `of` root on normal sibling sources with `RUST_MIN_STACK=16777216`:

```sh
cargo test -p unified-server --lib security_history_tests --locked
cargo test -p unified-server --lib security_sync_tests::authenticated_equal_peers_exchange_scoped_history_without_local_verifiers --locked
cargo test -p idp-service --features replica --lib --locked
cargo test -p storage-server --no-default-features --features network --lib --locked
just boundary-check
```

Recorded results: history8/8 (239.49s), equal-peer1/1 (121.03s), IdP45/45 (42.63s). Storage first49/50 with shutdown-stub BrokenPipe, then isolated1/1 and full50/50 (3.08s). Historical validation protects full stored history/source disclosure/receiver apply, not complete-record provenance approval; Keys wire matrix is not every Signers row and reliable remote Abort delivery is not guaranteed. of03 owns the later candidate/current-authorization boundary.

of03 producer receipt gives this literal prefix, from `of/.worktrees/execution-completion`, for each command below:

```sh
CARGO_TARGET_DIR=/home/nathan/code/rust/aicacia/of/.worktrees/execution-completion/target CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_DEV_OPT_LEVEL=1 RUST_MIN_STACK=67108864
```

```sh
cargo test -p storage-server --lib --no-default-features --features network --offline --locked real_production_database_admission_checks_identity_both_selections_and_local_failure -- --nocapture --test-threads=1
cargo test -p storage-server --lib --no-default-features --features network --offline --locked real_selected_sql_and_kv_queued_payloads_recheck_native_sender_policy_on_both_peers -- --nocapture --test-threads=1
cargo test -p storage-server --lib --no-default-features --features network --offline --locked real_selected_sql_denies_received_apply_and_retries_on_existing_connection -- --nocapture --test-threads=1
cargo test -p storage-server --lib --no-default-features --features network --offline --locked real_selected_kv_first_commit_survives_learned_denial_and_same_connection_retry -- --nocapture --test-threads=1
cargo test -p unified-server --lib --no-default-features --offline --locked security_sync::outbound_tests -- --nocapture --test-threads=1
```

Recorded results respectively: 1 pass394.88s; 1 pass282.61s; 1 pass402.85s; 1 pass442.44s; 4 pass345.98s. The receipt retains aggregate timeouts/fixture corrections and the SQL test-only300→600s bound change, not a new production deadline. These are accepted historical runs, not fresh runs under today's normal120–300s guidance.

Independent review used the same environment values except `CARGO_TARGET_DIR=/home/nathan/code/rust/aicacia/of/.worktrees/review-completion/target`, from that review worktree:

```sh
cargo test -p unified-server -p storage-server --lib --no-default-features --features storage-server/network --offline --locked native_socket_response_binds_namespace_even_when_resource_id_exists_in_new_application -- --nocapture --test-threads=1
cargo test -p storage-server --lib --no-default-features --features network --offline --locked real_selected_filesystem_parent_cancellation_releases_admitted_session -- --nocapture --test-threads=1
cargo test -p unified-server --lib --no-default-features --offline --locked desktop_bootstrap_orchestration_completes_both_public_contexts_and_retries_offline -- --nocapture --test-threads=1
```

Recorded results: 1 pass37.35s (Storage filter matched zero, not a suite pass); 1 pass60.17s; 1 pass208.74s. Initial cold compilations exceeded240s; resumed passes do not turn those timeouts into successes.

### Inherited limits and durable handoff

No globally synchronized authorization, instantaneous revocation, per-byte reauthorization or partition-wide single-use guarantee. Deterministic expiry of the same genuinely issued bearer across a response gate remains supplementary/unqualified due to native wall-clock seams; existing expiry guards are not that test. Historical503 cause remains unproven. of02 powerset180s and CRAP120s timeouts, of03 unchanged test-stub Clippy failures and all owner historical broad limitations remain recorded; no full workspace/platform score or pass is inferred. Follow-up scoped negotiation and managed retirement retain their owner-specific limits. of08 and profiling16 are not closed or fixed here.

Read-only provenance check at closure: original consumer root `of` is integration `290738bb2aa8fdca1c3ee42ae2da4c2df7ee6eed`, main `fc74d950b4a8f64ebd806c7422982bbc89d3a6d7`; accepted consumer commits are ancestors of integration. Existing root dirt is untouched. The final of02/of03 ticket Markdown is already tracked. The two linked receipt READMEs exist but are Git-ignored/untracked and must be preserved by the coordinator with explicit `git add -f` in `of`: `.scratch/offline-first-nodes/evidence/03-completion/README.md` and `.scratch/offline-first-nodes/evidence/completion-review/README.md`. This owner neither writes nor stages them. Raw logs and the currently empty historical `03-api-qualification/` directory are not silently represented as durable committed evidence; the final tracked Answers and available receipts are the accepted record. See [owner cleanup/handoff](../map.md#owner-cleanuphandoff--2026-10-12) for branch retirement and profiling reactivation prerequisites.
