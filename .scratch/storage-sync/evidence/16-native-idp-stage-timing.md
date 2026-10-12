# Temporary native transaction / conflict-inspection profiling

2026-10-12. Source-only handoff; **no build, test, Clippy, runtime probe, powerset or CRAP run authorized or performed by this agent**. This is instrumentation, not a generic fix or acceptance evidence.

## Provenance and scope

- Verified original `ofdb` root clean on `main`, HEAD `bad307121960ab9856742a9a386a3dff5e5a2192`.
- Created root branch `diagnostics/native-idp-stage-timing` directly from that exact main commit. No worktree created or consumer dependency/configuration modified. Root instrumentation was explicitly permitted because consumer dependencies resolve to this original owner checkout.
- Verified consumer `of/.worktrees/ticket08-full-nodes` clean, branch `offline-first-ticket08-full-nodes`, HEAD `d2720cb7f759b825e613af5d8a4f1dde4be4e852`.
- Read `AGENTS.md`, `docs/agents/issue-tracker.md`, `GLOSSARY.md`, `.scratch/storage-sync/spec.md` including machine-local policy, and ticket06. Loaded ponytail and TDD. No existing ticket is closed or requalified.
- Complexity: low (timers/counts only). Risk: observer overhead and overlapping async timings, medium for interpretation; no intended semantic change. Priority: pinpoint the demonstrated cold restriction-PUT bottleneck before choosing any generic fix.

## Existing RED evidence (read, not rerun)

Consumer `.scratch/offline-first-nodes/evidence/08-idp-stage-timing-run01.log` exists on disk, not in consumer HEAD. Exactly one test failed after 81.06s, process exit101, unchanged 10s Management restriction PUT deadline.

Measured stages: client_credentials Client lookup1.550412s, principal Client load1.052184s, live validation1.265924s; introspection caller authorization2.517252s, actor authorization1.355802s, Client loads0.414287/0.403614s; Device caller authorization2.075491s. Secret verification0.163856s and JWT signing0.000301s do not support targeting crypto. These results prove slow composed loads, **not** native admission, catalog reconstruction, or any particular owner defect.

TDD seam remains the user-selected existing exact consumer probe below. Instrumentation does not add public behavior; no new implementation-coupled logging test or mock harness is introduced. A new RED/GREEN cycle is deferred until the coordinator authorizes this same probe; no fresh RED execution or GREEN is claimed.

## Source review / measurement boundaries

- `crates/btree-redb/src/database.rs`: native admission lock wait and synchronous `begin_write` measured separately. Transaction body lifetime starts after successful `begin_write`; explicit commit and rollback have separate pre-completion body and completion timings. The admission guard remains held through body and completion. Return/error propagation and commit/abort choices remain unchanged.
- `crates/sql-engine/src/executor.rs`: `row_conflicts` lookup, schema, codec phases separately timed, including early error/cancellation via scope guards.
- `crates/sql-engine/src/schema.rs`: table catalog lookup and field catalog scan timed once per call, with consumed-entry counts. `row_conflicts` currently performs three table lookups: its initial lookup, `table_schema` lookup, and `columns` lookup. This is a source observation, not a measured cause or permission to cache.
- `crates/sql-engine-automerge/src/codec.rs`: catalog reconstruction sums time inside `documents.next().await`, once per scan on guard drop, with consumed-document count. This includes backend scan/change loading and Automerge reconstruction, excludes catalog consumer processing/yield suspension. The requested conflict document reconstruction and its column discovery are separately timed once per conflict inspection. No per-document/per-row log flood.
- `crates/sql-engine/src/stage_timing.rs`: private scope timer, std-gated clock, no public API or runtime dependency. Engine no_std builds retain no std clock.
- Every emitted message uses `INFO` target `of::idp_stage_timing`, fixed labels, integer elapsed microseconds and optional counts only. No SQL, table names, IDs, values, payloads, credentials, errors or secrets are formatted. Timers take no clock readings unless this target is enabled when the scope starts.
- Added direct `log = { version = "0.4", default-features = false }` declarations in the three participating crates: none previously had a direct logging dependency or existing configurable measurement channel. Concrete need is compatibility with the already-installed consumer stage-only logger without unconditional stderr or a callback API. `log`0.4.32 already existed transitively in owner lockfile; owner lock diff is exactly three dependency-list additions, no new package/version.
- Reviewed full scoped source diff; targeted `rustfmt --edition 2024 --config skip_children=true` on the six touched Rust files and `git --no-pager diff --check` passed. Compilation, feature qualification and runtime behavior remain unverified.

### Interpretation limits

Labels are nested/overlapping; do not sum all records as disjoint work. Native body is transaction lifetime (including await scheduling, nested work and logger overhead), not CPU time. Catalog reconstruction includes native reads/change loading, not pure Automerge CPU. Counts include deleted entries and entries before errors; partial scans report partial completed polls. A cancelled in-flight reconstruction poll contributes no duration for that incomplete poll.

Native transactions implicitly dropped on errors/cancellation have no explicit body/completion record; cancelled admission has no completed admission record. Scopes starting before logger installation stay untimed. Background work may interleave records, and no IDs/correlation data are added to distinguish transactions. Use lexical nesting on sequential request paths and repeated dominant timings as evidence, not a per-request causal attribution to all concurrent native records. The existing probe logger is installed immediately before PUT only when `OF_IDP_STAGE_TIMING=1` and `--features cli` is enabled.

## Coordinator run needs — NOT authorization to execute

1. Review the instrumentation commit returned with this handoff. Reserve the **exclusive Rust build slot**, inspect available memory, keep original shared `of/target`, jobs1, standard profiles and **MemoryMax=2G**, zero swap. No simultaneous owner/consumer build.
2. Keep original owner checkout on the instrumentation commit throughout the run. Existing `.worktrees/ofdb` link resolves to original `/home/nathan/code/rust/aicacia/ofdb`; read-only `realpath ../ofdb/crates/sql` from the consumer confirmed this. **No dependency wiring change is needed** for this root-branch delivery.
3. Consumer HEAD lockfile lacks the three new direct `log` dependency edges. Coordinator must approve/update only those three package dependency lists in consumer `Cargo.lock` before `--locked` execution and record that source delta. No registry version change or manifest/config edit is necessary. This agent did not write consumer files; running the command below against unchanged consumer HEAD would fail the lock gate.
4. After explicit authorization and lock review, run from `of/.worktrees/ticket08-full-nodes`:

```sh
systemd-run --user --wait --pipe --collect --unit=of-ticket08-ofdb-stage-timing-01 --property=MemoryMax=2G --property=MemorySwapMax=0 --property=RuntimeMaxSec=900 --working-directory=/home/nathan/code/rust/aicacia/of/.worktrees/ticket08-full-nodes env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS OF_IDP_STAGE_TIMING=1 RUST_MIN_STACK=67108864 CARGO_BUILD_JOBS=1 cargo test --offline --locked --jobs=1 --target-dir /home/nathan/code/rust/aicacia/of/target -p unified-server --features cli --lib local_provisioning_tests::full_nodes::probe::single_full_host_refresh_replay_and_restriction_put_with_extra_service_clients -- --exact --nocapture --test-threads=1
```

Outer terminal bound910000ms, systemd total build/run bound900s as existing handoff; existing whole-test120s/HTTP10s bounds unchanged. These are proposed run parameters, not an executed command or automatically authorized retry. If the 2G cap or bounds prevent completion, record incomplete/resource failure and return to coordinator; do not widen or rerun automatically.

5. Capture literal command, reviewed owner/consumer revisions plus consumer lock diff, complete output, exactly one matched test, status and timing counts in coordinator-owned evidence. Owner evidence can retain only stage measurements and findings without fixture IDs/credentials. Compare admission vs begin_write vs body vs completion, then conflict lookup/schema vs reconstruction. Resource/compile failure is not runtime evidence; a failed HTTP test can still supply diagnostic timings, not acceptance.
6. Only measured evidence can motivate a generic follow-up. No cache, permission weakening, alternative persistence, retry, deadline change, sync policy change or crypto rewrite has been introduced. Temporary instrumentation should be removed once the measurement question is answered; no merge to main or generic fix commit here.
