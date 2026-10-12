# 16 — Measured native catalog correction and profiling retirement

Status: ready-for-agent
Type: task
Work status: resolved
Owner: ofdb generic correction/qualification; consumer implementation remains of-owned
Priority: High
Complexity / risk: Low-to-medium / medium (native snapshot provenance, low-level interface migration, interpretation)

## Scope

Measure native transaction versus schema/catalog/conflict reconstruction, correct source-proven repeated reads using same-operation/native-snapshot data only, and retire temporary instrumentation after qualification. Preserve current schema/default/tombstone/conflict/error behavior, full SELECT decoding before predicates and largest-UUID rules. No cross-operation cache, index framework, permission/persistence/deadline weakening or consumer edits. Resolution is scoped to the measured Ubuntu owner correction and recorded consumer evidence, not deployment-wide latency guarantees, full owner/spec qualification or authorization to merge main.

## Historical instrumentation handoff (superseded by Answer)

- Source-only implementation and review recorded in [measurement handoff](../evidence/16-native-idp-stage-timing.md).
- Coordinator reports reviewer42596023 approved source b1d957d with no blockers; consumer437c9bf records the lock dependency-edge refresh.
- Coordinator attempt01 used 1500M/zero-swap/900s limits and was OOM-killed during compilation; the SAME existing consumer probe never ran. Literal executed invocation is now recorded in owner evidence from consumer receipt57426e363d413d3f29b16665459605683e9ad52f; no remaining command provenance gap.
- Runtime evidence is blocked on sufficient free memory (coordinator reports approximately1.9GiB available, swap full) and fresh exclusive-slot authorization. No build/retry is authorized by this ticket.
- Record measured native versus schema/catalog/reconstruction costs before proposing a generic fix. No GREEN or integration acceptance inferred from instrumentation.

## Comments

- 2026-10-12: Verified clean owner main bad3071 and clean consumer d2720cb. Dedicated owner root branch `diagnostics/native-idp-stage-timing`, no worktree/configuration change. Existing evidence proves slow request-path loads but does not yet isolate an owner cause. Source-only handoff; runtime evidence and any fix remain pending. Existing tickets/spec/map statuses untouched.
- 2026-10-12 attempt01 receipt: consumer437c9bf / ownerb1d957d; source reviewer42596023 approved, observer effects retained. Log reports oom-kill, runtime8min25.181s, CPU8min18.201s, memory1.4G, swap0B; no test executed and no bottleneck attribution or generic fix proven. Documentation-only update, no new build initiated, diagnostics branch preserved outside main.
- 2026-10-12 command receipt completed: read final section of consumer57426e363d413d3f29b16665459605683e9ad52f `.scratch/offline-first-nodes/evidence/08-full-nodes.md` and copied the coordinator-reported invocation verbatim into owner evidence. Receipt revision is distinct from attempted source437c9bf. Command provenance gap resolved; OOM/no-test/no-attribution and memory/review prerequisites unchanged. No runs.
- 2026-10-12 qualified integration: repeated native catalog reconstruction was measured and corrected through reviewed source cce094c/89e85d3/46fe260/2bbb158/defd321. Consumer four-run qualification below passed at one unchanged source with materially larger observed HTTP margins; earlier OOM/timeouts remain failures. Created `integration/native-catalog-reuse` from diagnostic3ec1055, preserving that diagnostic ref exactly; original root now exposes cleanup source72aae34 to linked consumers. Temporary timers/module and logging-only dependency edges removed; all corrections/tests/receipts retained. No main merge/push/provider/cache changes.

## Answer

**Resolved for the measured generic owner correction and profiling-retirement scope.** [Qualified integration receipt](../evidence/16-qualified-integration.md) records exact source, bounds, commands, statuses, review provenance and limitations. Main remains8a848b2; the prepared integration branch is not merged.

- Measurement isolated repeated current-catalog/layout reconstruction, not a proven networking defect/deadlock. Admission records lack holder correlation; no unsupported holder attribution or transaction/permission shortcut was introduced.
- Source reviews independently accepted (user/consumer receipt-reported): per-scan layout cce094c, same-operation field reuse89e85d3, identity reuse46fe260, raw-table reuse2bbb158, SELECT raw-catalog reuse defd321. Public facade unchanged; low-level RowCodec callers must adapt to catalog inputs. No fresh independently performed cleanup review is claimed.
- Public RED/GREEN performance checks and semantic native/in-memory cases retain complete raw tables/fields, largestUUID/default/fallback/error behavior, malformed-unrelated-before-key validation, tombstones and decoding of nonmatching malformed rows. No caches across operations or checks/deadlines weakened.
- Actual consumer source **be22adc86e1b1a375f65b0d544fde08f54a434ed**, original owner **3ec105590794331d62f67f33ccc4a7f56e623351**: unchanged restrictions30 **265.88s**, offline31 **223.51s**, grants32 **247.39s**, one explicit restrictions repeat33 **267.68s**, each1pass/0fail/exit0 with ordinary pair teardown. HTTP server-middleware maximum6.988472556s is not a client slack/SLA guarantee. Receipt a361aca [08-reviewed-select-correction-rerun.md](../../../../of/.worktrees/ticket08-full-nodes/.scratch/offline-first-nodes/evidence/08-reviewed-select-correction-rerun.md); historical genuine restrictions26 failure at earlier e15424f retained, not matrix-deferred or relabeled.
- Cleanup source **72aae34b75f0fd65f4feadfb49e13ae948b41f6c** on qualified branch passed requested focused validation after timer removal: **9 native facade +3 catalog semantic +18 sync catalog tests**, no_std engine/sync, strict affected-library and selected-test Clippy, formatting/whitespace. All serial4G/swap0/runtime900/jobs1/offline/locked/original shared target. No Cargo Hack/CRAP.
- Diagnostic branch preserved at exact **3ec105590794331d62f67f33ccc4a7f56e623351**. Cleanup manifests/owner lockfile and B-tree implementation match main; final runtime diff is corrections, not timers. Historical source, raw logs/review/receipts retained.

**Limits:** passing full-pair consumer runs precede instrumentation cleanup; no new consumer run on cleanup source is claimed. Qualification is machine-local representative behavior, not absolute10s reliability, arbitrary mutable kernels, all malformed-store failures, foreign platforms/providers/deployment or full-spec completion. Parent final review/main integration remains separately authorized; of ticket08/spec are not closed here.
