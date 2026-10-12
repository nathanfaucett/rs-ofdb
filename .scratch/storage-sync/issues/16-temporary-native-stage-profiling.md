# 16 — Temporary native transaction and conflict-inspection profiling

Status: ready-for-agent
Type: task
Work status: source approved; coordinator build OOM before test; runtime evidence blocked
Owner: ofdb generic instrumentation/evidence only
Priority: High
Complexity / risk: Low / medium (observer overhead and interpretation)

## Scope

On an exact main-based dedicated root branch, measure native transaction admission versus begin/body/commit/rollback and public conflict-inspection schema/catalog/Automerge reconstruction. Reuse the consumer's stage-only opt-in logger; fixed labels, elapsed/counts, no SQL/IDs/data. No generic optimization without measured evidence, no consumer writes or dependency wiring changes, no permission/persistence/deadline changes.

## Acceptance / pending evidence

- Source-only implementation and review recorded in [measurement handoff](../evidence/16-native-idp-stage-timing.md).
- Coordinator reports reviewer42596023 approved source b1d957d with no blockers; consumer437c9bf records the lock dependency-edge refresh.
- Coordinator attempt01 used 1500M/zero-swap/900s limits and was OOM-killed during compilation; the SAME existing consumer probe never ran. Literal executed invocation is now recorded in owner evidence from consumer receipt57426e363d413d3f29b16665459605683e9ad52f; no remaining command provenance gap.
- Runtime evidence is blocked on sufficient free memory (coordinator reports approximately1.9GiB available, swap full) and fresh exclusive-slot authorization. No build/retry is authorized by this ticket.
- Record measured native versus schema/catalog/reconstruction costs before proposing a generic fix. No GREEN or integration acceptance inferred from instrumentation.

## Comments

- 2026-10-12: Verified clean owner main bad3071 and clean consumer d2720cb. Dedicated owner root branch `diagnostics/native-idp-stage-timing`, no worktree/configuration change. Existing evidence proves slow request-path loads but does not yet isolate an owner cause. Source-only handoff; runtime evidence and any fix remain pending. Existing tickets/spec/map statuses untouched.
- 2026-10-12 attempt01 receipt: consumer437c9bf / ownerb1d957d; source reviewer42596023 approved, observer effects retained. Log reports oom-kill, runtime8min25.181s, CPU8min18.201s, memory1.4G, swap0B; no test executed and no bottleneck attribution or generic fix proven. Documentation-only update, no new build initiated, diagnostics branch preserved outside main.
- 2026-10-12 command receipt completed: read final section of consumer57426e363d413d3f29b16665459605683e9ad52f `.scratch/offline-first-nodes/evidence/08-full-nodes.md` and copied the coordinator-reported invocation verbatim into owner evidence. Receipt revision is distinct from attempted source437c9bf. Command provenance gap resolved; OOM/no-test/no-attribution and memory/review prerequisites unchanged. No runs.
