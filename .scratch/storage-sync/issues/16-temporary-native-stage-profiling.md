# 16 — Temporary native transaction and conflict-inspection profiling

Status: ready-for-agent
Type: task
Work status: source implemented; coordinator review/run authorization pending
Owner: ofdb generic instrumentation/evidence only
Priority: High
Complexity / risk: Low / medium (observer overhead and interpretation)

## Scope

On an exact main-based dedicated root branch, measure native transaction admission versus begin/body/commit/rollback and public conflict-inspection schema/catalog/Automerge reconstruction. Reuse the consumer's stage-only opt-in logger; fixed labels, elapsed/counts, no SQL/IDs/data. No generic optimization without measured evidence, no consumer writes or dependency wiring changes, no permission/persistence/deadline changes.

## Acceptance / pending evidence

- Source-only implementation and review recorded in [measurement handoff](../evidence/16-native-idp-stage-timing.md).
- Coordinator must review the stable instrumentation commit, authorize exclusive shared-target jobs1 build with 2G memory cap, and approve consumer lock dependency-edge refresh.
- The next execution is the SAME exact existing consumer probe at d2720cb, not a new test/workload or broad owner suite. No build/test is authorized by this ticket.
- Record measured native versus schema/catalog/reconstruction costs before proposing a generic fix. No GREEN or integration acceptance inferred from instrumentation.

## Comments

- 2026-10-12: Verified clean owner main bad3071 and clean consumer d2720cb. Dedicated owner root branch `diagnostics/native-idp-stage-timing`, no worktree/configuration change. Existing evidence proves slow request-path loads but does not yet isolate an owner cause. Source-only handoff; runtime evidence and any fix remain pending. Existing tickets/spec/map statuses untouched.
