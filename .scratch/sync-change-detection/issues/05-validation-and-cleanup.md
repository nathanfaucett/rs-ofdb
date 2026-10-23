# 05 — Validate convergence, simplify and update documentation

Status: Planned

Depends on: 04

## Work

- Run the full scenario matrix in [the specification](../spec.md), including interruption/retry, snapshot rewriting/compaction, metadata-only changes, dependency recovery, multiple peers, subscription gaps and concurrent commits during reconciliation.
- Compare issue 01 baseline counts with selective SQL and KV paths. Document retained broad-scan cases and any limitation on remote-change delivery.
- Remove obsolete traits, wrappers, duplicate discovery paths and stale exports rendered unnecessary by the implementation. Keep SQL-specific validation and KV-specific generation semantics; do not consolidate crates for symmetry.
- Update `docs/design.md`, `docs/goal.md`, `GLOSSARY.md` and affected crate documentation to implemented behavior. Resolve stale protocol versions and references to removed planning files only where relevant to this work. Follow domain documentation instructions when editing definitions.
- Document deliberate interface/wire changes and feature requirements without adding compatibility glue.
- Assess final complexity/risk and run final validation once implementation is complete.

## Acceptance

Focused correctness and performance assertions pass. Current documentation distinguishes committed wakeups, changed-identity invalidation, document history and Sync Sessions. No unbounded buffering or durable replay promise was introduced accidentally. Final checks are recorded with failures/blockers clearly separated from successful results.

## Final validation

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo hack test --feature-powerset --workspace --all-targets`
- `just crap`

These commands validate implementation, not this Markdown-only plan. Do not claim they ran during planning.

## Comments
