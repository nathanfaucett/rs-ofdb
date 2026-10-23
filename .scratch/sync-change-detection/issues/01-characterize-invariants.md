# 01 — Characterize correctness and discovery cost

Status: Planned

## Purpose

Establish executable evidence before changing interfaces or commit machinery. Read [the specification](../spec.md) first.

## Work

- Trace SQL local/explicit transactions, sync apply, schema cleanup, unique-key conflict tombstones and KV writes/imports to their real commit points. Identify every mutation path that could bypass logical change tracking.
- Inspect existing transaction `changes`, `MutationSummary`, backend commit notification and tests before inventing new bookkeeping.
- Add focused characterization tests for snapshot merge, metadata-only changes, combined metadata/document changes, multi-change incremental payloads, dependency order, failed/rolled-back transactions and snapshot rewriting/compaction.
- Establish how standalone incremental-key labels differ from actual Automerge change identities; test history available inside a snapshot versus separately exportable stored deltas.
- Capture deterministic baseline counts for an established SQL peer reconciling a one-row edit: document reconstruction, snapshot export and history enumeration.
- Record which assumptions pass, which are unverified, and any reproducible existing defects. Do not silently lock an existing correctness defect into the desired contract.

## Acceptance

The mutation-path inventory identifies a safe publication point and conservative fallback for untracked paths. Tests demonstrate the identity/metadata/history cases or expose a concrete blocker. Baseline measurements and the smallest proposed end-to-end slice are recorded under Comments. No production crate moves occur in this issue.

## Validation

Run the relevant existing and new tests for the touched packages with their required features. Final workspace feature-powerset and CRAP validation belongs to issue 05.

## Comments
