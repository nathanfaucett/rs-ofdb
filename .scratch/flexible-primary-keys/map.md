# Flexible Primary Keys - Roadmap

This map provides a high-level overview of the implementation plan for the Flexible Primary Keys feature.

## Overview

[View the full specification](./spec.md)

## Implementation Roadmap

The work is broken down into the following tickets:

1. **[01: Schema Expansion](./issues/01-schema-expansion.md)** - Adds the foundation for flexible validation.
2. **[02: Engine Implementation](./issues/02-engine-implementation.md)** - Implements the core logic of automatic unique index creation for non-UUID primary keys.
3. **[03: Schema Cleanup](./issues/03-schema-cleanup.md)** - Removes legacy UUID-specific validation.

## Status

- [x] Specification defined
- [x] Implementation started
- [x] Implementation complete
- [ ] Verification complete

Tickets 01–03 are complete. Ticket 03 verified that ticket 01 already removed the legacy validator and obsolete schema assertions; the existing schema tests pass with default features and without default features. The four independently reproduced review findings are now fixed; see ticket 02's review-fix evidence below. Feature-wide verification remains limited by the unchanged workspace feature failure and complexity timeout, not by outstanding findings.

## Review-fix validation — 2026-10-23

- All four findings have red/green regressions: flexible-key bootstrap/incremental sync, atomic rejection of replicated primary-index collisions, natural/composite conflict inspection and resolution (including transaction reads), and dropping redundant matching unique indexes without dropping final enforcement.
- PASS: affected engine, Automerge engine, and SQL sync unit/integration suites — 139 tests. PASS: changed-crate Clippy with `--all-targets --no-deps -- -D warnings`, workspace formatting check, and diff whitespace check. Full commands and behavioral notes are in [ticket 02](./issues/02-engine-implementation.md).
- Known limits from coordinator validation: workspace `cargo hack test --feature-powerset --all-targets` fails in unchanged B-tree sharded/no_std code; `just crap` timed out at 180/300 seconds. Neither broad check was rerun, and no unrelated feature or lint fixes were made. The full verification checkbox remains open rather than asserting these gates passed.
