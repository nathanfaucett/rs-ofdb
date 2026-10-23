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

Tickets 01–03 are complete. Ticket 03 verified that ticket 01 already removed the legacy validator and obsolete schema assertions; the existing schema tests pass with default features and without default features. Feature-wide verification remains pending; focused ticket checks do not complete that phase.
