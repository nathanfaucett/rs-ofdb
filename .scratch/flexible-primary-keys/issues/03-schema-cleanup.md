# 03: Schema Cleanup (Remove UUID restriction)

**What to build:** Remove the legacy `validate_uuid_primary_key` method from `TableSchema` and clean up the `sql-schema` crate, as the new `validate_primary_key` is now the standard.

**Blocked by:** 02: Engine Implementation (Primary Key Indexing)

**Status:** done

- [x] Remove `validate_uuid_primary_key` from `TableSchema`.
- [x] Cleanup any remaining legacy references in `sql-schema`.

## Comments

### 2026-10-23 — Cleanup verified

- Read the worktree `AGENTS.md`, issue-tracker conventions, glossary, feature spec, and completion comments for tickets 01 and 02. Started branch `flexible-primary-keys-cleanup` at integration tip `d4d4478`.
- Ticket 01 (`f8ae8e0`) already removed the legacy validator outright and updated the engine caller, honoring the no-wrapper rule. Reviewed that diff and all `sql-schema` source: no obsolete UUID-only or single-column primary-key assertions remain. `rg -n 'validate_uuid_primary_key' crates docs README.md GLOSSARY.md` returned no matches; remaining mentions in the feature spec/tickets are historical task descriptions, not code references.
- The only production `validate_primary_key` caller is the engine executor; no engine implementation changes were needed or made.
- Loaded the TDD skill and reviewed the ticket-established public `TableSchema::validate_primary_key` seam. Existing tests cover all eight supported non-null types, mixed-type and UUID-only composites, null components in either position, absent keys, and non-key null types. No coverage gap found; no redundant tests or artificial red/green cycle added.
- PASS: `cargo test -p ofdb-sql-schema --lib` (5 tests).
- PASS: `cargo test -p ofdb-sql-schema --lib --no-default-features` (5 tests).
- Risk/complexity: low, verification and tracker updates only; no production code or dependencies changed. Full feature-matrix testing and `just crap` were not run, as requested. Feature-wide verification remains pending; this closes implementation ticket 03 only.
