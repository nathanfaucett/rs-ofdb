# Explicit application SQL migrations

Status: ready-for-agent

## Problem Statement

Applications need to change their SQL schema without deleting existing data or writing their own migration runner. The password manager demonstrates this need: it creates tables with `CREATE TABLE IF NOT EXISTS`, but stores no application-schema version or migration history. Opening a vault assumes that its tables already have the current columns. Repeatable table creation does not upgrade or validate an existing schema.

ofdb already has Query Handles, explicit transactions, atomic catalog and data changes, queryable Schema Definitions, and `ALTER TABLE ADD COLUMN`. It has no shared interface for ordering application migrations, tracking completion, or rejecting changed migration definitions. Each application must otherwise implement these rules itself.

Application-schema migration is distinct from SQL Storage Compatibility. This feature does not upgrade incompatible physical storage or replicated payload formats.

## Solution

Provide an explicit migration operation on the SQL Query Handle, exposed through the Rust SQL facade and the TypeScript SQL binding. The caller supplies the complete ordered list of forward SQL migrations. The runner validates that list against stored history, executes pending migrations, and records each completed migration atomically with its changes.

Applications call the operation before using the schema that requires it. Opening a Database does not run migrations. Applications own migration SQL and decide when an upgrade is permitted. ofdb owns ordered execution, immutable history validation, transaction cleanup, and failure reporting.

Replication and sync behavior belong to the existing sync modules. They impose no additional requirements on this migration module.

## User Stories

1. As an application author, I want to initialize a new database through the same migration list used for upgrades, so that I maintain one schema path.
2. As an application author, I want to upgrade an existing database without resetting it, so that stored user data remains available.
3. As an application author, I want migrations to execute in version order, so that later migrations can depend on earlier changes.
4. As an application author, I want repeated migration calls to skip completed migrations, so that startup does not repeat data transformations.
5. As an application author, I want schema changes, data changes, and their completion record to commit together, so that history cannot report a partial migration as complete.
6. As an application author, I want a failed migration to stop the sequence, so that later migrations do not run against an incomplete schema.
7. As an application author, I want previously committed migrations to remain recorded after a later failure, so that a corrected call can resume at the pending migration.
8. As an application author, I want changed SQL for an applied migration to be rejected, so that two releases cannot silently assign different meanings to one version.
9. As an application author, I want invalid ordering, duplicate versions, and missing versions to be rejected, so that an ambiguous migration plan cannot change data.
10. As an application author, I want unknown stored migrations to be rejected, so that an older application cannot treat a newer schema as supported.
11. As an application author, I want malformed or unrelated migration metadata to be rejected without repair, so that the runner does not overwrite existing state.
12. As a Rust caller, I want migrations through the SQL Client, so that embedded and remote query use retain the same interface.
13. As a TypeScript caller, I want the same migration rules through the SQL Client, so that browser applications do not implement a second runner.
14. As an application author, I want concurrent calls on one backing store to use existing transaction conflict behavior, so that a stale call cannot independently commit the same migration.
15. As an application author, I want an uncertain commit to be reported without automatic replay, so that a lost response does not repeat a data transformation.
16. As an application author, I want SQL failures to identify the migration version without exposing SQL or parameter values, so that I can diagnose an upgrade failure safely.
17. As an application author, I want an explicit no-change result when all migrations are already applied, so that I can proceed with ordinary queries.
18. As an application author, I want unsupported schema operations to fail through normal SQL errors, so that the migration interface does not imply broader DDL support.

## Implementation Decisions

- Add one dedicated SQL migration module behind an explicit SQL Client migration operation. Keep Database responsible for construction, hosting, and sync setup; do not add a duplicate Database migration operation.
- Use the existing explicit transaction interface for both embedded and remote clients. Do not add a migration server, transport protocol, CLI, or transaction implementation.
- Implement execution and history rules once in Rust. Expose that implementation through the existing Wasm and TypeScript binding patterns rather than implementing a second runner in TypeScript.
- Each migration contains an integer version and a nonempty SQL statement batch. Versions start at one and must be contiguous and strictly increasing. The supplied list is the complete history through the caller's supported version, not only pending entries.
- An empty list is valid only when no migration history has been applied. It must not initialize metadata in an otherwise unmigrated database, and must reject existing applied versions unknown to the caller.
- Validate the full supplied list before starting mutations. Execute supplied SQL through the existing SQL translator and transaction execution path.
- Store runner-owned history in an ordinary SQL table with a documented name. Use one record per applied version with a deterministic fingerprint of the exact SQL text. Whitespace changes count as changed contents. Reuse an existing suitable fingerprint implementation where possible; do not add an unrelated framework.
- Validate the metadata table structure and history before executing pending SQL. Reject incompatible table-name collisions, malformed records, duplicate or missing versions, fingerprint mismatches, and stored versions absent from the supplied list. Do not reset, repair, overwrite, or infer missing history from existing application tables.
- Read and validate current history within the transaction that decides whether to run a migration. Do not rely on a pre-transaction check for concurrency control.
- Execute each pending migration and insert its completion record in one Engine Transaction. Initialize metadata within the first pending migration transaction when necessary. A rejected migration must leave neither partial application changes nor a false completion record.
- Commit one migration at a time. Earlier successful migrations remain committed if a later migration fails. Stop immediately on failure.
- After all migrations complete, return the supported current version and the versions applied by this invocation. An already-current call returns no applied versions and does not mutate history.
- Reuse existing transaction serialization or revision-conflict rejection for concurrent calls. Do not add locks, leases, or automatic conflict retries. A caller may explicitly invoke migration again with the complete unchanged list.
- A commit rejection can be uncertain. Preserve this distinction using existing error conventions, report the affected version, and do not automatically replay SQL. A subsequent explicit invocation reads history again before deciding what remains pending.
- Preserve structured underlying query and commit errors. Add migration validation context without parsing error messages or including SQL text, credentials, or data values.
- Always clean up uncommitted transaction handles on failure. Respect existing Query Handle deadlines and commit uncertainty semantics; do not invent a separate timeout policy.
- The first version supports SQL batches only. Application code that requires decryption keys or other application-specific data transformations remains outside the runner.
- Support only DDL and DML already supported by the SQL engine. In particular, current ALTER support is limited to ADD COLUMN. Do not implement rename, drop-column, type-change, or table-rebuild machinery as part of this feature.
- Keep facade entry modules thin and place implementation in dedicated modules. Update public documentation and domain terminology as needed during implementation.

## Testing Decisions

- Use the existing SQL Client as the external test seam. Tests must assert observable data, schema, history validation, results, and errors. Do not test private helper structure or fingerprint internals as the primary proof.
- Use existing SQL facade integration tests for Client transactions, local SQL schema evolution, remote queries, and persistence reopening as prior art.
- Cover fresh initialization, sequential upgrades, already-current calls, and empty lists against both empty and migrated databases.
- Verify that data survives an additive schema migration and that existing rows receive the engine's documented default behavior.
- Use a migration with a data transformation to prove that repeated calls do not execute applied work again.
- Make a migration change data or schema and then fail. Verify that its changes and completion record are absent, that later migrations did not run, and that earlier committed migrations remain intact.
- Reject invalid input ordering, duplicate versions, gaps, empty SQL, changed applied SQL, unknown stored versions, malformed history, and incompatible metadata-table collisions without application-data changes.
- Verify that a successful migration cannot commit without its history record and that failed history insertion rolls back that migration's changes.
- Verify uncertain commit handling without relying on message matching. A subsequent explicit call must inspect committed history and skip any migration that actually committed.
- Exercise concurrent migration attempts using existing backend transaction semantics. At most one attempt may commit each migration on that backing store; the other must observe completed history or receive the existing conflict/error result. Do not require transparent retries.
- Run equivalent public behavior through embedded and remote Rust Query Handles, with persistence-reopen coverage for native storage.
- Add real-browser TypeScript binding tests for initialization, upgrade, repeated invocation, failure rollback, and concurrent IndexedDB revision rejection. Follow the existing browser SQL harness rather than introducing another test interface.
- Verify structured migration context at the TypeScript binding, including version identity and preserved commit uncertainty. Do not expose SQL or stored secret values in error messages.
- Run focused tests during implementation. Final validation includes formatting, applicable Clippy checks, `cargo hack test --feature-powerset --all-targets`, `just crap`, and the affected TypeScript/browser checks. Record any environment blockers without claiming unrun checks passed.

## Out of Scope

- Replication, sync policy, peer coordination, or mixed-client-version policy. These remain concerns of the existing sync modules.
- Automatic physical database or wire-format migration.
- Automatic migration during Database construction or open.
- Down migrations, automatic downgrade, schema diffing, schema generation, and migration file discovery.
- New ALTER operations or general table-rebuild support.
- Callback migrations, decryption, encryption-key rotation, and application-specific content conversion.
- Automatic retries after query failures, conflicts, deadlines, or uncertain commits.
- Password-manager conversion, unrelated vault fixes, and its stale README cleanup. The app provides the motivating case; this feature belongs to ofdb.
- A new CLI, deployment system, backup system, or migration administration interface.

## Further Notes

The password manager source currently uses browser SQL, while its README describes an older application-provisioned schema. Base this work on current source behavior, not that README.

The proposed test seam is the existing SQL Client in Rust and TypeScript. It was presented to the user for confirmation when this spec was published.

Complexity: moderate. Reuse the existing transaction and binding paths; avoid parallel execution or history implementations. Risk: high because migration SQL changes persisted user data. Priority: establish the shared runner before applications duplicate migration history and failure handling.

No implementation or runtime validation was performed when this spec was written.
