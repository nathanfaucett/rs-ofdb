# Plan: Remove Table/Column/Index Generation UUIDs

## Goal and completion rule

Replace table, column, and index generation UUIDs with names everywhere in the project: table and index identity is the name; column identity is `(table name, column name)`. Keep row UUIDs, row tombstones, replication, and conflict resolution.

This is an intentional, sweeping breaking change across the entire project—not a staged or compatibility-preserving refactor. It includes kernel contracts and backends, every dependent crate and public API, storage layout, catalogs, codecs, replication/sync messages, serialized data, examples, tests, and active documentation. Intermediate commits and work-in-progress states are allowed to be inconsistent, uncompilable, and to contain a mixture of old and new representations. Do not revert a required breaking change or add a bridge merely to make an intermediate state compile. The completed migration must update all affected consumers and land in a coherent, buildable state with no old/new mixed mode. Change or delete old interfaces and representations rather than preserving them. There must be **no backwards compatibility, adapters, compatibility aliases, dual-read/write behavior, or migration path** for old APIs, persisted catalogs/data, serialized changes, or wire messages. Existing data may be discarded; do not add migration tooling.

Complete this plan only when **every checkbox below is verified**. Check off each item only after its stated evidence is available; do not mark an item complete because a related edit merely compiles. Do not run `cargo hack` or `cargo test --all-targets` for this plan.

## Breaking-change constraints

- Treat this as a project-wide redesign, not a compatibility-preserving refactor. Breaking intermediate work is permitted: individual edits or commits may leave the workspace uncompilable or inconsistent while dependent crates are being converted. Do not revert such changes or introduce temporary compatibility code just to restore intermediate compilation. Before considering the migration complete, update all affected workspace crates—including kernel implementations and every consumer—plus public APIs, persisted representations, wire formats, examples, tests, and active docs; the final workspace must be coherent and buildable. Breaking downstream users and making prior persisted data/protocol messages unusable are expected.
- Use table names directly as storage table identifiers throughout kernel, codecs, catalog, and sync APIs. Do not derive UUIDs or maintain a name-to-UUID mapping. Keep UUIDs only for row identity where required by the existing row model.
- Do not retain old types, methods, fields, serialized formats, protocol fields, or code paths to support prior versions. Do not add compatibility aliases, adapters, fallback parsing, dual formats, or migration code. Replace or delete old representations outright.
- No catalog/data migration is required; old stored data and serialized changes may become unreadable and may be discarded/reset.
- Preserve row-level deletion semantics. Do not remove schema deletion checks until name-based schema facts and drop/recreate behavior provide equivalent handling of deleted catalog rows.
- Prefer `no_std`-compatible existing code and dependencies. Keep `lib.rs` and `mod.rs` thin.

## Execution loop

The next unchecked item begins the coordinated breaking migration. Work may proceed in slices, and any slice—including changing `KernelTransaction` before all callers are converted—may temporarily leave the workspace uncompilable or inconsistent. This is acceptable and is not a reason to revert, stop, or add UUID/name adapters. Continue converting dependent code across schema facts/lookups, kernel backends, engine, codecs, storage, sync, tests, and examples as described in phases 2–4 until the final affected workspace builds and the focused tests pass. Phase 2–4 checkboxes are detailed implementation/verification tasks; they can be completed and checked off independently when their own stated evidence is available, even while other migration work remains incomplete. The phase 1 coordinated migration checkbox is a final gate for the complete cross-crate conversion, not a prerequisite that requires every dependent edit to be made in one commit. Then verify drop/recreate behavior and remaining items, checking off each only with its stated evidence. Search the entire workspace, examples, and active docs for consumers of removed interfaces; if an API or file differs from the repo, update this plan before checking its item off.

### 1. Establish baseline and identity rules

- [x] Inspect `git status` and relevant existing tests; record any pre-existing failures without overwriting user changes. **Verify:** status is known before edits. Baseline: only the four plan/design docs were modified.
- [x] **Coordinated name-identity migration (implement phases 2–4 together):** replace generation-based schema facts and lookups first or alongside their callers, then switch `KernelTransaction`, the in-memory and redb kernels, catalog storage identifiers, `BytesTable`/`RowTable`, index storage, executor, changes, codecs (including Automerge), state transfer, sync, tests, and examples to table names. Do not derive UUIDs from names or leave an old/new bridge. Keep row UUIDs and tombstones. Redb table names must be the logical names; in-memory tables must be keyed by names. **Verify:** `cargo check --workspace --all-targets` exits 0; focused kernel tests prove distinct names isolate equal row UUIDs and reopening redb uses the same name; source search finds no UUID table identifiers or UUID-to-name routing in kernel APIs/backends. This item stays unchecked until its whole cross-crate check passes; verify the more specific phase 2–4 items separately.
- [x] Specify and test the intended drop/recreate semantics for the same name: drops hide the table and index schema; same-name recreation retains row data and row tombstones and rebuilds index data; a tested concurrent replicated drop/recreate resolves to drop-wins. **Verify:** `cargo test -p engine-automerge` passes, including drop/recreate and replica-conflict regressions.

### 2. Change catalog and schema identity

- [x] Change `crates/engine/src/schema.rs` `SchemaChange` variants and catalog facts to use table, column, and index names; use `(table, column)` for column identity and name-based references for index fields. **Verify:** schema facts can be materialized, exported, and re-imported without generation UUID references.
- [x] Replace `table_id`, `column_id`, `index_generation_id`, and related ID/label lookups in `schema.rs` and `index.rs` with name-based lookups returning the schema information callers need. **Verify:** `cargo test -p engine --features in-memory` passes schema tests for creation/retrieval, missing names, and deleted table/column/index visibility.
- [x] Rework catalog deletion/visibility checks rather than simply deleting `table_deleted`, `column_deleted`, and `index_deleted`. **Verify:** `cargo test -p engine --features in-memory` passes schema tests for hidden deleted names and recreation; Automerge integration tests cover drop/recreate and index visibility.
- [x] Update `crates/engine/src/catalog.rs` and `crates/engine/src/state_transfer.rs` so catalog keys, referenced values, `RowMutation`, internal catalog storage identifiers, and active-table enumeration all use names consistently. Remove obsolete UUID storage identifiers and UUID-valued schema references. **Verify:** export/import and mutation tests pass with name-based catalog rows.

### 3. Change execution and change application

- [x] Update `crates/engine/src/executor.rs` create/add/drop/alter table and create/drop index operations to resolve names without generating schema identity UUIDs. **Verify:** `cargo test -p engine-automerge` passes DDL creation, drop/recreate, and index maintenance coverage.
- [x] Update insert, update, delete, select, index maintenance, and `crates/engine/src/change.rs` (`Change::row`, `ChangeKey::Row`, change application) to use table names; keep row UUIDs. **Verify:** DML, indexing, tombstone, and conflicting-change tests pass.
- [x] Remove the generation lookup methods from `crates/engine/src/engine.rs`; use name lookups or inline logic at all call sites. **Verify:** engine public-API tests compile and pass.

### 4. Change codec, storage, and sync

- [x] Update `crates/engine/src/codec.rs` `RowCodec` and related sync codec methods to use table names directly as table identifiers; keep row UUIDs as keys within each table. **Verify:** `codec_routes_rows_by_table_name_across_instances_and_reopen` passes, covering equal row UUID isolation across named tables and reads after opening a fresh codec on the same storage.
- [x] Update `crates/engine-automerge/src/codec.rs` and its tests, including schema-column document keys and row/change inventory routing. **Verify:** Automerge persistence, incremental changes, and row tombstones pass.
- [x] Update `crates/sync/src/state.rs`, `protocol.rs`, and `session.rs` to use name-based catalog and row keys/messages and active-table enumeration. **Verify:** state transfer, snapshot, incremental replication, and conflict tests pass between independent replicas.
- [x] Remove `crates/engine/src/id.rs`, its module/re-exports, and every remaining `TableGenerationId`, `ColumnGenerationId`, and `IndexGenerationId` use. **Verify:** a source search across `crates/` finds no remaining symbols or generation lookup APIs.

### 5. Align documentation

- [x] Update `docs/design.md` to describe name identity, name-keyed storage, catalog deletion, and row tombstones. **Verify:** no design claim requires generation UUIDs or name-derived storage IDs.
- [x] Update `docs/goal.md` to require name-based schema identity. **Verify:** no generation identity requirement remains.
- [x] Verify `docs/grpc-protocol-plan.md` does not define the sync wire protocol; it specifies only the query RPC and SQL/schema messages. Leave it unchanged rather than inventing sync fields there. **Verify:** the active plan contains no generation-ID protocol requirements; sync name keys are specified by `crates/sync/src/state.rs` and `protocol.rs`.
- [x] Search other active docs for obsolete generation-ID claims and update only applicable references. **Verify:** `docs/kv-plan.md` uses generations only for the separate key/value store; UUID references in `docs/design.md`, `docs/goal.md`, and `docs/grpc-protocol-plan.md` describe row primary keys or query values, not schema identity.

### 6. Final verification

- [x] Run `cargo fmt --all -- --check`; fix formatting in changed code. **Verify:** exit status 0.
- [x] Run relevant engine, engine-automerge, and sync tests, including their integration tests (use package-level commands without `--all-targets` as constrained above). **Verify:** each applicable command exits 0; inspect package names before running.
- [x] Inspect the final diff and search `crates/` and active docs for obsolete generation identity and UUID-keyed schema references. **Verify:** final `git diff --check` passes; search finds no generation-ID types/lookups or UUID-to-name routing. Remaining UUID references are row identity; all plan items are checked.

## Exit condition

All boxes checked with evidence, name-based schema identity throughout engine/storage/sync, and row tombstones and conflict resolution intact. If blocked by the environment, leave unverified boxes unchecked and report the exact check and blocker.
