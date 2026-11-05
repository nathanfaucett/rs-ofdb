# 01: Run ordered migrations through the SQL Client

**What to build:** Applications explicitly initialize and upgrade their SQL schema through the Query Handle. One Rust migration runner serves embedded and remote Clients and is exposed through the Wasm and TypeScript bindings. Applications supply a complete ordered migration list; completed work is skipped, and each pending migration commits atomically with its history record. Follow the parent SQL migrations spec in this feature directory. This ticket delivers the valid-plan path and transaction rollback; tickets 02 and 03 complete validation and failure handling.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Add an explicit SQL Client migration operation in Rust and TypeScript. Database construction and open do not run migrations or expose a duplicate migration operation.
- [ ] Accept a complete ordered list of forward migrations with integer versions starting at one and nonempty SQL batches. Store deterministic fingerprints of exact SQL text; whitespace changes count as changed contents. Keep one Rust implementation of execution and history rules.
- [ ] Store one completion record per applied version in an ordinary SQL table with a documented runner-owned name. Use existing SQL translation, explicit transactions, query errors, and binding patterns. Keep facade entry modules thin.
- [ ] Read history inside the transaction that decides whether to apply a migration. Commit each migration's SQL and history record in one Engine Transaction, including metadata initialization with the first migration when needed.
- [ ] Apply pending migrations in order, one commit per migration. Return the supported current version and the versions applied by this invocation. An already-current call reports no applied versions and does not change history.
- [ ] An empty list with no applied history returns version zero without creating metadata. Existing application tables do not imply applied migrations.
- [ ] A failed SQL batch or failed completion-record insertion rolls back that migration's schema and data changes, stops later migrations, and preserves earlier completed migrations. Release uncommitted transaction handles on failure.
- [ ] Repeated calls do not repeat data transformations. Use only SQL operations already supported by the engine; do not extend ALTER support or add callback migrations.
- [ ] Verify fresh initialization, sequential upgrades, default values on existing rows, repeat calls, empty calls, failure rollback, and persistence reopening through the public Rust Client. Cover embedded and remote query use with existing test patterns.
- [ ] Verify initialization, upgrade, repeated calls, and rollback through the public TypeScript Client in the existing real-browser SQL harness. Do not add a second runner or test-only public seam.
- [ ] Document explicit invocation, per-migration atomicity, result semantics, history ownership, supported SQL limits, and the separation from physical storage-format migration. Replication and sync policy are out of scope.
- [ ] Run focused Rust and TypeScript/browser checks. Record commands, results, and environment blockers without claiming unrun checks passed.
