# 02: Reject invalid migration plans and history

**What to build:** Applications receive a clear rejection when the supplied migration plan or stored history is incompatible. The migration operation fails without resetting metadata, inferring missing history, or changing application data. Apply the same Rust-owned rules through the Rust and TypeScript Query Handles. Follow the parent SQL migrations spec in this feature directory.

**Blocked by:** 01 — Run ordered migrations through the SQL Client.

**Status:** ready-for-agent

- [ ] Validate the complete supplied list before starting mutations. Reject duplicate versions, gaps, invalid ordering, invalid version values, and empty SQL batches.
- [ ] Validate the metadata table structure and all stored history before executing pending SQL. Reject incompatible metadata-table collisions, malformed records, duplicate or missing versions, and fingerprints that do not match the supplied exact SQL text.
- [ ] Reject stored versions absent from the supplied list, including an empty list against an already-migrated database. Do not treat an older application's migration list as a downgrade request.
- [ ] Do not repair, overwrite, delete, or reset incompatible history. Do not infer migration completion from application table contents or Schema Definitions.
- [ ] Make rejected calls leave application data and history unchanged. Validate stored history within the transaction used to decide whether to apply work, not only in a separate preflight read.
- [ ] Preserve structured underlying query errors and add useful migration validation context using existing error conventions. Identify the migration version when applicable without logging or exposing SQL text, credentials, or stored values.
- [ ] Test each rejection through the public Rust Client, including embedded and remote query use. Assert unchanged schema, application data, and history rather than private helper behavior.
- [ ] Test TypeScript input and error propagation through the public Client and existing browser harness. Binding validation must not replace authoritative Rust validation.
- [ ] Document immutable migration contents, full-list submission, metadata validation, and rejection of newer or malformed history. Keep replication and sync policy out of scope.
- [ ] Run focused tests and applicable formatting and lint checks. Record commands, results, and environment blockers.
