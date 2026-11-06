# 02: Add typed reads to SQL Transactions

**What to build:** Let npm callers use `Transaction.select` with the same inferred types, parser contract, tagged cells, and failures as `Client.select`. Preserve explicit caller ownership of transaction commit, rollback, and close. Qualify the complete typed-read feature through installed package consumers.

**Blocked by:** 01: Add typed reads to the SQL Client.

**Status:** ready-for-agent

- [ ] Add `Transaction.select` with the same argument order, synchronous parser input, inferred array element type, and structured failure contract as `Client.select`.
- [ ] Reuse the shared internal mapping/parsing implementation and the existing transaction execution path. Do not duplicate decoding logic, add a transaction-specific schema interface, or add a WASM method.
- [ ] Preserve existing parameter encoding, transport validation, inherited deadlines, one-operation-at-a-time rules, and owned-handle lifecycle behavior.
- [ ] Apply the same exact-name mapping, duplicate-name rejection, safe property-key handling, tagged-value preservation, one-result requirement, empty-array behavior, row order, and all-or-error parsing contract.
- [ ] Keep parser and result-count failures separate from transaction outcomes. They must not automatically commit, roll back, close, or retry a transaction, and must not claim rollback of prior work.
- [ ] Add public-interface browser tests for successful typed reads, inferred application field mapping, positional/named parameters, empty results, mapping failures, and parser failures inside a transaction.
- [ ] Verify failed parsing leaves caller-controlled transaction cleanup available. Demonstrate explicit rollback of pending writes and verify that a parser failure does not silently discard prior work.
- [ ] Verify closed and consumed transactions reject typed reads, and concurrent operations retain the existing busy behavior. Reuse existing transaction test patterns rather than adding a test-only execution interface.
- [ ] Extend installed-consumer type checks for transaction parser inference and invalid usage. Verify the complete feature's exports and declarations through both supported package entry points and packed artifacts.
- [ ] Update public documentation with transaction typed-read usage, parser-failure handling, explicit rollback/close, and the post-execution limit of result-count checks.
- [ ] Keep raw execution, commit, rollback, close, and existing error behavior unchanged. Add no schema-library runtime dependency or unrelated application changes.
- [ ] Run focused TypeScript, lint, browser, and packed-package checks for the completed feature.
- [ ] Run `cargo hack test --feature-powerset --all-targets` and `just crap` as final repository validation after implementation is complete. Report all command results and any environment limits or unrelated failures without fixing unrelated code.

## Comments

Completes the approved typed SQL responses spec after Ticket 01. The public Client and Transaction Query Handles are the test seam. Password-manager adoption, schema-library adapters, async parsers, and convenience cardinality methods remain out of scope.
