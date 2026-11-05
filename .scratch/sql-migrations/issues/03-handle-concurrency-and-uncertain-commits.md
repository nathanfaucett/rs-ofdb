# 03: Handle concurrent calls and uncertain commits

**What to build:** Applications can distinguish a failed migration from an uncertain commit and can explicitly invoke migration again without blindly replaying completed SQL. Concurrent calls on one backing store use existing transaction serialization or revision-conflict rejection. Expose consistent behavior through Rust and TypeScript Clients. Follow the parent SQL migrations spec in this feature directory.

**Blocked by:** 01 — Run ordered migrations through the SQL Client.

**Status:** ready-for-agent

- [ ] Use existing transaction behavior to ensure that concurrent calls on one backing store cannot independently commit the same migration. A competing call must observe completed history or receive the existing conflict/error result.
- [ ] Do not add migration locks, leases, automatic conflict retries, or automatic SQL replay. Replication, peer coordination, and sync policy remain out of scope.
- [ ] Preserve commit uncertainty through Rust, Wasm, and TypeScript error paths. Include the affected migration version without exposing SQL or stored values. Do not classify errors by message matching or claim that commit rejection proves rollback.
- [ ] A subsequent explicit call reads committed history in its decision transaction and skips any migration that actually committed. Use a data transformation to demonstrate that an uncertain response does not lead to duplicate execution when history records completion.
- [ ] Respect existing Query Handle deadlines and transaction cleanup rules. Do not introduce a separate migration timeout policy or imply that a timeout cancels remote work.
- [ ] Test concurrent calls through the public Client using existing backend semantics. Include a real-browser IndexedDB stale-revision rejection case and verify that the losing attempt leaves no partial migration changes.
- [ ] Use existing failure-injection or controlled-transport patterns to test uncertain commits and explicit follow-up calls through public interfaces. Preserve the distinction between pre-commit failure and an uncertain outcome in TypeScript errors.
- [ ] Verify uncommitted transaction cleanup after SQL failure, conflict, and deadline handling where supported. Tests must not assume that a locally abandoned commit cannot finish.
- [ ] Complete usage documentation for explicit retry, result inspection, per-migration commits, concurrency rejection, and uncertain outcomes. Leave the parent spec unchanged and do not require ticket 02 to start this work.
- [ ] Run focused Rust and TypeScript/browser checks and applicable formatting and Clippy checks. Once all feature tickets are complete, final feature validation must include `cargo hack test --feature-powerset --all-targets`, `just crap`, and affected TypeScript/browser checks. Record commands, results, and environment blockers; if another ticket is still pending, identify final validation as pending rather than claiming feature completion.
