# 06: Support query deadlines on browser timers

**What to build:** SQL and KV callers configure optional query and transaction deadlines that work in the browser without a Tokio runtime, while native deadline behavior remains intact.

**Blocked by:** 03 — Run persistent SQL statements through the browser Query Handle.

**Status:** ready-for-agent

- [ ] Use a small browser-compatible timer capability that can also support continuous sync retry timing; avoid a new general runtime framework.
- [ ] Preserve the existing public with_deadline behavior, no fixed default deadline and independent settings on cloned clients.
- [ ] SQL-text execution retains one deadline budget for translation plus execution; deadline semantics for transaction operations remain consistent with existing facade contracts.
- [ ] Untimed and timed browser queries do not require a Tokio runtime. Native consumers retain existing native timer behavior.
- [ ] Real-browser tests cover pending work timeout, independent client clones, SQL translation budgeting and explicit transaction operations for both facades.
- [ ] Test cancellation before persistence starts and timeout during an issued commit. Observe completion independently of the caller so committed state is not mistaken for rollback.
- [ ] Document that timeout or cancellation does not prove rollback, undo a completed write or guarantee hard interruption of computation.
