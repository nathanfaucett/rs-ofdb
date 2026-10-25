# 09: Run continuous browser synchronization with cancellation and retry

**What to build:** Both facades expose the existing shared change-driven sync lifecycle in the browser, driven by browser-compatible timers and an explicitly polled caller-owned runner.

**Blocked by:** 06 — Support query deadlines on browser timers; 07 — Synchronize browser KV replicas using the existing protocol; 08 — Synchronize browser SQL replicas using the existing protocol.

**Status:** ready-for-agent

- [ ] The shared ChangeSubscription runner performs initial reconciliation, commit coalescing and retry/backoff without requiring Tokio in the browser.
- [ ] Preserve retry timing from 100 ms to the 30-second cap, reset after success, and retry failed work without requiring a new commit.
- [ ] Capture selected work before reconciliation reads and acknowledge only successful selected work; commits during a callback remain pending for another pass.
- [ ] Dropping the runner cancels its in-flight callback or retry timer; closing all subscribed backends also stops in-flight work.
- [ ] SQL and KV retain the same subscription type and can merge into one broad-session runner; merged subscriptions are not used for selective logical mapping.
- [ ] Real-browser tests verify initial pass, idle behavior, coalescing, concurrent commits, failed retry, cancellation, backend closure and merged SQL/KV subscriptions.
- [ ] The library does not automatically start replication tasks or choose transports, peers or authorization. Caller transport callbacks may use browser-local futures.
- [ ] Document that cross-tab and remote-only writes need caller-provided wakeups or explicit broad recovery; do not add cross-tab coordination or claim durable replay.
- [ ] Native continuous runner regressions preserve existing scheduling, shutdown and cancellation behavior.
