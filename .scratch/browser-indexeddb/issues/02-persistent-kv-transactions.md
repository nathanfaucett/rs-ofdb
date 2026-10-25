# 02: Persist KV queries and explicit transactions in IndexedDB

**What to build:** A browser application asynchronously opens a named KV Embedded Handle, obtains its existing Query Handle, reads and writes typed values, transacts across browser event-loop turns, and reopens committed state after restart. This is the first durable vertical slice of the browser IndexedDB specification.

**Blocked by:** 01 — Make embedded storage safe for browser-local execution.

**Status:** ready-for-agent

- [ ] Add the opt-in indexeddb facade feature and asynchronous Database::open_indexeddb(name) constructor. Default features remain empty and failed persistent opens never fall back to memory.
- [ ] Implement shared byte persistence at the existing BTree seam using a fixed physical IndexedDB schema for logical tables, byte records and database metadata. Evaluate actual binding capabilities and features before adding minimal dependencies.
- [ ] Preserve existing document-key encoding, shared typed values, nested JSON, exact numeric representations, ordered scans, literal prefixes, expiry, generations and Tombstones.
- [ ] Verify byte-lexicographic ordering for empty keys, embedded zeros, prefixes and Unicode-derived keys without converting arbitrary binary keys to JavaScript strings.
- [ ] Explicit transactions use one consistent committed snapshot and revision plus Rust-owned staging. Reads and scans observe that snapshot and pending writes across unrelated browser awaits.
- [ ] Commit checks the persisted revision, applies the complete write set and advances revision metadata atomically inside one short IndexedDB read-write transaction. Independent handles and tabs cannot silently overwrite newer commits.
- [ ] Schedule dependent requests within IndexedDB's active request/event lifecycle; no keepalive loops, unrelated awaits or caller work occur inside native persistence transactions.
- [ ] Read-only and net byte no-op commits do not advance revision metadata. Rollback and dropping uncommitted staging persist nothing. A stale mutating transaction returns a conflict without automatic replay.
- [ ] Report success only after native transaction completion, not request success. Independently observe issued commit completion after caller cancellation so later committed invalidation integration cannot lose that outcome.
- [ ] Test aborts and commit failures for all-or-nothing persistence, transactions across event-loop delays, independent-handle/tab races, and actual page/worker restart persistence including Tombstones and history.
- [ ] Document and measure the initial O(database-size) snapshot materialization cost; do not claim selective sync implies selective physical reads.
