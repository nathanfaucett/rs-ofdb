# 03: Run persistent SQL statements through the browser Query Handle

**What to build:** A browser application asynchronously opens a named SQL Embedded Handle, obtains its existing Query Handle, creates a table, inserts and queries rows, and reopens those rows after restart using IndexedDB.

**Blocked by:** 02 — Persist KV queries and explicit transactions in IndexedDB.

**Status:** ready-for-agent

- [ ] Implement the SQL IndexedDB Kernel adapter using the shared byte persistence implementation and the existing database-wide Kernel transaction seam.
- [ ] Add the opt-in indexeddb facade feature and asynchronous Database::open_indexeddb(name) constructor without changing native path-based construction or empty default features.
- [ ] Support typed statement execution and optional SQL-text translation through the existing Client interface; Database does not gain ordinary query methods.
- [ ] Persist catalog, row, Tombstone and derived Index Record mutations together in one database-wide commit, never separately committed logical-table transactions.
- [ ] Represent logical SQL table creation and deletion as transactional data changes, not physical IndexedDB object-store upgrades.
- [ ] Preserve existing SQL key/value codecs and Automerge Row Reconciler behavior rather than introduce a separate browser SQL data model.
- [ ] Isolate SQL, KV and unrelated named databases through physical namespaces or validated store-kind metadata, rejecting incompatible reuse without replacement.
- [ ] Real-browser facade tests cover typed and SQL-text creation, insertion, selection, failed statement-batch atomicity and persistence after closing all owners and restarting a browser context.
- [ ] Supported SQL browser dependency graphs exclude Redb, native filesystem ownership and native networking. Native SQL behavior remains available and unchanged.
