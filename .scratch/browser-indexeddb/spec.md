# Browser Wasm persistence with IndexedDB

Status: ready-for-agent

## Problem Statement

Applications cannot currently use the public SQL and KV Embedded Handles in a browser with durable local storage. Persistent construction is tied to Redb and native filesystem ownership. Existing Wasm flags on some lower-level crates provide partial interop configuration, not a supported browser database.

Browser applications need to query, mutate, transact, reload and reconcile their local databases while offline, without loading Redb or requiring native networking or a Tokio runtime. Browser support must preserve the existing SQL and KV domain semantics rather than substitute a separate browser data model.

## Solution

Add IndexedDB as an explicitly selected embedded persistence backend for both public facades. Native applications retain Redb and in-memory backends. Browser applications asynchronously open a named database, obtain a Query Handle using the existing Embedded Handle interface, and use the existing query and explicit transaction operations.

Reuse the existing BTree and SQL Kernel seams and the existing Automerge storage and Row Reconciler implementations. IndexedDB owns byte persistence, not SQL execution or CRDT reconciliation. Both stores retain their distinct query and sync interfaces.

Deliver KV first as a working browser vertical slice, then SQL on the same persistence implementation, then browser-compatible synchronization and release validation. Completion of this specification requires both facades, not just the first KV slice.

## User Stories

1. As a browser application developer, I want to open a named persistent KV database, so that local state survives reloads.
2. As a browser application developer, I want to open a named persistent SQL database, so that relational application data survives reloads.
3. As a browser application developer, I want local reads and writes without connectivity, so that the application remains usable offline.
4. As a Rust developer, I want the existing Query Handle interface in Wasm, so that browser storage does not require rewriting application queries.
5. As a KV caller, I want shared typed values and nested JSON to retain their types, so that browser persistence preserves the data model.
6. As a KV caller, I want ordered range and literal-prefix scans, so that Unicode, NUL-containing and adjacent keys behave consistently across backends.
7. As a KV caller, I want absolute millisecond expiry to use the browser clock, so that expired values are hidden without changing their replicated history.
8. As a KV caller, I want generations and tombstones preserved after reload, so that stale replication cannot restore deleted state.
9. As a SQL caller, I want typed statements and optional SQL-text translation, so that both query styles work locally in the browser.
10. As a SQL caller, I want schema catalogs, constraints and derived indexes persisted atomically, so that interrupted writes cannot leave inconsistent relational state.
11. As a SQL caller, I want conflict inspection and explicit Resolution, so that offline concurrent edits remain inspectable and resolvable.
12. As a transaction caller, I want reads to observe a stable snapshot plus my pending writes, so that results do not mix unrelated committed states.
13. As a transaction caller, I want an explicit transaction to survive unrelated awaits between calls, so that browser event-loop turns do not silently commit it.
14. As a transaction caller, I want commit to persist all changes together, so that partial writes never become visible.
15. As a transaction caller, I want rollback and dropping an uncommitted handle to discard pending changes, so that abandoned work does not persist.
16. As a caller with multiple clients or tabs, I want stale writes rejected, so that independent handles cannot silently overwrite newer commits.
17. As an application developer, I want cloned clients to share storage and remain usable after dropping the Embedded Handle, so that existing ownership contracts remain useful.
18. As an application developer, I want quota, access, abort and version failures reported, so that unavailable persistence is not mistaken for a successful write.
19. As an application developer, I want failed persistent opens to remain failures, so that my application never silently falls back to volatile memory.
20. As an application developer, I want optional query deadlines to work without Tokio in the browser, so that browser queries can be bounded using the platform runtime.
21. As a local-first application developer, I want browser and native replicas to exchange existing sync state and native history, so that browser storage participates in the same convergence semantics.
22. As a sync caller, I want Committed Change Subscriptions to report successful local and incoming commits, so that change-driven reconciliation works in the browser.
23. As a sync caller, I want rollback, failed commit and byte-identical no-ops to remain silent, so that reconciliation settles without notification echoes.
24. As a sync caller, I want runner cancellation and retry/backoff to work on browser timers, so that I can explicitly control the Sync Session lifetime.
25. As an application developer, I want database names and store kinds isolated, so that SQL, KV and unrelated databases cannot collide accidentally.
26. As an application developer, I want connection upgrades and closure handled explicitly, so that another tab cannot leave database initialization hanging without an actionable outcome.
27. As a library maintainer, I want browser builds free of native persistence and networking dependencies, so that Wasm support is real rather than an unused feature declaration.
28. As a library maintainer, I want native behavior and thread-safety guarantees retained, so that browser support does not weaken native consumers.
29. As a library maintainer, I want tests in actual browsers, so that IndexedDB behavior is validated beyond successful compilation.

## Implementation Decisions

### Scope and public construction

- Add an opt-in `indexeddb` backend feature to each public facade. Facade default features stay empty.
- Provide an asynchronous `Database::open_indexeddb(name)` constructor. Native path-based construction stays distinct and unchanged; do not select persistence through a combined URI constructor.
- Ordinary queries and explicit transactions remain on Client. Database retains construction, lifecycle and sync setup responsibilities.
- Store kinds must have distinct physical namespaces or an atomically validated store-kind marker. Reject incompatible formats rather than replacing them or implicitly migrating them.
- Browser support initially targets single-threaded `wasm32-unknown-unknown` in Window and Dedicated Worker contexts. Do not require shared Wasm memory or cross-origin isolation.
- This is a Rust library browser capability, not an automatically generated JavaScript SDK. Existing interop derives do not by themselves define a supported JavaScript interface.

### Shared persistence implementation

- Introduce a shared IndexedDB byte-storage module and a SQL IndexedDB Kernel adapter. KV uses the existing byte BTree seam; SQL uses the existing database-wide Kernel transaction seam.
- Use byte keys and values, retaining the existing ordered document-key encoding, SQL row-key encoding, value codecs and Automerge history. Do not introduce a generic serializer framework for arbitrary Rust key types.
- Use a fixed IndexedDB schema containing logical-table metadata, byte records and database metadata. Logical SQL table creation and deletion are transactional data changes, not IndexedDB object-store schema upgrades.
- Identify records by logical table and binary key. Preserve Rust byte-lexicographic ordering, including empty keys and embedded zero bytes; test IndexedDB binary-key comparison rather than translating arbitrary bytes to JavaScript strings.
- A single commit must enlist records, logical-table metadata and revision metadata together. Never emulate a SQL commit with separately committed table transactions.
- Prefer a maintained Rust IndexedDB binding if it satisfies transaction completion, abort, binary key and worker requirements; otherwise use a small platform adapter. Confirm actual crate features before declaring dependencies. Avoid deprecated serialization features and unnecessary executor dependencies.

### Explicit transaction semantics

- Native IndexedDB transactions cannot be kept open across arbitrary caller awaits. Do not use transaction keepalive loops or assume request success means transaction commit success.
- For the initial correctness-first implementation, acquire one consistent committed snapshot and revision in a short IndexedDB read transaction. Execute caller operations against a Rust-owned snapshot and staged changes, with read-your-writes and stable scans.
- At commit, open one short IndexedDB read-write transaction, check the persisted revision, apply the complete staged changes and advance the revision atomically. A stale mutating transaction fails with a conflict; clients do not automatically replay user operations.
- Revision comparison and writes must occur inside that same native transaction. An instance-local mutex is not sufficient protection against another tab or independently opened handle.
- Schedule dependent IndexedDB requests using its active request/event lifecycle. Do not perform unrelated awaits, translation, authorization or network I/O inside the native persistence transaction.
- Read-only transactions do not advance the persisted revision or publish changes. Suppress net byte no-ops; represent logical table drops conservatively for invalidation without claiming a row-level change list is complete.
- Snapshot materialization is an explicit initial O(database-size) memory and transaction-start cost. Document and measure it; do not describe selective sync as bounded physical I/O when the backend loads the entire snapshot. Incremental snapshot loading or finer conflict detection is deferred until measured needs justify it.
- Rollback and dropping an uncommitted transaction discard Rust-owned staging. SQL's existing aborted-transaction rules continue to apply.
- Report commit success only after IndexedDB transaction completion. Timeout or cancellation during an issued commit does not prove rollback. Persistence completion and notification publication must remain observed even when the caller drops its future; attach completion handling independently of caller polling and retain only what completion needs.

### Wasm portability

- Audit and update storage traits, SQL Kernel, RowCodec, Row Reconciler, stream return types, boxed transaction callbacks and sync staging for browser-local futures and handles. Current unconditional Send/Sync requirements prevent a straightforward IndexedDB adapter.
- Keep the existing conceptual seams; use a consistent target-aware bound/future strategy rather than duplicate browser engines or unsafe Send/Sync assertions on JavaScript objects. Native builds must retain their existing Send/Sync contracts and spawned-query/sync support.
- Audit Wasm feature propagation for UUID randomness, Automerge actor generation, wall-clock timestamps, expiry and timers. A successful target compile is not evidence that SystemTime or Tokio timer calls work in a browser.
- Use browser-compatible clock and timer implementations for Wasm. Reuse the same small platform capability for facade deadlines and continuous retry timing where practical; retain native behavior.
- IndexedDB is browser I/O and may require std. Do not force the entire existing core to std or claim this persistence adapter is no_std when its dependencies require otherwise.
- Keep Redb, filesystem resources, native temporary staging, TCP/Unix hosting and native gRPC transport out of supported browser dependency graphs. Put native-only development dependencies behind suitable target configuration or isolate browser tests in an external consumer fixture.
- Existing workspace member globs already discover additional crates. Do not add duplicate member entries. Do not build the entire native CLI/server workspace for the browser target.

### Synchronization and notifications

- Reuse existing SQL and KV protocols, history validation, generation selection, Tombstone semantics, schema reconciliation, derived index maintenance and checked atomic apply.
- Browser backend selection alone must not change wire versions or logical payload formats. Test browser/native interoperability using current protocol fixtures and committed state.
- Browser subscriptions cover backend-shared clients and incoming replication after successful commit. Preserve independent selected-work acknowledgement, broad initial work, overflow fallback and later-commit retention.
- Subscriptions remain local invalidation hints, not durable replay or automatic cross-tab observation. Correctness across tabs comes from consistent reads and revision validation. Cross-tab wakeup delivery is out of scope; document that selected reconciliation needs broad recovery when external writes may have been missed.
- Continuous sync runs on browser-compatible timers with the existing retry policy, explicit caller-owned lifetime and no automatically started replication task. Transport, remote wakeups, authorization and peer selection remain caller-owned.
- SQL uses existing bounded memory staging in the browser. Native stage directories and native resource retirement remain native-only interfaces, not browser emulations.

### Browser lifecycle and errors

- Validate database schema/store-kind metadata at open. IndexedDB schema versions describe the physical adapter format, not user SQL DDL. Reject unsupported versions without destructive reset.
- Handle blocked opens and version-change events with a documented, bounded failure/closure policy. Close stale connections on version change and make subsequent operations fail clearly rather than silently reopen or lose staging.
- Preserve structured facade errors, including conflict and commit failure distinctions where available; include useful browser storage failure details without exposing stored values.
- Explain origin scoping, quota, private-mode limitations, eviction and user-cleared storage. Successful commit means IndexedDB completion, not a guarantee against eviction or every power-loss scenario. Never advertise native filesystem durability equivalence.

### Delivery order

1. Establish focused Wasm compile probes and browser tests; verify local-future strategy, clock/randomness support, binary ordering and the staged revision-checked commit contract before broad integration.
2. Implement the shared byte adapter and ship KV construction, query operations, transactions, persistence/reload and error handling as the first vertical slice.
3. Add the SQL Kernel adapter and facade support, including atomic schema/index maintenance, explicit transactions and conflict inspection/Resolution.
4. Integrate browser notifications, deadline timers, one-shot checked sync and the continuous sync lifecycle; verify browser/native logical interoperability.
5. Complete browser test automation, native regression checks, feature isolation checks and user-facing documentation. Remove obsolete or misleading Wasm feature declarations encountered in this scope rather than retain compatibility glue.

## Testing Decisions

- Primary acceptance seam: the existing public Embedded Handle → Query Handle → Transaction interface, in real browsers. SQL and KV deliberately remain separate interfaces; no new combined test-facing facade is needed.
- Focus lower-level tests at the existing BTree and Kernel seams only for invariants that are difficult to induce reliably from the facade: binary ordering, revision races, completion/abort behavior and atomic multi-table persistence.
- Good tests assert externally observable results, persisted state, structured errors and committed invalidation behavior. Do not assert internal helper calls, object layout, callback counts or a particular binding crate's implementation.
- Reuse the intent and scenarios of existing embedded-client ownership, native reopen, transaction rollback, SQL atomic batch, catalog/index lifecycle, conflict Resolution, KV expiry/generation/tombstone, and committed-invalidation tests. Do not port native filesystem or multi-thread harness assumptions into browser fixtures.
- Test KV typed values, empty blobs, nested JSON, full-width integer values and exact float representations without accidental JavaScript number coercion.
- Test ordered scans with empty, binary-prefix, NUL and Unicode keys, included/excluded/unbounded limits, and pending inserts/deletes.
- Test transactions with an actual browser event-loop delay between operations, stable snapshot reads, read-your-writes, explicit rollback, dropped staging and SQL aborted transactions.
- Test independent open handles and separate browser contexts with overlapping writes: exactly one stale writer is rejected, with no partial records, catalog state or derived indexes persisted.
- Test reopen after dropping all storage-owning handles and after actual page/worker restart. Verify live state, Tombstones, history, expiry and SQL indexes survive, not just currently visible values.
- Test native IndexedDB abort and commit errors without publishing invalidations or leaving partial writes. Use focused fault injection where real quota exhaustion is impractical; do not claim injected failures validate actual browser quota behavior.
- Test cancellation before persistence starts and during commit completion. A committed mutation must remain discoverable for reconciliation even if its caller stopped polling.
- Test shared client ownership after Database drop, independent subscriptions, net no-op silence, incoming sync fan-out, acknowledgement races and broad fallback.
- Test checked sync rejection and cancellation without partial incoming batches, replay silence, concurrent offline edits and convergence of SQL conflicts and KV generations across browser/native payload exchange.
- Run browser tests using wasm-bindgen-test or equivalent in headless Chromium and Firefox. Validate representative persistence and transaction-lifetime scenarios in WebKit before claiming support there. Node-only or mock IndexedDB execution is insufficient acceptance validation.
- Add package-specific Wasm checks for KV indexeddb, SQL indexeddb with typed statements, SQL indexeddb with SQL translation, and browser sync combinations. Assert native-only dependencies are absent. Keep native feature combinations separately validated; workspace-wide Wasm all-features builds are not a meaningful requirement.
- Final implementation validation includes rustfmt, focused Clippy checks, browser checks/tests, native regression checks, `cargo hack test --feature-powerset --all-targets`, and `just crap`. Record unavailable tooling or pre-existing failures rather than claiming a pass.

## Out of Scope

- Replacing Redb for native applications or removing native backends.
- A JavaScript/TypeScript SDK, npm package or application UI.
- Native gRPC clients/servers in the browser, a gRPC-Web redesign, or bundled WebSocket/WebRTC/Iroh transport integration.
- Automatic cross-tab wakeups, leader election or a SharedWorker database host.
- OPFS persistence, SQLite, filesystem emulation or alternate browser persistence engines.
- Migration of Redb files into IndexedDB, implicit conversion of incompatible persisted formats, or compatibility with older sync protocols.
- A persistent browser retirement fence equivalent to native exact-file retirement; native retirement remains explicitly native-only.
- Unbounded browser sync staging, durable peer frontiers or automatic background replication.
- Fine-grained optimistic conflict detection and lazy snapshot loading in the initial adapter.
- A claim that all workspace crates/features run under Wasm, or that every existing Wasm interop derive constitutes a supported JavaScript interface.

## Further Notes

- Complexity: high. The largest cross-cutting work is safe local-future support and platform capability isolation, not basic IndexedDB CRUD.
- Risk: high for atomicity, transaction lifetime, stale-writer detection and cancellation after persistence begins. Resolve these in the initial browser probes before facade expansion.
- Priority: foundational for durable browser local-first use. Deliver KV first to shorten feedback while preserving the complete SQL/KV goal.
- The earlier conversational outline was directional, not validated dependency configuration. It understated transaction risk, omitted unconditional Send/Sync constraints, proposed redundant workspace members and inappropriate workspace-wide Wasm builds, and included unverified dependency features. This specification supersedes those details.
- The primary facade testing seams are proposed from the existing project contracts, not explicitly confirmed by the user. Keep that expectation visible without blocking publication or introducing a planning interview.
- No implementation or browser validation is claimed by publishing this specification. Binding selection, exact supported-browser baseline and measured snapshot memory ceilings must be recorded during implementation.
