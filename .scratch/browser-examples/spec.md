# Runnable browser SQL and KV examples

Status: ready-for-agent

## Problem Statement

The project supports persistent embedded SQL and KV in browser Wasm, but its public quickstarts are Rust snippets and its browser harness is a test consumer. Neither provides a small application a developer can build, open, interact with, and reload to observe persistence.

Developers need a complete example of Wasm packaging, asynchronous browser startup, public facade usage, and IndexedDB persistence without adopting a frontend framework or a new JavaScript SDK.

## Solution

Provide one minimal runnable browser application with separate KV and SQL demonstrations, built and served through documented just commands. A user can save and retrieve a KV value, add and list SQL records, and reload the page to see committed data remain.

Database operations stay in Rust using the existing embedded facade APIs. Keep browser glue small, document handwritten JavaScript with JSDoc, and make loading, success, and failure visible. Verify the served application itself in Chromium and Firefox.

## User Stories

1. As a developer evaluating ofdb, I want a runnable browser example, so that I can see working persistence rather than assemble an application from snippets.
2. As a contributor, I want build and serve commands through just, so that the example follows the project's existing development workflow.
3. As a new contributor, I want explicit prerequisites and a local URL, so that I can run the example from a clean checkout.
4. As a Rust application author, I want database operations implemented through public Rust facades, so that the example teaches the supported integration model.
5. As a Rust application author, I want asynchronous initialization demonstrated, so that I know when database controls become usable.
6. As a user, I want to save a value under a KV Key, so that I can persist a small preference in the browser.
7. As a user, I want to retrieve a saved KV value, so that I can distinguish persisted data from uncommitted form contents.
8. As a user, I want a clear missing-value state, so that an absent key is not presented as an application failure.
9. As a user, I want to add a SQL record, so that I can see a real Query Operation executed through the SQL facade.
10. As a user, I want to list stored SQL records, so that I can inspect the committed results.
11. As a user, I want an empty SQL result displayed clearly, so that first use does not look broken.
12. As a user, I want my committed KV value to survive a page reload, so that the example proves browser persistence.
13. As a user, I want my committed SQL records to survive a page reload, so that the example proves SQL persistence as well.
14. As a returning user, I want startup to preserve my records, so that opening the example does not reset or duplicate my data.
15. As a user, I want loading and in-progress states, so that I do not trigger operations before initialization or accidentally submit a write twice.
16. As a user, I want success shown only after a write commits, so that the interface does not claim persistence prematurely.
17. As a user, I want actionable storage and query errors, so that I can understand failed operations rather than receive a silent memory fallback.
18. As a keyboard or assistive-technology user, I want labeled native controls and accessible status messages, so that I can operate both demonstrations.
19. As a maintainer, I want minimal, documented browser glue, so that the example does not become a second application framework or public SDK.
20. As a maintainer, I want browser reload smoke checks, so that broken packaging or startup cannot hide behind passing library tests.
21. As a maintainer, I want both Chromium and Firefox checked, so that the example reflects the supported browser scope.
22. As a contributor, I want isolated example test storage and bounded automation, so that checks do not alter my own example data or hang indefinitely.
23. As a developer, I want the origin and storage limitations explained, so that changing a local port or clearing browser data is not mistaken for a database defect.
24. As a native library maintainer, I want example-only browser dependencies isolated, so that a demonstration does not change native builds or default facade features.

## Implementation Decisions

- Deliver one small browser application with separate KV and SQL sections. Both are required; separate framework applications are unnecessary.
- Use the public embedded facade roles: Database constructs IndexedDB storage, Database creates the Client, and Client executes ordinary queries. Do not expose internal engine/storage APIs or duplicate the query interface on Database.
- Keep database construction, Query Operations, and schema setup in Rust compiled to single-threaded browser Wasm. Use minimal HTML, styling, and browser integration; no frontend framework is required.
- Use distinct, stable, example-specific IndexedDB database names for SQL and KV. Keep the same names across reloads on the same origin. Do not reuse library test database names or native storage.
- The KV section accepts a key and a text value and provides explicit save and retrieve actions. It displays the stored value or a missing-key state. Teaching every shared Value variant is unnecessary.
- The SQL section creates one simple table idempotently, accepts a text record, and lists committed records in a deterministic order. Initialization must not delete existing rows, insert repeat seed data, or reset the database on reload.
- Use SQL-text execution to illustrate the existing optional translator. Bind user input as query parameters rather than concatenating it into SQL. Any example-local browser bridge must remain narrow and is not a supported general-purpose JavaScript database API.
- Use only the facade features required for persistent KV and SQL-text queries. Sync, remote clients, Redb, native networking, and native filesystem staging are not required by the example.
- Preserve isolation of browser consumer dependencies from native workspace development dependencies, following the existing browser harness pattern where needed. Keep crate roots thin and follow normal dependency/version conventions.
- Configure and document the currently validated SQL Wasm stack and build profile explicitly. Ensure the documented build actually discovers the intended Cargo configuration. Do not imply that the example establishes default-stack support or a minimum memory requirement.
- Provide discoverable just commands for building and serving static Wasm/browser assets and for the smoke checks. Document tool installation, startup, local URL, and how to stop the server. Serving is intentionally long-running for users; automated runs must bound it and clean it up.
- Use existing Rust/Wasm tooling and a minimal static-serving mechanism. Do not use Python. Node.js, Playwright, and an npm environment are not prerequisites; prefer reuse of existing browser/WebDriver tooling before adding dependencies. Do not build a general-purpose server or browser automation framework.
- All required handwritten JavaScript, including example glue and any new browser automation helper, must have meaningful JSDoc covering its interface and non-obvious asynchronous behavior. Generated Wasm binding output is generated tooling output, not a handwritten documentation surface.
- Disable or guard database controls during initialization and relevant pending operations. Handle startup and per-operation errors visibly; report write success only after the facade operation completes successfully.
- Render user-controlled data as text rather than executable markup. Use labeled native controls, keyboard-accessible actions, and accessible status/error messages. Prevent accidental duplicate submissions while a write is pending.
- Re-render persisted data from fresh Client reads, including on initialization after reload; DOM contents or browser localStorage are not substitutes for IndexedDB database persistence.
- Explain that persistence is scoped to the browser origin and database name; changing the serving port changes the origin. Browser eviction, private-mode behavior, quota limits, and user-cleared site storage remain platform limitations, not promises of durable backup.
- Link the runnable example from current browser usage documentation. Keep it a small teaching artifact rather than an arbitrary SQL console, application template, or feature showcase.

## Testing Decisions

- The user confirmed the primary seam: the actual served example page in Chromium and Firefox. Exercise public controls and visible results, not internal Rust functions or a fake IndexedDB implementation.
- Build the documented example artifacts, serve them over a stable local HTTP origin, save a distinctive KV value and SQL record through the UI, and wait for visible commit success.
- Reload the page in the same browser context and origin, retrieve the KV value, and verify the SQL record remains. The check must cross a real page reload rather than retain an in-memory Client or assert against unchanged DOM state.
- Verify fresh storage displays sensible missing/empty states, subsequent startup does not duplicate or delete SQL records, and the controls become usable after initialization.
- Include safely rendered input containing markup-like text and SQL punctuation to ensure the application displays text and binds parameters rather than interpreting input as HTML or SQL syntax.
- Verify visible error handling through an appropriate controlled browser/storage failure where feasible. Do not add production-only injection APIs just to exercise the example UI.
- Isolate each automated run from contributor data using a fresh browser profile/context while preserving that context through reload. Ensure failures return nonzero, retain useful browser/build diagnostics, and close browser/server processes on failure or timeout.
- Reuse the existing browser harness and WebDriver/wasm-pack ecosystem where it can drive the served example. The smoke test must reach the actual page; rerunning only the existing Rust library harness is insufficient. The exact automation tool is not a new architectural requirement.
- Existing Rust browser persistence and transaction tests are prior art for database setup and expected behavior. Continue relying on them for rollback, revision conflicts, reconciliation, sync, and other library edge cases rather than duplicating the full behavioral suite through the UI.
- Compile and check the example as an independent browser consumer, verify supported dependency isolation, and apply Rust formatting and Clippy. Check handwritten JavaScript syntax and JSDoc coverage. Manually verify basic keyboard operation and readable loading/error states.
- Preserve honest validation reporting: Chromium and Firefox are required, WebKit is not; no claim of remote CI success, quota exhaustion testing, crash durability, or default-stack SQL support follows from a passing example smoke test.

## Out of Scope

- A JavaScript/TypeScript SDK, npm package, stable exported browser API, or generated API redesign.
- A frontend framework, application scaffold, arbitrary SQL console, schema editor, or production UI design system.
- Sync demonstrations, transports, peer authorization, remote hosting, cross-tab wakeups, or committed-change subscription UI.
- A Worker UI example, shared Wasm memory, or cross-origin isolation requirements; the application runs in Window. Existing library Worker tests remain unchanged.
- WebKit validation, mobile browser guarantees, new minimum browser versions, or private-mode support claims.
- A reset/delete-database UI, data migration tooling, backups, or destructive recovery from storage errors.
- Expanding the example to all SQL features, all KV Value representations, expiry, or transaction demonstrations.
- Rewriting the existing Python matrix runner; that migration has a separate spec.
- Resolving pre-existing broad validation failures or reducing the SQL engine's stack requirement.

## Further Notes

This example fills the packaging/startup/reload gap left by Rust snippets and library-level browser tests. Complexity is modest; the main risks are accidentally demonstrating memory-only state, destructive initialization, unbounded automation, or misleading persistence guarantees. Keep the UI small and make the reload check decisive.

The example can be implemented independently of the browser-validation migration. Both use the existing just interface; neither must wait for the other's internal orchestration choices.

The user confirmed the proposed scope and served-page test seam before publication. No runtime implementation, ticket decomposition, or commits are part of publishing this spec.
