# 02: Add SQL to the same browser example

**What to build:** Extend the existing browser page with a small SQL demonstration that adds and lists persistent records through the public Rust facade. Extend the same build/serve workflow and browser smoke check rather than creating a second application or test framework.

**Blocked by:** 01 — Ship a runnable persistent KV example.

**Status:** ready-for-agent

- [ ] A separate SQL section accepts a text record, adds it through the SQL Client, and lists committed records in deterministic order, including a clear empty-result state.
- [ ] Database construction, idempotent schema initialization, and queries stay in Rust using public Database and Client APIs with only indexeddb and SQL-text features required. User input uses bound parameters, not SQL string concatenation.
- [ ] SQL uses stable example-specific IndexedDB storage distinct from KV and tests. Startup and repeated reloads neither delete records nor duplicate seed data, and displayed records come from fresh database reads.
- [ ] The documented build explicitly applies the validated SQL Wasm stack and profile through the correct Cargo configuration. Do not claim default-stack support, a minimum memory requirement, or new browser guarantees.
- [ ] SQL controls inherit accessible loading, pending, success, and actionable error handling. Prevent accidental duplicate submissions, show success only after commit, and render all returned text safely. Document any new handwritten JavaScript with JSDoc.
- [ ] Extend the existing served-page smoke check in Chromium and Firefox to add a distinctive SQL record, await commit success, reload, and verify both the SQL record and KV value remain. Verify repeated startup preserves records without duplication and input containing markup and SQL punctuation remains literal data.
- [ ] Preserve isolated test storage, bounded automation, nonzero failure reporting, and browser/server cleanup. Do not duplicate the library transaction, reconciliation, or sync test suites in UI tests.
- [ ] Update build/serve and public usage instructions for both sections, verify clean-checkout reproduction, and run applicable example build/dependency, formatting, Clippy, JavaScript, keyboard, and controlled error-state checks.
- [ ] Keep the result one minimal teaching application: no framework, arbitrary SQL console, sync UI, reset/migration tools, or public JS SDK. No dependency on the separate just migration is introduced.
