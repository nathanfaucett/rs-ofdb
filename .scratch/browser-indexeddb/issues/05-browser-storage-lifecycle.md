# 05: Make browser storage failures and connection lifecycle actionable

**What to build:** Both public facades fail clearly and safely when browser persistence is unavailable, an incompatible database is opened, or another browser context changes the physical IndexedDB schema version.

**Blocked by:** 03 — Run persistent SQL statements through the browser Query Handle.

**Status:** ready-for-agent

- [ ] Validate physical format versions and store-kind metadata on open; unsupported formats fail without destructive reset or implicit migration.
- [ ] Define and implement a bounded, documented policy for blocked opens and schema upgrades so callers receive an actionable result rather than wait indefinitely.
- [ ] Close stale connections on version-change events and make subsequent operations fail clearly without silently reopening or losing staged transaction state.
- [ ] Preserve Client clone ownership and usability after Database drop while storage remains valid.
- [ ] Preserve structured facade errors for access, abort, conflict and commit failures where supported, with useful browser failure details that do not disclose stored values.
- [ ] Persistent open failures never silently select volatile storage.
- [ ] Test blocked opens and version-change closure across actual browser contexts, incompatible format rejection, independent database isolation and client lifetime behavior for both stores.
- [ ] Test failed commits for no partial persistence. Use focused injection where actual quota exhaustion is impractical and distinguish injected failures from real quota validation.
- [ ] Document origin scoping, private-mode limitations, quota, eviction and user-cleared storage. Do not equate IndexedDB completion with native filesystem durability or implement native retirement emulation.
