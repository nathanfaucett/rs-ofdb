# 03: Update example app and smoke tests to new API

**What to build:** The Svelte example uses `KvDatabase`/`SqlDatabase` → `.client()` → operations → transactions. Playwright smoke tests exercise the full Database → Client → Transaction path end to end in a real browser.

**Blocked by:** 01 (KV full stack), 02 (SQL full stack)

**Status:** ready-for-agent

- [ ] Example app updated to new Database/Client/Transaction API
- [ ] Playwright smoke tests cover KV save/retrieve, SQL insert/list, transaction commit/rollback
- [ ] `pnpm test` in example passes
