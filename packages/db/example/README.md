# Persistent browser KV and SQL example

A TypeScript-first example demonstrating persistent key-value and SQL stores in the browser using IndexedDB via the `@of/db` npm package (which wraps the ofdb WASM module).

The application schema, queries, parameters, row shaping, and UI all live in TypeScript. The Rust code is a generic engine bridge (`ofdb-wasm` crate) that opens IndexedDB stores and executes SQL text.

## Build and open

From the repository root:

```sh
# Build the @of/db npm package (compiles WASM via wasm-pack)
cd packages/db && pnpm run build

# Build the browser example TypeScript
cd packages/db/example && npm install && npm run build
```

### Development server (Vite)

```sh
cd packages/db/example && npx vite --port 8080 --host 127.0.0.1
```

Open **http://127.0.0.1:8080/** in Chromium or Firefox.

Wait for startup, enter a KV Key and text value, and select **Save**. Wait for **Saved and committed.**, then reload and select **Retrieve** under the same key. Startup also reads the default `theme` key afresh. Missing keys display **Key not found.**; an empty stored string is a successful retrieval, not a missing key. Tab moves between native controls; Enter in the key input or on Save submits the form. Multiline text is supported.

In the SQL section, enter a **Text record** and select **Add record** (or press Enter). Wait for **Added and committed.**, then reload: startup lists committed records afresh. **List records** explicitly refreshes the list. Fresh storage shows **No records yet.** Input containing HTML or SQL punctuation stays literal text. Neither section inserts seed data; opening/reloading never resets either store.

TypeScript sources are in `ts/`; `npm run build` compiles them to `dist/` for production.

### Production preview

```sh
cd packages/db/example && npx vite preview --port 8080 --host 127.0.0.1
```

## Storage and failures

The distinct stable IndexedDB names are **`ofdb-browser-example-kv`** and **`ofdb-browser-example-sql`**, separate from library test storage. Opening the page never resets it or inserts seed data. There is no memory fallback. Persistence is scoped to the **browser profile, origin (scheme, host, port), and database name**. `localhost` and `127.0.0.1`, different ports, and different profiles have separate storage. Keep the same URL and browser profile when checking reload persistence.

Browser eviction, private-mode restrictions, denied storage access, quota limits, and user-cleared site data can remove or prevent storage. A committed write is not a backup or a promise about power-loss durability. The page reports startup and query errors visibly; allow site storage/check free space and reload. If a write fails, retrieve the actual stored value before deciding whether to retry. Never clear site storage as a recovery step unless you intend to delete its data.

## Served-page checks (Playwright)

```sh
cd packages/db/example && npx playwright install --with-deps chromium firefox
cd packages/db/example && npx playwright test smoke.test.ts
```

The Playwright test suite checks:

- KV save, commit, reload, retrieve
- SQL add, commit, duplicate, reload listing
- Literal markup/SQL-punctuation safety in both sections
- Actionable denied-storage startup error in both sections

Both Chromium and Firefox are required; either failing returns nonzero with phase, UI status, and driver diagnostics.

## Focused contributor checks

```sh
# Build and check the @of/db package
cd packages/db && pnpm run check

# Check the browser example TypeScript
cd packages/db/example && npm run check
```
