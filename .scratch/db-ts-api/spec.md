Status: ready-for-agent

## Problem Statement

The `@of/db` TypeScript package (`packages/db/`) does not follow the Database/Client/Transaction facade pattern used by the Rust crates. The current API flattens Database and Client into single `KvStore`/`SqlStore` classes, omits scan operations, transactions, expiry, and deadline configuration, and uses manual JSON serialization instead of TypeScript-native generics.

## Solution

Restructure both the WASM bindings and the TypeScript wrapper to mirror the Rust facade pattern exactly: `Database` (Embedded Handle) opens the store and exposes `.client()`; `Client` (Query Handle) is the only query API with all operations, `.transaction()`, `.withDeadline()`, and `.clone()`; `Transaction` holds operations until `commit()`/`rollback()`.

## User Stories

1. As a browser developer, I want to open a KV Database and get a Client, so that I follow the same mental model as the Rust API.
2. As a browser developer, I want to call `client.get(key)` with a generic type parameter, so that I get typed values back without manual casting.
3. As a browser developer, I want to call `client.set(key, value, expiresAt)`, so that I can use KV Expiry.
4. As a browser developer, I want to call `client.scan(start, end)`, `client.scanPrefix(prefix)`, and `client.scanAll()`, so that I can enumerate KV entries.
5. As a browser developer, I want to create a KV Transaction via `client.transaction()`, so that I can batch operations atomically.
6. As a browser developer, I want to call `tx.commit()` or `tx.rollback()`, so that I control transaction lifecycle explicitly.
7. As a browser developer, I want to call `client.withDeadline(ms)` to get a cloned Client with a deadline, so that I can bound query time.
8. As a browser developer, I want to call `client.clone()`, so that I can share a Client handle independently.
9. As a browser developer, I want to open a SQL Database and get a Client, so that SQL follows the same pattern as KV.
10. As a browser developer, I want to call `client.execute(sql, params)` or `client.executeSql(sql, params)`, so that I can run SQL with positional or named parameters.
11. As a browser developer, I want to create a SQL Transaction via `client.transaction()`, so that I can run multiple statements atomically.
12. As a browser developer, I want to call `tx.execute(sql, params)` inside a transaction, so that I can batch SQL operations.
13. As a browser developer, I want `SqlResult` to include `columns` and `rows`, so that I can map positional rows to objects.
14. As a browser developer, I want typed error classes (`DbError`, `KvError`, `SqlError`), so that I can catch and handle errors by kind.
15. As a package maintainer, I want the TS source split into thin modules per repo conventions, so that `index.ts` only re-exports.
16. As a package maintainer, I want the WASM crate split into `KvDatabase`/`KvClient`/`KvTransaction` and `SqlDatabase`/`SqlClient`/`SqlTransaction`, so that the WASM API mirrors the Rust facades.

## Implementation Decisions

- The WASM crate (`crates/wasm/`) splits `Kv` into `KvDatabase`, `KvClient`, `KvTransaction`; `Sql` into `SqlDatabase`, `SqlClient`, `SqlTransaction`.
- `KvDatabase.open(name)` → `KvDatabase`; `KvDatabase.client()` → `KvClient`.
- `KvClient` exposes: `get`, `set` (with optional `expiresAt`), `delete`, `scan`, `scanPrefix`, `scanAll`, `transaction()`, `withDeadline(ms)` (returns clone), `clone()`.
- `KvTransaction` exposes the same read/write/scan ops plus `commit()` and `rollback()`.
- `SqlDatabase.open(name)` → `SqlDatabase`; `SqlDatabase.client()` → `SqlClient`.
- `SqlClient` exposes: `execute`, `executeSql`, `transaction()`, `withDeadline(ms)`, `clone()`.
- `SqlTransaction` exposes: `execute`, `executeSql`, `commit()`, `rollback()`.
- `QueryParams` is `DbValue[]` (positional) or `Record<string, DbValue>` (named).
- `SqlResult` shape: `{ columns: string[]; rows: DbValue[][] }`.
- `KVEntry` shape: `{ key: string; value: DbValue }`.
- TS module structure: `src/index.ts` (re-exports only), `src/wasm.ts`, `src/value.ts`, `src/kv/{database,client,transaction}.ts`, `src/sql/{database,client,transaction,result}.ts`.
- Error hierarchy: `DbError extends Error` with a `kind` field (`'timeout' | 'query' | 'connect' | 'transport' | 'internal'`); `KvError` and `SqlError` extend it.

## Testing Decisions

- Highest seam: browser integration tests via `wasm_bindgen_test` in `tests/browser/` (prior art: `tests/browser/src/indexeddb.rs`, `tests/browser/src/deadlines.rs`).
- TS-level tests: Playwright smoke tests in `packages/db/example/smoke.test.ts` (prior art exists).
- Each new WASM struct (`KvDatabase`, `KvClient`, `KvTransaction`, etc.) gets a `wasm_bindgen_test` covering open → client → operation → transaction → commit/rollback.
- Tests assert external behavior only: returned values, error kinds, persistence across reopen.

## Out of Scope

- Remote client connections (`Client::connect`) — WASM feature only supports `indexeddb`.
- Sync sessions, hosting, and server APIs.
- Streaming/async-iterable scan results.
- Automatic row-to-object mapping (callers map `SqlResult` themselves).

## Further Notes

- The GLOSSARY defines Embedded Handle (`Database`), Query Handle (`Client`), and Engine Transaction. The TS API must use the same vocabulary: `Database`, `Client`, `Transaction`.
- `Client::withDeadline` in Rust takes `Duration` and returns `Self` (clone). TS uses `withDeadline(ms: number): Client`.
- `Database` is not a query API — it only opens the store and returns a Client.
