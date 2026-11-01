/**
 * Re-exports the typed database API from @of/db.
 *
 * The browser example uses the npm package @of/db which wraps the ofdb WASM module.
 * All application logic (schema, queries, params, row shaping, UI) lives in TypeScript.
 */
export { KvStore, SqlStore, type DbValue, type SqlResult } from "@of/db";
