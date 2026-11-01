/**
 * @of/db - TypeScript wrapper for ofdb WASM (KV + SQL over IndexedDB)
 *
 * This package provides a typed interface to the ofdb WASM module,
 * which exposes persistent key-value and SQL stores backed by IndexedDB.
 */

// WASM module types (loaded dynamically)
interface WasmModule {
  default(): Promise<void>;
  Kv: {
    open(name: string): Promise<WasmKv>;
  };
  Sql: {
    open(name: string): Promise<WasmSql>;
  };
}

interface WasmKv {
  get(key: string): Promise<string | undefined>;
  set(key: string, value: string): Promise<void>;
  delete(key: string): Promise<void>;
}

interface WasmSql {
  execute(sql: string, params?: string): Promise<string>;
}

/** Any JSON-serializable value the database can store or return. */
export type DbValue =
  string | number | boolean | null | DbValue[] | { [key: string]: DbValue };

/** One SQL statement result: column names plus positional row values. */
export interface SqlResult {
  columns: string[];
  rows: DbValue[][];
}

let wasmModule: Promise<WasmModule> | undefined;

/** Load and initialize the Wasm module once per page. */
async function loadWasm(): Promise<WasmModule> {
  wasmModule ??= import("../pkg/ofdb_wasm.js").then(async (module) => {
    await module.default();
    return module as unknown as WasmModule;
  });
  return wasmModule;
}

/** Generic key-value store backed by IndexedDB. */
export class KvStore {
  private constructor(private readonly kv: WasmKv) {}

  /** Open a persistent key-value store.
   * @param name Database name (used as IndexedDB database name).
   */
  static async open(name: string): Promise<KvStore> {
    const { Kv } = await loadWasm();
    return new KvStore(await Kv.open(name));
  }

  /** Read a committed value, or undefined when the key is missing. */
  async get(key: string): Promise<DbValue | undefined> {
    const raw = await this.kv.get(key);
    return raw === undefined ? undefined : (JSON.parse(raw) as DbValue);
  }

  /** Commit a value. */
  async set(key: string, value: DbValue): Promise<void> {
    await this.kv.set(key, JSON.stringify(value));
  }

  /** Remove a committed value. */
  async delete(key: string): Promise<void> {
    await this.kv.delete(key);
  }
}

/** Generic SQL store backed by IndexedDB. */
export class SqlStore {
  private constructor(private readonly sql: WasmSql) {}

  /** Open a persistent SQL store without touching its schema.
   * @param name Database name (used as IndexedDB database name).
   */
  static async open(name: string): Promise<SqlStore> {
    const { Sql } = await loadWasm();
    return new SqlStore(await Sql.open(name));
  }

  /** Run SQL text and return every statement result.
   * @param sql SQL text, optionally with `$1` or `:name` placeholders.
   * @param params Positional values (array) or named values (object).
   */
  async execute(
    sql: string,
    params?: DbValue[] | { [name: string]: DbValue },
  ): Promise<SqlResult[]> {
    const raw = await this.sql.execute(
      sql,
      params === undefined ? undefined : JSON.stringify(params),
    );
    return JSON.parse(raw) as SqlResult[];
  }
}
