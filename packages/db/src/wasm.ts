// WASM module types (loaded dynamically)
interface WasmModule {
  default(): Promise<void>;
  KvDatabase: {
    open(name: string): Promise<WasmKvDatabase>;
  };
  SqlDatabase: {
    open(name: string): Promise<WasmSqlDatabase>;
  };
}

export interface WasmKvDatabase {
  client(): WasmKvClient;
}

export interface WasmKvClient {
  get(key: string): Promise<string | undefined>;
  set(key: string, value: string, expiresAt?: number): Promise<void>;
  delete(key: string): Promise<void>;
  scan(start: string, end: string): Promise<string>;
  scanPrefix(prefix: string): Promise<string>;
  scanAll(): Promise<string>;
  transaction(): Promise<WasmKvTransaction>;
  withDeadline(ms: number): WasmKvClient;
  cloneClient(): WasmKvClient;
}

export interface WasmKvTransaction {
  get(key: string): Promise<string | undefined>;
  set(key: string, value: string, expiresAt?: number): Promise<void>;
  delete(key: string): Promise<void>;
  scan(start: string, end: string): Promise<string>;
  scanPrefix(prefix: string): Promise<string>;
  scanAll(): Promise<string>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
}

export interface WasmSqlDatabase {
  client(): WasmSqlClient;
}

export interface WasmSqlClient {
  execute(sql: string, params?: string): Promise<string>;
  transaction(): Promise<WasmSqlTransaction>;
  withDeadline(ms: number): WasmSqlClient;
  cloneClient(): WasmSqlClient;
}

export interface WasmSqlTransaction {
  execute(sql: string, params?: string): Promise<string>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
}

let wasmModule: Promise<WasmModule> | undefined;

/** Load and initialize the WASM module once per page. */
export function loadWasm(): Promise<WasmModule> {
  wasmModule ??= import("../pkg/ofdb_wasm.js").then(async (module) => {
    await module.default();
    return module as unknown as WasmModule;
  });
  return wasmModule;
}
