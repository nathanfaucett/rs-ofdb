// WASM module types (loaded dynamically)
interface WasmModule {
  default(): Promise<void>;
  SqlDatabase: {
    open(name: string): Promise<WasmSqlDatabase>;
  };
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
