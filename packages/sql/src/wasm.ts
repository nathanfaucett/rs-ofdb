import type {
  SqlClient,
  SqlDatabase,
  SqlTransaction,
} from "../pkg/ofdb_wasm.js";

export type WasmSqlDatabase = SqlDatabase;
export type WasmSqlClient = SqlClient;
export type WasmSqlTransaction = SqlTransaction;

let wasmModule: Promise<typeof import("../pkg/ofdb_wasm.js")> | undefined;

/** Load and initialize the generated WASM module once per realm. Failed loads can be retried. */
export function loadWasm(): Promise<typeof import("../pkg/ofdb_wasm.js")> {
  wasmModule ??= import("../pkg/ofdb_wasm.js")
    .then(async (module) => {
      await module.default();
      return module;
    })
    .catch((error: unknown) => {
      wasmModule = undefined;
      throw error;
    });
  return wasmModule;
}
