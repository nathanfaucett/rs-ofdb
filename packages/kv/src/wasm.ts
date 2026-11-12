// WASM module types (loaded dynamically)
import { KvChangeSubscription } from "../pkg/ofdb_wasm.js";

interface WasmModule {
  default(): Promise<void>;
  KvDatabase: {
    open(name: string): Promise<WasmKvDatabase>;
  };
}

export interface WasmKvDatabase {
  client(): WasmKvClient;
  synchronize(
    transport: unknown,
    role: unknown,
    config: unknown,
  ): Promise<void>;
  synchronize_checked(
    transport: unknown,
    role: unknown,
    config: unknown,
    check: () => Promise<void>,
  ): Promise<void>;
  sync_changes(): Promise<KvChangeSubscription>;
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

let wasmModule: Promise<WasmModule> | undefined;

/** Load and initialize the WASM module once per page. */
export function loadWasm(): Promise<WasmModule> {
  wasmModule ??= import("../pkg/ofdb_wasm.js").then(async (module) => {
    await module.default();
    return module as unknown as WasmModule;
  });
  return wasmModule;
}
