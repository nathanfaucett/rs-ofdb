import { KvClient } from "./client.js";
import { KvError, wrapError } from "./error.js";
import type { WasmKvDatabase } from "./wasm.js";
import { loadWasm } from "./wasm.js";

/** Embedded KV database handle. Opens the store and creates query handles. */
export class KvDatabase {
  private readonly wasm: WasmKvDatabase;

  private constructor(wasm: WasmKvDatabase) {
    this.wasm = wasm;
  }

  /** Open a persistent KV database backed by IndexedDB. */
  static async open(name: string): Promise<KvDatabase> {
    try {
      const { KvDatabase: WasmKvDatabase } = await loadWasm();
      const wasm = await WasmKvDatabase.open(name);
      return new KvDatabase(wasm);
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Create a query handle sharing this database's storage. */
  client(): KvClient {
    return new KvClient(this.wasm.client());
  }
}
