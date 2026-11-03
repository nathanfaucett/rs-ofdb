import { Client } from "./client.js";
import { Error, wrapError } from "./error.js";
import type { WasmKvDatabase } from "./wasm.js";
import { loadWasm } from "./wasm.js";

/** Embedded KV database handle. Opens the store and creates query handles. */
export class Database {
  private readonly wasm: WasmKvDatabase;

  private constructor(wasm: WasmKvDatabase) {
    this.wasm = wasm;
  }

  /** Open a persistent KV database backed by IndexedDB. */
  static async open(name: string): Promise<Database> {
    try {
      const { KvDatabase: WasmKvDatabase } = await loadWasm();
      const wasm = await WasmKvDatabase.open(name);
      return new Database(wasm);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Create a query handle sharing this database's storage. */
  client(): Client {
    return new Client(this.wasm.client());
  }
}
