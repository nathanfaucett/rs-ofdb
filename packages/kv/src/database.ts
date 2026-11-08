import { Client } from "./client.js";
import type { WasmKvDatabase } from "./wasm.js";
import { loadWasm } from "./wasm.js";
import {
  JsKvSyncTransport,
  KvSyncRole,
  KvSyncConfig,
  KvChangeSubscription,
  JsKvSyncError,
} from "../pkg/ofdb_wasm.js";

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

  /** Synchronize with a remote peer using the given transport. */
  async synchronize(
    transport: JsKvSyncTransport,
    role: KvSyncRole,
    config: KvSyncConfig,
  ): Promise<void> {
    try {
      await this.wasm.synchronize(transport, role, config);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Synchronize with a remote peer, checking caller authorization before each batch. */
  async synchronize_checked(
    transport: JsKvSyncTransport,
    role: KvSyncRole,
    config: KvSyncConfig,
    check: () => Promise<void>,
  ): Promise<void> {
    try {
      await this.wasm.synchronize_checked(transport, role, config, check);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Get the change subscription for continuous synchronization. */
  async sync_changes(): Promise<KvChangeSubscription> {
    try {
      return await this.wasm.sync_changes();
    } catch (error) {
      throw wrapError(error, Error);
    }
  }
}

// Re-export sync types and values
export {
  JsKvSyncTransport,
  KvSyncRole,
  KvSyncConfig,
  KvChangeSubscription,
  JsKvSyncError,
};
