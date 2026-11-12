import { Client } from "./client.js";
import { Handle } from "./handle.js";
import type { WasmSqlDatabase } from "./wasm.js";
import { loadWasm } from "./wasm.js";
import { wrapError, Error as SqlError } from "./error.js";
import {
  JsPushSyncTransport,
  SqlSyncRole,
  JsSqlSyncError,
  PushConfig,
  PushSynchronizeConfig,
  PushSynchronizeResult,
} from "../pkg/ofdb_wasm.js";

/** Embedded SQL database handle. Opens the store and creates query handles. */
export class Database {
  private readonly handle: Handle<WasmSqlDatabase>;

  private constructor(wasm: WasmSqlDatabase) {
    this.handle = new Handle(wasm);
  }

  /** Open a persistent SQL database backed by IndexedDB. */
  static async open(name: string): Promise<Database> {
    try {
      const { SqlDatabase: WasmSqlDatabase } = await loadWasm();
      const wasm = await WasmSqlDatabase.open(name);
      return new Database(wasm);
    } catch (error) {
      throw wrapError(error, SqlError);
    }
  }

  /** Release this handle. Independently owned clients remain usable. */
  close(): void {
    this.handle.close();
  }

  /** Create a query handle sharing this database's storage. */
  client(): Client {
    return new Client(this.handle.get().client());
  }

  /** Synchronize with a remote peer using the given transport. */
  async synchronize(
    transport: JsPushSyncTransport,
    config: PushSynchronizeConfig,
  ): Promise<PushSynchronizeResult> {
    try {
      return await this.handle.get().synchronize(transport, config);
    } catch (error) {
      throw wrapError(error, SqlError);
    }
  }
}

// Re-export sync types and values
export {
  JsPushSyncTransport,
  SqlSyncRole,
  JsSqlSyncError,
  PushConfig,
  PushSynchronizeConfig,
  PushSynchronizeResult,
};
export { SqlSyncDirection } from "../pkg/ofdb_wasm.js";
