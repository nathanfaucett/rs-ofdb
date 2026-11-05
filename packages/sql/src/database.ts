import { Client } from "./client.js";
import { Error, wrapError } from "./error.js";
import { Handle } from "./handle.js";
import type { WasmSqlDatabase } from "./wasm.js";
import { loadWasm } from "./wasm.js";
import {
  JsSqlSyncTransport,
  SqlSessionConfig,
  SqlSyncRole,
  type SqlSyncResult,
  JsSqlSyncError,
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
      throw wrapError(error, Error);
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
    transport: JsSqlSyncTransport,
    config: SqlSessionConfig,
    role: SqlSyncRole,
  ): Promise<SqlSyncResult> {
    try {
      return await this.handle.get().synchronize(transport, config, role);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Synchronize with a remote peer, checking caller authorization before each batch. */
  async synchronize_checked(
    transport: JsSqlSyncTransport,
    config: SqlSessionConfig,
    role: SqlSyncRole,
    check: () => Promise<void>,
  ): Promise<SqlSyncResult> {
    try {
      return await this.handle
        .get()
        .synchronize_checked(transport, config, role, check);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }
}

// Re-export sync types and values
export { JsSqlSyncTransport, SqlSessionConfig, SqlSyncRole, JsSqlSyncError };
export type { SqlSyncResult };
export { SqlSyncDirection } from "../pkg/ofdb_wasm.js";
