import { SqlClient } from "./client.js";
import { SqlError, wrapError } from "./error.js";
import { Handle } from "./handle.js";
import type { WasmSqlDatabase } from "./wasm.js";
import { loadWasm } from "./wasm.js";

/** Embedded SQL database handle. Opens the store and creates query handles. */
export class SqlDatabase {
  private readonly handle: Handle<WasmSqlDatabase>;

  private constructor(wasm: WasmSqlDatabase) {
    this.handle = new Handle(wasm);
  }

  /** Open a persistent SQL database backed by IndexedDB. */
  static async open(name: string): Promise<SqlDatabase> {
    try {
      const { SqlDatabase: WasmSqlDatabase } = await loadWasm();
      const wasm = await WasmSqlDatabase.open(name);
      return new SqlDatabase(wasm);
    } catch (error) {
      throw wrapError(error, SqlError);
    }
  }

  /** Release this handle. Independently owned clients remain usable. */
  close(): void {
    this.handle.close();
  }

  /** Create a query handle sharing this database's storage. */
  client(): SqlClient {
    return new SqlClient(this.handle.get().client());
  }
}
