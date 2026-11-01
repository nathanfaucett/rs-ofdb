import { SqlError, wrapError } from "../error";
import { loadWasm } from "../wasm";
import type { WasmSqlDatabase } from "../wasm";
import { SqlClient } from "./client";

/** Embedded SQL database handle. Opens the store and creates query handles. */
export class SqlDatabase {
  private constructor(private readonly wasm: WasmSqlDatabase) {}

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

  /** Create a query handle sharing this database's storage. */
  client(): SqlClient {
    return new SqlClient(this.wasm.client());
  }
}
