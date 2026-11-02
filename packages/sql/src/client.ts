import { SqlError, wrapError } from "./error.js";
import type { QueryParams, SqlResult } from "./result.js";
import { SqlTransaction } from "./transaction.js";
import type { WasmSqlClient } from "./wasm.js";

/** SQL query handle — the only API for SQL reads and writes. */
export class SqlClient {
  private readonly wasm: WasmSqlClient;

  constructor(wasm: WasmSqlClient) {
    this.wasm = wasm;
  }

  /** Run SQL text and return every statement result.
   * @param sql SQL text, optionally with `$1` or `:name` placeholders.
   * @param params Positional values (array) or named values (object).
   */
  async execute(sql: string, params?: QueryParams): Promise<SqlResult[]> {
    try {
      const raw = await this.wasm.execute(
        sql,
        params === undefined ? undefined : JSON.stringify(params),
      );
      return JSON.parse(raw) as SqlResult[];
    } catch (error) {
      throw wrapError(error, SqlError);
    }
  }

  /** Alias for execute(). */
  async executeSql(sql: string, params?: QueryParams): Promise<SqlResult[]> {
    return this.execute(sql, params);
  }

  /** Start a transaction that holds statements until commit or rollback. */
  async transaction(): Promise<SqlTransaction> {
    try {
      const wasm = await this.wasm.transaction();
      return new SqlTransaction(wasm);
    } catch (error) {
      throw wrapError(error, SqlError);
    }
  }

  /** Return a clone of this client with a request deadline in milliseconds. */
  withDeadline(ms: number): SqlClient {
    return new SqlClient(this.wasm.withDeadline(ms));
  }

  /** Return an independent clone sharing the same storage. */
  clone(): SqlClient {
    return new SqlClient(this.wasm.cloneClient());
  }
}
