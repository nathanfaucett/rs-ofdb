import { SqlError, wrapError } from "../error.js";
import type { WasmSqlTransaction } from "../wasm.js";
import type { QueryParams, SqlResult } from "./result.js";

/** SQL transaction — holds statements until commit or rollback. */
export class SqlTransaction {
  constructor(private wasm: WasmSqlTransaction) {}

  /** Run SQL text within the transaction.
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

  /** Commit all pending statements. */
  async commit(): Promise<void> {
    try {
      await this.wasm.commit();
    } catch (error) {
      throw wrapError(error, SqlError);
    }
  }

  /** Discard all pending statements. */
  async rollback(): Promise<void> {
    try {
      await this.wasm.rollback();
    } catch (error) {
      throw wrapError(error, SqlError);
    }
  }
}
