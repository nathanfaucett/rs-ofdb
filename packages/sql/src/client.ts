import { SqlError, wrapError } from "./error.js";
import { Handle } from "./handle.js";
import type { QueryParams, SqlResult } from "./result.js";
import { decodeResults, encodeParams } from "./result.js";
import { SqlTransaction } from "./transaction.js";
import type { WasmSqlClient } from "./wasm.js";

/** SQL query handle — the only API for SQL reads and writes. */
export class SqlClient {
  private readonly handle: Handle<WasmSqlClient>;

  constructor(wasm: WasmSqlClient) {
    this.handle = new Handle(wasm);
  }

  /** Release this handle after in-flight calls settle. No new calls are allowed. */
  close(): void {
    this.handle.close();
  }

  /** Run SQL text and return every statement result.
   * @param sql SQL text, optionally with `$1` or `:name` placeholders.
   * @param params Positional values (array) or named values (object).
   */
  async execute(sql: string, params?: QueryParams): Promise<SqlResult[]> {
    try {
      const encoded = encodeParams(params);
      const raw = await this.handle.run((wasm) => wasm.execute(sql, encoded));
      return decodeResults(raw);
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
      const wasm = await this.handle.run((wasm) => wasm.transaction());
      return new SqlTransaction(wasm);
    } catch (error) {
      throw wrapError(error, SqlError);
    }
  }

  /** Return a clone of this client with a request deadline in milliseconds. */
  withDeadline(ms: bigint): SqlClient {
    if (typeof ms !== "bigint" || ms <= 0n || ms > 18446744073709551615n) {
      throw new SqlError(
        "query",
        "deadline must be a positive u64 bigint in milliseconds",
      );
    }
    return new SqlClient(this.handle.get().with_deadline(ms));
  }

  /** Return an independent clone sharing the same storage. */
  clone(): SqlClient {
    return new SqlClient(this.handle.get().clone_client());
  }
}
