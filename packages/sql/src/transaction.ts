import { CommitError, Error, wrapError } from "./error.js";
import { Handle } from "./handle.js";
import type { QueryParams, Result } from "./result.js";
import { decodeResults, encodeParams } from "./result.js";
import { parseRows, type RowParser } from "./select.js";
import type { WasmSqlTransaction } from "./wasm.js";

/** SQL transaction — holds statements until commit or rollback. */
export class Transaction {
  private readonly handle: Handle<WasmSqlTransaction>;

  constructor(wasm: WasmSqlTransaction) {
    this.handle = new Handle(wasm);
  }

  /** Discard uncommitted work after any in-flight execution settles. */
  close(): void {
    this.handle.close();
  }

  /**
   * Execute SQL and parse the single statement result.
   * Works with SELECT, or INSERT/UPDATE/DELETE with RETURNING.
   * @param sql SQL text (one statement)
   * @param parser Synchronous row → T
   * @param params Optional positional/named parameters
   */
  async execute<T>(
    sql: string,
    parser: RowParser<T>,
    params?: QueryParams,
  ): Promise<T[]> {
    try {
      const encoded = encodeParams(params);
      const raw = await this.handle.run(
        (wasm) => wasm.execute(sql, encoded),
        true,
      );
      return parseRows(decodeResults(raw), parser);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /**
   * Execute SQL and return raw results for all statements.
   * Use for multi-statement batches or when typed parsing is not needed.
   * @param sql SQL text (one or more statements)
   * @param params Optional positional/named parameters
   */
  async execute_raw(sql: string, params?: QueryParams): Promise<Result[]> {
    try {
      const encoded = encodeParams(params);
      const raw = await this.handle.run(
        (wasm) => wasm.execute(sql, encoded),
        true,
      );
      return decodeResults(raw);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Consumes the transaction. Rejection does not prove rollback: reopen and read. */
  async commit(): Promise<void> {
    const wasm = this.handle.take();
    try {
      await wasm.commit();
    } catch (error) {
      throw new CommitError(error);
    }
  }

  /** Discard all pending statements. */
  async rollback(): Promise<void> {
    try {
      await this.handle.take().rollback();
    } catch (error) {
      throw wrapError(error, Error);
    }
  }
}
