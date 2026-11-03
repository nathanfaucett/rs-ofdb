import { CommitError, Error, wrapError } from "./error.js";
import { Handle } from "./handle.js";
import type { QueryParams, Result } from "./result.js";
import { decodeResults, encodeParams } from "./result.js";
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

  /** Run SQL text within the transaction.
   * @param sql SQL text, optionally with `$1` or `:name` placeholders.
   * @param params Positional values (array) or named values (object).
   */
  async execute(sql: string, params?: QueryParams): Promise<Result[]> {
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

  /** Alias for execute(). */
  async executeSql(sql: string, params?: QueryParams): Promise<Result[]> {
    return this.execute(sql, params);
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
