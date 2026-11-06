import { Error, wrapError } from "./error.js";
import { Handle } from "./handle.js";
import {
  type Migration,
  MigrationError,
  type MigrationResult,
} from "./migration.js";
import type { QueryParams, Result } from "./result.js";
import { decodeResults, encodeParams } from "./result.js";
import { parseRows, type RowParser } from "./select.js";
import { Transaction } from "./transaction.js";
import type { WasmSqlClient } from "./wasm.js";

/** SQL query handle — the only API for SQL reads and writes. */
export class Client {
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
  async execute(sql: string, params?: QueryParams): Promise<Result[]> {
    try {
      const encoded = encodeParams(params);
      const raw = await this.handle.run((wasm) => wasm.execute(sql, encoded));
      return decodeResults(raw);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Parse one statement result after execution. Parsing does not undo writes. */
  async select<T>(
    sql: string,
    parser: RowParser<T>,
    params?: QueryParams,
  ): Promise<T[]> {
    return parseRows(await this.execute(sql, params), parser);
  }

  /** Apply the complete migration list. Each pending version commits with its history record. */
  async migrate(migrations: readonly Migration[]): Promise<MigrationResult> {
    try {
      const raw = await this.handle.run((wasm) =>
        wasm.migrate(JSON.stringify(migrations)),
      );
      return JSON.parse(raw) as MigrationResult;
    } catch (error) {
      if (
        error instanceof globalThis.Error &&
        "migration" in error &&
        typeof error.migration === "string"
      ) {
        throw new MigrationError(
          error as globalThis.Error & { migration: string },
        );
      }
      throw wrapError(error, Error);
    }
  }

  /** Alias for execute(). */
  async executeSql(sql: string, params?: QueryParams): Promise<Result[]> {
    return this.execute(sql, params);
  }

  /** Start a transaction that holds statements until commit or rollback. */
  async transaction(): Promise<Transaction> {
    try {
      const wasm = await this.handle.run((wasm) => wasm.transaction());
      return new Transaction(wasm);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Return a clone of this client with a request deadline in milliseconds. */
  withDeadline(ms: bigint): Client {
    if (typeof ms !== "bigint" || ms <= 0n || ms > 18446744073709551615n) {
      throw new Error(
        "query",
        "deadline must be a positive u64 bigint in milliseconds",
      );
    }
    return new Client(this.handle.get().with_deadline(ms));
  }

  /** Return an independent clone sharing the same storage. */
  clone(): Client {
    return new Client(this.handle.get().clone_client());
  }
}
