import { Error as SqlError } from "./error.js";
import type { Result } from "./result.js";
import type { DbValue } from "./value.js";

/** Exact projected column names and lossless tagged cells. Missing columns stay absent. */
export type Row = Readonly<Record<string, DbValue>>;

/** A synchronous parser; the returned value determines the selected element type. */
export type RowParser<T> = (
  row: Row,
) => T & (Extract<T, PromiseLike<unknown>> extends never ? unknown : never);

export type SelectErrorReason = "result-count" | "duplicate-column" | "parser";

/** Mapping or parsing failed after SQL execution. Causes can contain caller data. */
export class SelectError extends SqlError {
  readonly reason: SelectErrorReason;
  readonly rowIndex: number | undefined;

  constructor(
    reason: SelectErrorReason,
    rowIndex?: number,
    options?: ErrorOptions,
  ) {
    super(
      "query",
      reason === "parser"
        ? `SQL row parser failed at row ${rowIndex}`
        : `SQL row mapping failed: ${reason}`,
      options,
    );
    this.name = "SelectError";
    this.reason = reason;
    this.rowIndex = rowIndex;
  }
}

export function parseRows<T>(results: Result[], parser: RowParser<T>): T[] {
  const result = results[0];
  if (results.length !== 1 || result === undefined) {
    throw new SelectError("result-count");
  }
  if (new Set(result.columns).size !== result.columns.length) {
    throw new SelectError("duplicate-column");
  }
  return result.rows.map((cells, rowIndex) => {
    const row = Object.fromEntries(
      cells.map((cell, index) => [result.columns[index], cell]),
    );
    try {
      return parser(row);
    } catch (cause) {
      throw new SelectError("parser", rowIndex, { cause });
    }
  });
}
