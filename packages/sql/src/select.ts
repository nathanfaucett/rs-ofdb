import { SqlError } from "./error.js";
import type { Result } from "./result.js";
import type { DbValue } from "./value.js";

/** Exact projected column names and lossless tagged cells. Missing columns stay absent. */
export type Row = Readonly<Record<string, DbValue>> & {
  [k: string]: DbValue | undefined;
};

/** Internal row builder type for constructing rows with null prototype. */
type RowBuilder = Record<string, DbValue>;

/** A synchronous parser; the returned value determines the selected element type. */
export type RowParser<T> = (
  row: Row,
) => T & (Extract<T, PromiseLike<unknown>> extends never ? unknown : never);

export type ExecuteErrorReason = "result-count" | "duplicate-column" | "parser";

/** Mapping or parsing failed after SQL execution. Causes can contain caller data. */
export class ExecuteError extends SqlError {
  readonly reason: ExecuteErrorReason;
  readonly rowIndex: number | undefined;

  constructor(
    reason: ExecuteErrorReason,
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
    this.name = "ExecuteError";
    this.reason = reason;
    this.rowIndex = rowIndex;
  }
}

export function parseRows<T>(results: Result[], parser: RowParser<T>): T[] {
  const result = results[0];
  if (results.length !== 1 || result === undefined) {
    throw new ExecuteError("result-count");
  }
  if (new Set(result.columns).size !== result.columns.length) {
    throw new ExecuteError("duplicate-column");
  }
  return result.rows.map((cells, rowIndex) => {
    const row: RowBuilder = Object.create(null);
    for (let i = 0; i < cells.length; i++) {
      const column = result.columns[i];
      if (column !== undefined) {
        const cell = cells[i];
        if (cell !== undefined) {
          Object.defineProperty(row, column, {
            value: cell,
            writable: true,
            enumerable: true,
            configurable: true,
          });
        }
      }
    }
    try {
      const parsed = parser(row as Row);
      if (parsed !== null && typeof parsed === "object" && "then" in parsed) {
        const p = parsed as PromiseLike<unknown>;
        p.then(undefined, (reason) => {
          throw new ExecuteError("parser", rowIndex, { cause: reason });
        });
        throw new ExecuteError("parser", rowIndex, {
          cause: new Error(
            "Parser returned a promise; synchronous parser required",
          ),
        });
      }
      return parsed;
    } catch (cause) {
      throw new ExecuteError("parser", rowIndex, { cause });
    }
  });
}
