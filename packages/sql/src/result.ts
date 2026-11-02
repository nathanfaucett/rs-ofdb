import type { DbValue } from "./value.js";
import { validateValue } from "./value.js";

/** One SQL statement result: column names plus positional row values. */
export interface SqlResult {
  columns: string[];
  rows: DbValue[][];
}

/** Positional or named query parameters. */
export type QueryParams = DbValue[] | { [name: string]: DbValue };

export function encodeParams(
  params: QueryParams | undefined,
): string | undefined {
  if (params === undefined) return undefined;
  if (
    params === null ||
    typeof params !== "object" ||
    (!Array.isArray(params) &&
      Object.getPrototypeOf(params) !== Object.prototype)
  ) {
    throw new TypeError("parameters must be an array or named object");
  }
  for (const value of Object.values(params)) validateValue(value);
  return JSON.stringify(params);
}

export function decodeResults(raw: string): SqlResult[] {
  const results: unknown = JSON.parse(raw);
  if (!Array.isArray(results)) throw new TypeError("invalid SQL results");
  for (const result of results) {
    if (
      !result ||
      !Array.isArray(result.columns) ||
      !result.columns.every((column: unknown) => typeof column === "string") ||
      !Array.isArray(result.rows)
    ) {
      throw new TypeError("invalid SQL result");
    }
    for (const row of result.rows) {
      if (!Array.isArray(row) || row.length !== result.columns.length)
        throw new TypeError("invalid SQL row");
      for (const value of row) validateValue(value);
    }
  }
  return results as SqlResult[];
}
