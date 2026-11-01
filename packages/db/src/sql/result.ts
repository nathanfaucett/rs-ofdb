import type { DbValue } from "../value";

/** One SQL statement result: column names plus positional row values. */
export interface SqlResult {
  columns: string[];
  rows: DbValue[][];
}

/** Positional or named query parameters. */
export type QueryParams = DbValue[] | { [name: string]: DbValue };
