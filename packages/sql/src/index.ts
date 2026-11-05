export { Client } from "./client.js";
export { Database } from "./database.js";
export type { DbErrorKind } from "./error.js";
export { DbError, CommitError, Error } from "./error.js";
export type { QueryParams, Result } from "./result.js";
export { Transaction } from "./transaction.js";
export type { DbValue, JsonValue, ValueType } from "./value.js";

// Sync exports - import to ensure they're included in bundle
import {
  JsSqlSyncTransport,
  SqlSessionConfig,
  SqlSyncRole,
  SqlSyncDirection,
  JsSqlSyncError,
} from "./database.js";
import type { SqlSyncResult } from "./database.js";

export {
  JsSqlSyncTransport,
  SqlSessionConfig,
  SqlSyncRole,
  SqlSyncDirection,
  JsSqlSyncError,
};
export type { SqlSyncResult };
