export { Client } from "./client.js";
export { Database } from "./database.js";
export type { DbErrorKind } from "./error.js";
export { CommitError, DbError, Error } from "./error.js";
export type {
  Migration,
  MigrationPhase,
  MigrationQueryKind,
  MigrationResult,
} from "./migration.js";
export { MigrationError } from "./migration.js";
export type { QueryParams, Result } from "./result.js";
export type { Row, RowParser, SelectErrorReason } from "./select.js";
export { SelectError } from "./select.js";
export { Transaction } from "./transaction.js";
export type { DbValue, JsonValue, ValueType } from "./value.js";

import type { SqlSyncResult } from "./database.js";
// Sync exports - import to ensure they're included in bundle
import {
  JsSqlSyncError,
  JsSqlSyncTransport,
  SqlSessionConfig,
  SqlSyncDirection,
  SqlSyncRole,
} from "./database.js";

export type { SqlSyncResult };
export {
  JsSqlSyncError,
  JsSqlSyncTransport,
  SqlSessionConfig,
  SqlSyncDirection,
  SqlSyncRole,
};
