import { SqlError } from "./error.js";

/** One forward SQL batch. Versions start at one and must be contiguous. */
export interface Migration {
  readonly version: number;
  readonly description: string;
  readonly sql: string;
}

export interface MigrationResult {
  readonly currentVersion: number;
  readonly appliedVersions: number[];
}

export type MigrationPhase =
  "Plan" | "Begin" | "History" | "Execute" | "Record" | "Commit";
export type MigrationQueryKind =
  | "Validation"
  | "Rejected"
  | "Unsupported"
  | "Storage"
  | "Internal"
  | "Transport"
  | "Protocol"
  | "Timeout"
  | "LimitExceeded";

/** SQL and data are excluded from this error. Commit failure does not prove rollback. */
export class MigrationError extends SqlError {
  readonly version: number | undefined;
  readonly phase: MigrationPhase;
  readonly queryKind: MigrationQueryKind;
  readonly outcome: "unknown" | undefined;

  constructor(cause: globalThis.Error & { migration: string }) {
    const details = JSON.parse(cause.migration) as {
      version: number | null;
      phase: MigrationPhase;
      queryKind: MigrationQueryKind;
      outcome: "unknown" | null;
    };
    super(
      details.queryKind === "Timeout" ? "timeout" : "query",
      cause.message,
      { cause },
    );
    this.name = "MigrationError";
    this.version = details.version ?? undefined;
    this.phase = details.phase;
    this.queryKind = details.queryKind;
    this.outcome = details.outcome ?? undefined;
  }
}
