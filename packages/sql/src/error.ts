export type DbErrorKind =
  "timeout" | "query" | "connect" | "transport" | "internal";

/** Base error for all @ofsdk/sql operations. */
export class DbError extends globalThis.Error {
  readonly kind: DbErrorKind;

  constructor(kind: DbErrorKind, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "DbError";
    this.kind = kind;
  }
}

/** Error from a SQL store operation. */
export class SqlError extends DbError {
  constructor(kind: DbErrorKind, message: string, options?: ErrorOptions) {
    super(kind, message, options);
    this.name = "SqlError";
  }
}

/** A started commit failed. Its durable outcome must be checked by reopening and reading. */
export class CommitError extends SqlError {
  readonly outcome = "unknown";

  constructor(cause: unknown) {
    super("internal", "SQL commit failed; durable outcome is unknown", {
      cause,
    });
    this.name = "CommitError";
  }
}

/** Wrap a raw WASM/JS error into a typed DbError subclass. */
export function wrapError(error: unknown, fallback: typeof SqlError): DbError {
  if (error instanceof DbError) return error;
  const message =
    error instanceof globalThis.Error ? error.message : String(error);
  return new fallback("internal", message, { cause: error });
}
