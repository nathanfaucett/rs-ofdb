export type DbErrorKind =
  | "timeout"
  | "query"
  | "connect"
  | "transport"
  | "internal";

/** Base error for all @ofdb/kv operations. */
export class DbError extends Error {
  readonly kind: DbErrorKind;

  constructor(kind: DbErrorKind, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "DbError";
    this.kind = kind;
  }
}

/** Error from a KV store operation. */
export class KvError extends DbError {
  constructor(kind: DbErrorKind, message: string, options?: ErrorOptions) {
    super(kind, message, options);
    this.name = "KvError";
  }
}

/** Wrap a raw WASM/JS error into a typed DbError subclass. */
export function wrapError(error: unknown, fallback: typeof KvError): DbError {
  if (error instanceof DbError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new fallback("internal", message, { cause: error });
}
