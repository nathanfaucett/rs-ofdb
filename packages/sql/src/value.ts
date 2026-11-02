/** The owner lossless codec format. Numbers are strings, never JS numbers. */
export type JsonValue =
  | { type: "null" }
  | { type: "bool"; value: boolean }
  | { type: "string"; value: string }
  | { type: "i64" | "u64" | "f64"; value: string }
  | { type: "array"; value: JsonValue[] }
  | { type: "object"; value: { [key: string]: JsonValue } };

export type ValueType =
  | "Null"
  | "Type"
  | "Uuid"
  | "Bool"
  | "Integer"
  | "Float"
  | "Text"
  | "Json"
  | "Blob";

export type DbValue =
  | { type: "null" }
  | { type: "bool"; value: boolean }
  | { type: "integer" | "float" | "text" | "blob" | "uuid"; value: string }
  | { type: "type"; value: ValueType }
  | { type: "json"; value: JsonValue };

type TaggedObject = { type?: unknown; value?: unknown; [key: string]: unknown };

function object(value: unknown): TaggedObject {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.getPrototypeOf(value) !== Object.prototype
  ) {
    throw new TypeError("expected a tagged value object");
  }
  return value as TaggedObject;
}

function integer(value: unknown, unsigned: boolean): void {
  if (typeof value !== "string" || !/^-?(0|[1-9]\d*)$/.test(value))
    throw new TypeError("expected a decimal string");
  const n = BigInt(value);
  if (
    n < (unsigned ? 0n : -9223372036854775808n) ||
    n > (unsigned ? 18446744073709551615n : 9223372036854775807n)
  ) {
    throw new RangeError("integer out of range");
  }
}

/** Validate both caller values and generated binding results without coercion. */
export function validateValue(value: unknown, nested = false): void {
  const entry = object(value);
  if (Object.keys(entry).some((key) => key !== "type" && key !== "value"))
    throw new TypeError("unknown value fields");
  const payload = entry.value;
  const tag = entry.type;
  if (tag === "null" && !Object.hasOwn(entry, "value")) return;
  if (tag === "bool" && typeof payload === "boolean") return;
  if (typeof payload === "string") {
    if (tag === (nested ? "string" : "text")) return;
    if (tag === (nested ? "i64" : "integer")) {
      integer(payload, false);
      return;
    }
    if (nested && tag === "u64") {
      integer(payload, true);
      return;
    }
    if (tag === (nested ? "f64" : "float") && /^[0-9a-fA-F]{16}$/.test(payload))
      return;
    if (
      !nested &&
      tag === "uuid" &&
      /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
        payload,
      )
    )
      return;
    if (
      !nested &&
      tag === "blob" &&
      /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
        payload,
      )
    )
      return;
    if (
      !nested &&
      tag === "type" &&
      [
        "Null",
        "Type",
        "Uuid",
        "Bool",
        "Integer",
        "Float",
        "Text",
        "Json",
        "Blob",
      ].includes(payload)
    )
      return;
  }
  if (!nested && tag === "json") {
    validateValue(payload, true);
    return;
  }
  if (nested && tag === "array" && Array.isArray(payload)) {
    for (const item of payload) validateValue(item, true);
    return;
  }
  if (nested && tag === "object") {
    for (const item of Object.values(object(payload)))
      validateValue(item, true);
    return;
  }
  throw new TypeError("invalid tagged value");
}
