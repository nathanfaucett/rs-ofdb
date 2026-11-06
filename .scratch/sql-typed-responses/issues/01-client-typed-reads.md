# 01: Add typed reads to the SQL Client

**What to build:** Let npm SQL callers read runtime-checked application objects through `Client.select`, using a caller-supplied synchronous parser such as a Zod or Valibot schema parser. Infer the returned array element type from that parser. Preserve the existing tagged transport and raw execution interface.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Add `Client.select` with arguments SQL text, synchronous parser callback, and optional existing `QueryParams`, in that order. Parser input is a readonly column-name-to-`DbValue` object; return types are inferred without an unchecked caller-selected result type.
- [ ] Reuse existing execution, parameter encoding, transport validation, deadlines, and handle ownership. Add no WASM method or duplicate execution path.
- [ ] Keep shared row mapping and parsing internal so the Transaction can reuse it. Add no standalone batch decoder, custom schema language, built-in scalar decoder set, library adapter, or runtime npm dependency.
- [ ] Require exactly one statement result. Reject zero or multiple statement results after execution. Do not claim read-only enforcement or rollback on rejection.
- [ ] Map columns by exact case-sensitive names, independent of projection order. Reject duplicate names even for zero-row results. Preserve arbitrary names, including `__proto__`, `constructor`, and `prototype`, without changing object prototypes.
- [ ] Preserve all tagged cells without coercion, unwrapping, or defaults. Let the caller's parser own required fields, nullability, extra fields, field renaming, numeric conversion, and domain checks.
- [ ] Return rows in result order, or reject at the first parser failure without returning a partial array. For zero rows, return an empty array without calling the parser; document that required schema fields are not checked in that case.
- [ ] Expose distinguishable structured failures for result count, duplicate columns, and parser failures, following package error conventions. Parser failures retain the original cause and zero-based row index; existing execution failures remain unchanged.
- [ ] Package-generated errors contain no SQL or cell contents. Document that caller-provided parser causes can contain sensitive data and must not be logged automatically.
- [ ] Keep `Database`, raw `Result` and `DbValue`, parameter formats, `execute`, and `executeSql` behavior unchanged. Parsing failure does not undo a completed Query Operation or cause automatic retries.
- [ ] Add public-interface browser tests for mapping, reordered and aliased columns, duplicate names, case-sensitive names, unusual property keys, empty and multiple rows, and multiple statement results.
- [ ] Test lossless parser input for every supported tagged value category, including i64 limits, exact float bits, blobs, and nested JSON. Demonstrate explicit bigint conversion and application field renaming with representative encrypted-record and count parsers.
- [ ] Test malformed transport rejection before parsing, first-failure behavior, structured error fields and cause, no partial array, positional/named parameters, and closed handles. Verify post-execution rejection does not imply rollback.
- [ ] Extend installed-consumer checks for inferred result types and invalid parser/parameter usage. Verify exports and declarations through both supported package entry points and the existing packed-package workflow.
- [ ] Document the parser contract, schema-library usage, lossless values, empty-result limit, and post-execution failure behavior. Do not add a schema library to runtime dependencies for examples.
- [ ] Run focused TypeScript, lint, browser, and packed-package checks. Report the commands and results, including any blocked checks. Leave final cross-feature validation to Ticket 02 after both slices are complete.

## Comments

Implements the Client slice of the approved typed SQL responses spec. Password-manager caller changes and Rust conversion changes are out of scope. Use the existing public Query Handle as the test seam; do not introduce a test-only execution interface.
