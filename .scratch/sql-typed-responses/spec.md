# Typed SQL responses for npm

Status: ready-for-agent

## Problem Statement

The npm SQL Query Handle returns statement results containing column names and positional tagged cells. It validates the transport format, but it does not decode rows into application types.

Callers repeat column indexes, cell-type checks, field renaming, and array mapping. The password manager repeats this work for encrypted records and migration count checks. Some reads substitute empty values for missing results, which can hide a malformed result.

Rust already supports runtime-checked row conversion through `FromRow`, `FromValue`, and `rows_as`. A TypeScript generic cast alone would not provide equivalent runtime checks.

A custom schema language would add unnecessary code. Applications already use schema libraries: the password manager uses Valibot, and other callers can use Zod.

## Solution

Add a typed `select` method to the npm SQL `Client` and `Transaction` Query Handles. It accepts SQL text, a synchronous parser callback, and optional existing query parameters, in that order. It returns an array whose element type is inferred from the parser return type.

The package maps each result row into an object keyed by exact column names. Values remain tagged `DbValue` cells. The caller supplies a parser that checks and transforms this object with its preferred library or application code.

Keep the implementation small: one shared row-mapping and parsing implementation, exposed through matching Query Handle methods. Do not add a custom decoder language, a required schema-library dependency, or library-specific adapters.

Raw `execute` remains the correct interface for statement batches and lossless data transfer.

## User Stories

1. As an npm caller, I want typed row objects so that I do not repeat positional cell access.
2. As a TypeScript caller, I want the result type inferred from my parser so that declared types follow runtime validation.
3. As a Valibot user, I want to use my existing schemas so that I do not learn another schema language.
4. As a Zod user, I want to use my existing schemas without a package-specific adapter.
5. As an application developer, I want to rename SQL columns into application fields through my parser.
6. As an application developer, I want exact column-name mapping so that reordered projections do not change field meanings.
7. As an application developer, I want duplicate column names rejected so that a join cannot silently overwrite a cell.
8. As an application developer, I want SQL NULL preserved so that my schema can distinguish null from a missing field.
9. As an application developer, I want invalid rows to reject the whole read so that I never receive a partial decoded array.
10. As an application developer, I want empty query results to return an empty array.
11. As a caller handling integers, I want decimal strings preserved until my parser explicitly converts them so that i64 precision is not lost.
12. As a caller handling blobs, UUIDs, floats, or JSON, I want tagged type distinctions preserved until explicit conversion.
13. As a caller using parameters, I want the existing positional and named parameter formats to work unchanged.
14. As a transaction caller, I want the same typed-read behavior as a client caller.
15. As a caller handling failures, I want the failing row identified without cell values or SQL appearing in package-generated error messages.
16. As a migration author, I want raw tagged results unchanged so that copying values does not require decoding and re-encoding them.
17. As a package consumer, I want matching runtime exports and declarations in both supported package entry points.
18. As a maintainer, I want shared mapping logic so that client and transaction behavior cannot drift.

## Implementation Decisions

- Keep `Database` as the Embedded Handle. Add no query methods to it.
- Add `select` to `Client` and `Transaction`, with arguments SQL text, parser callback, and optional `QueryParams`.
- Accept a synchronous parser from a readonly column-name-to-`DbValue` object to an inferred application type. Asynchronous schema parsing is not part of this feature.
- Reuse existing `execute` behavior for transport validation, parameter encoding, deadlines, handle ownership, and transaction concurrency rules. Do not add another execution path or WASM method.
- Require exactly one statement result. Zero or multiple statement results reject rather than silently selecting the first result.
- This check occurs after execution. `select` is not a read-only guard, SQL parser, or pre-execution batch guard.
- Map names exactly and case-sensitively. Reject duplicate names before constructing row objects, including when there are no rows. Use existing row-width validation.
- Treat arbitrary column names safely, including `__proto__`, `constructor`, and `prototype`. Names must remain data keys and must not change an object's prototype.
- Preserve every cell's tagged representation. Do not unwrap cells, coerce numbers, parse JSON into ordinary JS values, or merge text and UUID types.
- Pass all projected columns to the parser. The caller's schema determines required fields, extra-field handling, nullability, field renaming, conversions, and domain constraints.
- A missing key stays missing. Do not supply null, empty strings, zero, or other defaults.
- Parse rows in result order. Return the complete parsed array or reject on the first parser failure. Do not expose a partial array or automatically retry.
- With no rows, return an empty array without invoking the parser. This does not validate schema-required fields against column metadata; document that limit.
- Preserve existing execution errors. A parser failure must retain its original cause and identify its zero-based row index through a structured error. Follow existing package error conventions. Package-generated messages must not include SQL or cell payloads; caller-provided causes can contain sensitive data and are not safe to log automatically.
- Result-count and duplicate-column errors must be distinguishable without message matching. Keep the error interface limited to these SQL mapping failures and parser failure context.
- Parsing happens after the Query Operation. A parse failure does not roll back completed client writes or automatically end a transaction. The caller still owns commit, rollback, and close decisions.
- Keep shared mapping implementation internal for this first version. Do not add a standalone batch decoder unless a demonstrated caller requires it.
- Keep raw `Result`, `DbValue`, `QueryParams`, `execute`, and `executeSql` behavior unchanged.
- Add no runtime npm dependency. Do not implement built-in scalar decoders or a custom schema definition interface.
- Document examples with caller-supplied parsers. Show how an existing schema library can validate tagged cells, without making that library required.

## Testing Decisions

- Use the public Query Handle interface as the main test seam. Exercise the same contract on `Client` and `Transaction`; do not add a test-only execution interface.
- Follow the existing real-browser binding, parameter, transaction, and installed-consumer test patterns.
- Check named mapping with reordered projections and aliases, duplicate names, case-sensitive names, and unusual property names.
- Check zero rows, multiple rows, and multiple statement results. Verify the parser is not called for an empty result.
- Check parser input preserves tagged NULL, UUID, text, bool, i64 limits, float bits, blobs, value types, and nested lossless JSON.
- Check a schema can rename fields and explicitly convert an integer decimal string to `bigint` without precision loss.
- Check malformed values fail existing transport validation before application parsing.
- Check a parser failure identifies the row, retains the cause, stops further parsing, and returns no partial array.
- Check package-generated errors do not include SQL or cell contents. Do not assert that arbitrary parser causes are free of sensitive values.
- Check positional and named parameters, closed handles, and existing transaction behavior remain intact.
- Check a decode failure does not imply rollback. Demonstrate explicit caller-owned transaction cleanup and document post-execution result-count rejection.
- Extend installed-consumer type checks to prove inferred array element types and reject invalid parser/parameter usage. Test the published declarations, not only source imports.
- Verify both supported package entry points and packed artifacts through the existing browser and package qualification workflows.
- Run focused TypeScript, lint, and browser checks first. Run the repository-required feature-powerset tests and complexity check as final validation. Report blocked or failed commands accurately; do not fix unrelated failures.

## Out of Scope

- Required Zod, Valibot, or Standard Schema dependencies or protocols.
- Library-specific adapters and a custom row/column/value decoder language.
- Compile-time SQL analysis, schema introspection, generated query types, or an ORM.
- Async parsers, streaming results, optional-row/single-row/scalar convenience methods, and batch typed decoding.
- Plain JavaScript value parameters or automatic output conversions.
- Changes to Rust conversion rules, transport codecs, WASM bindings, storage format, or database schema.
- New remote npm Query Handles, automatic retries, read-only enforcement, or rollback guarantees.
- Password-manager migration repair, table naming changes, or unrelated handle-lifecycle fixes.
- Migrating password-manager callers in this package implementation. Use those reads as acceptance examples; a consumer change can follow once the package interface is validated.

## Further Notes

- Initial complexity is moderate because inference, failure context, and package declarations must agree. Storage-format risk is low because execution and tagged transport remain unchanged. Priority is driven by repeated manual decoding in the password manager.
- Use a real encrypted-record schema and a count schema to assess caller verbosity before adding helpers. Tagged-cell schema verbosity is a known tradeoff, not a reason to pre-build another schema library.
- Rust provides useful precedent for named lookup and runtime validation. Exact numeric-coercion parity is not a goal; application parsers control conversions explicitly.
- Inspection confirmed the interface gap. No implementation, prototype, or validation run was performed while preparing this spec.
