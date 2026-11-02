# @ofdb/sql

Local SQL over IndexedDB. The package includes generated JavaScript, declarations,
WASM, and the IndexedDB adapter. It has no runtime npm dependencies, server,
identity provider, native module, or Node import. No worker or sync task starts
automatically.

## Install the artifact

Copy `ofdb-sql-0.1.0.tgz` and `SHA256SUMS` from the build output into the consuming
checkout, for example under `vendor/`. Check the checksum in that directory and
install the tarball:

```sh
(cd vendor && sha256sum --check SHA256SUMS)
pnpm add ./vendor/ofdb-sql-0.1.0.tgz
```

Commit the tarball and consumer lockfile, or place the same immutable tarball in
an artifact registry. No sibling checkout path is needed. This package is
packable; it has not been published to npm. Version `0.1.0` alone does not identify
a build: retain its checksum. No database format migration is provided.

## Public interface

```ts
SqlDatabase.open(name: string): Promise<SqlDatabase>
database.client(): SqlClient
database.close(): void

client.execute(sql: string, params?: QueryParams): Promise<SqlResult[]>
client.executeSql(sql: string, params?: QueryParams): Promise<SqlResult[]>
client.transaction(): Promise<SqlTransaction>
client.clone(): SqlClient
client.withDeadline(milliseconds: bigint): SqlClient
client.close(): void

transaction.execute(sql: string, params?: QueryParams): Promise<SqlResult[]>
transaction.executeSql(sql: string, params?: QueryParams): Promise<SqlResult[]>
transaction.commit(): Promise<void>
transaction.rollback(): Promise<void>
transaction.close(): void

type QueryParams = DbValue[] | { [name: string]: DbValue }
interface SqlResult { columns: string[]; rows: DbValue[][] }
```

`executeSql` is an alias. Use `$1` positional or `:name` named parameters.
Deadline milliseconds must be a positive `bigint` within `u64`; `1000n` means one
second. No default deadline or automatic retry is applied. The wrapper imports
the generated WASM declarations directly, including `with_deadline(bigint)` and
`clone_client()`. There is no hand-maintained binding signature interface.

```ts
import { SqlDatabase } from "@ofdb/sql";

const database = await SqlDatabase.open("vault");
const client = database.client();
try {
  await client.execute(
    "CREATE TABLE entries (id INTEGER PRIMARY KEY, text TEXT)",
  );
  await client.execute("INSERT INTO entries VALUES ($1, $2)", [
    { type: "integer", value: "9223372036854775807" },
    { type: "text", value: "YWJj" },
  ]);
  const results = await client.execute("SELECT id,text FROM entries");
  // results[0].rows[0] has the same two tagged values, not JS numbers or guessed blobs.
} finally {
  client.close();
  database.close();
}
```

## Lossless values

Parameters and results use the existing owner codec,
`ofdb_kv::{parse_value, encode_value}`. SQL does not implement another Rust codec.
The TS boundary validates input and output; Rust validates parameters again.
Plain values, unknown fields/tags, malformed payloads, overflow, and undefined
values are rejected, not coerced. The SQL engine still checks schema constraints.

| `type`    | `value`                                                                                 |
| --------- | --------------------------------------------------------------------------------------- |
| `null`    | Omit this field entirely                                                                |
| `bool`    | Boolean                                                                                 |
| `integer` | Signed i64 decimal string, e.g. `"-9223372036854775808"`                                |
| `float`   | Exact IEEE-754 bits: 16 hexadecimal digits, e.g. `"8000000000000000"` for negative zero |
| `text`    | String, including empty or base64-looking text                                          |
| `blob`    | Standard padded base64 string, including `""` for empty bytes                           |
| `uuid`    | Hyphenated UUID string; output is canonical lowercase                                   |
| `type`    | `Null`, `Type`, `Uuid`, `Bool`, `Integer`, `Float`, `Text`, `Json`, or `Blob`           |
| `json`    | A tagged `JsonValue`, described below                                                   |

`JsonValue` uses `{type:"null"}`, `{type:"bool",value:boolean}`,
`{type:"string",value:string}`, `{type:"i64",value:decimalString}`,
`{type:"u64",value:decimalString}`, `{type:"f64",value:hexBits}`,
`{type:"array",value:JsonValue[]}`, or
`{type:"object",value:{[key:string]:JsonValue}}`. Numbers nested in JSON never pass
through a JS number. Object keys, including `$number`, remain ordinary keys. This
is the KV owner's collision-free tagged JSON format, **not** the SQL CLI's
`$number` shorthand. Output normalizes representations; it preserves values and
types, not the input spelling of numbers or UUIDs. The transport accepts all
float bit patterns; database arithmetic is not a bit-preservation operation.

## Ownership and commit outcomes

- Each database, client, clone, deadline client, and transaction is an owned
  handle. Close each handle in `finally`. `close()` is idempotent.
- A database can close while its clients remain usable. A client can close while
  its independently owned transactions or clones remain usable.
- Closing rejects new operations immediately. An in-flight call retains its Rust
  handle until it settles, then releases it. Closing does not cancel that call.
- A transaction permits one operation at a time. Concurrent execute, commit, or
  rollback attempts reject with a busy error, without consuming the transaction.
- `commit()` and `rollback()` consume the transaction once started, including on
  failure. Reuse rejects as closed. Closing an uncommitted transaction discards its
  pending writes; it does not commit them.
- Resolved `commit()` means the native IndexedDB transaction completed. A started
  commit that rejects throws `SqlCommitError` with `outcome: "unknown"` and an
  underlying `cause`. Reopen and read before retrying. Timeout, page termination,
  or a lost completion callback is not proof of rollback. Other operations throw
  `SqlError`; raw WASM failures currently use its `internal` category.
- The page can stop after native commit but before the caller sees completion.
  No API can report an outcome to a page that no longer exists.

## Browser and bundler requirements

Use a browser realm with WebAssembly, BigInt, ES modules, and IndexedDB. Use an
origin that permits IndexedDB. Storage can still be denied, evicted, or deleted;
this is not backup or protection against OS/browser data loss.

The WASM build reserves a **16 MiB linear stack**. This is required by the tested
SQL transaction workload, not a measured minimum. The default stack trapped.
The stack is built into the artifact; consuming apps need no Rust configuration.

The package works in Window and a dedicated module Worker. There is no automatic
worker: create one with `new Worker(new URL('./worker.js', import.meta.url),
{type:'module'})`. Import `@ofdb/sql` inside the worker and close its handles before
termination. Test/build budgets must also bound worker startup and shutdown.

TypeScript consumers must include `ESNext.Disposable` in `compilerOptions.lib`
(along with their normal ES and DOM libraries), because the generated WASM
declarations expose optional disposal methods. Runtime disposal through this
wrapper uses `close()` and does not require `Symbol.dispose`. TypeScript `5.9.3`
is tested without `skipLibCheck`.

Vite `8.3.5` consumes the root ESM export and emits WASM assets for both Window and
Worker. `@ofdb/sql/browser` is the prebuilt browser ESM export: serve it with the
adjacent `pkg/` directory intact. Do not inline the generated module without also
rewriting/copying its WASM asset URL. Serve `.wasm` as `application/wasm`; CSP must
permit the modules, worker if used, and WebAssembly compilation. A failed WASM
fetch can be retried in the same realm. No Node polyfills are needed.

## Rebuild and qualify

Use Rust `1.99.1` with `wasm32-unknown-unknown`, Node `26.5.0`, and pnpm `12.4.1`.
The build checks Rust `1.99.1` and wasm-pack `0.13.1`; the latter is an exact package
dev dependency. `Cargo.lock` pins wasm-bindgen `0.2.130`. `pnpm-lock.yaml` pins JS
build dependencies. Run from the repository root:

```sh
pnpm install --frozen-lockfile
pnpm --filter @ofdb/sql run build:all
pnpm --filter @ofdb/sql exec playwright install chromium
pnpm --filter @ofdb/sql test
pnpm --filter @ofdb/sql run test:package
```

`build:all` uses a release build, a fixed stack, the lockfile, and no wasm-opt step.
The build script bounds WASM compilation to 240 seconds. An existing
`CARGO_TARGET_DIR` can be used to share build dependencies. `BROWSER_PATH` can
select an existing Chromium executable. Each browser test is bounded to 30
seconds. `test:package` packs the tarball, writes `artifacts/SHA256SUMS`, installs
it offline into an isolated consumer, checks installed declarations, builds with
Vite, then repeats the real-browser suite. It replaces only its generated
`artifacts/consumer/` directory.

Output: `packages/sql/artifacts/ofdb-sql-0.1.0.tgz`. The raw binary is
`packages/sql/pkg/ofdb_wasm_bg.wasm`. See
[`docs/sql-bindings-validation.md`](../../docs/sql-bindings-validation.md) for
measured evidence and limits. This qualification does not include sync, app
integration, the KV JS wrapper, or a different browser engine.
