# Embedded and remote query clients

Status: implementation complete. Embedded and remote clients, SQL transactions, CLI dispatch, caller migration, and focused validation are complete. Full feature powerset and CRAP checks remain excluded as requested.

## Goal

Extend `ofdb_kv::Client` and `ofdb_sql::Client` to support embedded storage as well as remote connections. Keep `Database` as the embedded control handle. Use each store's `Client` for all CLI queries.

This plan is self-contained. Do not require the older `facade-cli-plan.md`; that file is absent from the current checkout.

## Confirmed contract

- SQL and KV remain separate libraries, client types, and query interfaces. Do not add a shared SQL/KV query abstraction.
- `Database::client(&self) -> Client` is infallible and is the only embedded client constructor. Do not add `Client::open()`, `Client::in_memory()`, or a combined URI constructor.
- `Database` constructs storage and exposes embedded database controls: hosting and sync. It does not expose direct query methods or transaction creation. Callers use `Database::client()` for every query and transaction.
- `Client` is the only facade query API for ordinary reads, writes, and transactions, for both embedded and remote storage. It never exposes hosting, sync, or a public database accessor. Do not implement `Deref` to `Database`.
- An embedded client owns shared storage references, has no lifetime parameter, and remains usable after the originating `Database` is dropped.
- `Client: Clone` remains supported. Clones share storage or connections, not copies of database contents. Deadline configuration is independent per clone. Preserve `Debug` without exposing stored data.
- SQL `Client` exposes `execute()`, SQL-gated `execute_sql()`, generic translator helpers, and `transaction()`. `Database` does not expose these query methods or transaction creation.
- `Client::transaction()` is asynchronous for both backends and returns one `Transaction` handle. The handle supports typed statement execution, translator helpers, commit, and rollback. Remote transactions use a server-side transaction stream; dropping the handle rolls back uncommitted work.
- KV clients expose existing `get`, `set`, `delete`, `scan`, `scan_prefix`, and `scan_all` behavior unchanged, and `Client::transaction()` groups those operations atomically on embedded and remote stores.
- `Client::with_deadline(Duration)` applies to both backends, without a default. Each SQL translation helper uses one budget for translation and execution; do not restart the budget for execution.
- Library query deadlines exclude connection establishment. The CLI retains its outer budget for construction/connection and query completion.
- Embedded queries with a configured deadline require a Tokio runtime with time enabled. Queries without a deadline must work without Tokio.
- Timeout does not prove rollback or guarantee interruption of blocking storage work. No automatic retries or background replication.
- Default features stay empty. Embedded-only builds must not enable remote transport; remote-only builds must not enable embedded storage or sync.

## Implementation baseline and constraints

Paths below are relative to the `ofdb` workspace root.

| Area                | Files                                                                | Current state                                                                                                                                                       |
| ------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| KV facade           | `crates/kv/src/client.rs`, `database.rs`, `lib.rs`, `Cargo.toml`     | `Client` supports embedded and remote queries and transactions. KV query operations on `Database` were made crate-private; facade calls dispatch through `Client`.  |
| SQL facade          | `crates/sql/src/client.rs`, `database.rs`, `lib.rs`, `Cargo.toml`    | `Client` supports embedded and remote queries and transactions. Ordinary query methods are crate-private helpers; public Database transaction creation was removed. |
| SQL helper behavior | `crates/sql-engine/src/engine.rs`                                    | Generic helpers translate locally, then execute a statement batch. `translate_and_select` requires exactly one result and uses `rows_as()`.                         |
| SQL CLI             | `crates/sql-cli/src/main.rs`                                         | All query targets now dispatch through `Client`.                                                                                                                    |
| KV CLI              | `crates/kv-cli/src/main.rs`                                          | All query targets now dispatch through `Client`.                                                                                                                    |
| Existing tests      | `crates/sql/tests`, `crates/kv/tests`, both CLI `tests/cli.rs` files | Facade tests cover client behavior. Lower-level KV store tests remain unchanged.                                                                                    |
| Docs                | `CONTEXT.md`, `docs/design.md`, facade READMEs                       | The corrected contract is recorded: `Client` is the only query and SQL transaction API; `Database` remains the embedded control handle.                             |

Blocking Redb work already occurs inside async operations. Do not add worker tasks, `spawn_blocking`, or a global lock to claim hard timeout interruption. SQL transaction timeouts do not prove rollback or interruption; a timed-out remote transaction is dropped and the server rolls it back when the stream closes.

## Implementation approach

Use a private feature-gated backend in each `Client`: shared embedded storage or remote connection. Reuse existing storage/engine clones; do not reopen files, copy data, or add another ownership layer when existing clones suffice.

Keep `Client` as the only public facade query and transaction path. Move shared execution logic into private helpers or backend types. Do not keep public query wrappers or transaction creation on `Database`. Do not create a public query trait or a new crate.

## 1. KV client and Database query API cleanup

Write scope: `crates/kv/src`, `crates/kv/Cargo.toml`, and facade tests under `crates/kv/tests`.

- [x] Make shared KV storage clonable internally using the existing `KvStore` clones. Keep storage private; adding public `Database: Clone` is not required.
- [x] Add `Database::client(&self) -> Client` with an owned shared-storage backend and no deadline.
- [x] Replace the client's channel-only representation with a private backend enum. Gate embedded and remote variants independently.
- [x] Dispatch all six query methods to the selected backend. Reuse existing transaction helpers; do not duplicate KV query semantics.
- [x] Remove `get`, `set`, `delete`, `scan`, `scan_prefix`, and `scan_all` from the public `Database` API. Keep shared storage/query helpers private and route facade queries through `Client` only.
- [x] Migrate KV facade tests and callers from `Database` queries to `Database::client()`; keep lower-level `kv_store` tests unchanged.
- [x] Confirm `Database` exposes only construction and host/sync control, not a second facade query interface.
- [x] Preserve structured remote status mapping and store-specific embedded error categories; remote error tests pass.
- [x] Gate channel, endpoint, protocol, connector imports, `connect`, `connect_unix`, and status decoding on `remote`.
- [x] Export `Client` whenever `remote`, `in-memory`, or `redb` is enabled. Keep the default build free of query backends.
- [x] Enable Tokio time for embedded deadlines and keep Tokio `net` activation on `remote`.
- [x] Apply the optional timer to the complete selected query future. Construct no Tokio timer on the no-deadline path.
- [x] Preserve `Clone` and `Debug` without requiring private storage to implement `Debug`.
- [x] Update client API docs and compile-fail checks for restricted control access.

## 2. SQL client and Database query API cleanup

Write scope: `crates/sql/src`, `crates/sql/Cargo.toml`, and facade tests under `crates/sql/tests`.

- [x] Add `Database::client(&self) -> Client`, using the existing shared engine clone.
- [x] Replace the remote-only representation with a private embedded/remote backend enum. Preserve `Clone` and `Debug`.
- [x] Export `Client` for `remote`, `in-memory`, or `redb`; gate remote dependencies and constructors on `remote`.
- [x] Enable Tokio time for embedded deadlines without enabling remote transport.
- [x] Dispatch `execute(Vec<Statement>)` through the selected backend. Keep empty-batch rejection and one transaction per statement batch.
- [x] Expose these generic query helpers on `Client`, with matching signatures and bounds:
  - `translate_and_execute<T: Translator>(&self, query: &str, translator: &T)`.
  - `translate_and_execute_with_params<T: Translator>(&self, query: &str, params: Option<&QueryParams>, translator: &T)`.
  - `translate_and_select<T: Translator, U: FromRow>(&self, query: &str, translator: &T)`.
- [x] Keep generic translator helpers available without `sql`; gate built-in SQL-text translation on `sql`.
- [x] Translate locally for both client backends and use existing typed-statement execution remotely. Added a separate streaming RPC for explicit multi-call remote transactions.
- [x] Preserve one-result validation and structured SQL error categories in translator helpers; tests cover invalid result counts and translation errors.
- [x] Use a private untimed execution path so SQL helpers use one deadline budget for translation, execution, and typed result conversion.
- [x] Untimed embedded execution works without Tokio; configured deadline requirements are documented.
- [x] Remove direct query methods and generic translator query helpers from the public `Database` API: `execute`, `execute_sql`, `translate_and_execute`, `translate_and_execute_with_params`, and `translate_and_select`. Put the public query API on `Client` only.
- [x] Keep `Database` construction and server/sync controls. Remove `Database::transaction()` and expose `Client::transaction()` returning `Transaction`.
- [x] Remove every ordinary data/schema query operation from `Database`, including `index_schema`, `index_lookup`, `table_schema`, `create_table`, `drop_table`, `row_conflicts`, and `resolve_row`. Use `Client` statements/helpers for supported behavior; remove convenience APIs with no supported Client equivalent.
- [x] Move embedded execution helpers behind crate-private APIs. Do not call a public `Database` query method from `Client`.
- [x] Keep lifecycle, hosting, and sync on `Database`; do not keep query APIs or transaction creation there.
- [x] Update client docs and compile-fail control-access checks.

## 3. CLI dispatch

Depends on sections 1 and 2.

Write scope: both CLI `src/main.rs` files and their `tests/cli.rs` files.

- [x] In each CLI, construct a store-specific `Client` from the selected target: remote constructors for endpoints/sockets, `Database::client()` for memory/files.
- [x] Ensure every facade query and transaction call site and integration test uses `Client`; no ordinary query, DDL, or transaction is called directly on `Database`.
- [x] Gate client imports and query dispatch on each supported query backend.
- [x] Use one KV command dispatcher. Delete `execute_database` and its duplicate command handling.
- [x] Use one SQL execution/result-encoding path after target selection.
- [x] Retain explicit target validation and errors for unavailable backends.
- [x] Keep input reading outside the timeout and construction/connection plus query/result encoding inside the CLI's outer budget.
- [x] Preserve command names, parameters, expiry behavior, output encodings, stdout/stderr rules, exit codes, and Unix platform gates.
- [x] No server, sync, administration, interactive, or retry commands were added.

## 4. Focused regression coverage

- [x] Added public-facade tests for embedded KV and SQL clients with in-memory and Redb storage.
- [x] Verify clients created from one `Database` share visible writes. `Database` has no direct query methods to compare against.
- [x] Verified clients remain usable after dropping `Database` with memory and Redb; Redb data reopens after all handles are dropped.
- [x] Verified untimed embedded queries under `futures::executor::block_on`, without a Tokio runtime.
- [x] Verified configured deadlines with pending work under Tokio, timeout categories, and independent clone configuration.
- [x] Verified SQL translation is within the deadline using a pending translator. Tested generic helpers locally and remotely, including params, typed results, translation errors, and invalid result counts.
- [x] Verified remote facade calls and structured errors with embedded features enabled, and verified remote-only builds compile without embedded dependencies. The remote runtime tests use a local server and therefore enable an embedded backend.
- [x] Covered KV opaque bytes, expiry, range bounds, ordered scans, literal prefixes, missing gets, and delete-missing success through facade tests.
- [x] Add API tests confirming transactions are available on embedded and remote `Client`, but hosting/sync are not.
- [x] Ran existing CLI regressions against unified dispatch; local and remote output tests passed.

## 5. Focused build checks

Run from the `ofdb` workspace root. These are checks for this change, not a request to repeat prior full validation.

- [x] `cargo fmt --all -- --check` passed.
- [x] `git diff --check` passed.
- [x] Checked facade feature combinations with no features, `in-memory`, `redb`, `remote`, and `remote,in-memory,redb` for SQL and KV.
- [x] Checked generic SQL helpers without built-in SQL support, then checked `sql` with embedded-only and remote-only facade features.
- [x] Reviewed normal dependency trees. KV and SQL remote-only trees include transport dependencies but no embedded storage/engine; embedded-only trees include storage/engine but no remote transport.
- [x] `cargo test -p ofdb-kv --no-default-features --features remote,in-memory,redb,server --tests` passed, including embedded client memory and Redb tests.
- [x] `cargo test -p ofdb-sql --no-default-features --features remote,in-memory,redb,server,sql --tests` and `--doc` passed, including embedded/remote transactions and compile-fail API tests.
- [x] KV and SQL CLI integration tests passed with `--no-default-features --features remote,in-memory,redb`.
- [x] CLI feature checks passed: `cargo check -p ofdb-kv-cli --no-default-features --features in-memory`, `... --features remote`; same checks passed for `ofdb-sql-cli`.
- [x] `cargo clippy -p ofdb-kv -p ofdb-sql -p ofdb-kv-cli -p ofdb-sql-cli --no-default-features --features remote,in-memory,redb,sql -- -D warnings` passed.

Do not run `cargo hack`/the full feature powerset or `just crap` for this handoff. The user explicitly excluded the prior full validation steps. Record focused command results and any blockers; do not claim unrun checks passed.

## 6. Docs and completion

- [x] Updated `CONTEXT.md` and `docs/design.md` to record embedded clients and SQL transaction roles.
- [x] Update embedded and remote client examples to show `Client::transaction()` and deadline caveats.
- [x] Search affected Markdown docs and update stale client-role and transaction descriptions.
- [x] Marked completed checks. Full feature powerset and CRAP review remain excluded as requested; registry publication checks are outside this implementation.
- [x] Preserve unrelated workspace changes. No packages were published, no commit or branch was created, and storage/sync were not redesigned.

## Order, risk, and acceptance

KV and SQL facade work was implemented in parallel with disjoint write scopes. The CLIs now construct the corresponding `Client` for embedded and remote queries. Focused checks passed as recorded above.

Complexity: moderate; existing storage clones and query contracts can be reused. Main risks: SQL helper error parity, feature-gate leaks, nested deadline budgets, recursive dispatch, and accidental runtime requirements on untimed embedded queries.

Embedded/remote clients and CLI dispatch are in place. Public query operations and transaction creation have been removed from `Database`; SQL transactions use `Client::transaction()` for embedded and remote backends. The remote transaction stream changes the SQL RPC protocol; there is no stored-format change. Caller migration and focused validation passed. Full feature powerset and CRAP review remain excluded.
