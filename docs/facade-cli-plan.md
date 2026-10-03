# SQL and KV facade / CLI plan

Status: implementation is complete for the agreed facades, hosting/sync entry points, and client-only CLIs. Consumer crate namespaces have been updated across the open workspaces. Focused full-target tests and compile-fail API checks pass for SQL, KV, and both CLIs. Remaining work is the workspace feature-powerset run, CRAP review, and registry-backed package verification. Checked items mark completed changes or validated decisions; unchecked items remain.

## Goal

Publish separate `ofdb-sql` and `ofdb-kv` libraries for embedded and remote queries. Provide separate client-only `sql-cli` and `kv-cli` executables. Keep database hosting and sync setup in library interfaces, outside remote clients and both CLIs.

Domain terms and ownership are defined in [CONTEXT.md](../CONTEXT.md). Existing persistence and replication semantics are described in [design.md](design.md).

## Agreed decisions

- `ofdb-sql` is SQL-only; `ofdb-kv` is KV-only. Neither library exports both clients.
- The repository root becomes workspace-only, with no root crate.
- Each facade has an embedded `Database` and a remote `Client`. Within each store, query methods, results, and errors are consistent between the handles.
- SQL and KV do not share a query interface, data model, or storage interface.
- Embedded handles support local transactions and sync setup. Remote handles provide queries, not sync setup or transactions across calls.
- Sync uses explicit sessions. The caller supplies transport, peers, authorization, and scheduling. No automatic background replication.
- Two separate binaries, `sql-cli` and `kv-cli`, consume their corresponding facades. Both are query clients only, with no server, sync, or administration commands.
- SQL retains typed statements and SQL-text helpers. Initial remote SQL queries use existing statement execution.
- Default features are empty; remote, embedded storage, hosting, and sync are enabled explicitly.
- Query callers never supply timestamps or timestamp providers.
- Clients do not automatically retry requests. Library request deadlines are caller-configurable, with no fixed default.
- Facades live in `crates/sql` and `crates/kv`; the existing KV implementation moves to `crates/kv-store`.
- CLI packages are `ofdb-sql-cli` in `crates/sql-cli` and `ofdb-kv-cli` in `crates/kv-cli`. Both provide one-shot queries, not interactive shells.
- Use explicit constructors and structured query errors within each store.
- Features are `std`, `remote`, `in-memory`, `redb`, `server`, and `sync`, plus `sql` for SQL-text helpers. They may coexist without merging handle roles.
- The no-timestamp rule excludes clock arguments/providers, not explicit KV expiry settings.
- Remove unused `wasm` and `examples-util` crates.
- Shared packages use `ofdb-btree`, `ofdb-btree-redb`, and `ofdb-btree-automerge`. KV packages use `ofdb-kv-store`, `ofdb-kv-client`, `ofdb-kv-server`, `ofdb-kv-proto`, and `ofdb-kv-sync`, separate from facade `ofdb-kv`.
- Remote-only builds exclude the local engine, Redb, Automerge, and sync implementation.
- SQL-specific directories use `sql-*`; SQL package names use `ofdb-sql-*`. Shared B-tree crates do not receive a SQL prefix.
- Replace obsolete interfaces and update callers. Do not add old-name re-exports or compatibility wrappers.

## CLI and connection contract

- Each invocation requires exactly one target: `--database PATH`, `--memory`, `--endpoint URL`, or `--unix-socket PATH`. There is no implicit default database. `Database::open(path)` and CLI `--database PATH` may create a missing database at that explicit path.
- SQL accepts exactly one input: `--query SQL` or `--file PATH`; `--file -` reads stdin. The full input executes as one statement batch, with parameter support aligned with the library.
- KV set accepts `set KEY --value TEXT` or `set KEY --file PATH`; `--file -` reads stdin. File/stdin values remain opaque bytes. Optional `--expires-at UNIX_MS` sets an absolute expiry deadline.
- KV commands are `get KEY`, `set KEY`, `delete KEY`, `scan START END`, `scan-prefix PREFIX`, and `scan-all`.
- Successful KV set/delete emits no stdout and exits 0. Scans emit an ordered JSON array of key/value entries, with base64 values.
- SQL results and KV scans use JSON. Retain SQL value tags; encode integers as decimal strings, blobs as base64, and floats as exact 64-bit hexadecimal representations. Apply the encoding recursively to nested JSON numbers and preserve signed/unsigned and SQL/JSON null distinctions. KV get outputs raw bytes without an added newline.
- Optional `--params-file PATH` uses the same SQL value codec with the existing positional/named parameter containers and binding rules. The parameter file is separate from SQL stdin; do not add placeholder syntax.
- Exit 0 means success, 1 means query failure or missing KV get, and 2 means invalid command usage. Errors go to stderr. Empty values and empty scans are successful.
- CLI `--timeout` is optional positive integer seconds, with no fixed default. Reject zero, negative, and invalid values as usage errors. Library deadline configuration uses `Duration`. One budget starts after input is read and covers remote connection plus query completion, or embedded query completion. Libraries likewise have no fixed request deadline. Timeout is not proof of rollback or a promise of hard interruption.
- Async remote construction establishes a connection immediately and reuses it for later queries. Requests are not automatically retried.
- Empty SQL batches are invalid on embedded and remote handles.

## KV and error contract

- Range scans include start and exclude end; start greater than or equal to end produces an empty result.
- Scans return one visible value per key in ascending UTF-8 key order. Prefixes are literal; an empty prefix matches all keys.
- Delete of a missing or already tombstoned key succeeds without a returned existence flag.
- Set without expiry clears previous expiry. Past deadlines are accepted and make values immediately invisible; expiry is not a tombstone.
- No new pagination, conditional writes, TTL-duration options, or alternate range bounds.
- Remote requests preserve store-specific categories through structured error details. Do not recover categories from message text. Keep protocol, transport, and timeout failures distinguishable from store query failures.

## Current constraints

- `src/database.rs` exposes one SQL `Database` enum for embedded and remote modes. Several methods reject remote use at runtime. `src/api.rs` exports engine and sync types.
- SQL now has distinct embedded `Database` and remote `Client` facades. Remote clients expose statement execution only; embedded handles retain schema, transaction, conflict, and sync operations.
- The SQL facade exposes embedded hosting through `Database::serve_tcp` and `serve_unix` when `server` is enabled.
- The remote SQL client uses a SQL-only query contract. The facade exposes an optional `Client::with_deadline(Duration)` request deadline with no default.
- SQL `execute(Vec<Statement>)` uses one engine transaction for the batch. The standard `EngineExecutor` preserves that scope. SQL `BEGIN`, `COMMIT`, and `ROLLBACK` are not supported.
- KV has a public `ofdb-kv` facade over separate store, client, server, and sync crates. Its facade-owned errors preserve storage/query categories through versioned structured remote details; protocol, transport, and timeout failures remain distinct.
- KV embedded reads use the process clock internally; remote reads use server time. Public handles take no timestamp provider.
- Remote KV writes commit one store transaction per call. Expiry is an absolute deadline, not a TTL. Remote `Client::with_deadline(Duration)` has no default.
- KV sync uses explicit transport sessions with full snapshots. Incoming batches commit separately. It does not use SQL's manifest/incremental-change protocol.
- `sql-cli` and `kv-cli` now exist as separate one-shot binaries. Local/remote CLI integration tests pass; both help outputs exclude hosting and sync commands.
- Internal path dependencies have registry version requirements. Packages required by facade feature graphs are publishable; `ofdb-sql-test` remains private. `cargo package` reaches registry lookup but cannot verify dependencies that are not yet available in the registry. No packages have been published.
- The unused WASM and examples-util crates have been removed.
- SQL uses a separate documented lossless JSON codec in the SQL CLI. It does not change stored value serialization.
- SQL parameters remain positional vectors or named maps of typed values. The translator binds parameters across the full statement batch before sending statements remotely.
- SQL and KV remote errors preserve store-specific categories through structured details. KV facade errors do not expose Tonic status types.
- KV `remote` no longer activates the B-tree crate through `std`; local B-tree dependencies are enabled only by embedded storage and sync features. CI checks both remote-only dependency trees.

These facts describe the current implementation and focused checks recorded above. Do not infer rollback from a lost remote response or assume all storage adapters have the same isolation guarantees.

## 1. Decisions and interface specifications

Checked items are settled design decisions, not implemented code. Unchecked specifications below turn those decisions into precise interfaces and tests before implementing their callers.

All interviewed user choices and the shared design are confirmed. Remaining specifications implement the agreed contracts below.

- [x] Make the root workspace-only; all packages live below `crates/`.
- [x] Use `crates/sql`, `crates/kv`, `crates/kv-store`, `crates/sql-cli`, and `crates/kv-cli` for the agreed package layout.
- [x] Use `Database` for embedded handles and `Client` for remote handles in each facade.
- [x] Use explicit embedded and remote constructors instead of one mode-selecting URI constructor.
- [x] Establish remote connections immediately through async constructors. Reuse connections; do not automatically retry requests.
- [x] Require exactly one explicit CLI target: database path, memory, endpoint URL, or Unix socket.
- [x] Specify constructor signatures and connection/request deadline configuration. Keep local transactions on embedded handles.
- [x] Retain SQL-text helpers and typed statements; initial remote SQL supports existing statement execution.
- [x] Preserve existing KV query/expiry behavior as listed in the KV contract; do not add new KV operations.
- [x] Keep embedded-only SQL schema/index/conflict and transaction operations on `Database`, outside the common remote query interface.
- [x] Reject empty SQL batches on both embedded and remote handles.
- [x] Specify per-operation atomicity, visibility, ordering, and failure behavior. SQL batches use one local engine transaction; KV calls and sync batches retain their documented commit scope. No transaction spans remote calls.
- [x] Use structured query errors within each store.
- [x] Preserve store-specific categories through structured remote error details rather than message matching.
- [x] Define concrete per-store error types and wire details independent of engine and Tonic types. KV facade errors own `ErrorKind`; versioned wire details preserve category and message.
- [x] Remove caller-supplied query timestamps and timestamp providers from facade interfaces.
- [x] Keep explicit expiry settings; the no-timestamp rule applies to clock arguments/providers.
- [x] Specify internal clock behavior and deterministic tests. Embedded reads use the local process clock; remote reads use the host clock. Public handles accept no timestamp provider.
- [x] Use empty default features.
- [x] Use `std`, `remote`, `in-memory`, `redb`, `server`, and `sync`, plus `sql` for SQL-text helpers.
- [x] Specify dependency forwarding and feature combinations, including remote-only, embedded-memory, embedded-Redb, hosting, and embedded-plus-sync. Remote-only dependency inspection confirms no local engine, Redb, Automerge, or sync implementation dependencies.
- [x] Remove the unused WASM and examples-util crates.
- [x] Use the agreed `ofdb-btree*` and `ofdb-kv-*` package namespaces; name the lower-level KV implementation `ofdb-kv-store`.
- [x] Use separate binaries `sql-cli` and `kv-cli`; neither provides hosting, sync, or administration.
- [x] Use packages `ofdb-sql-cli` and `ofdb-kv-cli` in `crates/sql-cli` and `crates/kv-cli`; support one-shot queries only.
- [x] Settle target selection, SQL input, KV set input, JSON/raw output modes, and exit codes as listed in the CLI contract.
- [x] Use tagged SQL JSON values with decimal-string integers, base64 blobs, and exact 64-bit hexadecimal floats, including nested JSON numbers.
- [x] Support optional `--params-file PATH` with the same value codec and existing parameter containers/binding rules.
- [x] Use the agreed KV command set, optional absolute expiry, silent successful writes, and ordered JSON scan entries with base64 values.
- [x] Document the exact SQL JSON schema and parameter-file examples in the SQL CLI README using the agreed value codec.
- [x] Do not automatically retry requests.
- [x] Keep library request deadlines caller-configurable, with no fixed default.
- [x] Use optional CLI `--timeout`, with no fixed default.
- [x] Use one CLI timeout budget from completion of input reading through connection/query completion.
- [x] Use positive integer seconds for CLI timeout and `Duration` for library deadline configuration.
- [x] Preserve open-or-create behavior for explicit database paths; never select or create a default path.
- [x] Specify failure reporting. A lost write response can mean the write committed; cancellation and timeout do not guarantee interruption or rollback.

Done when: both facade contracts, feature matrices, and CLI command contracts are written down, with no unsupported remote methods hidden in a common handle.

## 2. Rename SQL-specific crates

| Current directory/package | Target directory       | Target package              |
| ------------------------- | ---------------------- | --------------------------- |
| Root `ofdb` facade        | `sql`                  | `ofdb-sql`                  |
| `engine`                  | `sql-engine`           | `ofdb-sql-engine`           |
| `engine-automerge`        | `sql-engine-automerge` | `ofdb-sql-engine-automerge` |
| `engine-redb`             | `sql-engine-redb`      | `ofdb-sql-engine-redb`      |
| `query`                   | `sql-query`            | `ofdb-sql-query`            |
| `schema`                  | `sql-schema`           | `ofdb-sql-schema`           |
| `value`                   | `sql-value`            | `ofdb-sql-value`            |
| `macros`                  | `sql-macros`           | `ofdb-sql-macros`           |
| `test`                    | `sql-test`             | `ofdb-sql-test`             |
| `client`                  | `sql-client`           | `ofdb-sql-client`           |
| `server`                  | `sql-server`           | `ofdb-sql-server`           |
| `proto`                   | `sql-proto`            | `ofdb-sql-proto`            |
| `protocol`                | `sql-protocol`         | `ofdb-sql-protocol`         |
| `sync`                    | `sql-sync`             | `ofdb-sql-sync`             |
| `sql-translator`          | unchanged              | `ofdb-sql-translator`       |

Additional package moves and names:

| Current/new package                      | Target directory | Target package                                          |
| ---------------------------------------- | ---------------- | ------------------------------------------------------- |
| Existing KV implementation               | `kv-store`       | `ofdb-kv-store`                                         |
| New KV facade                            | `kv`             | `ofdb-kv`                                               |
| `btree`, `btree-redb`, `btree-automerge` | unchanged        | `ofdb-btree`, `ofdb-btree-redb`, `ofdb-btree-automerge` |
| `kv-client`, `kv-server`, `kv-sync`      | unchanged        | `ofdb-kv-client`, `ofdb-kv-server`, `ofdb-kv-sync`      |
| `proto-kv`                               | unchanged        | `ofdb-kv-proto`                                         |
| New SQL CLI                              | `sql-cli`        | `ofdb-sql-cli`                                          |
| New KV CLI                               | `kv-cli`         | `ofdb-kv-cli`                                           |

- [x] Move root facade sources, tests, and examples to their new SQL package. Remove root package/library/example/test definitions; retain workspace configuration.
- [x] Move the directories and update package names, workspace members, dependency paths, dependency keys, and feature forwarding.
- [x] Rename shared/KV packages to their agreed namespaces. Keep existing directories unless a facade name requires a move.
- [x] Delete unused wasm and examples-util crates and remove their manifest, script, and workflow references.
- [x] Update Rust imports, explicit library names, generated macro paths, macro tests, examples, and integration tests.
- [x] Update build scripts, protobuf generation paths, `justfile`, workflows, and documentation.
- [x] Keep crate naming changes separate from wire-protocol or persistence-format changes. This plan does not require changing stored state or protobuf package names.
- [x] Regenerate affected lockfiles through Cargo. Remove obsolete files and old public aliases.

Done when: renamed crates build and no old SQL package/import/path references remain, except intentional historical documentation.

## 3. Isolate query contracts from embedded implementations

- [x] Remove the engine dependency from the remote SQL client. The query error contract is in the SQL query crate.
- [x] Keep remote statement/result conversion in SQL protocol crates without pulling in engine, Redb, Automerge, or sync dependencies.
- [x] Define the KV query error/result contract and map local storage and remote failures to it without exposing Tonic as the facade contract. Structured details preserve categories; timeout, transport, protocol, and store query failures remain distinguishable.
- [x] Make optional features follow the agreed dependency matrix. KV `std` no longer activates B-tree storage; local storage and sync features enable B-tree explicitly.
- [x] Add remote-only dependency-graph checks for both facades. SQL remote excludes engine, B-tree, Redb, Automerge, and sync; KV remote excludes B-tree, store, Redb, Automerge, and sync. CI now checks these graphs.

Done when: both remote clients compile independently of local storage and sync, and share query contracts only with their own embedded adapters.

## 4. Refactor the SQL facade

- [x] Replace the mixed `Database` interface with an embedded `Database` and a remote `Client`. Remove obsolete runtime-unsupported methods and dispatch code.
- [x] Implement the agreed common SQL query methods and result/error types on both handles.
- [x] Preserve SQL translation and batch execution behavior. Do not add remote transaction sessions or transaction SQL commands as incidental work.
- [x] Expose local transaction creation, execution, commit, and rollback only through the embedded interface.
- [x] Expose SQL sync setup/session entry points only through embedded/server interfaces, using caller-supplied transport and configuration.
- [x] Provide explicit embedded and eager remote constructors. Add an optional caller-set request deadline with no default.
- [x] Keep `lib.rs` and module roots thin. Separate query-facing exports from embedded/sync exports through modules and feature gates.

Done when: a developer can run the same supported SQL query against an embedded or remote handle, and cannot call sync or local transactions on a remote handle.

## 5. Add the KV facade

- [x] Add the `ofdb-kv` package over the existing KV implementation. Do not reuse the public facade name for the lower-level store package.
- [x] Add an embedded `Database` and a remote `Client` with matching KV query operations, results, and errors.
- [x] Implement feature-gated in-memory and Redb construction by reusing existing B-tree adapters.
- [x] Hide transaction and clock setup for normal embedded queries according to the agreed contract. Keep explicit local transactions available separately.
- [x] Preserve generation, tombstone, opaque-byte, and expiry semantics. Do not turn expiry into deletion or silently replace absolute deadlines with TTLs.
- [x] Expose the existing KV sync sessions on embedded/server interfaces. Preserve snapshot exchange and divergent-history rejection; do not replace them with SQL sync.
- [x] Reuse the existing remote KV protocol and client implementation. Change the protocol only where required by an agreed query contract.

Done when: equivalent embedded and remote KV queries use the same public contract, including binary values, expiry, missing keys, and scans.

## 6. Provide embedded/server control

- [x] Expose SQL and KV hosting through their own store-specific interfaces and optional dependencies.
- [x] Wire embedded handles into their corresponding query executors/services. Do not publish a combined SQL/KV library facade.
- [x] Expose only the host's approved query operations remotely. Keep sync configuration and local transactions outside remote query contracts.
- [x] Provide explicit sync session entry points for hosts. Reuse existing SQL and KV sync configuration and transport interfaces.
- [x] Document ownership of listeners, sockets, database handles, shutdown, and partial sync progress in the facade READMEs.

Done when: each store can be hosted and synchronized independently, with no remote sync-configuration interface.

## 7. Build the two client-only CLIs

- [x] Add binary-only packages `ofdb-sql-cli` and `ofdb-kv-cli` under `crates/sql-cli` and `crates/kv-cli`, producing `sql-cli` and `kv-cli`. Each depends only on its corresponding facade, with no combined SQL/KV library interface.
- [x] Implement local/remote target selection through the corresponding facade constructors.
- [x] Implement one-shot client commands by calling facade query operations. Do not add interactive shells.
- [x] Implement required, mutually exclusive target/input options and optional request timeout. Reject missing or conflicting options as invalid usage. Do not duplicate query translation, storage, protocol conversion, or sync logic in command handlers.

- [x] Implement the agreed output and exit-code contract. Preserve arbitrary KV bytes; focused tests cover binary, empty, and ordered scan values.
- [x] Implement and document the lossless SQL JSON codec and parameter-file parsing without changing stored serialization. Focused tests cover large integers, signed zero, infinities, NaN bit patterns, nested JSON numbers, blobs, and SQL/JSON null distinctions.
- [x] Implement one timeout budget after input reading, with no default and no automatic retries. Do not report timeout as proof of rollback.
- [x] Add command help and examples for embedded and remote queries only. Do not add server, sync, or administration commands.

Done when: both binaries cross only their own facade's query interface, with no hosting, sync, or administration commands/dependencies.

## 8. Update existing consumers

Do this with the relevant rename/interface changes, not as a compatibility layer after them.

- [x] Update this workspace's examples, tests, macros, and utility crates with the renamed package graph.
- [x] Update SQL consumer dependency names and imports in the listed `of` crates and Tauri app.
- [x] Update direct SQL sync dependencies in `of/crates/idp-server` and `of/crates/management-service`.
- [x] Update KV/shared dependency names and imports in `of/crates/storage-service`, `of/crates/storage-server`, and `offs/crates/file-system`.
- [x] Search all four repositories for stale package/import namespaces. No old `ofdb::`, `kv_sync::`, or aliased KV store references remain in consumers; `ofnet` source was not changed.
- [ ] Run focused checks for every changed consumer. `idp-db`, `idp-model`, `idp-server`, `management-server`, `management-service`, `storage-server`, `storage-service`, and `file-system` pass. The Tauri app check is blocked by current unrelated `idp-server` API changes (`storage_router` and `with_storage_file_systems` no longer exist).

Done when: consumers use the new names and role-specific interfaces directly, with no old-name adapter code.

## 9. Make the publication graph complete

- [x] Generate dependency-ordered package lists for the public remote, embedded, hosting, and sync feature graphs. The lists below follow current manifest dependencies; external registry availability is not implied.
- [x] Add registry versions alongside internal paths where required for publication. Internal path dependencies use `major.minor` requirements, full dependency tables, and `default-features = false`.
- [x] Remove `publish = false` from packages required by public facade feature graphs. Keep test-only `ofdb-sql-test` private.
- [x] Check package names, descriptions, repository metadata, licenses, and README files. `cargo metadata --no-deps` succeeds. Packaged build/protobuf inputs still need registry-backed package verification.
- [ ] Verify packages in dependency order. `cargo package --allow-dirty` now reaches crates.io lookup and fails because internal prerequisites are not yet available there; do not publish without approval.
- [ ] Test external consumers using registry dependencies for remote-only and embedded-plus-sync configurations. This requires the prerequisite packages to be published or otherwise available from a test registry.
- [x] Record release constraint: do not publish crates until separately approved.

Current dependency order (dependencies first; external crates omitted):

| Facade feature graph | Publication order                                                                                                                                       |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SQL remote           | `ofdb-sql-value`, `ofdb-sql-schema`, `ofdb-sql-query`, `ofdb-sql-proto`, `ofdb-sql-protocol`, `ofdb-sql-client`, `ofdb-sql`                             |
| SQL embedded memory  | `ofdb-btree`, `ofdb-btree-automerge`, `ofdb-sql-value`, `ofdb-sql-schema`, `ofdb-sql-query`, `ofdb-sql-engine`, `ofdb-sql-engine-automerge`, `ofdb-sql` |
| SQL embedded Redb    | `ofdb-btree`, `ofdb-btree-redb`, `ofdb-btree-automerge`, SQL value/schema/query/engine packages as above, `ofdb-sql-engine-redb`, `ofdb-sql`            |
| SQL hosting          | SQL remote and selected embedded backend prerequisites, then `ofdb-sql-proto`, `ofdb-sql-protocol`, `ofdb-sql-server`, `ofdb-sql`                       |
| SQL sync             | SQL embedded-memory prerequisites, `ofdb-sql-translator`, `ofdb-sql-sync`, `ofdb-sql`                                                                   |
| KV remote            | `ofdb-kv-proto`, `ofdb-kv`                                                                                                                              |
| KV embedded memory   | `ofdb-btree`, `ofdb-kv-store`, `ofdb-kv`                                                                                                                |
| KV embedded Redb     | `ofdb-btree`, `ofdb-btree-redb`, `ofdb-kv-store`, `ofdb-kv`                                                                                             |
| KV hosting           | `ofdb-btree`, `ofdb-kv-store`, `ofdb-kv-proto`, `ofdb-kv-server`, `ofdb-kv`                                                                             |
| KV sync              | `ofdb-btree`, `ofdb-btree-automerge`, `ofdb-kv-store`, `ofdb-kv-sync`, `ofdb-kv`                                                                        |
| CLI binaries         | The corresponding facade, then `ofdb-sql-cli` or `ofdb-kv-cli`                                                                                          |

Done when: both public facades have a verified publishable dependency graph for every supported feature set.

## 10. Tests, docs, and final validation

- [x] Run separate SQL and KV query contract tests against local and remote adapters. SQL coverage is in `crates/sql/tests/local_sql.rs` and `remote.rs`; KV coverage is in `local_kv.rs`, `remote_kv.rs`, and `crates/kv/src/database.rs`. Do not use a shared SQL/KV query abstraction.
- [x] Test SQL remote batch rollback on failure, local/remote error parity, rejection of empty batches, transaction-command rejection, and local transaction commit/rollback in `crates/sql/tests/local_sql.rs` and `remote.rs`.
- [x] Test KV transaction atomicity, scan ordering/ranges, missing versus empty values, expiry boundaries, generation renewal after deletion, and deterministic clock behavior in `local_kv.rs`, `remote_kv.rs`, and `crates/kv-store/tests/replication.rs`.
- [x] Test explicit sync success, interrupted/failing transfers, batch rollback, tombstones, and KV divergent-history rejection in `cluster_sync.rs`, `cluster_chaos.rs`, and `crates/kv-store/tests/replication.rs`.
- [x] Add compile-fail doctests showing remote handles lack sync/local-transaction methods and each facade lacks the other store's query exports (`cargo test -p ofdb-sql --doc --features remote,in-memory,redb,server,sync,sql` and `cargo test -p ofdb-kv --doc --features remote,in-memory,redb,server,sync`).
- [x] Check that remote-only dependency trees exclude engine, Redb, Automerge, and sync implementations.
- [x] Add CLI integration tests for local and remote operations, failure exits, SQL batches, binary KV values, ordered scans, quiet successful writes, host shutdown, and absence of hosting/sync commands in both help outputs.
- [x] Verify no automatic retry: each remote query method sends one RPC directly, with no retry loop. Library deadlines remain caller-configurable.
- [x] Fix local SQL error mapping so invalid engine queries report `Rejected`, matching the SQL server/client category.
- [x] Update the root README, `CONTEXT.md`, `docs/design.md`, facade/CLI READMEs, feature guidance, and examples. There is no release-notes file; release notes remain a release-time task.
- [x] Update CI for renamed packages, feature-matrix tests, metadata validation, and remote dependency checks. Registry-backed package verification remains blocked until prerequisites are available.
- [x] Run `cargo fmt --all -- --check` in `ofdb`; prior formatting checks passed in `of` and `offs`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings` in `ofdb` with the default feature set. SQL and KV Clippy also pass with their embedded, remote, hosting, and sync features enabled; this caught and fixed the oversized Redb transaction variant by boxing it.
- [ ] Run `cargo hack test --feature-powerset --all-targets --workspace` as final feature validation. SQL all-target tests pass with `in-memory,redb,remote,server,sql`; KV all-target tests pass with `in-memory,redb,remote,server,sync`; both CLI suites and compile-fail doctests pass with their local and remote features. SQL also passes a no-default-features check and SQL/KV both pass feature-specific Clippy. Cargo Hack enumerates 445 workspace configurations. The full matrix remains unverified: `target/` is about 20 GB with 21 GB free, and a previous matrix attempt exhausted disk space.
- [ ] Review functions above the default CRAP threshold of 30. The existing `/tmp/lcov.info` is stale and mismatches 10 current source files; `cargo crap` reports 25 of 856 above threshold, so those results are not a sound final review.
- [ ] Build and verify packaged external examples. A standalone path-dependency consumer using both embedded and remote SQL/KV handles with sync enabled passes `cargo check`; `cargo package --list` includes the facade sources, READMEs, and examples. Registry package verification remains blocked: `cargo package -p ofdb-sql --allow-dirty --no-verify` cannot find `ofdb-sql-client` in crates.io. Do not publish without approval. Focused consumer checks pass except for the Tauri app, which is blocked by unrelated router API changes documented in section 8.

Done when: the agreed contracts are tested, required validation passes or remaining blockers are explicitly recorded, and published usage needs no private path dependencies.

## Complexity, risk, and order

- Naming: low logic complexity; high cross-repository breakage risk. Inventory and update callers together.
- Query contracts and feature isolation: highest priority. Complete before facade/CLI implementation to avoid importing local engine types into remote clients again.
- Facades: medium complexity. Reuse engines, adapters, protocol conversions, and sync sessions.
- CLI: medium complexity, low domain risk if it stays a facade consumer. Keep binary data and failure behavior explicit.
- Publication: high dependency risk. Audit early; verify after the dependency and feature work.

Recommended order: settle contracts and audit publication → rename and update references → isolate query contracts → implement facades and hosting → implement CLI → finish consumer migration and package verification → final validation. No storage migration, new replication protocol, or background sync system is required by this plan.
