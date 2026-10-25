# 08: Synchronize browser SQL replicas using the existing protocol

**What to build:** An IndexedDB-backed browser SQL replica reconciles schema, Logical Rows, conflicts and deletion metadata through caller-supplied transport using existing bounded memory staging and checked apply semantics.

**Blocked by:** 04 — Preserve full SQL transaction and reconciliation semantics.

**Status:** done

- [x] Support checked broad, selected and scoped Sync Sessions through browser-compatible public facade interfaces without changing the current SQL protocol version or logical payload formats.
- [x] Reuse existing Row Reconciler capabilities, schema reconciliation, history validation, checked apply and bounded memory staging. Native stage-directory interfaces remain native-only.
- [x] Preserve atomic complete catalog apply, Logical Row and Tombstone state, derived Index Record maintenance and per-batch caller authorization checks.
- [x] Integrate Committed Change Subscriptions for shared clients, conflict Resolution and incoming replication, publishing after successful persistence even if the caller stopped polling.
- [x] Preserve independent selected-work acknowledgement, later-commit retention, initial broad selection and overflow/gap fallback. Failed, rolled-back and net byte no-op transactions remain silent.
- [x] Real-browser tests cover bootstrap, concurrent offline edits, conflict inspection/Resolution after sync, cancellation, denied apply, recovery, catalog lifecycle and replay silence.
- [x] Malformed or rejected incoming batches cannot partially persist catalog, row or index state; prior completed batches survive interrupted sessions and retry converges.
- [x] Verify browser/native logical interoperability using current native payloads or generated fixtures, including deletion metadata and native history, without adding bundled network transports.
- [x] Document external-write and remote-only wakeup limits and avoid claiming selective physical I/O from the initial full-snapshot backend.


## Comments

### Implementation and validation — 2026-10-25

Confirmed a clean `/home/nathan/code/rust/aicacia/ofdb-browser-08` worktree on
`browser-indexeddb-08`, exactly at requested base
`30e56c93e8660bf08c561e959b927f42d4f942fe`, before editing. Read AGENTS, GLOSSARY,
the complete browser spec, this ticket, prior ticket notes 01–07 and the browser
README. Loaded TDD; the user explicitly approved Database → Client → Transaction,
existing sync APIs and lower persistence seams only for hard invariants. No user
interview or parallel implementers. Complexity/risk: high for cancellation,
checked apply and history validation; priority: complete one-shot SQL browser sync.

Implemented IndexedDB facade subscriptions from the existing shared Kernel signal.
The byte adapter already independently observes issued native commit completion;
SQL now consumes that same signal rather than wrapping queries or publishing from
caller futures. Existing checked broad, selected and scoped paths dispatch directly
to IndexedDB and reuse bounded memory stages, Row Reconciler history checks,
complete catalog apply, data batches and derived index maintenance. No protocol or
logical payload format changed. Native file stage types, directory methods and
tempfile dependencies are target-gated, including explicitly enabling native-stage
in a browser build; removed the now-obsolete browser file-factory branch.

A failing real-browser scoped test found that historical validation incorrectly
required natural primary keys to equal internal UUID row identities. Corrected
validation to require non-null immutable historical primary-key values, with the
row-identity equality check retained for a single UUID primary key. Natural keys
and UUID-containing composite keys now reconcile through scoped checked APIs.
Existing native scoped security/rejection regressions still pass.

Added 12 SQL acceptance tests in `tests/browser/src/sql_sync.rs`, common transport
and fixture verification in `sql_sync_support.rs`, and native generation/consumption
in `tests/sql_protocol.rs`. `sql_native_fixture.json` is committed native-generated
v7 bootstrap and subsequent incremental/deletion/catalog traffic, exact response
bytes and complete retained native state/history/deletion metadata. Both browser
and native consumers verify the committed bytes without regenerating expectations.
Browser tests cover bootstrap, independent selected acknowledgement and later
commits, initial All, scoped directions/history/candidate denial, missing native
dependencies and snapshot recovery, offline conflicts/Resolution, Tombstones,
drop/recreation, overflow/gaps, independent-open limits and settled replay silence.

Cancellation tests stop at checked catalog/data apply boundaries and during actual
issued native data commits. Earlier completed catalog batches survive subsequent
denial/cancellation; fresh retry converges. Failed native abort/injected quota,
interrupted staging and malformed catalog input persist no partial catalog/index
state or invalidation. A malformed data document with a valid advertised digest
rolls back the complete data batch and indexes, preserving the earlier catalog.
Session and custom memory-stage budgets reject before apply. SQL deadline tests
also verify no early notification and eventual publication after query/explicit
transaction timeout or cancellation, independently of caller polling.

Updated facade and browser READMEs: external opens/tabs and remote-only writes do
not provide local wakeups, explicit broad recovery is necessary, and selected
logical work does not avoid O(database-size) IndexedDB snapshots. Bounded sync
staging is not a bound on physical snapshot memory. Continuous browser runner
acceptance remains ticket 09.

#### Passing validation

Commands below are relative to this worktree. Because the editor registers only
the sibling integration root, execution used explicit manifest/worktree paths.
Browser runs used the installed Flatpak browser binaries via ticket 01's ignored
capabilities JSON and matching ChromeDriver. Native builds used a 16 MiB thread
stack unless a test explicitly selects its own stack. Browser fixture uses its
existing optimized dev profile and 16 MiB linear stack. `WASM_BINDGEN_TEST_TIMEOUT=120`.

- `wasm-pack test --headless --chrome --chromedriver DRIVER tests/browser --features sync`:
  **80 passed**, Chrome **156.0.8078.47**, Window.
- `wasm-pack test --headless --firefox tests/browser --features sync`:
  **80 passed**, Firefox **158.0.1**, Window.
- Chrome `--features worker,sync`: **77 passed**, Dedicated Worker.
- Firefox `--features worker,sync`: **77 passed**, Dedicated Worker.
  The three page/iframe-only tests are not included in Worker mode.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path tests/browser/Cargo.toml --features sync --test sql_protocol --test kv_protocol --test native`:
  **4 passed**, including unchanged committed SQL/KV fixture consumption and native
  Send/Sync/spawned query, transaction and sync regressions.
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql-sync --features native-stage`:
  **36 passed** (9 unit, 23 catalog/protocol, 4 native staging).
- `RUST_MIN_STACK=16777216 cargo test -p ofdb-sql --features in-memory,redb,sql,sync,native-stage --test changes --test commit_invalidation --test scoped_sync --test facade_sync --test conflicts`:
  **51 passed** (5 + 1 + 25 + 11 + 9). This was invoked together with the failing
  sync_stack test described below; the five listed suites each completed successfully.
- Fixture `cargo clippy --tests --target wasm32-unknown-unknown --features sync -- -D warnings`
  and `--features worker,sync`: passed.
- Native `cargo clippy -p ofdb-sql -p ofdb-sql-sync -p ofdb-sql-engine-automerge --features ofdb-sql/in-memory,ofdb-sql/sql,ofdb-sql/native-stage -- -D warnings`: passed.
- `cargo check -p ofdb-sql --lib --target wasm32-unknown-unknown --no-default-features --features indexeddb,sync`
  and `--features indexeddb,sql,sync,native-stage`: passed.
- Fixture browser test compile with explicitly enabled `ofdb-sql/native-stage`:
  passed. Normal browser dependency tree with `sync,ofdb-sql/native-stage` contains
  no Redb, engine-redb, tempfile, tonic, Iroh or SQL/KV server/protocol packages.
- Workspace and browser fixture `cargo fmt --all -- --check`, and `git diff --check`:
  passed.
- Integration tip was confirmed at `30e56c9`; `git merge --no-edit browser-indexeddb`
  in this worktree reported **Already up to date**. No integration changes were
  omitted and no conflict resolution was needed.

#### Red phases, failures and validation limits

- Initial real Chrome subscription test failed with Unsupported from
  `Database::sync_changes()`; enabling the actual Kernel subscription fixed it.
  The natural-key scoped test then failed with the historical UUID-only check;
  correcting that invariant made natural/composite scoped acceptance pass.
- One intermediate scoped-denial test waited for both peers after an early local
  rejection. Replaced that harness wait with cancellation/discard of the peer once
  a failure is observed, consistent with the documented failed-transport contract.
- An earlier Firefox Window run had **78 passed, 1 failed** on the first fresh
  IndexedDB open's documented **5,000 ms timeout** while native compilation was
  running. An unchanged isolated rerun passed 79/79; the final expanded suite passed
  80/80. No timeout-policy change or automatic-open retry masks this observation.
- Required final `RUST_MIN_STACK=16777216 cargo hack test --feature-powerset --all-targets`
  **failed at combination 8/1035**, BTree `--no-default-features --features sharded`,
  with eight missing std/async_stream/in-memory export/ToString errors. Reproduced
  all eight on untouched base/integration `30e56c9` using
  `cargo test --locked -p ofdb-btree --no-default-features --features sharded --all-targets`.
  The remaining full feature matrix did not run; not a full-matrix pass.
- Native `--test sync_stack` **aborted with SIGABRT** when its explicitly configured
  **two-MiB Tokio worker** overflowed. Reproduced the same overflow on untouched
  `30e56c9` with identical `in-memory,redb,sql,sync,native-stage` features. Raising
  RUST_MIN_STACK cannot override that test's explicit worker size. No two-MiB stack
  support is claimed and the unrelated native regression was not rewritten.
- Required `just crap` **timed out at 200 seconds** during coverage compilation,
  after several earlier crates completed; **no complete coverage or CRAP score**.
  An initial native `ofdb-sql-sync --all-features` attempt also timed out within
  a combined 240-second invocation while compiling; the focused native-stage suite
  was then run successfully. No complete all-features/Iroh test pass is claimed.
- Actual quota exhaustion, whole-browser/OS crash or power loss, WebKit/Safari,
  default/minimum-stack support, bounded whole-database snapshot memory and ticket
  09 continuous browser lifecycle acceptance are not claimed.

Implementation and ticket acceptance are done in the documented browser/stack
configuration. Broader workspace matrix, low-stack native regression and CRAP
validation remain incomplete/failing as explicitly recorded above.
