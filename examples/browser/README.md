# Persistent browser KV example

A small standalone Wasm application: `Database::open_indexeddb` constructs the
persistent store, `Database::client()` creates the query handle, and Rust `Client`
methods save and retrieve text. The JavaScript bridge is example-local, not a
supported JS SDK. SQL is a separate follow-up ticket; KV is runnable on its own.

## Build and open

From the repository root, install a current stable Rust toolchain (including the
repository's rustfmt and Clippy components), the Wasm target, `wasm-pack`, and `just`:

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-pack --locked
cargo install just --locked
just browser-example-build
just browser-example-serve
```

Open **http://127.0.0.1:8080/** in Chromium or Firefox. Wait for startup, enter a
KV Key and text value, and select **Save**. Wait for **Saved and committed.**, then
reload and select **Retrieve** under the same key. Startup also reads the default
`theme` key afresh. Missing keys display **Key not found.**; an empty stored string
is a successful retrieval, not a missing key. Tab moves between native controls;
Enter in the key input or on Save submits the form. Multiline text is supported.

The serve command builds the actual Wasm assets first and stays running. Use
**Ctrl-C** in that terminal to stop it. To choose another port:

```sh
just browser-example-serve 8081
```

The local-only static server is a small Rust development helper; it is not a
production server or application runtime. No Python, Node, npm, frontend framework,
public JavaScript SDK, Tokio application runtime, or external static-server install
is required. Initial Cargo/wasm-pack installation and dependency downloads need
network access. The application then runs locally without remote database services.

## Storage and failures

The stable IndexedDB name is **`ofdb-browser-example-kv`**. Opening the page never
resets it or inserts seed data. There is no memory fallback. Persistence is scoped
to the **browser profile, origin (scheme, host, port), and database name**.
`localhost` and `127.0.0.1`, different ports, and different profiles have separate
storage. Keep the same URL and browser profile when checking reload persistence.

Browser eviction, private-mode restrictions, denied storage access, quota limits,
and user-cleared site data can remove or prevent storage. A committed write is not
a backup or a promise about power-loss durability. The page reports startup and
query errors visibly; allow site storage/check free space and reload. If a write
fails, retrieve the actual stored value before deciding whether to retry. Never
clear site storage as a recovery step unless you intend to delete its data.

## Served-page checks

Install Chromium/Chrome plus a matching **chromedriver**, and Firefox plus
**geckodriver**. Put drivers and browsers on PATH/in their normal installation
locations, then run:

```sh
just browser-example-smoke
```

Existing drivers obtained by `wasm-pack test` can be reused. For nonstandard paths,
set `CHROMEDRIVER`, `GECKODRIVER`, `CHROME_BINARY`, and/or `FIREFOX_BINARY` to the
corresponding executable paths. Only in a suitably isolated environment where the
Chrome sandbox cannot run, set `CHROME_NO_SANDBOX=1`; normal runs retain it.
On Unix, `kill` must be available for process-group cleanup.

The smoke helper uses the WebDriver HTTP protocol, not Playwright. It serves the
built page on an ephemeral loopback origin and launches fresh driver-managed
profiles, never the contributor's regular profile. It checks fresh missing state,
real Tab/Enter save, visible commit success, reload in the same session, fresh
retrieval, markup-like text safety, and an actionable denied-storage startup error.
The storage-denial override lives only in the automation script, not the app.
Both Chromium and Firefox are required; either failing returns nonzero with phase,
UI status, and driver diagnostics. Redirect stdout/stderr to retain a log if desired.

Every WebDriver HTTP call is bounded to 55 seconds, page navigation to 30 seconds,
async scripts to 45 seconds, and UI waits to 15 seconds. Driver startup has a
10-second polling budget (individual HTTP calls still have their 55-second bound).
The finite phase sequence cannot retry forever. On success, failure, or these
timeouts, the session is closed and the driver is reaped; Unix process-group
cleanup also kills remaining driver/browser descendants. The in-process static
server exits with the helper. Interrupting the helper with SIGKILL cannot run
cleanup, like any process; do not use SIGKILL for routine shutdown.

The app and helper are independent Cargo workspaces with committed lockfiles.
Only `ofdb-kv/indexeddb` is enabled in the Wasm consumer; no in-memory, Redb, SQL,
sync, remote client, native networking or filesystem staging is enabled. The
native-only helper's HTTP/JSON dependencies cannot unify into the Wasm consumer.
The optimized dev build is explicit; this KV-only example does not establish SQL
stack support or a minimum memory requirement.

Focused contributor checks:

```sh
cargo fmt --manifest-path examples/browser/Cargo.toml -- --check
cargo fmt --manifest-path examples/browser/tools/Cargo.toml -- --check
cargo clippy --manifest-path examples/browser/Cargo.toml --target wasm32-unknown-unknown -- -D warnings
cargo clippy --manifest-path examples/browser/tools/Cargo.toml --all-targets -- -D warnings
cargo tree --manifest-path examples/browser/Cargo.toml --target wasm32-unknown-unknown --edges normal
```

Browser execution parses both handwritten JS files; their interfaces and async
behavior are documented with JSDoc. Also review the native labels, focus outlines,
loading messages and error text when changing the UI. The smoke check is not a
screen-reader audit, real quota exhaustion test, crash-durability test, WebKit
validation, or a claim of remote CI success.
