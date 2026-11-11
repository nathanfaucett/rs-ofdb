# ofdb-sync-runtime

The shared, sync-owned continuous reconciliation lifecycle for SQL and KV. It depends only on generic B-tree committed notifications, not either store's engine or sync protocol. Both sync crates enable it through their opt-in `continuous` feature and re-export its `ChangeSubscription`; SQL sync remains `no_std` by default.

Facades construct this wrapper from the backend's generic subscription and expose it as `Database::sync_changes() -> std::io::Result<ChangeSubscription>`. The facade API and subscription expose no raw Tokio types. SQL and KV subscriptions can merge into one **broad-session** runner (not a selective logical mapper):

```rust,ignore
let changes = sql.sync_changes()?.merge(kv.sync_changes()?);
changes.run(|| authorized_sql_and_kv_exchange()).await;
```

`run` performs an initial pass, then waits for committed changes without idle polling. It consumes coalesced wakeups and captures selected work before each pass, then acknowledges only that watermark on success, so commits during the callback remain pending. Wakeup consumption alone never acknowledges selected work. Failed work retries without requiring another commit, using exponential backoff from 100 ms to 30 seconds, reset after success. The caller supplies authorized transport in a `FnMut() -> Future<Output = Result<(), E>>` callback and reports its errors.

The runner spawns no tasks: the caller owns and polls its future. Browser Wasm uses the shared platform browser timer and accepts local (non-`Send`) callback futures without a Tokio runtime; native callers require a Tokio runtime with time enabled and retain `Send` futures when the callback is `Send`. Browser timers are cooperative and may be throttled by the browser. Dropping its future cancels the current callback or retry timer. Closing all merged backends also cancels in-flight work, including retry waits; a backend already closed before polling never starts the callback. Query handles and any storage captured by the callback retain their backends, so callers must drop those handles too when relying on backend closure. `changed().await -> std::io::Result<()>` supports coalesced admission invalidation, returning `BrokenPipe` after closure and pending notifications; it is not a second scheduling API.

`select()` returns an opaque `ChangeWork`; capture it before reconciliation reads and call `acknowledge(work)` only after successful delivery of that selection. Failure/cancellation drops work without acknowledging it. Each peer needs an independent, unmerged subscription, mapped to its corresponding store. Clones/new subscribers start broad; recreate subscriptions on reconnect/restart. The shared signal retains 128 commits capped at 128 physical invalidations / 16 KiB of identity bytes per commit. Oversized commits, lag/gaps and unknown writes select All rather than silently losing work. Tokens may combine retained commits; these limits bound notification retention, not session memory or document history.

A subscription does not retain storage or monitor external/raw writes outside the shared backend. SQL memory, shared Redb wrappers and SQL/KV IndexedDB backends support notifications; KV memory does not. IndexedDB subscriptions cover shared handles, not independently opened connections or other tabs. Local subscriptions cannot detect remote-only commits or external-tab writes. Missed external writes require explicit broad recovery; subscriptions are not durable replay. Callers supply authorized remote wakeups/receive delivery or explicitly reconcile both endpoints, even without local work. Neither `run` nor a local wakeup alone guarantees bidirectional realtime delivery or durable replay.

B-tree owns only generic signals, bounded physical invalidation, notification merging, selected-work acknowledgement and closure observation. All sync callbacks, initial-pass scheduling, retry policy and timers live here.
