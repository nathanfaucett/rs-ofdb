# ofdb-sync-runtime

The shared, sync-owned continuous reconciliation lifecycle for SQL and KV. It depends only on generic B-tree committed notifications, not either store's engine or sync protocol. Both sync crates enable it through their opt-in `continuous` feature and re-export its `ChangeSubscription`; SQL sync remains `no_std` by default.

Facades construct this wrapper from the backend's generic subscription and expose it as `Database::sync_changes() -> std::io::Result<ChangeSubscription>`. The facade API and subscription expose no raw Tokio types. SQL and KV subscriptions merge into one runner:

```rust,ignore
let changes = sql.sync_changes()?.merge(kv.sync_changes()?);
changes.run(|| authorized_sql_and_kv_exchange()).await;
```

`run` performs an initial pass, then waits for committed changes without idle polling. It consumes pending notifications before each pass so changes committed during the callback remain pending for another pass. Failed work retries without requiring another commit, using exponential backoff from 100 ms to 30 seconds, reset after success. The caller supplies authorized transport in a `FnMut() -> Future<Output = Result<(), E>>` callback and reports its errors.

The runner spawns no tasks and requires a Tokio runtime with time enabled. Dropping its future cancels the current callback or retry timer. Closing all merged backends also cancels in-flight work. `changed().await -> std::io::Result<()>` supports coalesced admission invalidation, returning `BrokenPipe` after closure and pending notifications; it is not a second scheduling API.

B-tree owns only generic signals, notification merging, acknowledgement and closure observation. All sync callbacks, initial-pass scheduling, retry policy and timers live here.
