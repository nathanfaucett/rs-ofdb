# ofdb-kv-sync

`ofdb-kv-sync` reconciles KV stores over a caller-owned reliable, ordered transport. It opens no sockets and supplies no authentication or encryption. KV wire **v3** exchanges logical-key selections, native Automerge history inventories, missing changes, and snapshots for new generations or dependency recovery. Older wire versions are rejected; there is no compatibility path. The store owns document validation and generation/tombstone reconciliation; this crate owns framing and exchange.

`synchronize_checked` performs broad discovery in both directions, but transfers only missing history for matching generations. `synchronize_selected_checked` accepts `&[KvInvalidation]`: compare the **union of both peers' logical keys**, re-reading each latest generation regardless of the invalidated generation. Either peer's `All` forces broad discovery. Capture selected work **before** opening export snapshots and acknowledge it **only after success**. New peers, reconnects, unknown writes and notification gaps must select `All`. Inventories and native exports include snapshot-contained and bundled history; physical incremental-key labels are never treated as Automerge hashes.

```rust,ignore
let work = changes.select();
let invalidations = work.as_ref()
    .map(|work| store.committed_invalidations("kv", work))
    .unwrap_or_default();
ofdb_kv_sync::synchronize_selected_checked(
    &store, &invalidations, &mut transport,
    ofdb_kv_sync::SyncRole::Initiator, Default::default(),
    || core::future::ready(Ok(())),
).await?;
if let Some(work) = work {
    assert!(changes.acknowledge(work));
}
```

Keep an independent subscription for every peer; map tokens only against the corresponding store/backend. The signal retains 128 commits capped at 128 physical invalidations / 16 KiB of identity bytes per commit; oversized commits or lag require All. Tokens may combine retained commits. `changed()` consumes wakeups, not selected work; failure/cancellation must not acknowledge it. `All` overrides partial key selections. Do not use merged subscriptions for selective logical mapping. The facade offers success-only selection/acknowledgement directly. With optional `continuous`, `ChangeSubscription::run` remains a broad-session scheduler with retry/backoff and explicit caller-owned lifetime; it spawns no task.

**Remote delivery is caller-driven.** Local subscriptions observe incoming replication once applied locally, but cannot observe commits made only at a remote peer. Arrange an authorized remote wakeup/receive path or explicitly invoke reconciliation, even if local selected work is empty. Reconnect with a fresh subscription for broad comparison. Local wakeups alone do not provide bidirectional realtime delivery. Generic KV memory subscriptions remain unsupported; use caller-driven broad sessions.

Caller authorization/cancellation is checked before **each complete incoming batch**, including recovery. Every native change's claimed hash and dependency list is validated against its payload. A missing dependency rolls back the entire batch and requests snapshots; recovered state must contain the promised changes or a newer generation. Snapshots merge concurrent same-generation history rather than replacing it. Malformed batches roll back atomically. Earlier committed batches remain on session failure; the session is not globally atomic or a globally frozen snapshot. Discard transport after failure and retry. Known history produces no writes, so replay and fan-out settle without echo.

Frame/count limits apply to inventory and data frames. A single document's inventory or complete delta must fit one frame; oversized frames fail explicitly (increase the configured byte budget). Discovery retains the selected inventory in memory; broad fallback scales with the whole store, while ordinary selective passes inspect only selected keys. No durable peer frontier or history cursor is persisted. Concurrent commits after selection remain pending for another pass. Expiry remains read-time visibility, not a timed change event.
