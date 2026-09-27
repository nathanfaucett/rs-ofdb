# kv-sync

`kv-sync` explicitly exchanges self-contained winning KV snapshots over a caller-owned transport. Its public `KvSnapshot` type owns the wire/serde representation; `kv` owns document validation and generation conflict resolution and exposes snapshot parts (`DocumentChangeKey` plus Automerge bytes) without depending on `kv-sync`. It does not open sockets or provide authentication, encryption, reconnection, or background synchronization.

```rust,ignore
let (left_transport, right_transport) = connected_transports();
let (left, right) = futures::join!(
    kv_sync::synchronize(&left_store, &mut left_transport, kv_sync::SyncRole::Initiator, Default::default()),
    kv_sync::synchronize(&right_store, &mut right_transport, kv_sync::SyncRole::Responder, Default::default()),
);
left?;
right?;
```

The transport must deliver frames reliably and in order for each direction. The session ends with a `Finished` control-frame exchange, which also rejects trailing duplicate end/control frames. The caller owns connection security and retry policy. A failed received batch is rolled back; batches committed earlier in the session remain committed, so retry the entire session. Snapshot imports are idempotent. Concurrent writes are not included consistently across peers; run another session to exchange them.

V1 exports every winning snapshot into memory at session start and transfers complete snapshot batches on every session. Memory and network cost therefore scale with the entire store, including tombstones and expired winners. Configure frame/count limits for the expected store size. This session is not globally atomic or a continuous synchronization guarantee.
