# Native SQL/KV resource retirement

Implemented on the isolated `native-resource-retirement` branch based on
`storage-sync-integration` at `4cd65c17c08733c7eb74ef65850380c2ba74f8c6`.
**Independently reviewed and corrected; owner contract qualified for integration.**
The review of `2b424b5` found and test-first repaired a concurrent native-open fence
bypass. Qualification and limits are recorded in
[the owner follow-up](../.scratch/native-resource-retirement/issues/01-public-native-retirement.md) and
[the independent review](../.scratch/native-resource-retirement-review/review.md).
Actual integration and consumer handoff are recorded locally in owner ticket 15.

## Public contract

With the `redb` feature, both `ofdb_sql::Database` and `ofdb_kv::Database` expose:

```rust
pub async fn retire_and_remove(&self) -> std::io::Result<()>;
pub fn remove_retired(path: impl AsRef<std::path::Path>) -> std::io::Result<()>;
```

`retire_and_remove` retires the **entire dedicated native file** opened by that
Database, including a KV database opened with `open_with_table(path, "kv")`. It is
not key deletion, table deletion, stage-only cleanup, a replicated Tombstone, or
peer deletion. SQL and KV calls are independent phases; their caller coordinates
resource-level completion. A failure in KV does not undo SQL removal.

The gate is shared by SQL Database clones, all embedded Client clones/new clients,
explicit transactions, direct sync-state operations, and every Database sync
variant. Creating `client()` after retirement is harmless: its operations deny.
Ordinary native query/transaction operations return their existing storage error
category; sync returns the existing `PreApply` error. Retirement itself returns IO
errors, without inventing transport or domain decisions.

Once a retirement future is polled:

1. Gate new operations. Atomically publish and fsync `<path>.ofdb-retired`, including
   the native file identity and exact identities/paths of owner-native stages.
   Managed native opens, marker publication and restart cleanup serialize namespace
   IO within the process; an opener cannot pass the marker check then recreate the
   resource after removal. Query/transaction drain does not hold that namespace lock.
2. Abort active sync waits without sending/waiting for network messages. Executors
   must continue polling or drop admitted futures for them to drain. Already
   admitted local operations finish; idle explicit transactions are rolled back by
   releasing their native state. Later transaction operations/commit deny.
3. Drain admitted operations and release owned native persistence/transaction/stage
   handles. Native writer admission is asynchronous and held only for a native
   transaction, not a transport wait or pre-apply callback. A pending sync does not
   prevent local work.
4. Remove only recorded owned stage files, then the requested persistence file.
   Refuse changed file identities/symlinks/non-files rather than removing another
   resource. Cleanup failures propagate. No containing directory is swept/deleted.

Successful native stage closes remove their in-memory tracking entry. Failed stage
closes remain tracked; the durable retirement record allows their exact cleanup to
be retried after process exit. Native `synchronize_with_stage_directory*` variants
attach this owner tracking automatically. Caller-provided custom stage factories
remain caller-owned: their custom cleanup is not a native retirement guarantee.

`remove_retired` resumes the recorded cleanup without issuing query handles. It
probes the native database lock and refuses a still-live owner before deleting any
recorded files. Both APIs are idempotent after removal and retryable after recorded
cleanup failure. Neither deletes the permanent retirement marker.

## Preconditions and explicit limits

- **Unix native retirement only.** Other native platforms retain construction/query
  support, but this retirement operation requires Unix file identity; cross-platform
  retirement has not been qualified.
- The caller exclusively owns the database pathname and reserved marker namespace.
  A file must belong to one logical resource, not an unrelated shared multi-table
  store. No hard-link/alternate-path owner coordination or protection against
  concurrent external namespace replacement is promised. Exclusive namespace
  ownership includes excluding concurrent constructors/cleanup in other processes;
  the process-local namespace lock is not a cross-process lifecycle coordinator.
  Marker data is trusted
  owner metadata, not untrusted wire input.
- **Permanent fence; no recreation/ungate API.** Ordinary opens of a retired pathname
  deny even after successful removal. Do not have consumers unlink the marker to
  simulate a new lifecycle. Recreating/reselecting the same pathname needs a
  separately designed owner lifecycle contract.
- **Built-in native hosting is unmanaged.** Attempting `serve_*` can issue raw backend
  clones to server tasks. This owner returns `Unsupported` on retirement thereafter,
  before changing the gate or marker, even if hosting later ends. Close hosting and
  all its handles, drop the old owner, and reopen a fresh managed owner before using
  retirement. Remote clients and independently constructed raw engines/stores are
  not revocable handles of this managed Database. The refusal is tested; remote-host
  revocation is not delivered.
- If durable marker publication fails, the in-process owner remains gated, but an
  unpersisted request cannot be reconstructed after restart. The caller retains its
  own pending intent and retries; missing/malformed markers fail cleanup closed.
  Dropping an unpolled retirement future does not initiate retirement.
- Native directory fsync/atomic marker publication are used. The tested abrupt exit
  occurs after durable publication and before drain. This is not power-loss testing,
  exhaustive syscall failure injection, or recovery of stage files left by a crash
  **before** the retirement record is published.

No `of`/`offs` files, catalogs, selections, grants, transports, resource IDs, or
replicated history are changed by this owner implementation. Consumer completion
and any future lifecycle transition remain caller/domain responsibilities.
