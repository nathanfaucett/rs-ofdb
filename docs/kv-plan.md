# KV crate plan

## Goal and scope

Add `crates/kv`: an async, transactional key/value store with UTF-8 `String` keys, Automerge values, optional expiration, and generation-scoped tombstones. Use `redb` for durable storage and the existing `btree` traits for an in-memory test backend. Keep it separate from the SQL Engine and its row identity; do not add a new storage trait or replication protocol just for KV.

**Done when:** the in-memory and redb backends pass the same public API tests; reopening redb preserves data and tombstones; expired/tombstoned entries never appear in ordinary reads or scans; and explicit snapshot exchange makes two replicas converge to the greatest UUIDv7 generation.

**Implementation status:** local storage and explicit snapshot exchange are implemented and focused tests pass. The feature-powerset test gate passes. Workspace `just crap` still fails on 11 functions outside KV; KV itself remains below threshold. Network transport and automatic peer synchronization are not implemented.

## Domain and behavior

- A **logical key** is arbitrary UTF-8 text, including empty strings, colons, Unicode, and embedded NULs. It may have multiple immutable **generations**, each identified by a UUIDv7 and represented by one Automerge document. A generation's UUID never changes during normal updates.
- A **value** is opaque bytes (`Vec<u8>`), not a row or a serialized Rust type. Empty bytes are a valid live value. The document root stores `value` (Automerge bytes), `expires_at` (optional Unix milliseconds as a signed integer), and `tombstone` (boolean). No value and `tombstone = true` means a deleted generation; an absent expiry is distinct from zero. Reject malformed documents rather than treating them as missing keys.
- `get(key, now)` considers all generations for `key`, picks the generation with the greatest UUID bytes, and then checks _that generation only_ for tombstone and expiry. It does not fall back to an older live generation. Expiry is exclusive of visibility: `now >= expires_at` is expired. Scans return at most one visible value per logical key in UTF-8 byte order and apply the same rule. Pass `now` explicitly or use one injectable clock read per operation so tests and scans have consistent results.
- `set` on a live, non-tombstoned latest generation updates its Automerge document with an incremental change (including expiry changes). `delete` tombstones the latest generation by clearing its value and expiry and setting `tombstone = true`; deleting a missing/already tombstoned key is a no-op. `set` after a tombstone creates a new UUIDv7 generation and removes records for older generations in the same transaction. The new generation becomes the durable high-water mark; peers receiving its snapshot replace their older generations as well. A later generation wins even if it is itself deleted or expired. A delete without a newer generation must retain its tombstone so peers can learn about the deletion.
- Concurrently created generations on different machines compete by UUIDv7 byte ordering; the greater UUID wins regardless of whether either generation is tombstoned. UUIDv7 is an ordering policy, **not** a causal clock: clocks can disagree, so a later real-world write may have a smaller UUID. Equal UUIDs with incompatible key/document identities are an error, not an arbitrary winner. Do not use Automerge's per-field conflict selection to choose between generations.

## Storage layout

Use one underlying `BTree<Vec<u8>, Vec<u8>>` per KV store, backed by `btree_redb::RedbByteBTree` in production and `btree::InMemoryBTree<Vec<u8>, Vec<u8>>` in tests. `RedbByteBTree` adapts redb's ordered `Bytes` key type to the required byte-vector BTree API. Wrap it with `btree_automerge::AutomergeChangeStore` and reuse `DocumentChangeKey` for snapshot/incremental encoding and document-ID range bounds. Do not create a parallel change-key format.

The conceptual record is `KEY:UUIDv7:change-hash -> Automerge snapshot/incremental bytes`. The _actual_ ordered binary representation must be unambiguous for arbitrary strings: `DocumentId = escaped UTF-8 key + terminator + 16 UUID bytes`, where each zero byte in the key is escaped as `[0, 255]` and the key terminator is `[0, 0]`. `DocumentChangeKey::encode_ordered()` then appends its existing escaped-ID terminator, `DocumentType`, and 32-byte hash. Decode the document ID strictly: valid UTF-8, correctly terminated key, exactly 16 UUID bytes of version 7, and no trailing bytes. Never parse a colon-separated key; colons are valid key content. Verify that the byte ordering groups records by logical key, sorts generations by UUID bytes, and sorts each generation's changes as expected by `btree-automerge`.

For example, `a:b` and `a\0b` must remain distinct; `get("a")` must not include `ab`; scanning `["a", "b")` includes all generations of `a` but not `b`. Use bounds derived from the encoded logical-key prefix or a bounded document-ID range; do not scan the whole database for each `get`.

Automerge's `DocumentType::Metadata` is **not** the KV tombstone: `btree-automerge` reconstruction treats that record as removing the document. Store the KV tombstone in the Automerge document. Snapshots/incrementals retain the existing `DocumentChangeKey` type and 32-byte hash conventions; do not pretend that `hash_heads` is necessarily a single Automerge `ChangeHash`.

## API and transaction boundary

- Provide a small generic store over an existing `BTree<Vec<u8>, Vec<u8>>`, and a transaction over its `BTreeTransaction` rather than inventing another backend trait. Expose `get`, `scan`, `set`, `delete`, `commit`, and `rollback`; keep read/write visibility within a transaction. Consider a separate read-only handle only if the existing `BTreeRead` abstraction makes it useful.
- `set(key, value, expires_at)` and `delete(key)` must read the current winning generation and write its Automerge change in **one** storage transaction. Compute the next UUIDv7 when creating a generation, and ensure it is greater than all known generations for that key; fail explicitly if a suitable UUID cannot be generated. Recheck the winner inside the write transaction so competing local writers cannot silently overwrite a newer generation. Do not overwrite a tombstone in place.
- `export_snapshot` returns the current winning generation as a self-contained Automerge snapshot, including tombstones and expired winners. `import_snapshot` validates document ID, change type, payload, hash and value schema; writes idempotently; discards older generations when a greater UUIDv7 arrives; and retains a winning tombstone until superseded. Incrementals are rejected, not queued: retry with the latest snapshot. Import several records in one transaction and commit once, or roll back on error. Never accept a delayed older generation over the persisted winner. Do not make `set` a disguised import API. Network transport, peer state, and background expiry sweeps are out of scope.
- Keep the redb open/create and table initialization in a small adapter or example; `kv` logic should depend on the trait, not on redb types. Add a convenience constructor only if it does not force redb on in-memory users. Avoid changing `ofdb` root API until a concrete caller needs it.

## Incremental implementation plan

Work top to bottom. Each step should be a separate, reviewable change; do not start the next step until its exit check passes. Keep `lib.rs` limited to module declarations/re-exports. Use the existing traits and dependencies; do not add a new storage trait. The public API is async because `BTree`/`BTreeTransaction` methods are async.

### Phase 0 — Complete: verify prerequisites (no KV implementation)

1. In `btree-automerge`, add focused tests for sequential incrementals, concurrent changes, snapshot compaction, metadata records, and bounded ID scans. Verify reconstruction with hash-ordered changes. Fix shared reconstruction/compaction only if a test demonstrates a defect; do not work around it in KV.
2. The bounded `AutomergeChangeStore::range` implementation and its included/excluded bound tests are already complete. Do not redo them.
3. Test `btree-redb` with `Vec<u8>` keys and values: named-table reopen, ordered range boundaries, transaction read-your-writes, commit, and rollback. Add tests only for missing behavior.

**Exit check:** focused `cargo test -p btree-automerge` and `cargo test -p btree-redb` pass. Record any discovered API constraints before proceeding.

### Phase 1 — Complete: add the crate

1. Add `crates/kv` to workspace members and create its manifest. Depend on `btree`, `btree-automerge`, `automerge`, and `uuid`; add `btree-redb` only as a dev-dependency for integration tests. Follow existing full-table dependency declarations and feature conventions; do not build redb opening/table setup into the generic KV library.
2. Export only the crate's eventual public types from thin `lib.rs`. Implementations go in sibling modules.
3. Run `cargo check -p kv` and `cargo test -p kv` before adding behavior.

**Exit check:** the new package builds and is selectable independently with `-p kv`.

### Phase 2 — Complete: document ID codec and ordered ranges

1. Add a private codec for `String + UUIDv7 <-> DocumentId`, following the binary format above. Reject invalid UTF-8, invalid terminators/escapes, non-v7 UUIDs, short/long UUID payloads, and trailing bytes.
2. Add helpers to derive an exact logical-key document-ID range and a logical-key scan range. Confirm lexicographic behavior against the existing ordered `DocumentChangeKey` encoding; avoid whole-database scans for `get`.
3. Test empty and ordinary keys, Unicode, colons, embedded NULs, prefix-related keys, UUID ordering, malformed encodings, and ranges such as exact `a` versus `ab` and scan `["a", "b")`.

**Exit check:** codec and range tests pass without involving Automerge reconstruction or storage backends.

### Phase 3 — Complete: value document semantics

1. Implement a module that creates, reads, and mutates the Automerge value document. Store bytes, optional signed Unix-millisecond expiry, and tombstone in root fields; do not use `DocumentType::Metadata` as a KV tombstone.
2. Reject missing/wrong-type fields, invalid timestamps, and tombstones carrying a value. Keep absent expiry distinct from zero and empty bytes distinct from missing value.
3. Test round trips, empty bytes, absent/zero expiry, exact expiry boundary, malformed documents, and tombstone invariants.

**Exit check:** pure document behavior tests pass; no backend or public store API required yet.

### Phase 4 — Complete: in-memory transactional public API

1. Implement the generic store and transaction over `BTree<Vec<u8>, Vec<u8>>` and its associated `BTreeTransaction`. Use `AutomergeChangeStore` and existing change-key APIs. Define the public API with explicit `now` for reads/scans; use an injected UUIDv7 generator for writes so tests are deterministic. Do not add a system-clock abstraction in the first version.
2. Implement `get` and `scan` first, then `set` and `delete`. `set` updates a live latest generation; on a tombstoned/missing key it creates a strictly greater UUIDv7 generation. `delete` on missing/already tombstoned keys is a no-op. Reject a generated UUID that does not exceed the known maximum; never silently write a losing generation.
3. Perform every mutation and superseded-generation removal in the same underlying transaction. Test read-your-writes, rollback, commit, expiry/tombstone visibility, and no fallback to older generations.
4. Use `InMemoryBTree` as the first backend and test stored `DocumentChangeKey` decoding plus snapshot/incremental encoding.

**Exit check:** all domain and transactional behavior passes against the in-memory backend. Avoid designing import/export here; local storage does not imply synchronization.

### Phase 5 — Complete: redb parity; workspace-wide gates noted below

1. Add integration tests running the public API against `RedbByteBTree` (the redb adapter for byte-vector keys and values); include close/reopen persistence of a live value and tombstone. Keep redb creation and table initialization in the test adapter/example, not in `kv`.
2. Add a short usage example for in-memory and redb after the API has settled.
3. Run `cargo test -p kv`, focused `cargo test -p btree-automerge`, `cargo test -p btree-redb`, `cargo fmt --all --check`, relevant `cargo clippy -p kv`, and `just crap`. Finally run `cargo hack test --feature-powerset --all-targets` if installed; report it as unavailable if not installed.

**Validation note:** focused tests, workspace default tests, formatting, KV clippy, and `cargo hack test --feature-powerset --all-targets` pass. `just crap` still flags 11 existing workspace functions outside KV; KV-specific CRAP passes.

**Exit check:** both backends pass the same public API tests and redb reopen tests pass. This completes the local-only MVP; state clearly that no cross-machine convergence is implemented.

### Phase 6 — Complete: explicit snapshot exchange (not network sync)

`KvSnapshot` carries a winning generation's `DocumentChangeKey` and full Automerge snapshot; no incremental dependencies need to be queued. `import_snapshot` rejects incrementals and malformed records, ignores delayed lower generations, and retains a winning tombstone until superseded. Equal-generation snapshots must be identical or causally ordered; divergent histories error. The caller controls the transaction: batch imports and commit once for atomicity, or roll back if any import fails. Tests cover duplicate and stale imports, both exchange orders between independent stores, winning tombstones, expired winners, later changes to losing generations, and a peer that missed the prior tombstone. Replication requires an external transport and explicit exchange; no automatic sync is provided.

**Exit check:** two independent stores converge to the greatest UUIDv7 generation after exchanging retained changes in either order, without allowing delayed old changes to resurrect pruned generations.

## Known risks and explicit limits

- UUIDv7 winner selection is deterministic but not globally chronological under clock skew. Local recreation rejects a generated UUIDv7 that is not greater than the current generation; snapshot import chooses the greater generation even when clocks disagree.
- `btree-automerge` currently orders snapshots before increments and changes within a type by hash, not by causal order. Reconstruction/compaction must be proven against that layout before relying on it for synced KV changes.
- Replacing an old tombstone is safe for eventual _current-state_ convergence only if the new winning generation is durable and propagated. A peer that has not yet received the replacement can temporarily show its old value. A delete with no replacement cannot discard its tombstone. Export and import are explicit; do not advertise automatic synchronization across machines.
