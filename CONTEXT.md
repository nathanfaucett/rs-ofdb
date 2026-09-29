# Domain Context

## Engine Transaction

One transaction owns one complete read snapshot or write set for an engine operation batch. A write transaction commits all enlisted catalog, schema, index, Automerge row, and tombstone changes together, or rolls them all back.

## Logical Row

A Logical Row is the row value the Engine reads, writes, and indexes. Its stored representation is selected by the Row Reconciler.

## Row Reconciler

A Row Reconciler stores and resolves Logical Rows through an Engine Transaction. The Automerge Row Reconciler uses one Automerge document per Logical Row with stable column keys. The Engine is the only path for applying local or incoming Automerge changes to an engine-managed Logical Row. Concurrent values for one column are retained as a Conflict; Automerge canonical ordering selects the visible value.

## Row Identity

A user Logical Row has an immutable UUID primary key and belongs to a Table Generation; that pair identifies its Automerge document. A catalog row's UUIDv7 Generation is part of its primary-key identity, never a value column.

## Index Record

An Index Record maps an index key to a row UUID. The engine derives and updates Index Records from canonical visible Logical Rows in the same Engine Transaction. A unique-index conflict retains all rows; index lookup selects the canonical row.

## Conflict

A Conflict retains concurrent candidate values or objects that cannot all be active. A deterministic canonical ordering selects the visible candidate. An explicit Resolution is the only operation that settles a Conflict.

## Generation

A Generation is the immutable UUIDv7 identity of a Table, Column, Index, or Index Field. A user Logical Row uses its table Generation and its own UUID. Catalog identities are scoped by parent identity and logical name or position; names may be reused by a new Generation after the former is Tombstoned. Dependents of an inactive Generation remain replication facts but are not visible.

## Sync State Unit

A Sync State Unit is sync-owned canonical transferable state for one Logical Row, Table, Column, Index, or Index Field. It contains its identity, state bytes, deletion metadata where applicable, and a digest. Sync asks the Engine to apply it atomically while the Engine maintains derived Index Records.

## Sync Manifest

A Sync Manifest maps each sync-owned state-unit identity to its digest. A Sync Session exchanges manifests, transfers mismatched units in batches, and retries by exchanging manifests again. Normal realtime updates transfer only missing incremental payloads identified by opaque sync change IDs; full state units are for bootstrap and dependency recovery. Automerge implements `sync::SyncRowCodec`, while the Engine stores no protocol state, frontier, checkpoint, envelope log, or quarantine state.

## Tombstone

A Tombstone is ordinary deleted-row state for a Logical Row or catalog Generation, never a catalog value column. For reusable catalog names, it participates in selecting the greatest Generation but does not appear in search results or allow fallback to an older live Generation. Later changes to that Generation are Superseded; they are retained as replication facts but do not alter visible state. An explicit Restore creates a new Generation.

## KV Key

A KV Key is a UTF-8 label for a value. It may have multiple KV Generations; the generation with the greatest UUIDv7 determines the visible state.

## KV Generation

A KV Generation is an immutable UUIDv7 identity for one KV Key's Automerge value. A tombstone ends that generation; setting the same key again creates a new generation rather than restoring the old one.
