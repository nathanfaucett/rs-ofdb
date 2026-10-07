# SQL row identity and unique-key ordering

Every SQL Logical Row, including all four internal schema tables, uses one UUIDv7 as its primary key, storage ID, Automerge document ID, and sync ID within its table name. Table names identify logical tables; schema-record UUIDs do not create table generations. This replaces deterministic UUIDv5 schema keys, which force same-name recreation to reuse a permanently tombstoned row.

Distinct rows with the same unique key remain distinct documents. The largest complete UUID wins canonical selection, including conflicts on table and index names; concurrent changes within one document retain the Automerge conflict and explicit resolution model. UUID ordering is deterministic but is not reliable real-time ordering across peer clocks.

Automerge actor IDs identify writers and remain separate from row/document IDs. Concurrent peers must not share a row-derived actor ID because their independent actor sequences can collide.

Unique-key reconciliation permanently tombstones smaller contenders. Tombstones participate in largest-UUID selection, so deleting the winner never promotes an older contender. A different row with the same unique key and a larger UUID can supersede the deleted claim; it does not clear the old row's tombstone. These rules apply to schema definitions and application rows alike.

Dropping a table deletes its contents, including offline writes against the dropped definition. Recreating the same name starts empty. Distinguishing stale writes from valid post-recreation writes requires causal knowledge, not a comparison between row and schema UUID timestamps; table-name storage isolation remains unchanged.

Independent same-name creation with compatible schemas preserves application rows from both peers. Compatibility requires the same field names, types, defaults, primary key, and unique constraints; field order may differ, but index field order may not. For incompatible definitions, select the larger definition UUID and permanently tombstone application rows that do not fit the winning schema. Tombstoning a losing schema-definition row does not perform a table drop.

Recreating a dropped table must observe the drop; a larger UUID alone does not authorize recreation. This causal restriction is specific to table recreation. Ordinary unique-key replacement rows may supersede a tombstoned claim with a larger UUID without observing that tombstone.

Dropping an index leaves application rows unchanged. Index recreation creates a new definition and fields and rebuilds derived entries from live table rows in one transaction.

The winning table-definition row selects one complete schema. Field records use ordinary relationship columns to identify their owning definition; fields from losing definitions are not combined into a new schema. These relationships do not scope application-row storage.

Reconciliation matches row values by field name, applies valid defaults for missing fields, discards fields absent from the winning schema, and tombstones rows with invalid types or primary keys. It performs no automatic type conversion. Remaining unique-key collisions follow largest-UUID selection.

A concurrent drop that recreation has not observed defeats that recreation, regardless of UUID order. A later recreation must observe all relevant drops. Deleting a losing definition during unique-key reconciliation is not a drop event.

Local writes reject duplicate live unique-key owners, including duplicate table and index names. Largest-UUID reconciliation handles independently created replicated conflicts. A local replacement whose UUID is not larger than the deleted unique-key owner fails with a clear error; the engine never changes a supplied primary key or reports a hidden write as successful. Generated schema IDs are subject to the same admission check.

A table drop atomically tombstones its application rows, field definitions, owned index definitions, and index fields and removes derived index storage. Retained deletion history also governs stale dependent state received later. These rules require coordinated schema, row, tombstone, and index reconciliation without assigning application rows a table-generation storage scope.
