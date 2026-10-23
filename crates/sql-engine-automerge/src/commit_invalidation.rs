use std::collections::{BTreeMap, BTreeSet};

use btree::{ChangeWork, StorageInvalidation};
use btree_automerge::DocumentChangeKey;
use engine::{Engine, EngineResult, Kernel, Uuid, catalog_table_for_storage};

use crate::AutomergeRowCodec;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SqlInvalidation {
    All,
    Table(String),
    Row { table: String, row: Uuid },
}

/// Map committed physical changes above storage. Catalog changes require dependency
/// reconciliation; unknown user-table keys require a table scan. Derived indexes are
/// not replicated identities. Select work before calling this or opening export snapshots.
pub async fn committed_invalidations<K: Kernel>(
    engine: &Engine<K, AutomergeRowCodec>,
    work: &ChangeWork,
) -> EngineResult<Vec<SqlInvalidation>> {
    let mut result = BTreeSet::new();
    let mut tables = BTreeMap::new();
    for invalidation in work.invalidations() {
        let table = match invalidation {
            StorageInvalidation::All => return Ok(vec![SqlInvalidation::All]),
            StorageInvalidation::Table(table) | StorageInvalidation::Key { table, .. } => table,
        };
        if catalog_table_for_storage(table).is_some() {
            return Ok(vec![SqlInvalidation::All]);
        }
        if !tables.contains_key(table) {
            let is_table = engine.table_schema(table).await.is_ok();
            if !is_table && engine.index_schema(table).await.is_err() {
                return Ok(vec![SqlInvalidation::All]);
            }
            tables.insert(table.clone(), is_table);
        }
        if !tables[table] {
            continue;
        }
        let row = match invalidation {
            StorageInvalidation::Key { key, .. } => DocumentChangeKey::decode_ordered(key)
                .ok()
                .and_then(|key| Uuid::from_slice(key.id()).ok())
                .filter(|row| engine::validate_row_id(table, row).is_ok()),
            _ => None,
        };
        result.insert(match row {
            Some(row) => SqlInvalidation::Row {
                table: table.clone(),
                row,
            },
            None => SqlInvalidation::Table(table.clone()),
        });
    }
    let broad_tables = result
        .iter()
        .filter_map(|invalidation| match invalidation {
            SqlInvalidation::Table(table) => Some(table.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    result.retain(|invalidation| match invalidation {
        SqlInvalidation::Row { table, .. } => !broad_tables.contains(table),
        _ => true,
    });
    Ok(result.into_iter().collect())
}
