use alloc::vec::Vec;

use value::Row;

use crate::{EngineError, EngineResult, RowIdentity};

pub const ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID: &str = "column_id";

pub const ENGINE_TABLES_STORAGE: &str = "__engine_tables";
pub const ENGINE_TABLE_FIELDS_STORAGE: &str = "__engine_table_fields";
pub const ENGINE_INDICES_STORAGE: &str = "__engine_indices";
pub const ENGINE_INDEX_FIELDS_STORAGE: &str = "__engine_index_fields";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogTable {
    Tables,
    TableFields,
    Indices,
    IndexFields,
}

impl CatalogTable {
    pub const fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Tables => &["deleted"],
            Self::TableFields => &["type", "default", "position", "primary_key", "deleted"],
            Self::Indices => &["table", "unique", "deleted"],
            Self::IndexFields => &["column", "deleted"],
        }
    }

    pub const fn storage(self) -> &'static str {
        match self {
            Self::Tables => ENGINE_TABLES_STORAGE,
            Self::TableFields => ENGINE_TABLE_FIELDS_STORAGE,
            Self::Indices => ENGINE_INDICES_STORAGE,
            Self::IndexFields => ENGINE_INDEX_FIELDS_STORAGE,
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Tables => 0,
            Self::TableFields => 1,
            Self::Indices => 2,
            Self::IndexFields => 3,
        }
    }
}

pub fn catalog_table_for_storage(storage: &str) -> Option<CatalogTable> {
    [
        CatalogTable::Tables,
        CatalogTable::TableFields,
        CatalogTable::Indices,
        CatalogTable::IndexFields,
    ]
    .into_iter()
    .find(|table| table.storage() == storage)
}

pub fn catalog_row_identity(table: CatalogTable, key: &Row) -> EngineResult<RowIdentity> {
    let encoded_key = postcard::to_allocvec(key).map_err(EngineError::custom)?;
    let mut identity = Vec::with_capacity(encoded_key.len() + 1);
    identity.push(table.tag());
    identity.extend(encoded_key);
    Ok(RowIdentity::catalog(identity))
}

pub fn catalog_key_from_identity(
    identity: &RowIdentity,
) -> EngineResult<Option<(CatalogTable, Row)>> {
    let RowIdentity::Catalog(identity) = identity else {
        return Ok(None);
    };
    let Some((tag, encoded_key)) = identity.split_first() else {
        return Err(EngineError::custom("Invalid catalog row identity"));
    };
    let table = match tag {
        0 => CatalogTable::Tables,
        1 => CatalogTable::TableFields,
        2 => CatalogTable::Indices,
        3 => CatalogTable::IndexFields,
        _ => return Err(EngineError::custom("Invalid catalog row identity")),
    };
    let key = postcard::from_bytes(encoded_key).map_err(EngineError::custom)?;
    Ok(Some((table, key)))
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use value::{Row, Value};

    use super::{CatalogTable, catalog_key_from_identity, catalog_row_identity};

    #[test]
    fn catalog_identity_is_deterministic_and_keyed_by_catalog_table() {
        let key = Row::new(vec![Value::from("users"), Value::from("name")]);
        let identity = catalog_row_identity(CatalogTable::TableFields, &key)
            .expect("catalog key serialization must succeed");
        assert_eq!(
            identity,
            catalog_row_identity(CatalogTable::TableFields, &key)
                .expect("catalog key serialization must succeed")
        );
        assert_eq!(
            catalog_key_from_identity(&identity).expect("catalog key deserialization must succeed"),
            Some((CatalogTable::TableFields, key.clone()))
        );
        assert_ne!(
            identity,
            catalog_row_identity(CatalogTable::IndexFields, &key)
                .expect("catalog key serialization must succeed")
        );
        assert_ne!(
            catalog_row_identity(
                CatalogTable::TableFields,
                &Row::new(vec![Value::from("users"), Value::from("id")])
            )
            .expect("catalog key serialization must succeed"),
            identity
        );
    }
}
