use alloc::{
    collections::BTreeMap,
    format,
    string::{String, ToString},
    vec::Vec,
};

use futures::{StreamExt, pin_mut};
use schema::{ColumnSchema, ColumnSchemaIndex, IndexSchema, TableSchema};
use uuid::{Uuid, Variant, Version};
use value::{Row, Value, ValueType};

use crate::{EngineError, EngineResult, KernelTransaction, RowCodec, RowIdentity};

pub const ENGINE_TABLES_STORAGE: &str = "__engine_tables";
pub const ENGINE_TABLE_FIELDS_STORAGE: &str = "__engine_table_fields";
pub const ENGINE_INDICES_STORAGE: &str = "__engine_indices";
pub const ENGINE_INDEX_FIELDS_STORAGE: &str = "__engine_index_fields";
pub const ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID: &str = "column_id";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableRow {
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableFieldRow {
    pub name: String,
    pub r#type: ValueType,
    pub default: Value,
    pub position: i64,
    pub primary_key: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexRow {
    pub name: String,
    pub table: String,
    pub unique: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexFieldRow {
    pub position: i64,
    pub column: String,
    pub column_id: Uuid,
}

impl TryFrom<&Row> for TableRow {
    type Error = EngineError;

    fn try_from(row: &Row) -> EngineResult<Self> {
        Ok(Self {
            name: row
                .values
                .first()
                .and_then(Value::to_text)
                .ok_or(EngineError::custom("Invalid table name"))?,
        })
    }
}

impl TryFrom<&Row> for TableFieldRow {
    type Error = EngineError;

    fn try_from(row: &Row) -> EngineResult<Self> {
        let values = &row.values;
        Ok(Self {
            name: values
                .first()
                .and_then(Value::to_text)
                .ok_or(EngineError::custom("Invalid column name"))?,
            r#type: values
                .get(1)
                .and_then(Value::to_type)
                .ok_or(EngineError::custom("Invalid column type"))?,
            default: values
                .get(2)
                .cloned()
                .ok_or(EngineError::custom("Invalid column default"))?,
            position: values
                .get(3)
                .and_then(Value::to_integer)
                .ok_or(EngineError::custom("Invalid column position"))?,
            primary_key: values
                .get(4)
                .and_then(Value::to_bool)
                .ok_or(EngineError::custom("Invalid primary key"))?,
        })
    }
}

impl TryFrom<&Row> for IndexRow {
    type Error = EngineError;

    fn try_from(row: &Row) -> EngineResult<Self> {
        let values = &row.values;
        Ok(Self {
            name: values
                .first()
                .and_then(Value::to_text)
                .ok_or(EngineError::custom("Invalid index name"))?,
            table: values
                .get(1)
                .and_then(Value::to_text)
                .ok_or(EngineError::custom("Invalid index table"))?,
            unique: values
                .get(2)
                .and_then(Value::to_bool)
                .ok_or(EngineError::custom("Invalid index uniqueness"))?,
        })
    }
}

impl TryFrom<&Row> for IndexFieldRow {
    type Error = EngineError;

    fn try_from(row: &Row) -> EngineResult<Self> {
        let values = &row.values;
        Ok(Self {
            position: values
                .first()
                .and_then(Value::to_integer)
                .ok_or(EngineError::custom("Invalid index field position"))?,
            column: values
                .get(1)
                .and_then(Value::to_text)
                .ok_or(EngineError::custom("Invalid index column"))?,
            column_id: values
                .get(2)
                .and_then(Value::to_uuid)
                .ok_or(EngineError::custom("Invalid index column identity"))?,
        })
    }
}

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
            Self::Tables => &["name"],
            Self::TableFields => &["name", "type", "default", "position", "primary_key"],
            Self::Indices => &["name", "table", "unique"],
            Self::IndexFields => &["position", "column", "column_id"],
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

pub(crate) fn catalog_id(id: Uuid) -> EngineResult<RowIdentity> {
    if id.get_version() != Some(Version::SortRand) || id.get_variant() != Variant::RFC4122 {
        return Err(EngineError::custom("Catalog identity must be UUIDv7"));
    }
    Ok(RowIdentity::catalog(id.as_bytes().to_vec()))
}

pub(crate) fn child_id(parent: &RowIdentity, id: Uuid) -> EngineResult<RowIdentity> {
    let RowIdentity::Catalog(bytes) = parent else {
        return Err(EngineError::custom("Invalid catalog parent"));
    };
    if bytes.len() != 16 && bytes.len() != 32 {
        return Err(EngineError::custom("Invalid catalog parent identity"));
    }
    let RowIdentity::Catalog(child) = catalog_id(id)? else {
        unreachable!()
    };
    let mut key = bytes.clone();
    key.extend(child);
    Ok(RowIdentity::catalog(key))
}

fn belongs_to(row: &RowIdentity, parent: &RowIdentity) -> bool {
    match (row, parent) {
        (RowIdentity::Catalog(row), RowIdentity::Catalog(parent)) => {
            row.len() == parent.len() + 16 && row.starts_with(parent)
        }
        _ => false,
    }
}

pub(crate) async fn ensure<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
) -> EngineResult<()> {
    for table in [
        ENGINE_TABLES_STORAGE,
        ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_INDICES_STORAGE,
        ENGINE_INDEX_FIELDS_STORAGE,
    ] {
        codec.ensure_table(transaction, table).await?;
    }
    Ok(())
}

pub(crate) async fn find_named<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    storage: &str,
    name: &str,
) -> EngineResult<Option<RowIdentity>> {
    Ok(latest_named(transaction, codec, storage, name)
        .await?
        .and_then(|(id, deleted)| (!deleted).then_some(id)))
}

pub(crate) async fn latest_named<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    storage: &str,
    name: &str,
) -> EngineResult<Option<(RowIdentity, bool)>> {
    let rows = codec.scan_row_states(transaction, storage);
    pin_mut!(rows);
    let mut latest = None;
    while let Some(row) = rows.next().await {
        let (id, value, deleted) = row?;
        if value.values.first().and_then(Value::as_text) == Some(name)
            && latest.as_ref().is_none_or(|(previous, _)| &id > previous)
        {
            latest = Some((id, deleted));
        }
    }
    Ok(latest)
}

pub(crate) fn index_storage(id: &RowIdentity) -> String {
    let mut name = String::from("__engine_index_");
    for byte in id.to_bytes() {
        name.push_str(&format!("{byte:02x}"));
    }
    name
}

pub(crate) async fn lookup_table_id<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<RowIdentity> {
    find_named(transaction, codec, ENGINE_TABLES_STORAGE, name)
        .await?
        .ok_or(EngineError::InvalidQuery("Table not found"))
}

pub(crate) async fn user_row_identity<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
    row: Uuid,
) -> EngineResult<RowIdentity> {
    let RowIdentity::Catalog(parent) = lookup_table_id(transaction, codec, table).await? else {
        return Err(EngineError::custom("Invalid table identity"));
    };
    let parent: [u8; 16] = parent
        .as_slice()
        .try_into()
        .map_err(|_| EngineError::custom("Invalid table identity"))?;
    Ok(RowIdentity::scoped_user(Uuid::from_bytes(parent), row))
}

pub(crate) async fn active_user_row<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
    row: &RowIdentity,
) -> EngineResult<bool> {
    let RowIdentity::ScopedUser { table: owner, .. } = row else {
        return Ok(false);
    };
    let active = match lookup_table_id(transaction, codec, table).await {
        Ok(RowIdentity::Catalog(active)) => active,
        Err(EngineError::InvalidQuery("Table not found")) => return Ok(false),
        Err(error) => return Err(error),
        _ => return Ok(false),
    };
    Ok(active.as_slice() == owner)
}

pub(crate) async fn lookup_index_id<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<RowIdentity> {
    active_index(transaction, codec, name)
        .await?
        .map(|(id, _)| id)
        .ok_or(EngineError::InvalidQuery("Index not found"))
}

async fn active_index<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<Option<(RowIdentity, Row)>> {
    let Some((id, deleted)) =
        latest_named(transaction, codec, ENGINE_INDICES_STORAGE, name).await?
    else {
        return Ok(None);
    };
    if deleted {
        return Ok(None);
    }
    let value = codec
        .get_row(transaction, ENGINE_INDICES_STORAGE, &id)
        .await?
        .ok_or(EngineError::custom("Missing index row"))?;
    let table = IndexRow::try_from(&value)?.table;
    let Ok(parent) = lookup_table_id(transaction, codec, &table).await else {
        return Ok(None);
    };
    Ok(belongs_to(&id, &parent).then_some((id, value)))
}

pub(crate) async fn lookup_table_name<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<String> {
    lookup_table_id(transaction, codec, name).await?;
    Ok(name.to_string())
}

pub(crate) async fn columns<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
) -> EngineResult<Vec<(String, ColumnSchema)>> {
    let parent = lookup_table_id(transaction, codec, table).await?;
    let rows = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(rows);
    let mut latest = BTreeMap::new();
    while let Some(row) = rows.next().await {
        let (id, value, deleted) = row?;
        if !belongs_to(&id, &parent) {
            continue;
        }
        let name = value
            .values
            .first()
            .and_then(Value::to_text)
            .ok_or(EngineError::custom("Invalid column name"))?;
        let entry = latest
            .entry(name)
            .or_insert_with(|| (id.clone(), value.clone(), deleted));
        if id > entry.0 {
            *entry = (id, value, deleted);
        }
    }
    let mut result = Vec::new();
    for (_, (_, value, deleted)) in latest {
        if deleted {
            continue;
        }
        let field = TableFieldRow::try_from(&value)?;
        let column = ColumnSchema {
            name: field.name.clone(),
            r#type: field.r#type,
            default: field.default,
            primary_key: field.primary_key,
        };
        result.push((field.position, field.name, column));
    }
    result.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    Ok(result
        .into_iter()
        .map(|(_, name, schema)| (name, schema))
        .collect())
}

pub(crate) async fn column_id<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
    name: &str,
) -> EngineResult<Uuid> {
    let parent = lookup_table_id(transaction, codec, table).await?;
    let rows = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(rows);
    let mut latest: Option<(RowIdentity, bool)> = None;
    while let Some(row) = rows.next().await {
        let (id, value, deleted) = row?;
        if belongs_to(&id, &parent)
            && value.values.first().and_then(Value::as_text) == Some(name)
            && latest.as_ref().is_none_or(|(previous, _)| id > *previous)
        {
            latest = Some((id, deleted));
        }
    }
    let Some((RowIdentity::Catalog(bytes), false)) = latest else {
        return Err(EngineError::InvalidQuery("Index column not found"));
    };
    let id: [u8; 16] = bytes
        .get(16..)
        .ok_or(EngineError::custom("Invalid column identity"))?
        .try_into()
        .map_err(|_| EngineError::custom("Invalid column identity"))?;
    Ok(Uuid::from_bytes(id))
}

pub(crate) async fn table_schema<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<TableSchema> {
    lookup_table_name(transaction, codec, name).await?;
    table_schema_for(transaction, codec, name, name.to_string()).await
}

pub(crate) async fn table_schema_for<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
    name: String,
) -> EngineResult<TableSchema> {
    Ok(TableSchema {
        name,
        columns: columns(transaction, codec, table)
            .await?
            .into_iter()
            .map(|(_, column)| column)
            .collect(),
    })
}

pub(crate) async fn index_schema<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<Option<IndexSchema>> {
    let Some((id, value)) = active_index(transaction, codec, name).await? else {
        return Ok(None);
    };
    let index = IndexRow::try_from(&value)?;
    let table = index.table;
    let unique = index.unique;
    let rows = codec.scan_row_states(transaction, ENGINE_INDEX_FIELDS_STORAGE);
    pin_mut!(rows);
    let mut latest = BTreeMap::new();
    while let Some(row) = rows.next().await {
        let (field_id, field, deleted) = row?;
        if !belongs_to(&field_id, &id) {
            continue;
        }
        let position = IndexFieldRow::try_from(&field)?.position;
        let entry = latest
            .entry(position)
            .or_insert_with(|| (field_id.clone(), field.clone(), deleted));
        if field_id > entry.0 {
            *entry = (field_id, field, deleted);
        }
    }
    let mut fields = Vec::new();
    for (position, (_, field, deleted)) in latest {
        if deleted {
            continue;
        }
        let field = IndexFieldRow::try_from(&field)?;
        let column = field.column;
        let referenced_column_id = field.column_id;
        if referenced_column_id != column_id(transaction, codec, &table, &column).await? {
            return Err(EngineError::InvalidQuery(
                "Index column identity is inactive",
            ));
        }
        fields.push((position, column));
    }
    let schema_columns = columns(transaction, codec, &table).await?;
    let column_indices = fields
        .into_iter()
        .map(|(_, column)| {
            schema_columns
                .iter()
                .position(|(name, _)| *name == column)
                .map(|position| position as ColumnSchemaIndex)
                .ok_or(EngineError::InvalidQuery("Index column not found"))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    Ok(Some(IndexSchema {
        name: name.into(),
        table_name: table,
        column_indices,
        unique,
    }))
}

pub(crate) async fn active_table_names<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
) -> EngineResult<Vec<String>> {
    let rows = codec.scan_row_states(transaction, ENGINE_TABLES_STORAGE);
    pin_mut!(rows);
    let mut result = Vec::new();
    while let Some(row) = rows.next().await {
        let (_, value, _) = row?;
        result.push(TableRow::try_from(&value)?.name);
    }
    result.sort();
    result.dedup();
    let mut active = Vec::new();
    for name in result {
        if lookup_table_id(transaction, codec, &name).await.is_ok() {
            active.push(name);
        }
    }
    Ok(active)
}
