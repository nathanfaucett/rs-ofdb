use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};

use futures::{StreamExt, pin_mut};
use schema::{ColumnSchema, ColumnSchemaIndex, IndexSchema, TableSchema};
use uuid::Uuid;
use value::{Row, Value, ValueType};

use crate::{EngineError, EngineResult, KernelTransaction, RowCodec};

pub const ENGINE_TABLES_STORAGE: &str = "__engine_tables";
pub const ENGINE_TABLE_FIELDS_STORAGE: &str = "__engine_table_fields";
pub const ENGINE_INDICES_STORAGE: &str = "__engine_indices";
pub const ENGINE_INDEX_FIELDS_STORAGE: &str = "__engine_index_fields";

pub(crate) fn validate_row_id(id: &Uuid) -> EngineResult<()> {
    let bytes = id.as_bytes();
    if bytes[6] >> 4 == 7 && bytes[8] & 0xc0 == 0x80 {
        Ok(())
    } else {
        Err(EngineError::InvalidQuery("Primary key must be a UUIDv7"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableRow {
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableFieldRow {
    pub table_id: Uuid,
    pub table: String,
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
    pub table_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexFieldRow {
    pub index_id: Uuid,
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
            table_id: values
                .first()
                .and_then(Value::to_uuid)
                .ok_or(EngineError::custom("Invalid column table identity"))?,
            table: values
                .get(1)
                .and_then(Value::to_text)
                .ok_or(EngineError::custom("Invalid column table"))?,
            name: values
                .get(2)
                .and_then(Value::to_text)
                .ok_or(EngineError::custom("Invalid column name"))?,
            r#type: values
                .get(3)
                .and_then(Value::to_type)
                .ok_or(EngineError::custom("Invalid column type"))?,
            default: values
                .get(4)
                .cloned()
                .ok_or(EngineError::custom("Invalid column default"))?,
            position: values
                .get(5)
                .and_then(Value::to_integer)
                .ok_or(EngineError::custom("Invalid column position"))?,
            primary_key: values
                .get(6)
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
            table_id: values
                .get(3)
                .and_then(Value::to_uuid)
                .ok_or(EngineError::custom("Invalid index table identity"))?,
        })
    }
}

impl TryFrom<&Row> for IndexFieldRow {
    type Error = EngineError;

    fn try_from(row: &Row) -> EngineResult<Self> {
        let values = &row.values;
        Ok(Self {
            index_id: values
                .first()
                .and_then(Value::to_uuid)
                .ok_or(EngineError::custom("Invalid index identity"))?,
            position: values
                .get(1)
                .and_then(Value::to_integer)
                .ok_or(EngineError::custom("Invalid index field position"))?,
            column: values
                .get(2)
                .and_then(Value::to_text)
                .ok_or(EngineError::custom("Invalid index column"))?,
            column_id: values
                .get(3)
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
            Self::TableFields => &[
                "table_id",
                "table",
                "name",
                "type",
                "default",
                "position",
                "primary_key",
            ],
            Self::Indices => &["name", "table", "unique", "table_id"],
            Self::IndexFields => &["index_id", "position", "column", "column_id"],
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

pub(crate) async fn reconcile_schema_names<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
) -> EngineResult<()> {
    let states = {
        let rows = codec.scan_row_states(transaction, ENGINE_TABLES_STORAGE);
        pin_mut!(rows);
        let mut states = Vec::new();
        while let Some(row) = rows.next().await {
            let (id, value, deleted) = row?;
            states.push((id, TableRow::try_from(&value)?.name, deleted));
        }
        states
    };
    let mut states = states;
    for (id, name, deleted) in &mut states {
        if *deleted {
            continue;
        }
        let events = codec.table_drop_events(transaction, name).await?;
        let observed = codec.observed_table_drops(transaction, id).await?;
        let unobserved: Vec<_> = events
            .into_iter()
            .filter(|event| !observed.contains(event))
            .collect();
        if !unobserved.is_empty() {
            codec
                .delete_row(transaction, ENGINE_TABLES_STORAGE, id)
                .await?;
            for event in unobserved {
                codec.drop_table_definition(transaction, id, event).await?;
            }
            *deleted = true;
        }
    }
    for (id, name, _) in &states {
        if codec.table_definition_was_dropped(transaction, id).await? {
            delete_dropped_table_dependents(transaction, codec, name, id).await?;
        }
    }
    let mut winners: Vec<(String, Uuid)> = Vec::new();
    for (id, name, _) in &states {
        if let Some((_, winner)) = winners.iter_mut().find(|(key, _)| key == name) {
            if id > winner {
                *winner = *id;
            }
        } else {
            winners.push((name.clone(), *id));
        }
    }
    for (id, name, deleted) in states {
        let winner = winners
            .iter()
            .find(|(key, _)| key == &name)
            .expect("schema name has a winning row")
            .1;
        if id != winner && !deleted {
            codec
                .delete_row(transaction, ENGINE_TABLES_STORAGE, &id)
                .await?;
        }
    }
    let states = {
        let rows = codec.scan_row_states(transaction, ENGINE_INDICES_STORAGE);
        pin_mut!(rows);
        let mut states = Vec::new();
        while let Some(row) = rows.next().await {
            let (id, value, deleted) = row?;
            states.push((id, IndexRow::try_from(&value)?, deleted));
        }
        states
    };
    let mut states = states;
    for (id, index, deleted) in &mut states {
        if *deleted {
            delete_index_dependents(transaction, codec, id, false).await?;
            continue;
        }
        if codec
            .table_definition_was_dropped(transaction, &index.table_id)
            .await?
        {
            delete_index_dependents(transaction, codec, id, true).await?;
            *deleted = true;
        }
    }
    for (id, index, deleted) in &mut states {
        if *deleted || lookup_index_id(transaction, codec, &index.name).await? != *id {
            continue;
        }
        let invalid = match index_schema(transaction, codec, &index.name).await {
            Ok(_) => false,
            Err(EngineError::InvalidQuery("Index column not found")) => true,
            Err(error) => return Err(error),
        };
        if !invalid {
            continue;
        }
        codec
            .delete_row(transaction, ENGINE_INDICES_STORAGE, id)
            .await?;
        let fields = {
            let rows = codec.scan_row_states(transaction, ENGINE_INDEX_FIELDS_STORAGE);
            pin_mut!(rows);
            let mut ids = Vec::new();
            while let Some(entry) = rows.next().await {
                let (field_id, field, field_deleted) = entry?;
                if !field_deleted
                    && field.values.first().and_then(Value::as_uuid).copied() == Some(*id)
                {
                    ids.push(field_id);
                }
            }
            ids
        };
        for field in fields {
            codec
                .delete_row(transaction, ENGINE_INDEX_FIELDS_STORAGE, &field)
                .await?;
        }
        let storage = index_storage(id);
        transaction.ensure_table(&storage).await?;
        transaction.drop_table(&storage).await?;
        *deleted = true;
    }
    let mut winners: Vec<(String, Uuid)> = Vec::new();
    for (id, index, _) in &states {
        if let Some((_, winner)) = winners.iter_mut().find(|(key, _)| key == &index.name) {
            if id > winner {
                *winner = *id;
            }
        } else {
            winners.push((index.name.clone(), *id));
        }
    }
    for (id, index, deleted) in states {
        let winner = winners
            .iter()
            .find(|(key, _)| key == &index.name)
            .expect("index name has a winning row")
            .1;
        if id != winner {
            delete_index_dependents(transaction, codec, &id, !deleted).await?;
        }
    }
    Ok(())
}

async fn delete_dropped_table_dependents<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    table: &str,
    table_id: &Uuid,
) -> EngineResult<()> {
    let fields = {
        let rows = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
        pin_mut!(rows);
        let mut ids = Vec::new();
        while let Some(entry) = rows.next().await {
            let (id, value, deleted) = entry?;
            if !deleted && TableFieldRow::try_from(&value)?.table_id == *table_id {
                ids.push(id);
            }
        }
        ids
    };
    for id in fields {
        codec
            .delete_row(transaction, ENGINE_TABLE_FIELDS_STORAGE, &id)
            .await?;
    }
    let table_is_active = match lookup_table_id(transaction, codec, table).await {
        Ok(_) => true,
        Err(EngineError::InvalidQuery("Table not found")) => false,
        Err(error) => return Err(error),
    };
    let rows = {
        let rows = codec.scan_row_states(transaction, table);
        pin_mut!(rows);
        let mut ids = Vec::new();
        while let Some(entry) = rows.next().await {
            let (id, _, deleted) = entry?;
            if !deleted
                && (!table_is_active
                    || codec
                        .row_has_dropped_definition(transaction, table, &id)
                        .await?)
            {
                ids.push(id);
            }
        }
        ids
    };
    for id in rows {
        codec.delete_row(transaction, table, &id).await?;
    }
    Ok(())
}

async fn delete_index_dependents<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    index_id: &Uuid,
    delete_definition: bool,
) -> EngineResult<()> {
    if delete_definition {
        codec
            .delete_row(transaction, ENGINE_INDICES_STORAGE, index_id)
            .await?;
    }
    let fields = {
        let rows = codec.scan_row_states(transaction, ENGINE_INDEX_FIELDS_STORAGE);
        pin_mut!(rows);
        let mut ids = Vec::new();
        while let Some(entry) = rows.next().await {
            let (field_id, field, deleted) = entry?;
            if !deleted && field.values.first().and_then(Value::as_uuid).copied() == Some(*index_id)
            {
                ids.push(field_id);
            }
        }
        ids
    };
    for field in fields {
        codec
            .delete_row(transaction, ENGINE_INDEX_FIELDS_STORAGE, &field)
            .await?;
    }
    let storage = index_storage(index_id);
    transaction.ensure_table(&storage).await?;
    transaction.drop_table(&storage).await?;
    Ok(())
}

pub(crate) async fn largest_schema_name_claim<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    storage: &str,
    name: &str,
) -> EngineResult<Option<Uuid>> {
    let rows = codec.scan_row_states(transaction, storage);
    pin_mut!(rows);
    let mut largest = None;
    while let Some(entry) = rows.next().await {
        let (id, value, _) = entry?;
        let row_name = match storage {
            ENGINE_TABLES_STORAGE => TableRow::try_from(&value)?.name,
            ENGINE_INDICES_STORAGE => IndexRow::try_from(&value)?.name,
            _ => return Err(EngineError::custom("Invalid schema name storage")),
        };
        if row_name == name && largest.is_none_or(|largest_id| id > largest_id) {
            largest = Some(id);
        }
    }
    Ok(largest)
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

pub(crate) fn index_storage(id: &Uuid) -> String {
    let mut name = String::from("__engine_index_");
    for byte in id.as_bytes() {
        name.push_str(&format!("{byte:02x}"));
    }
    name
}

pub(crate) async fn lookup_table_id<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<Uuid> {
    let rows = codec.scan_row_states(transaction, ENGINE_TABLES_STORAGE);
    pin_mut!(rows);
    let mut selected = None;
    while let Some(entry) = rows.next().await {
        let (id, value, deleted) = entry?;
        if deleted || TableRow::try_from(&value)?.name != name {
            continue;
        }
        if selected
            .as_ref()
            .is_none_or(|(selected_id, _)| id > *selected_id)
        {
            selected = Some((id, value));
        }
    }
    selected
        .map(|(id, _)| id)
        .ok_or(EngineError::InvalidQuery("Table not found"))
}

pub(crate) async fn active_user_row<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
    row: &Uuid,
) -> EngineResult<bool> {
    let _ = row;
    match lookup_table_id(transaction, codec, table).await {
        Ok(_) => Ok(true),
        Err(EngineError::InvalidQuery("Table not found")) => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) async fn lookup_index_id<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<Uuid> {
    active_index(transaction, codec, name)
        .await?
        .map(|(id, _)| id)
        .ok_or(EngineError::InvalidQuery("Index not found"))
}

async fn active_index<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
) -> EngineResult<Option<(Uuid, Row)>> {
    let rows = codec.scan_row_states(transaction, ENGINE_INDICES_STORAGE);
    pin_mut!(rows);
    let mut selected = None;
    while let Some(entry) = rows.next().await {
        let (id, value, deleted) = entry?;
        if deleted || IndexRow::try_from(&value)?.name != name {
            continue;
        }
        if selected
            .as_ref()
            .is_none_or(|(selected_id, _)| id > *selected_id)
        {
            selected = Some((id, value));
        }
    }
    let Some((id, value)) = selected else {
        return Ok(None);
    };
    let index = IndexRow::try_from(&value)?;
    let table = index.table;
    if codec
        .table_definition_was_dropped(transaction, &index.table_id)
        .await?
        || lookup_table_id(transaction, codec, &table).await.is_err()
    {
        return Ok(None);
    }
    Ok(Some((id, value)))
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
    let table_id = lookup_table_id(transaction, codec, table).await?;
    let rows = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(rows);
    let mut result = Vec::new();
    while let Some(row) = rows.next().await {
        let (_, value, deleted) = row?;
        if deleted {
            continue;
        }
        let field = TableFieldRow::try_from(&value)?;
        if field.table_id != table_id || field.table != table {
            continue;
        }
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
    let table_id = lookup_table_id(transaction, codec, table).await?;
    let rows = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(rows);
    let mut selected = None;
    while let Some(entry) = rows.next().await {
        let (id, value, deleted) = entry?;
        if deleted {
            continue;
        }
        let field = TableFieldRow::try_from(&value)?;
        if field.table_id != table_id || field.name != name {
            continue;
        }
        if selected.is_none_or(|selected_id| id > selected_id) {
            selected = Some(id);
        }
    }
    selected.ok_or(EngineError::InvalidQuery("Index column not found"))
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

async fn column_reference_is_active<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
    name: &str,
    id: Uuid,
) -> EngineResult<bool> {
    let rows = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(rows);
    while let Some(entry) = rows.next().await {
        let (field_id, value, deleted) = entry?;
        if field_id != id || deleted {
            continue;
        }
        let field = TableFieldRow::try_from(&value)?;
        if field.table == table && field.name == name {
            return Ok(true);
        }
    }
    Ok(false)
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
    let mut fields = Vec::new();
    while let Some(row) = rows.next().await {
        let (_, value, deleted) = row?;
        if deleted {
            continue;
        }
        let field = IndexFieldRow::try_from(&value)?;
        if id != field.index_id {
            continue;
        }
        fields.push((field.position, field));
    }
    fields.sort_by_key(|(position, _)| *position);
    let mut active_fields = Vec::new();
    for (position, field) in fields {
        let column = field.column;
        let referenced_column_id = field.column_id;
        if !column_reference_is_active(transaction, codec, &table, &column, referenced_column_id)
            .await?
        {
            return Err(EngineError::InvalidQuery(
                "Index column identity is inactive",
            ));
        }
        active_fields.push((position, column));
    }
    let schema_columns = columns(transaction, codec, &table).await?;
    let column_indices = active_fields
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
