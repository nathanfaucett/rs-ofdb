use alloc::{string::String, vec, vec::Vec};

use futures::{StreamExt, pin_mut};
use schema::IndexSchema;
use value::{Row, Value};

use crate::{
    EngineError, EngineResult, KernelTransaction, RowCodec, RowTable,
    executor::{materialize_defaults, row_id},
    schema::{
        ENGINE_INDICES_STORAGE, index_schema, index_storage, lookup_index_id, table_schema_for,
    },
};

async fn indexes_for_table<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
) -> EngineResult<Vec<(String, IndexSchema)>> {
    let entries = codec.scan_rows(transaction, ENGINE_INDICES_STORAGE);
    pin_mut!(entries);
    let mut names: Vec<String> = Vec::new();
    while let Some(entry) = entries.next().await {
        let (_, value) = entry?;
        if value.values.get(1).and_then(Value::as_text) == Some(table)
            && let Some(name) = value.values.first().and_then(Value::to_text)
        {
            names.push(name);
        }
    }
    names.sort();
    names.dedup();
    let mut result = Vec::new();
    for name in names {
        if let Some(schema) = index_schema(transaction, codec, &name).await?
            && schema.table_name == table
        {
            result.push((
                index_storage(&lookup_index_id(transaction, codec, &name).await?),
                schema,
            ));
        }
    }
    Ok(result)
}

fn schema_row_is_valid(schema: &schema::TableSchema, id: uuid::Uuid, row: &Row) -> bool {
    row.values.len() == schema.columns.len()
        && schema
            .columns
            .iter()
            .zip(&row.values)
            .all(|(column, value)| {
                (value == &Value::Null || value.r#type() == column.r#type)
                    && (!column.primary_key || value.as_uuid() == Some(&id))
            })
}

fn record_key(schema: &IndexSchema, row_id: uuid::Uuid, row: &Row) -> EngineResult<Row> {
    let mut values = Vec::with_capacity(schema.column_indices.len() + 2);
    for column in &schema.column_indices {
        values.push(
            row.values
                .get(*column as usize)
                .cloned()
                .ok_or(EngineError::InvalidQuery(
                    "Index column is missing from row",
                ))?,
        );
    }
    values.push(Value::Uuid(row_id));
    Ok(Row::new(values))
}

pub(crate) async fn lookup<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
    values: &Row,
) -> EngineResult<Option<Row>> {
    let schema = index_schema(transaction, codec, name)
        .await?
        .ok_or(EngineError::InvalidQuery("Index not found"))?;
    if values.values.len() != schema.column_indices.len() {
        return Err(EngineError::InvalidQuery(
            "Index key has the wrong column count",
        ));
    }
    let storage = index_storage(&lookup_index_id(transaction, codec, name).await?);
    let entries = transaction.scan_entries_owned(&storage);
    pin_mut!(entries);
    let mut winner = None;
    while let Some(entry) = entries.next().await {
        let (key, _) = entry?;
        if key.values.get(..values.values.len()) == Some(values.values.as_slice()) {
            winner = key.values.last().and_then(Value::as_uuid).copied();
            break;
        }
    }
    match winner {
        Some(row) => codec.get_row(transaction, &schema.table_name, &row).await,
        None => Ok(None),
    }
}

pub(crate) async fn rebuild_table<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    table: &str,
    enforce_unique: bool,
) -> EngineResult<()> {
    let indexes = indexes_for_table(transaction, codec, table).await?;
    for (name, _) in &indexes {
        transaction.ensure_table(name).await?;
        let stale = {
            let entries = transaction.scan_entries_owned(name);
            pin_mut!(entries);
            let mut keys = Vec::new();
            while let Some(entry) = entries.next().await {
                keys.push(entry?.0);
            }
            keys
        };
        for key in stale {
            transaction.remove_entry(name, &key).await?;
        }
    }
    let schema = table_schema_for(transaction, codec, table, table.into()).await?;
    let mut states = {
        let stream = codec.scan_row_states(transaction, table);
        pin_mut!(stream);
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row?);
        }
        rows
    };
    let mut valid_ids = Vec::new();
    for (id, row, deleted) in &states {
        let stale_definition = codec
            .row_has_dropped_definition(transaction, table, id)
            .await?;
        if !stale_definition && schema_row_is_valid(&schema, *id, row) {
            valid_ids.push(*id);
        } else if !deleted {
            codec.delete_row(transaction, table, id).await?;
        }
    }
    states.retain(|(id, _, _)| valid_ids.contains(id));
    if !enforce_unique {
        let mut losers = Vec::new();
        for (_, index) in indexes.iter().filter(|(_, index)| index.unique) {
            let mut owners: Vec<(uuid::Uuid, Vec<Value>)> = Vec::new();
            for (id, row, _) in &states {
                let row = materialize_defaults(&schema, row.clone());
                let key = record_key(index, *id, &row)?;
                let values = key.values[..key.values.len() - 1].to_vec();
                if values.iter().any(|value| matches!(value, Value::Null)) {
                    continue;
                }
                if let Some((winner, _)) =
                    owners.iter_mut().find(|(_, existing)| *existing == values)
                {
                    if id > winner {
                        losers.push(*winner);
                        *winner = *id;
                    } else {
                        losers.push(*id);
                    }
                } else {
                    owners.push((*id, values));
                }
            }
        }
        losers.sort();
        losers.dedup();
        for id in losers {
            if !codec.row_is_deleted(transaction, table, &id).await? {
                codec.delete_row(transaction, table, &id).await?;
            }
        }
    }
    let rows = {
        let stream = codec.scan_rows(transaction, table);
        pin_mut!(stream);
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row?);
        }
        rows
    };
    for (_, row) in rows {
        update_row(
            transaction,
            codec,
            table,
            None,
            Some(&materialize_defaults(&schema, row)),
            enforce_unique,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn update_row<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    table: &str,
    old: Option<&Row>,
    new: Option<&Row>,
    enforce_unique: bool,
) -> EngineResult<()> {
    let schema = table_schema_for(transaction, codec, table, table.into()).await?;
    for (name, index) in indexes_for_table(transaction, codec, table).await? {
        transaction.ensure_table(&name).await?;
        if let Some(row) = old {
            transaction
                .remove_entry(
                    &name,
                    &record_key(
                        &index,
                        row_id(&schema, row)?,
                        &materialize_defaults(&schema, row.clone()),
                    )?,
                )
                .await?;
        }
        if let Some(row) = new {
            let row = materialize_defaults(&schema, row.clone());
            let key = record_key(&index, row_id(&schema, &row)?, &row)?;
            if enforce_unique
                && index.unique
                && !key.values.iter().any(|value| matches!(value, Value::Null))
                && (unique_key_exists(transaction, &name, &key).await?
                    || deleted_unique_claim_exists(
                        transaction,
                        codec,
                        table,
                        &schema,
                        &index,
                        &key,
                    )
                    .await?)
            {
                return Err(EngineError::InvalidQuery("Unique index violation"));
            }
            transaction.put_entry(&name, key, Row::new(vec![])).await?;
        }
    }
    Ok(())
}

async fn deleted_unique_claim_exists<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    table: &str,
    schema: &schema::TableSchema,
    index: &IndexSchema,
    key: &Row,
) -> EngineResult<bool> {
    let candidate_id = key
        .values
        .last()
        .and_then(Value::as_uuid)
        .copied()
        .ok_or(EngineError::InvalidQuery("Index key has no row UUID"))?;
    let unique_values = &key.values[..key.values.len() - 1];
    let rows = codec.scan_row_states(transaction, table);
    pin_mut!(rows);
    while let Some(entry) = rows.next().await {
        let (id, row, deleted) = entry?;
        if deleted && id > candidate_id {
            let row = materialize_defaults(schema, row);
            let previous_key = record_key(index, id, &row)?;
            if previous_key.values.get(..unique_values.len()) == Some(unique_values) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn unique_key_exists<T: KernelTransaction>(
    transaction: &T,
    table: &str,
    key: &Row,
) -> EngineResult<bool> {
    let entries = transaction.scan_entries(table);
    pin_mut!(entries);
    let unique_values = &key.values[..key.values.len() - 1];
    while let Some(entry) = entries.next().await {
        let (candidate, _) = entry?;
        if candidate.values.get(..unique_values.len()) == Some(unique_values) {
            return Ok(true);
        }
    }
    Ok(false)
}
