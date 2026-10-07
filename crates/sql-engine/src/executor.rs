use alloc::{boxed::Box, string::String, vec, vec::Vec};

use futures::{StreamExt, pin_mut};
use query::{
    AlterTableOperation, DataDefinition, Query, QueryAggregate, QueryColumn, QueryCountTarget,
    QueryDelete, QueryExpr, QueryExprValue, QueryHavingCountOperator, QueryInsert,
    QueryInsertValue, QueryInsertValues, QueryJoinKind, QueryOrderBy, QueryResult,
    QueryResultColumn, QuerySelect, QuerySortDirection, QueryUpdate, QueryUpdateAssignment,
};
use schema::{ColumnSchema, TableSchema};
use uuid::Uuid;
use value::{Row, Value};

fn next_uuid(timestamp_provider: TimestampProvider) -> EngineResult<Uuid> {
    Ok(Uuid::new_v7(timestamp_provider()))
}

use crate::{
    Change, EngineError, EngineResult,
    change::apply_local_change,
    codec::RowCodec,
    engine::{Engine, TimestampProvider},
    kernel::{Kernel, KernelTransaction},
    schema::{
        ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_TABLES_STORAGE, catalog_table_for_storage, column_id, columns as schema_columns,
        ensure as ensure_schema, index_storage, largest_schema_name_claim, lookup_index_id,
        lookup_table_id, lookup_table_name, table_schema as schema_table_schema,
    },
};

pub async fn execute_statement<K, R>(
    engine: &Engine<K, R>,
    statements: Vec<query::Statement>,
) -> EngineResult<Vec<QueryResult>>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    let mut transaction = engine.kernel.transaction().await?;
    let mut changes = Vec::new();
    match Box::pin(execute_in_transaction(
        engine,
        &mut transaction,
        statements,
        &mut changes,
    ))
    .await
    {
        Ok(results) => {
            transaction.commit().await?;
            Ok(results)
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}

pub(crate) async fn execute_in_transaction<K, R>(
    engine: &Engine<K, R>,
    transaction: &mut K::Transaction,
    statements: Vec<query::Statement>,
    changes: &mut Vec<Change>,
) -> EngineResult<Vec<QueryResult>>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    ensure_schema(transaction, engine.reconciler.as_ref()).await?;
    let mut results = Vec::with_capacity(statements.len());
    for statement in statements {
        let result = match statement {
            query::Statement::Query(query) => {
                execute_query(
                    transaction,
                    engine.reconciler.as_ref(),
                    engine.timestamp_provider,
                    changes,
                    query,
                )
                .await
            }
            query::Statement::DataDefinition(ddl) => {
                execute_ddl(
                    transaction,
                    engine.reconciler.as_ref(),
                    engine.timestamp_provider,
                    changes,
                    ddl,
                )
                .await
            }
        }?;
        results.push(result);
    }
    Ok(results)
}

async fn execute_query<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    query: Query,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match query {
        Query::Insert(insert) => {
            insert_row(transaction, reconciler, timestamp_provider, changes, insert).await
        }
        Query::InsertValues(insert) => {
            insert_values(transaction, reconciler, timestamp_provider, changes, insert).await
        }
        Query::Select(select) => select_rows(transaction, reconciler, select).await,
        Query::Update(update) => {
            update_rows(transaction, reconciler, timestamp_provider, changes, update).await
        }
        Query::Delete(delete) => {
            delete_rows(transaction, reconciler, timestamp_provider, changes, delete).await
        }
    }
}

async fn execute_ddl<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    ddl: DataDefinition,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match &ddl {
        DataDefinition::CreateTable { .. }
        | DataDefinition::CreateTableWithIndexes { .. }
        | DataDefinition::DropTable { .. }
        | DataDefinition::AlterTable { .. } => {
            execute_table_ddl(transaction, reconciler, timestamp_provider, changes, ddl).await
        }
        DataDefinition::CreateIndex { .. }
        | DataDefinition::CreateIndexUnresolved { .. }
        | DataDefinition::DropIndex { .. } => {
            execute_index_ddl(transaction, reconciler, timestamp_provider, changes, ddl).await
        }
        _ => Err(EngineError::Unsupported(
            "only CREATE TABLE and ALTER TABLE ADD COLUMN are supported",
        )),
    }
}

async fn execute_table_ddl<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    ddl: DataDefinition,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match ddl {
        DataDefinition::CreateTable {
            schema,
            if_not_exists,
        } => {
            if lookup_table_name(transaction, codec, &schema.name)
                .await
                .is_ok()
            {
                return if if_not_exists {
                    Ok(QueryResult::default())
                } else {
                    Err(EngineError::InvalidQuery("Table already exists"))
                };
            }
            create_table(transaction, codec, timestamp_provider, changes, schema).await?;
            Ok(QueryResult::default())
        }
        DataDefinition::CreateTableWithIndexes {
            schema,
            indexes,
            if_not_exists,
        } => {
            if lookup_table_name(transaction, codec, &schema.name)
                .await
                .is_ok()
            {
                return if if_not_exists {
                    Ok(QueryResult::default())
                } else {
                    Err(EngineError::InvalidQuery("Table already exists"))
                };
            }
            create_table(transaction, codec, timestamp_provider, changes, schema).await?;
            for index in indexes {
                create_index(transaction, codec, timestamp_provider, changes, index).await?;
            }
            Ok(QueryResult::default())
        }
        DataDefinition::DropTable {
            table_name,
            if_exists,
        } => {
            let table = match lookup_table_name(transaction, codec, &table_name).await {
                Ok(table) => table,
                Err(_) if if_exists => return Ok(QueryResult::default()),
                Err(error) => return Err(error),
            };
            let id = lookup_table_id(transaction, codec, &table).await?;
            drop_table_contents(transaction, codec, timestamp_provider, changes, &table, id)
                .await?;
            Ok(QueryResult::default())
        }
        DataDefinition::AlterTable {
            table_name,
            operations,
            if_exists,
        } => {
            alter_table(
                transaction,
                codec,
                timestamp_provider,
                changes,
                table_name,
                operations,
                if_exists,
            )
            .await
        }
        _ => unreachable!(),
    }
}

async fn execute_index_ddl<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    ddl: DataDefinition,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match ddl {
        DataDefinition::CreateIndex {
            schema,
            if_not_exists,
        } => {
            if crate::schema::index_schema(transaction, codec, &schema.name)
                .await?
                .is_some()
            {
                return if if_not_exists {
                    Ok(QueryResult::default())
                } else {
                    Err(EngineError::InvalidQuery("Index already exists"))
                };
            }
            create_index(transaction, codec, timestamp_provider, changes, schema).await?;
            Ok(QueryResult::default())
        }
        DataDefinition::CreateIndexUnresolved {
            index_name,
            table_name,
            column_names,
            unique,
            if_not_exists,
        } => {
            create_unresolved_index(
                transaction,
                codec,
                timestamp_provider,
                changes,
                index_name,
                table_name,
                column_names,
                unique,
                if_not_exists,
            )
            .await
        }
        DataDefinition::DropIndex {
            index_name,
            if_exists,
        } => {
            drop_index(
                transaction,
                codec,
                timestamp_provider,
                changes,
                &index_name,
                if_exists,
            )
            .await?;
            Ok(QueryResult::default())
        }
        _ => unreachable!(),
    }
}

async fn create_unresolved_index<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    index_name: String,
    table_name: String,
    column_names: Vec<String>,
    unique: bool,
    if_not_exists: bool,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if crate::schema::index_schema(transaction, codec, &index_name)
        .await?
        .is_some()
    {
        return if if_not_exists {
            Ok(QueryResult::default())
        } else {
            Err(EngineError::InvalidQuery("Index already exists"))
        };
    }
    let column_indices =
        unresolved_index_columns(transaction, codec, &table_name, &column_names).await?;
    create_index(
        transaction,
        codec,
        timestamp_provider,
        changes,
        schema::IndexSchema {
            name: index_name,
            table_name,
            column_indices,
            unique,
        },
    )
    .await?;
    Ok(QueryResult::default())
}

async fn unresolved_index_columns<T, R>(
    transaction: &T,
    codec: &R,
    table_name: &str,
    names: &[String],
) -> EngineResult<Vec<u32>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let schema = table_schema(transaction, codec, table_name).await?;
    names
        .iter()
        .map(|name| {
            schema
                .columns
                .iter()
                .position(|column| column.name == *name)
                .map(|index| index as u32)
                .ok_or(EngineError::InvalidQuery("Index column not found"))
        })
        .collect()
}

async fn alter_table<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    table_name: String,
    operations: Vec<AlterTableOperation>,
    if_exists: bool,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = match lookup_table_name(transaction, codec, &table_name).await {
        Ok(table) => table,
        Err(_) if if_exists => return Ok(QueryResult::default()),
        Err(error) => return Err(error),
    };
    let table_id = lookup_table_id(transaction, codec, &table_name).await?;
    let mut names: Vec<_> = table_schema(transaction, codec, &table_name)
        .await?
        .columns
        .into_iter()
        .map(|column| column.name)
        .collect();
    for (position, operation) in (names.len()..).zip(operations) {
        let AlterTableOperation::AddColumn(column) = operation else {
            return Err(EngineError::Unsupported(
                "only ALTER TABLE ADD COLUMN is supported",
            ));
        };
        if names.iter().any(|name| name == &column.name) {
            return Err(EngineError::InvalidQuery("Column already exists"));
        }
        if column.primary_key {
            return Err(EngineError::InvalidQuery("Cannot add a primary-key column"));
        }
        add_column(
            transaction,
            codec,
            timestamp_provider,
            changes,
            table_id,
            &table,
            position,
            &column,
        )
        .await?;
        names.push(column.name);
    }
    Ok(QueryResult::default())
}

async fn drop_table_contents<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    table: &str,
    table_id: Uuid,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let rows = {
        let rows = codec.scan_rows(transaction, table);
        pin_mut!(rows);
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await {
            ids.push(row?.0);
        }
        ids
    };
    for id in rows {
        catalog_delete(transaction, codec, changes, timestamp_provider, table, &id).await?;
    }

    let indexes = {
        let rows = codec.scan_row_states(transaction, ENGINE_INDICES_STORAGE);
        pin_mut!(rows);
        let mut indexes = Vec::new();
        while let Some(row) = rows.next().await {
            let (id, value, deleted) = row?;
            if value.values.get(1).and_then(Value::as_text) == Some(table) {
                indexes.push((id, deleted));
            }
        }
        indexes
    };
    for (index_id, index_deleted) in indexes {
        let fields = {
            let rows = codec.scan_row_states(transaction, ENGINE_INDEX_FIELDS_STORAGE);
            pin_mut!(rows);
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await {
                let (id, value, deleted) = row?;
                if !deleted
                    && value.values.first().and_then(Value::as_uuid).copied() == Some(index_id)
                {
                    ids.push(id);
                }
            }
            ids
        };
        for id in fields {
            catalog_delete(
                transaction,
                codec,
                changes,
                timestamp_provider,
                ENGINE_INDEX_FIELDS_STORAGE,
                &id,
            )
            .await?;
        }
        if !index_deleted {
            catalog_delete(
                transaction,
                codec,
                changes,
                timestamp_provider,
                ENGINE_INDICES_STORAGE,
                &index_id,
            )
            .await?;
        }
        let storage = index_storage(&index_id);
        transaction.ensure_table(&storage).await?;
        transaction.drop_table(&storage).await?;
    }

    let fields = {
        let rows = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
        pin_mut!(rows);
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await {
            let (id, value, deleted) = row?;
            if !deleted && crate::schema::TableFieldRow::try_from(&value)?.table == table {
                ids.push(id);
            }
        }
        ids
    };
    for id in fields {
        catalog_delete(
            transaction,
            codec,
            changes,
            timestamp_provider,
            ENGINE_TABLE_FIELDS_STORAGE,
            &id,
        )
        .await?;
    }
    let table_definitions = {
        let rows = codec.scan_row_states(transaction, ENGINE_TABLES_STORAGE);
        pin_mut!(rows);
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await {
            let (id, value, _) = row?;
            if crate::schema::TableRow::try_from(&value)?.name == table {
                ids.push(id);
            }
        }
        ids
    };
    let event = catalog_delete(
        transaction,
        codec,
        changes,
        timestamp_provider,
        ENGINE_TABLES_STORAGE,
        &table_id,
    )
    .await?;
    for definition in table_definitions {
        codec
            .drop_table_definition(transaction, &definition, event)
            .await?;
    }
    Ok(())
}

async fn drop_index<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    name: &str,
    if_exists: bool,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if crate::schema::index_schema(transaction, codec, name)
        .await?
        .is_none()
    {
        return if if_exists {
            Ok(())
        } else {
            Err(EngineError::InvalidQuery("Index not found"))
        };
    }
    let id = lookup_index_id(transaction, codec, name).await?;
    let fields = {
        let rows = codec.scan_row_states(transaction, ENGINE_INDEX_FIELDS_STORAGE);
        pin_mut!(rows);
        let mut ids = Vec::new();
        while let Some(entry) = rows.next().await {
            let (field_id, value, deleted) = entry?;
            if !deleted && value.values.first().and_then(Value::as_uuid).copied() == Some(id) {
                ids.push(field_id);
            }
        }
        ids
    };
    for field in fields {
        catalog_delete(
            transaction,
            codec,
            changes,
            timestamp_provider,
            ENGINE_INDEX_FIELDS_STORAGE,
            &field,
        )
        .await?;
    }
    let storage = index_storage(&id);
    catalog_delete(
        transaction,
        codec,
        changes,
        timestamp_provider,
        ENGINE_INDICES_STORAGE,
        &id,
    )
    .await?;
    transaction.drop_table(&storage).await
}

async fn create_index<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    schema: schema::IndexSchema,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table_schema = table_schema(transaction, codec, &schema.table_name).await?;
    if schema.name.is_empty() || schema.column_indices.is_empty() {
        return Err(EngineError::InvalidQuery(
            "Index requires a name and column",
        ));
    }
    if catalog_table_for_storage(&schema.name).is_some()
        || schema.name.starts_with("__engine_index_")
    {
        return Err(EngineError::InvalidQuery(
            "Catalog table names are reserved",
        ));
    }
    if schema
        .column_indices
        .iter()
        .any(|index| *index as usize >= table_schema.columns.len())
    {
        return Err(EngineError::InvalidQuery("Index column is out of range"));
    }

    let table = lookup_table_name(transaction, codec, &schema.table_name).await?;
    let columns = schema_columns(transaction, codec, &table).await?;
    let index_columns = schema
        .column_indices
        .iter()
        .map(|position| {
            columns
                .get(*position as usize)
                .map(|(name, _)| name.clone())
                .ok_or(EngineError::InvalidQuery("Index column is out of range"))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let id = next_uuid(timestamp_provider)?;
    if largest_schema_name_claim(transaction, codec, ENGINE_INDICES_STORAGE, &schema.name)
        .await?
        .is_some_and(|deleted_id| id <= deleted_id)
    {
        return Err(EngineError::InvalidQuery(
            "New index identity must exceed its deleted identity",
        ));
    }
    let table_id = lookup_table_id(transaction, codec, &table).await?;
    let storage = index_storage(&id);
    codec.ensure_table(transaction, &storage).await?;
    catalog_put(
        transaction,
        codec,
        changes,
        timestamp_provider,
        ENGINE_INDICES_STORAGE,
        id,
        Row::new(vec![
            Value::from(schema.name),
            Value::from(table.clone()),
            Value::Bool(schema.unique),
            Value::Uuid(table_id),
        ]),
    )
    .await?;
    for (position, column) in index_columns.into_iter().enumerate() {
        let column_id = column_id(transaction, codec, &table, &column).await?;
        let field = next_uuid(timestamp_provider)?;
        catalog_put(
            transaction,
            codec,
            changes,
            timestamp_provider,
            ENGINE_INDEX_FIELDS_STORAGE,
            field,
            Row::new(vec![
                Value::Uuid(id),
                Value::Integer(position as i64),
                Value::from(column),
                Value::Uuid(column_id),
            ]),
        )
        .await?;
    }
    crate::index::rebuild_table(transaction, codec, &table, true).await
}

async fn create_table<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    table_schema: TableSchema,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    table_schema
        .validate_uuid_primary_key()
        .map_err(EngineError::InvalidQuery)?;
    if catalog_table_for_storage(&table_schema.name).is_some()
        || table_schema.name.starts_with("__engine_index_")
    {
        return Err(EngineError::InvalidQuery(
            "Catalog table names are reserved",
        ));
    }
    let table = table_schema.name.clone();
    let id = next_uuid(timestamp_provider)?;
    let observed_drops = codec.table_drop_events(transaction, &table).await?;
    if largest_schema_name_claim(transaction, codec, ENGINE_TABLES_STORAGE, &table)
        .await?
        .is_some_and(|deleted_id| id <= deleted_id)
    {
        return Err(EngineError::InvalidQuery(
            "New table identity must exceed its deleted identity",
        ));
    }
    codec.ensure_table(transaction, &table).await?;
    catalog_put(
        transaction,
        codec,
        changes,
        timestamp_provider,
        ENGINE_TABLES_STORAGE,
        id,
        Row::new(vec![Value::from(table.clone())]),
    )
    .await?;
    codec
        .set_observed_table_drops(transaction, &id, observed_drops)
        .await?;
    for (position, column) in table_schema.columns.iter().enumerate() {
        add_column(
            transaction,
            codec,
            timestamp_provider,
            changes,
            id,
            &table,
            position,
            column,
        )
        .await?;
    }
    Ok(())
}

async fn add_column<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    table_id: Uuid,
    table: &str,
    position: usize,
    column: &ColumnSchema,
) -> EngineResult<()> {
    let position =
        i64::try_from(position).map_err(|_| EngineError::InvalidQuery("Too many columns"))?;
    let id = next_uuid(timestamp_provider)?;
    catalog_put(
        transaction,
        codec,
        changes,
        timestamp_provider,
        ENGINE_TABLE_FIELDS_STORAGE,
        id,
        Row::new(vec![
            Value::Uuid(table_id),
            Value::from(table),
            Value::from(column.name.as_str()),
            column.r#type.into(),
            column.default.clone(),
            Value::Integer(position),
            Value::Bool(column.primary_key),
        ]),
    )
    .await
}

async fn catalog_put<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    changes: &mut Vec<Change>,
    timestamp_provider: TimestampProvider,
    storage: &str,
    id: Uuid,
    row: Row,
) -> EngineResult<()> {
    let fields: Vec<_> = (0..row.values.len()).collect();
    let encoded = codec
        .encode_row(transaction, storage, &id, &row, &fields)
        .await?;
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::row(
            next_uuid(timestamp_provider)?,
            storage.into(),
            id,
            Some(encoded),
        ),
    )
    .await
}

async fn catalog_delete<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    changes: &mut Vec<Change>,
    timestamp_provider: TimestampProvider,
    storage: &str,
    id: &Uuid,
) -> EngineResult<Uuid> {
    let change_id = next_uuid(timestamp_provider)?;
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::row(change_id, storage.into(), *id, None),
    )
    .await?;
    Ok(change_id)
}

async fn insert_row<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    insert: QueryInsert,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let schema = table_schema(transaction, reconciler, &insert.table).await?;
    let row_value = materialize_insert(&schema, insert.row, timestamp_provider)?;
    let row = row_id(&schema, &row_value)?;
    let table = lookup_table_name(transaction, reconciler, &insert.table).await?;
    if row_is_deleted(reconciler, transaction, &table, row).await? {
        return Err(EngineError::InvalidQuery("Primary key was deleted"));
    }
    if reconciler
        .get_row(transaction, &table, &row)
        .await?
        .is_some()
    {
        return Err(EngineError::InvalidQuery("Primary key already exists"));
    }
    let changed_columns: Vec<_> = (0..row_value.values.len()).collect();
    let value = reconciler
        .encode_row(transaction, &table, &row, &row_value, &changed_columns)
        .await?;
    apply_local_change(
        transaction,
        reconciler,
        changes,
        Change::row(
            next_uuid(timestamp_provider)?,
            table.clone(),
            row,
            Some(value),
        ),
    )
    .await?;

    let Some(returning) = insert.returning else {
        return Ok(QueryResult::default());
    };
    let mut result = Vec::with_capacity(returning.len());
    let mut columns = Vec::with_capacity(returning.len());
    for name in returning {
        if name == "*" {
            result.extend(row_value.values.iter().cloned());
            columns.extend(schema.columns.iter().map(|column| QueryResultColumn {
                name: column.name.clone(),
                source_table: Some(insert.table.clone()),
                source_column: Some(column.name.clone()),
            }));
            continue;
        }
        let index = schema
            .columns
            .iter()
            .position(|column| column.name == name)
            .ok_or(EngineError::InvalidQuery("Unknown RETURNING column"))?;
        result.push(row_value.values[index].clone());
        columns.push(QueryResultColumn {
            name: name.clone(),
            source_table: Some(insert.table.clone()),
            source_column: Some(name),
        });
    }
    Ok(QueryResult::new_with_columns(
        vec![Row::new(result)],
        columns,
    ))
}

async fn row_is_deleted<T, R>(
    reconciler: &R,
    transaction: &T,
    table: &str,
    row: Uuid,
) -> EngineResult<bool>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    reconciler.row_is_deleted(transaction, table, &row).await
}

async fn insert_values<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    insert: QueryInsertValues,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let schema = table_schema(transaction, reconciler, &insert.table).await?;
    let primary_key = schema
        .columns
        .iter()
        .position(|column| column.primary_key)
        .ok_or(EngineError::InvalidQuery(
            "Table requires exactly one UUID primary key",
        ))?;
    if let Some(target) = insert.on_conflict_do_nothing.as_ref().or_else(|| {
        insert
            .on_conflict_do_update
            .as_ref()
            .map(|(target, _)| target)
    }) && target.as_slice() != [schema.columns[primary_key].name.as_str()]
    {
        return Err(EngineError::InvalidQuery(
            "ON CONFLICT target must be the UUID primary key",
        ));
    }
    let columns = if insert.columns.is_empty() {
        schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    } else {
        insert.columns
    };
    let mut result = QueryResult::default();
    for row_values in insert.rows {
        let mut values = vec![None; schema.columns.len()];
        if columns.len() != row_values.len() {
            return Err(EngineError::InvalidQuery(
                "INSERT column/value count mismatch",
            ));
        }
        for (column, value) in columns.iter().zip(row_values) {
            let index = schema
                .columns
                .iter()
                .position(|candidate| candidate.name == *column)
                .ok_or(EngineError::InvalidQuery("Unknown INSERT column"))?;
            if values[index].is_some() {
                return Err(EngineError::InvalidQuery("Duplicate INSERT column"));
            }
            values[index] = Some(value);
        }
        let row = Row::new(
            values
                .into_iter()
                .enumerate()
                .map(|(index, value)| match value {
                    Some(QueryInsertValue::Value(value)) => Ok(value),
                    Some(QueryInsertValue::Default) | None if index == primary_key => {
                        next_uuid(timestamp_provider).map(Value::Uuid)
                    }
                    Some(QueryInsertValue::Default) | None => {
                        Ok(schema.columns[index].default.clone())
                    }
                })
                .collect::<EngineResult<Vec<_>>>()?,
        );
        if insert.on_conflict_do_nothing.is_some() || insert.on_conflict_do_update.is_some() {
            let row_id = row_id(&schema, &row)?;
            let table = lookup_table_name(transaction, reconciler, &insert.table).await?;
            if let Some(mut existing) = reconciler.get_row(transaction, &table, &row_id).await? {
                let Some((_, assignments)) = &insert.on_conflict_do_update else {
                    continue;
                };
                let original = existing.values.clone();
                let mut changed_columns = Vec::with_capacity(assignments.len());
                for assignment in assignments {
                    let index = column_index(
                        &schema,
                        &insert.table,
                        &assignment.column,
                        "Unknown ON CONFLICT assignment column",
                    )?;
                    if schema.columns[index].primary_key || changed_columns.contains(&index) {
                        return Err(EngineError::InvalidQuery(
                            "Invalid ON CONFLICT assignment column",
                        ));
                    }
                    existing.values[index] = match &assignment.value {
                        QueryExprValue::Value(value) => value.clone(),
                        QueryExprValue::ExcludedColumn(column) => {
                            let source_index = schema
                                .columns
                                .iter()
                                .position(|candidate| candidate.name == *column)
                                .ok_or(EngineError::InvalidQuery("Unknown EXCLUDED column"))?;
                            row.values[source_index].clone()
                        }
                        QueryExprValue::Column(_) => {
                            return Err(EngineError::InvalidQuery(
                                "ON CONFLICT assignment requires a value or EXCLUDED column",
                            ));
                        }
                    };
                    if existing.values[index] != original[index] {
                        changed_columns.push(index);
                    }
                }
                if changed_columns.is_empty() {
                    continue;
                }
                let encoded = reconciler
                    .encode_row(transaction, &table, &row_id, &existing, &changed_columns)
                    .await?;
                apply_local_change(
                    transaction,
                    reconciler,
                    changes,
                    Change::row(
                        next_uuid(timestamp_provider)?,
                        table.clone(),
                        row_id,
                        Some(encoded),
                    ),
                )
                .await?;
                continue;
            }
        }
        let row_result = insert_row(
            transaction,
            reconciler,
            timestamp_provider,
            changes,
            QueryInsert {
                table: insert.table.clone(),
                row,
                returning: insert.returning.clone(),
            },
        )
        .await?;
        if result.columns.is_empty() {
            result.columns = row_result.columns;
        }
        result.rows.extend(row_result.rows);
    }
    Ok(result)
}

async fn update_rows<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    update: QueryUpdate,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if !update.from.joins.is_empty() {
        return Err(EngineError::Unsupported("UPDATE JOIN"));
    }
    let schema = table_schema(transaction, reconciler, &update.from.table).await?;
    let returning = update
        .returning
        .as_ref()
        .map(|columns| projection(&schema, &update.from.table, columns))
        .transpose()?;
    let mut returned_rows = Vec::new();
    let assignments = assignments(&schema, &update.from.table, &update.assignments)?;
    let rows = matching_rows(
        transaction,
        reconciler,
        &update.from.table,
        &schema,
        update.predicate.as_ref(),
    )
    .await?;

    let table = lookup_table_name(transaction, reconciler, &update.from.table).await?;
    for (id, mut row) in rows {
        let original = row.values.clone();
        for (index, value) in &assignments {
            row.values[*index] = match value {
                QueryExprValue::Value(value) => value.clone(),
                QueryExprValue::Column(column) => {
                    let source_index = column_index(
                        &schema,
                        &update.from.table,
                        column,
                        "Unknown assignment column",
                    )?;
                    original[source_index].clone()
                }
                QueryExprValue::ExcludedColumn(_) => {
                    return Err(EngineError::InvalidQuery(
                        "EXCLUDED is only valid in ON CONFLICT assignments",
                    ));
                }
            };
        }
        if row_id(&schema, &row)? != id {
            return Err(EngineError::InvalidQuery("Cannot update the primary key"));
        }
        let changed_columns: Vec<_> = assignments.iter().map(|(index, _)| *index).collect();
        let value = reconciler
            .encode_row(transaction, &table, &id, &row, &changed_columns)
            .await?;
        apply_local_change(
            transaction,
            reconciler,
            changes,
            Change::row(
                next_uuid(timestamp_provider)?,
                table.clone(),
                id,
                Some(value),
            ),
        )
        .await?;
        if let Some(returning) = &returning {
            returned_rows.push(Row::new(
                returning
                    .iter()
                    .map(|(index, _)| row.values[*index].clone())
                    .collect(),
            ));
        }
    }

    match returning {
        Some(columns) => Ok(QueryResult::new_with_columns(
            returned_rows,
            columns.into_iter().map(|(_, column)| column).collect(),
        )),
        None => Ok(QueryResult::default()),
    }
}

async fn delete_rows<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    delete: QueryDelete,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if !delete.from.joins.is_empty() {
        return Err(EngineError::Unsupported("DELETE JOIN"));
    }
    let schema = table_schema(transaction, reconciler, &delete.from.table).await?;
    let returning = delete
        .returning
        .as_ref()
        .map(|columns| projection(&schema, &delete.from.table, columns))
        .transpose()?;
    let mut returned_rows = Vec::new();
    let rows = matching_rows(
        transaction,
        reconciler,
        &delete.from.table,
        &schema,
        delete.predicate.as_ref(),
    )
    .await?;

    let table = lookup_table_name(transaction, reconciler, &delete.from.table).await?;
    for (row_id, row) in rows {
        apply_local_change(
            transaction,
            reconciler,
            changes,
            Change::row(next_uuid(timestamp_provider)?, table.clone(), row_id, None),
        )
        .await?;
        if let Some(returning) = &returning {
            returned_rows.push(Row::new(
                returning
                    .iter()
                    .map(|(index, _)| row.values[*index].clone())
                    .collect(),
            ));
        }
    }

    match returning {
        Some(columns) => Ok(QueryResult::new_with_columns(
            returned_rows,
            columns.into_iter().map(|(_, column)| column).collect(),
        )),
        None => Ok(QueryResult::default()),
    }
}

pub(crate) async fn row_conflicts<T, R>(
    transaction: &T,
    reconciler: &R,
    table_name: &str,
    key: &Row,
) -> EngineResult<Vec<String>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = lookup_table_name(transaction, reconciler, table_name).await?;
    let row = key_row_id(key)?;
    let schema = table_schema(transaction, reconciler, table_name).await?;
    reconciler
        .conflicted_columns(transaction, &table, &row)
        .await?
        .into_iter()
        .map(|index| {
            schema
                .columns
                .get(index)
                .map(|column| column.name.clone())
                .ok_or(EngineError::custom(
                    "Conflicted column is missing from schema",
                ))
        })
        .collect()
}

pub(crate) async fn row_conflict_values<T, R>(
    transaction: &T,
    reconciler: &R,
    table_name: &str,
    key: &Row,
) -> EngineResult<Vec<(String, Vec<Value>)>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = lookup_table_name(transaction, reconciler, table_name).await?;
    let row = key_row_id(key)?;
    let schema = table_schema(transaction, reconciler, table_name).await?;
    reconciler
        .conflict_values(transaction, &table, &row)
        .await?
        .into_iter()
        .map(|(index, values)| {
            let name = schema
                .columns
                .get(index)
                .map(|column| column.name.clone())
                .ok_or(EngineError::custom(
                    "Conflicted column is missing from schema",
                ))?;
            Ok((name, values))
        })
        .collect()
}

pub(crate) async fn resolve_row<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    table_name: &str,
    key: &Row,
    values: Vec<(String, Value)>,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if values.is_empty() {
        return Err(EngineError::InvalidQuery(
            "Resolution requires an assignment",
        ));
    }
    let table = lookup_table_name(transaction, reconciler, table_name).await?;
    let row_id = key_row_id(key)?;
    let schema = table_schema(transaction, reconciler, table_name).await?;
    let conflicts = reconciler
        .conflicted_columns(transaction, &table, &row_id)
        .await?;
    let mut row = reconciler
        .get_row(transaction, &table, &row_id)
        .await?
        .ok_or(EngineError::InvalidQuery("Row not found"))?;
    let mut changed_columns = Vec::with_capacity(values.len());
    for (name, value) in values {
        let index = schema
            .columns
            .iter()
            .position(|column| column.name == name)
            .ok_or(EngineError::InvalidQuery("Unknown resolution column"))?;
        if !conflicts.contains(&index) {
            return Err(EngineError::InvalidQuery(
                "Resolution column is not conflicted",
            ));
        }
        if schema.columns[index].primary_key {
            return Err(EngineError::InvalidQuery("Cannot resolve the primary key"));
        }
        if changed_columns.contains(&index) {
            return Err(EngineError::InvalidQuery("Resolution column is repeated"));
        }
        row.values[index] = value;
        changed_columns.push(index);
    }
    let value = reconciler
        .encode_resolution(transaction, &table, &row_id, &row, &changed_columns)
        .await?;
    let mut changes = Vec::new();
    apply_local_change(
        transaction,
        reconciler,
        &mut changes,
        Change::row(
            next_uuid(timestamp_provider)?,
            table.clone(),
            row_id,
            Some(value),
        ),
    )
    .await?;
    Ok(())
}

async fn matching_rows<T, R>(
    transaction: &T,
    reconciler: &R,
    table_name: &str,
    schema: &TableSchema,
    predicate: Option<&QueryExpr>,
) -> EngineResult<Vec<(Uuid, Row)>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = lookup_table_name(transaction, reconciler, table_name).await?;
    let stream = reconciler.scan_rows(transaction, &table);
    pin_mut!(stream);
    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let (row_id, row) = item?;
        let row = materialize_defaults(schema, row);
        if predicate_matches(schema, table_name, &row, predicate)? {
            rows.push((row_id, row));
        }
    }

    Ok(rows)
}

fn assignments(
    schema: &TableSchema,
    table: &str,
    assignments: &[QueryUpdateAssignment],
) -> EngineResult<Vec<(usize, QueryExprValue)>> {
    if assignments.is_empty() {
        return Err(EngineError::InvalidQuery("UPDATE requires an assignment"));
    }

    assignments
        .iter()
        .map(|assignment| {
            let index = column_index(
                schema,
                table,
                &assignment.column,
                "Unknown assignment column",
            )?;
            match &assignment.value {
                QueryExprValue::Column(column) => {
                    column_index(schema, table, column, "Unknown assignment column")?;
                }
                QueryExprValue::ExcludedColumn(_) => {
                    return Err(EngineError::InvalidQuery(
                        "EXCLUDED is only valid in ON CONFLICT assignments",
                    ));
                }
                QueryExprValue::Value(_) => {}
            }
            if schema.columns[index].primary_key {
                return Err(EngineError::InvalidQuery("Cannot update the primary key"));
            }
            Ok((index, assignment.value.clone()))
        })
        .collect()
}

fn column_index(
    schema: &TableSchema,
    table: &str,
    column: &QueryColumn,
    unknown_column: &'static str,
) -> EngineResult<usize> {
    if !column.table.is_empty() && column.table != table {
        return Err(EngineError::InvalidQuery("Unknown column table"));
    }
    schema
        .columns
        .iter()
        .position(|schema_column| schema_column.name == column.column)
        .ok_or(EngineError::InvalidQuery(unknown_column))
}

async fn select_rows<T, R>(
    transaction: &T,
    reconciler: &R,
    select: QuerySelect,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if !select.from.joins.is_empty() {
        if select.having.is_some() {
            return Err(EngineError::Unsupported("HAVING"));
        }
        return select_join_rows(transaction, reconciler, select).await;
    }
    let mut select = select;
    if let Some(QueryExpr::InSubquery {
        expr,
        subquery,
        negated,
    }) = select.predicate.clone()
    {
        let values = Box::pin(in_subquery_values(transaction, reconciler, &subquery)).await?;
        let in_list = QueryExpr::InList {
            expr,
            list: values
                .into_iter()
                .map(|value| QueryExpr::Value(QueryExprValue::Value(value)))
                .collect(),
            negated,
        };
        select.predicate = Some(in_list);
    }
    let distinct = select.distinct;

    let schema = table_schema(transaction, reconciler, &select.from.table).await?;
    let mut projection = projection(&schema, &select.from.table, &select.projection)?;
    for (position, concat) in select.text_concats.iter().enumerate() {
        let Some(concat) = concat else { continue };
        let index = column_index(
            &schema,
            &select.from.table,
            &concat.column,
            "Unknown projection column",
        )?;
        if schema.columns[index].r#type != value::ValueType::Text {
            return Err(EngineError::InvalidQuery(
                "Text concatenation requires a text column",
            ));
        }
        let (_, column) = projection
            .get_mut(position)
            .ok_or(EngineError::InvalidQuery("Invalid computed projection"))?;
        column.name = concat.alias.clone();
        column.source_table = None;
        column.source_column = None;
    }
    let ordering = order_columns(&schema, &select.from.table, &select.order_by)?;
    let table = lookup_table_name(transaction, reconciler, &select.from.table).await?;
    let stream = reconciler.scan_rows(transaction, &table);
    pin_mut!(stream);
    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let (_, row) = item?;
        let row = materialize_defaults(&schema, row);
        if !predicate_matches(&schema, &select.from.table, &row, select.predicate.as_ref())? {
            continue;
        }
        rows.push(row);
    }
    if distinct {
        let mut unique_rows: Vec<Row> = Vec::new();
        let mut projected = Vec::new();
        for row in rows {
            let key = Row::new(
                projection
                    .iter()
                    .map(|(index, _)| row.values[*index].clone())
                    .collect(),
            );
            if !projected.contains(&key) {
                projected.push(key);
                unique_rows.push(row);
            }
        }
        rows = unique_rows;
    }

    if !select.group_by.is_empty() {
        if select.group_by.len() != 1
            || select.projection.len() != 1
            || select.aggregates.len() != 1
        {
            return Err(EngineError::Unsupported("GROUP BY shape"));
        }
        let group_index = column_index(
            &schema,
            &select.from.table,
            &select.group_by[0],
            "Unknown GROUP BY column",
        )?;
        let count_target = match &select.aggregates[0] {
            QueryAggregate::Count(QueryCountTarget::AllRows) => None,
            QueryAggregate::Count(QueryCountTarget::Single(column)) => Some(
                schema
                    .columns
                    .iter()
                    .position(|candidate| candidate.name == *column)
                    .ok_or(EngineError::InvalidQuery("Unknown aggregate column"))?,
            ),
            _ => return Err(EngineError::Unsupported("GROUP BY aggregate")),
        };
        if select.having.is_some() && count_target.is_some() {
            return Err(EngineError::Unsupported("HAVING requires COUNT(*)"));
        }
        let mut groups: Vec<(Value, usize, usize)> = Vec::new();
        for row in rows {
            let key = row.values[group_index].clone();
            if let Some((_, count, non_null_count)) =
                groups.iter_mut().find(|(value, _, _)| *value == key)
            {
                *count += 1;
                if count_target.is_none_or(|index| !matches!(row.values[index], Value::Null)) {
                    *non_null_count += 1;
                }
            } else {
                let non_null_count = usize::from(
                    count_target.is_none_or(|index| !matches!(row.values[index], Value::Null)),
                );
                groups.push((key, 1, non_null_count));
            }
        }
        groups.retain(|(_, count, _)| {
            select
                .having
                .as_ref()
                .is_none_or(|having| match having.operator {
                    QueryHavingCountOperator::GreaterThan => {
                        i64::try_from(*count).is_ok_and(|count| count > having.value)
                    }
                    QueryHavingCountOperator::GreaterThanOrEquals => {
                        i64::try_from(*count).is_ok_and(|count| count >= having.value)
                    }
                })
        });
        let ordering = order_columns(&schema, &select.from.table, &select.order_by)?;
        if ordering.iter().any(|(index, _)| *index != group_index) {
            return Err(EngineError::Unsupported("GROUP BY ORDER BY column"));
        }
        if let Some((_, direction)) = ordering.first() {
            groups.sort_by(|left, right| {
                let ordering = left.0.cmp(&right.0);
                match direction {
                    QuerySortDirection::Asc => ordering,
                    QuerySortDirection::Desc => ordering.reverse(),
                }
            });
        }
        let start = select.offset.unwrap_or(0).min(groups.len());
        let end = select
            .limit
            .and_then(|limit| start.checked_add(limit))
            .unwrap_or(groups.len())
            .min(groups.len());
        let group_column = projection
            .first()
            .cloned()
            .ok_or(EngineError::InvalidQuery("Missing group projection"))?;
        let count_name = match count_target {
            Some(_) => "COUNT(column)",
            None => "COUNT(*)",
        };
        let result_rows = groups
            .into_iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .map(|(key, count, non_null_count)| {
                let count = if count_target.is_some() {
                    non_null_count
                } else {
                    count
                };
                let count = i64::try_from(count)
                    .map_err(|_| EngineError::InvalidQuery("Aggregate count overflow"))?;
                Ok(Row::new(vec![key, Value::Integer(count)]))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        let mut result_columns = vec![group_column.1];
        result_columns.push(QueryResultColumn {
            name: count_name.into(),
            source_table: Some(select.from.table),
            source_column: None,
        });
        return Ok(QueryResult::new_with_columns(result_rows, result_columns));
    }

    if select.having.is_some() {
        return Err(EngineError::Unsupported("HAVING requires GROUP BY"));
    }
    if !select.aggregates.is_empty() {
        if select.aggregates.len() != 1
            || !ordering.is_empty()
            || select.limit.is_some()
            || select.offset.is_some()
        {
            return Err(EngineError::Unsupported("combined aggregate SELECT"));
        }
        let (name, count) = match &select.aggregates[0] {
            QueryAggregate::Count(QueryCountTarget::AllRows) => ("COUNT(*)", rows.len()),
            QueryAggregate::Count(QueryCountTarget::Single(column)) => {
                let index = schema
                    .columns
                    .iter()
                    .position(|candidate| candidate.name == *column)
                    .ok_or(EngineError::InvalidQuery("Unknown aggregate column"))?;
                (
                    "COUNT(column)",
                    rows.iter()
                        .filter(|row| !matches!(row.values[index], Value::Null))
                        .count(),
                )
            }
            _ => return Err(EngineError::Unsupported("aggregate function")),
        };
        let count = i64::try_from(count)
            .map_err(|_| EngineError::InvalidQuery("Aggregate count overflow"))?;
        return Ok(QueryResult::new_with_columns(
            vec![Row::new(vec![Value::Integer(count)])],
            vec![QueryResultColumn {
                name: name.into(),
                source_table: Some(select.from.table),
                source_column: None,
            }],
        ));
    }

    if !ordering.is_empty() {
        rows.sort_by(|left, right| {
            for (index, direction) in &ordering {
                let order = left.values[*index].cmp(&right.values[*index]);
                let order = match direction {
                    QuerySortDirection::Asc => order,
                    QuerySortDirection::Desc => order.reverse(),
                };
                if !order.is_eq() {
                    return order;
                }
            }
            core::cmp::Ordering::Equal
        });
    }

    let start = select.offset.unwrap_or(0).min(rows.len());
    let end = select
        .limit
        .and_then(|limit| start.checked_add(limit))
        .unwrap_or(rows.len())
        .min(rows.len());
    let rows = rows
        .into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .map(|row| {
            Row::new(
                projection
                    .iter()
                    .enumerate()
                    .map(|(position, (index, _))| {
                        let value = row.values[*index].clone();
                        match select.text_concats.get(position).and_then(Option::as_ref) {
                            Some(concat) => match value {
                                Value::Text(mut text) => {
                                    text.push_str(&concat.literal);
                                    Value::Text(text)
                                }
                                Value::Null => Value::Null,
                                _ => unreachable!("text concatenation source was validated"),
                            },
                            None => value,
                        }
                    })
                    .collect(),
            )
        })
        .collect();

    Ok(QueryResult::new_with_columns(
        rows,
        projection.into_iter().map(|(_, column)| column).collect(),
    ))
}

async fn select_join_rows<T, R>(
    transaction: &T,
    reconciler: &R,
    select: QuerySelect,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if select.from.joins.len() != 1 || !select.aggregates.is_empty() || select.distinct {
        return Err(EngineError::Unsupported("complex SELECT JOIN"));
    }
    let join = &select.from.joins[0];
    if !matches!(join.kind, QueryJoinKind::Inner | QueryJoinKind::Left) {
        return Err(EngineError::Unsupported("JOIN type"));
    }
    if select.predicate.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
    {
        return Err(EngineError::Unsupported("complex SELECT JOIN"));
    }
    let left_schema = table_schema(transaction, reconciler, &select.from.table).await?;
    let right_schema = table_schema(transaction, reconciler, &join.table).await?;
    let left_table = lookup_table_name(transaction, reconciler, &select.from.table).await?;
    let right_table = lookup_table_name(transaction, reconciler, &join.table).await?;
    let left_stream = reconciler.scan_rows(transaction, &left_table);
    pin_mut!(left_stream);
    let mut left_rows = Vec::new();
    while let Some(item) = left_stream.next().await {
        left_rows.push(materialize_defaults(&left_schema, item?.1));
    }
    let right_stream = reconciler.scan_rows(transaction, &right_table);
    pin_mut!(right_stream);
    let mut right_rows = Vec::new();
    while let Some(item) = right_stream.next().await {
        right_rows.push(materialize_defaults(&right_schema, item?.1));
    }
    let mut columns = Vec::new();
    for requested in &select.projection {
        let sources = if requested.column == "*" {
            vec![
                (&select.from.table, &left_schema),
                (&join.table, &right_schema),
            ]
        } else if requested.table == select.from.table {
            vec![(&select.from.table, &left_schema)]
        } else if requested.table == join.table {
            vec![(&join.table, &right_schema)]
        } else {
            return Err(EngineError::InvalidQuery("Unknown column table"));
        };
        for (table, schema) in sources {
            for column in &schema.columns {
                if requested.column == "*" || requested.column == column.name {
                    columns.push(((*table).clone(), column.name.clone()));
                }
            }
        }
    }
    let mut result = Vec::new();
    for left in left_rows {
        let mut matched = false;
        for right in &right_rows {
            let combined = Row::new(left.values.iter().chain(&right.values).cloned().collect());
            if !join_matches(
                &left_schema,
                &right_schema,
                &select.from.table,
                &join.table,
                &combined,
                &join.on,
            )? {
                continue;
            }
            matched = true;
            result.push(Row::new(
                columns
                    .iter()
                    .map(|(table, column)| {
                        let schema = if table == &select.from.table {
                            &left_schema
                        } else {
                            &right_schema
                        };
                        let offset = if table == &select.from.table {
                            0
                        } else {
                            left_schema.columns.len()
                        };
                        combined.values[offset
                            + schema
                                .columns
                                .iter()
                                .position(|c| c.name == *column)
                                .expect("projected column exists")]
                        .clone()
                    })
                    .collect(),
            ));
        }
        if !matched && matches!(join.kind, QueryJoinKind::Left) {
            result.push(Row::new(
                columns
                    .iter()
                    .map(|(table, column)| {
                        if table == &select.from.table {
                            left.values[left_schema
                                .columns
                                .iter()
                                .position(|c| c.name == *column)
                                .expect("projected column exists")]
                            .clone()
                        } else {
                            Value::Null
                        }
                    })
                    .collect(),
            ));
        }
    }
    let result_columns = columns
        .into_iter()
        .map(|(table, name)| QueryResultColumn {
            name: name.clone(),
            source_table: Some(table),
            source_column: Some(name),
        })
        .collect();
    Ok(QueryResult::new_with_columns(result, result_columns))
}

fn join_matches(
    left: &TableSchema,
    right: &TableSchema,
    left_name: &str,
    right_name: &str,
    row: &Row,
    expr: &QueryExpr,
) -> EngineResult<bool> {
    let value = |column: &QueryColumn| -> EngineResult<Value> {
        let (schema, offset) = if column.table == left_name {
            (left, 0)
        } else if column.table == right_name {
            (right, left.columns.len())
        } else {
            return Err(EngineError::InvalidQuery("Unknown column table"));
        };
        let index = schema
            .columns
            .iter()
            .position(|candidate| candidate.name == column.column)
            .ok_or(EngineError::InvalidQuery("Unknown predicate column"))?;
        Ok(row.values[offset + index].clone())
    };
    match expr {
        QueryExpr::Equals(a, b) => {
            let eval = |expr: &QueryExpr| -> EngineResult<Value> {
                match expr {
                    QueryExpr::Value(QueryExprValue::Column(c)) => value(c),
                    QueryExpr::Value(QueryExprValue::Value(v)) => Ok(v.clone()),
                    _ => Err(EngineError::Unsupported("JOIN condition")),
                }
            };
            let (a, b) = (eval(a)?, eval(b)?);
            Ok(!matches!(a, Value::Null) && !matches!(b, Value::Null) && a == b)
        }
        _ => Err(EngineError::Unsupported("JOIN condition")),
    }
}

fn order_columns(
    schema: &TableSchema,
    table: &str,
    order_by: &[QueryOrderBy],
) -> EngineResult<Vec<(usize, QuerySortDirection)>> {
    order_by
        .iter()
        .map(|order| {
            let index = column_index(schema, table, &order.by, "Unknown ORDER BY column")?;
            Ok((index, order.direction.clone()))
        })
        .collect()
}

async fn in_subquery_values<T, R>(
    transaction: &T,
    reconciler: &R,
    select: &QuerySelect,
) -> EngineResult<Vec<Value>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if !select.from.joins.is_empty()
        || select.projection.len() != 1
        || !select.aggregates.is_empty()
        || !select.group_by.is_empty()
        || select.having.is_some()
    {
        return Err(EngineError::Unsupported("IN subquery shape"));
    }
    let result = Box::pin(select_rows(transaction, reconciler, select.clone())).await?;
    result
        .rows
        .into_iter()
        .map(|row| {
            row.values
                .into_iter()
                .next()
                .ok_or(EngineError::InvalidQuery("IN subquery returned no column"))
        })
        .collect()
}

fn predicate_matches(
    schema: &TableSchema,
    table: &str,
    row: &Row,
    predicate: Option<&QueryExpr>,
) -> EngineResult<bool> {
    match predicate {
        Some(predicate) => Ok(matches!(
            evaluate_expr(schema, table, row, predicate)?,
            Value::Bool(true)
        )),
        None => Ok(true),
    }
}

fn evaluate_expr(
    schema: &TableSchema,
    table: &str,
    row: &Row,
    expr: &QueryExpr,
) -> EngineResult<Value> {
    match expr {
        QueryExpr::Value(QueryExprValue::Value(value)) => Ok(value.clone()),
        QueryExpr::Value(QueryExprValue::Column(column)) => Ok(row.values
            [column_index(schema, table, column, "Unknown predicate column")?]
        .clone()),
        QueryExpr::Value(QueryExprValue::ExcludedColumn(_)) => Err(EngineError::InvalidQuery(
            "EXCLUDED is only valid in ON CONFLICT assignments",
        )),
        QueryExpr::Not(expr) => match evaluate_expr(schema, table, row, expr)? {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            Value::Null => Ok(Value::Null),
            _ => Err(EngineError::InvalidQuery("NOT requires a boolean value")),
        },
        QueryExpr::Like { expr, pattern } => {
            let value = evaluate_expr(schema, table, row, expr)?;
            let pattern = evaluate_expr(schema, table, row, pattern)?;
            match (value, pattern) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Text(value), Value::Text(pattern)) => {
                    Ok(Value::Bool(like_matches(&value, &pattern)))
                }
                _ => Err(EngineError::InvalidQuery("LIKE requires text values")),
            }
        }
        QueryExpr::IsNull(expr) => Ok(Value::Bool(matches!(
            evaluate_expr(schema, table, row, expr)?,
            Value::Null
        ))),
        QueryExpr::IsNotNull(expr) => Ok(Value::Bool(!matches!(
            evaluate_expr(schema, table, row, expr)?,
            Value::Null
        ))),
        QueryExpr::Equals(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left == right)
        }
        QueryExpr::NotEquals(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left != right)
        }
        QueryExpr::LessThan(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left < right)
        }
        QueryExpr::LessThanOrEquals(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left <= right)
        }
        QueryExpr::GreaterThan(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left > right)
        }
        QueryExpr::GreaterThanOrEquals(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left >= right)
        }
        QueryExpr::InList {
            expr,
            list,
            negated,
        } => {
            let value = evaluate_expr(schema, table, row, expr)?;
            if matches!(value, Value::Null) {
                return Ok(Value::Null);
            }
            let mut contains_null = false;
            for item in list {
                let candidate = evaluate_expr(schema, table, row, item)?;
                if matches!(candidate, Value::Null) {
                    contains_null = true;
                } else if value == candidate {
                    return Ok(Value::Bool(!negated));
                }
            }
            if contains_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Bool(*negated))
            }
        }
        QueryExpr::And(left, right) => Ok(Value::Bool(
            matches!(evaluate_expr(schema, table, row, left)?, Value::Bool(true))
                && matches!(evaluate_expr(schema, table, row, right)?, Value::Bool(true)),
        )),
        QueryExpr::Or(left, right) => Ok(Value::Bool(
            matches!(evaluate_expr(schema, table, row, left)?, Value::Bool(true))
                || matches!(evaluate_expr(schema, table, row, right)?, Value::Bool(true)),
        )),
        _ => Err(EngineError::Unsupported("SELECT predicate")),
    }
}

fn like_matches(value: &str, pattern: &str) -> bool {
    let value: Vec<char> = value.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    let mut previous = vec![false; pattern.len() + 1];
    previous[0] = true;
    for (index, token) in pattern.iter().enumerate() {
        previous[index + 1] = *token == '%' && previous[index];
    }
    for character in value {
        let mut current = vec![false; pattern.len() + 1];
        for (index, token) in pattern.iter().enumerate() {
            current[index + 1] = if *token == '%' {
                current[index] || previous[index + 1]
            } else {
                (*token == '_' || *token == character) && previous[index]
            };
        }
        previous = current;
    }
    previous[pattern.len()]
}

fn compare_expr(
    schema: &TableSchema,
    table: &str,
    row: &Row,
    left: &QueryExpr,
    right: &QueryExpr,
    compare: impl FnOnce(&Value, &Value) -> bool,
) -> EngineResult<Value> {
    let left = evaluate_expr(schema, table, row, left)?;
    let right = evaluate_expr(schema, table, row, right)?;
    Ok(Value::Bool(
        !matches!(left, Value::Null) && !matches!(right, Value::Null) && compare(&left, &right),
    ))
}

pub(crate) async fn table_schema<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    reconciler: &R,
    name: &str,
) -> EngineResult<TableSchema> {
    schema_table_schema(transaction, reconciler, name).await
}

pub(crate) fn materialize_defaults(schema: &TableSchema, mut row: Row) -> Row {
    row.values.extend(
        schema.columns[row.values.len()..]
            .iter()
            .map(|column| column.default.clone()),
    );
    row
}

fn materialize_insert(
    schema: &TableSchema,
    row: Row,
    timestamp_provider: TimestampProvider,
) -> EngineResult<Row> {
    if row.values.len() > schema.columns.len() {
        return Err(EngineError::InvalidQuery(
            "INSERT row has the wrong column count",
        ));
    }
    let mut values = row.values;
    let primary_key = schema
        .columns
        .iter()
        .position(|column| column.primary_key)
        .ok_or(EngineError::InvalidQuery(
            "Table requires exactly one UUID primary key",
        ))?;
    if values.len() < schema.columns.len() && primary_key == 0 {
        let timestamp = timestamp_provider();
        values.insert(0, Value::Uuid(Uuid::new_v7(timestamp)));
    }
    let mut row = Row::new(values);
    if row.values.len() < schema.columns.len() {
        row = materialize_defaults(schema, row);
    }
    if row.values.len() != schema.columns.len() {
        return Err(EngineError::InvalidQuery(
            "INSERT row has the wrong column count",
        ));
    }
    Ok(row)
}

pub(crate) fn row_id(schema: &TableSchema, row: &Row) -> EngineResult<Uuid> {
    let primary_keys: Vec<_> = schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key.then_some(index))
        .collect();
    let [index] = primary_keys.as_slice() else {
        return Err(EngineError::InvalidQuery(
            "Table requires exactly one UUID primary key",
        ));
    };
    let id = row
        .values
        .get(*index)
        .and_then(Value::as_uuid)
        .copied()
        .ok_or(EngineError::InvalidQuery("Primary key must be a UUID"))?;
    crate::schema::validate_row_id(&id)?;
    Ok(id)
}

fn key_row_id(key: &Row) -> EngineResult<Uuid> {
    match key.values.as_slice() {
        [Value::Uuid(id)] => Ok(*id),
        _ => Err(EngineError::InvalidQuery("Primary key must be a UUID")),
    }
}

fn projection(
    schema: &TableSchema,
    table: &str,
    projection: &[QueryColumn],
) -> EngineResult<Vec<(usize, QueryResultColumn)>> {
    let mut result = Vec::new();
    for requested in projection {
        if requested.column == "*" {
            result.extend(schema.columns.iter().enumerate().map(|(index, column)| {
                (
                    index,
                    QueryResultColumn {
                        name: column.name.clone(),
                        source_table: Some(String::from(table)),
                        source_column: Some(column.name.clone()),
                    },
                )
            }));
            continue;
        }
        if !requested.table.is_empty() && requested.table != table {
            return Err(EngineError::InvalidQuery("Unknown projection table"));
        }
        let (index, column) = schema
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == requested.column)
            .ok_or(EngineError::InvalidQuery("Unknown projection column"))?;
        result.push((
            index,
            QueryResultColumn {
                name: column.name.clone(),
                source_table: Some(String::from(table)),
                source_column: Some(column.name.clone()),
            },
        ));
    }
    Ok(result)
}
