use proto::{
    ColumnSchema as ProtoColumnSchema, IndexSchema as ProtoIndexSchema,
    TableSchema as ProtoTableSchema, ValueType as ProtoValueType,
};
use schema::{ColumnSchema, IndexSchema, TableSchema};

use crate::{QueryServiceError, value_from_proto, value_to_proto};

pub fn column_to_proto(column: ColumnSchema) -> ProtoColumnSchema {
    ProtoColumnSchema {
        name: column.name,
        r#type: value_type_to_proto(column.r#type) as i32,
        default: Some(value_to_proto(column.default)),
        primary_key: column.primary_key,
    }
}

pub fn column_from_proto(column: ProtoColumnSchema) -> Result<ColumnSchema, QueryServiceError> {
    let value_type = ProtoValueType::try_from(column.r#type)
        .map_err(|_| QueryServiceError::Invalid("unknown column ValueType".into()))?;
    let r#type = value_type_from_proto(value_type)
        .ok_or_else(|| QueryServiceError::Invalid("unspecified column ValueType".into()))?;
    let default = column
        .default
        .ok_or_else(|| QueryServiceError::Invalid("missing column default".into()))?;
    Ok(ColumnSchema {
        name: column.name,
        r#type,
        default: value_from_proto(default)?,
        primary_key: column.primary_key,
    })
}

pub fn index_to_proto(index: IndexSchema) -> ProtoIndexSchema {
    ProtoIndexSchema {
        name: index.name,
        table_name: index.table_name,
        column_indices: index.column_indices,
        unique: index.unique,
    }
}

pub fn index_from_proto(index: ProtoIndexSchema) -> IndexSchema {
    IndexSchema {
        name: index.name,
        table_name: index.table_name,
        column_indices: index.column_indices,
        unique: index.unique,
    }
}

pub fn table_to_proto(table: TableSchema) -> ProtoTableSchema {
    ProtoTableSchema {
        name: table.name,
        columns: table.columns.into_iter().map(column_to_proto).collect(),
    }
}

pub fn table_from_proto(table: ProtoTableSchema) -> Result<TableSchema, QueryServiceError> {
    Ok(TableSchema {
        name: table.name,
        columns: table
            .columns
            .into_iter()
            .map(column_from_proto)
            .collect::<Result<_, _>>()?,
    })
}

fn value_type_to_proto(value_type: value::ValueType) -> ProtoValueType {
    match value_type {
        value::ValueType::Null => ProtoValueType::Null,
        value::ValueType::Type => ProtoValueType::Type,
        value::ValueType::Uuid => ProtoValueType::Uuid,
        value::ValueType::Bool => ProtoValueType::Bool,
        value::ValueType::Integer => ProtoValueType::Integer,
        value::ValueType::Float => ProtoValueType::Float,
        value::ValueType::Text => ProtoValueType::Text,
        value::ValueType::Json => ProtoValueType::Json,
        value::ValueType::Blob => ProtoValueType::Blob,
    }
}

fn value_type_from_proto(value_type: ProtoValueType) -> Option<value::ValueType> {
    match value_type {
        ProtoValueType::Null => Some(value::ValueType::Null),
        ProtoValueType::Type => Some(value::ValueType::Type),
        ProtoValueType::Uuid => Some(value::ValueType::Uuid),
        ProtoValueType::Bool => Some(value::ValueType::Bool),
        ProtoValueType::Integer => Some(value::ValueType::Integer),
        ProtoValueType::Float => Some(value::ValueType::Float),
        ProtoValueType::Text => Some(value::ValueType::Text),
        ProtoValueType::Json => Some(value::ValueType::Json),
        ProtoValueType::Blob => Some(value::ValueType::Blob),
        ProtoValueType::Unspecified => None,
    }
}
