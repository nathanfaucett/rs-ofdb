use std::collections::BTreeMap;

use proto::{
    JsonArray, JsonNumber as ProtoJsonNumber, JsonObject, JsonObjectEntry,
    JsonValue as ProtoJsonValue, Value as ProtoValue, ValueType as ProtoValueType, json_number,
    json_value, value as proto_value,
};
use value::{JsonNumber as DomainJsonNumber, JsonValue as DomainJsonValue, Value as DomainValue};

use crate::QueryServiceError;

pub fn value_to_proto(value: DomainValue) -> ProtoValue {
    let kind = match value {
        DomainValue::Null => proto_value::Kind::Null(()),
        DomainValue::Type(value_type) => {
            proto_value::Kind::Type(value_type_to_proto(value_type) as i32)
        }
        DomainValue::Uuid(uuid) => proto_value::Kind::Uuid(uuid.as_bytes().to_vec()),
        DomainValue::Bool(value) => proto_value::Kind::BoolValue(value),
        DomainValue::Integer(value) => proto_value::Kind::Integer(value),
        DomainValue::Float(value) => proto_value::Kind::Float(value),
        DomainValue::Text(value) => proto_value::Kind::Text(value),
        DomainValue::Json(value) => proto_value::Kind::Json(json_to_proto(value)),
        DomainValue::Blob(value) => proto_value::Kind::Blob(value),
    };
    ProtoValue { kind: Some(kind) }
}

pub fn value_from_proto(value: ProtoValue) -> Result<DomainValue, QueryServiceError> {
    let kind = value
        .kind
        .ok_or_else(|| QueryServiceError::Invalid("missing Value kind".into()))?;
    match kind {
        proto_value::Kind::Null(_) => Ok(DomainValue::Null),
        proto_value::Kind::Type(value_type) => {
            let value_type = ProtoValueType::try_from(value_type)
                .map_err(|_| QueryServiceError::Invalid("unknown ValueType".into()))?;
            let value_type = value_type_from_proto(value_type)
                .ok_or_else(|| QueryServiceError::Invalid("unspecified ValueType".into()))?;
            Ok(DomainValue::Type(value_type))
        }
        proto_value::Kind::Uuid(bytes) => uuid::Uuid::from_slice(&bytes)
            .map(DomainValue::Uuid)
            .map_err(|_| QueryServiceError::Invalid("UUID must contain exactly 16 bytes".into())),
        proto_value::Kind::BoolValue(value) => Ok(DomainValue::Bool(value)),
        proto_value::Kind::Integer(value) => Ok(DomainValue::Integer(value)),
        proto_value::Kind::Float(value) => Ok(DomainValue::Float(value)),
        proto_value::Kind::Text(value) => Ok(DomainValue::Text(value)),
        proto_value::Kind::Json(value) => json_from_proto(value).map(DomainValue::Json),
        proto_value::Kind::Blob(value) => Ok(DomainValue::Blob(value)),
    }
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

fn json_number_to_proto(number: DomainJsonNumber) -> ProtoJsonNumber {
    let kind = match number {
        DomainJsonNumber::I64(value) => json_number::Kind::I64(value),
        DomainJsonNumber::U64(value) => json_number::Kind::U64(value),
        DomainJsonNumber::F64(value) => json_number::Kind::F64(value),
    };
    ProtoJsonNumber { kind: Some(kind) }
}

fn json_number_from_proto(number: ProtoJsonNumber) -> Result<DomainJsonNumber, QueryServiceError> {
    match number
        .kind
        .ok_or_else(|| QueryServiceError::Invalid("missing JsonNumber kind".into()))?
    {
        json_number::Kind::I64(value) => Ok(DomainJsonNumber::I64(value)),
        json_number::Kind::U64(value) => Ok(DomainJsonNumber::U64(value)),
        json_number::Kind::F64(value) => Ok(DomainJsonNumber::F64(value)),
    }
}

fn json_to_proto(value: DomainJsonValue) -> ProtoJsonValue {
    let kind = match value {
        DomainJsonValue::Null => json_value::Kind::Null(()),
        DomainJsonValue::Bool(value) => json_value::Kind::BoolValue(value),
        DomainJsonValue::Number(value) => json_value::Kind::Number(json_number_to_proto(value)),
        DomainJsonValue::String(value) => json_value::Kind::String(value),
        DomainJsonValue::Array(values) => json_value::Kind::Array(JsonArray {
            values: values.into_iter().map(json_to_proto).collect(),
        }),
        DomainJsonValue::Object(entries) => json_value::Kind::Object(JsonObject {
            entries: entries
                .into_iter()
                .map(|(key, value)| JsonObjectEntry {
                    key,
                    value: Some(json_to_proto(value)),
                })
                .collect(),
        }),
    };
    ProtoJsonValue { kind: Some(kind) }
}

fn json_from_proto(value: ProtoJsonValue) -> Result<DomainJsonValue, QueryServiceError> {
    match value
        .kind
        .ok_or_else(|| QueryServiceError::Invalid("missing JsonValue kind".into()))?
    {
        json_value::Kind::Null(_) => Ok(DomainJsonValue::Null),
        json_value::Kind::BoolValue(value) => Ok(DomainJsonValue::Bool(value)),
        json_value::Kind::Number(value) => {
            json_number_from_proto(value).map(DomainJsonValue::Number)
        }
        json_value::Kind::String(value) => Ok(DomainJsonValue::String(value)),
        json_value::Kind::Array(JsonArray { values }) => values
            .into_iter()
            .map(json_from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map(DomainJsonValue::Array),
        json_value::Kind::Object(JsonObject { entries }) => {
            let mut object = BTreeMap::new();
            for entry in entries {
                let value = entry.value.ok_or_else(|| {
                    QueryServiceError::Invalid("missing JSON object value".into())
                })?;
                if object.insert(entry.key, json_from_proto(value)?).is_some() {
                    return Err(QueryServiceError::Invalid(
                        "duplicate JSON object key".into(),
                    ));
                }
            }
            Ok(DomainJsonValue::Object(object))
        }
    }
}
