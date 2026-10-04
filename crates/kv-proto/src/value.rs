use std::collections::BTreeMap;

use crate::kvdb::{
    JsonArray, JsonNumber as ProtoJsonNumber, JsonObject, JsonObjectEntry,
    JsonValue as ProtoJsonValue, Value as ProtoValue, ValueType as ProtoValueType, json_number,
    json_value, value as proto_value,
};
use value::{JsonNumber as DomainJsonNumber, JsonValue as DomainJsonValue, Value as DomainValue};

use tonic::Status;

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

pub fn value_from_proto(value: ProtoValue) -> Result<DomainValue, Status> {
    let kind = value
        .kind
        .ok_or_else(|| Status::invalid_argument("missing Value kind"))?;
    match kind {
        proto_value::Kind::Null(_) => Ok(DomainValue::Null),
        proto_value::Kind::Type(value_type) => {
            let value_type = ProtoValueType::try_from(value_type)
                .map_err(|_| Status::invalid_argument("unknown ValueType"))?;
            let value_type = value_type_from_proto(value_type)
                .ok_or_else(|| Status::invalid_argument("unspecified ValueType"))?;
            Ok(DomainValue::Type(value_type))
        }
        proto_value::Kind::Uuid(bytes) => uuid::Uuid::from_slice(&bytes)
            .map(DomainValue::Uuid)
            .map_err(|_| Status::invalid_argument("UUID must contain exactly 16 bytes")),
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

fn json_number_from_proto(number: ProtoJsonNumber) -> Result<DomainJsonNumber, Status> {
    match number
        .kind
        .ok_or_else(|| Status::invalid_argument("missing JsonNumber kind"))?
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

fn json_from_proto(value: ProtoJsonValue) -> Result<DomainJsonValue, Status> {
    match value
        .kind
        .ok_or_else(|| Status::invalid_argument("missing JsonValue kind"))?
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
                let value = entry
                    .value
                    .ok_or_else(|| Status::invalid_argument("missing JSON object value"))?;
                if object.insert(entry.key, json_from_proto(value)?).is_some() {
                    return Err(Status::invalid_argument("duplicate JSON object key"));
                }
            }
            Ok(DomainJsonValue::Object(object))
        }
    }
}

pub fn required_value(value: Option<ProtoValue>) -> Result<DomainValue, Status> {
    value_from_proto(value.ok_or_else(|| Status::invalid_argument("missing KV value"))?)
}
pub fn optional_value(value: Option<ProtoValue>) -> Result<Option<DomainValue>, Status> {
    value.map(value_from_proto).transpose()
}
pub fn scan_entries(
    entries: Vec<crate::kvdb::ScanEntry>,
) -> Result<Vec<(String, DomainValue)>, Status> {
    entries
        .into_iter()
        .map(|entry| Ok((entry.key, required_value(entry.value)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use value::{JsonNumber, JsonValue, Value, ValueType};

    use super::{optional_value, required_value, value_from_proto, value_to_proto};
    use crate::kvdb::{self, json_value, value as proto_value};

    #[test]
    fn all_values_preserve_wire_types_and_float_bits() {
        let values = [
            Value::Null,
            Value::Type(ValueType::Json),
            Value::Uuid(uuid::Uuid::nil()),
            Value::Bool(true),
            Value::Integer(i64::MIN),
            Value::Float(-0.0),
            Value::Float(f64::from_bits(0x7ff8000000000042)),
            Value::Text("text".into()),
            Value::Blob(vec![0, 255]),
            Value::Json(JsonValue::Object(
                [(
                    "numbers".into(),
                    JsonValue::Array(vec![
                        JsonValue::Number(JsonNumber::I64(i64::MIN)),
                        JsonValue::Number(JsonNumber::U64(u64::MAX)),
                        JsonValue::Number(JsonNumber::F64(f64::from_bits(0x7ff8000000000042))),
                    ]),
                )]
                .into(),
            )),
        ];
        for value in values {
            let bytes = value_to_proto(value).encode_to_vec();
            let wire = kvdb::Value::decode(bytes.as_slice()).expect("decode protobuf value");
            let decoded = value_from_proto(wire).expect("decode domain value");
            assert_eq!(value_to_proto(decoded).encode_to_vec(), bytes);
        }
        assert_eq!(optional_value(None).expect("missing get value"), None);
    }

    #[test]
    fn rejects_missing_invalid_and_old_wire_values() {
        assert!(required_value(None).is_err());
        for kind in [
            None,
            Some(proto_value::Kind::Type(0)),
            Some(proto_value::Kind::Type(100)),
            Some(proto_value::Kind::Uuid(vec![0])),
            Some(proto_value::Kind::Json(kvdb::JsonValue { kind: None })),
        ] {
            assert!(value_from_proto(kvdb::Value { kind }).is_err());
        }
        let duplicate = kvdb::JsonObjectEntry {
            key: "same".into(),
            value: Some(kvdb::JsonValue {
                kind: Some(json_value::Kind::Null(())),
            }),
        };
        let value = kvdb::Value {
            kind: Some(proto_value::Kind::Json(kvdb::JsonValue {
                kind: Some(json_value::Kind::Object(kvdb::JsonObject {
                    entries: vec![duplicate.clone(), duplicate],
                })),
            })),
        };
        assert!(value_from_proto(value).is_err());
        // The old bytes field is reserved, not interpreted as a typed value.
        let old = [0x12, 0x03, 1, 2, 3];
        let request = kvdb::SetRequest::decode(old.as_slice()).expect("decode unknown old field");
        assert!(required_value(request.value).is_err());
    }
}
