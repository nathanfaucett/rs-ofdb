use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Map, Value as Json};
use value::{JsonNumber, JsonValue, Value, ValueType};

pub fn parse_value(value: &Json) -> Result<Value, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "each KV value must be a tagged object".to_owned())?;
    let tag = object
        .get("type")
        .and_then(Json::as_str)
        .ok_or_else(|| "KV value tag must contain string `type`".to_owned())?;
    if object.keys().any(|key| key != "type" && key != "value") {
        return Err("unknown value fields".into());
    }
    let payload = object.get("value");
    match tag {
        "null" if payload.is_none() => Ok(Value::Null),
        "type" => match payload
            .and_then(Json::as_str)
            .ok_or_else(|| "type value requires string payload".to_owned())?
        {
            "Null" => Ok(Value::Type(ValueType::Null)),
            "Type" => Ok(Value::Type(ValueType::Type)),
            "Uuid" => Ok(Value::Type(ValueType::Uuid)),
            "Bool" => Ok(Value::Type(ValueType::Bool)),
            "Integer" => Ok(Value::Type(ValueType::Integer)),
            "Float" => Ok(Value::Type(ValueType::Float)),
            "Text" => Ok(Value::Type(ValueType::Text)),
            "Json" => Ok(Value::Type(ValueType::Json)),
            "Blob" => Ok(Value::Type(ValueType::Blob)),
            other => Err(format!("unknown KV type tag: {other}")),
        },
        "uuid" => Ok(Value::Uuid(
            payload
                .and_then(Json::as_str)
                .ok_or_else(|| "uuid requires string payload".to_owned())?
                .parse()
                .map_err(|error| format!("invalid UUID: {error}"))?,
        )),
        "bool" => {
            Ok(Value::Bool(payload.and_then(Json::as_bool).ok_or_else(
                || "bool requires boolean payload".to_owned(),
            )?))
        }
        "integer" => Ok(Value::Integer(
            payload
                .and_then(Json::as_str)
                .ok_or_else(|| "integer requires decimal string payload".to_owned())?
                .parse()
                .map_err(|error| format!("invalid integer: {error}"))?,
        )),
        "float" => {
            let bits = payload
                .and_then(Json::as_str)
                .ok_or_else(|| "float requires 16-digit hexadecimal payload".to_owned())?;
            if bits.len() != 16 {
                return Err("float payload must have 16 hexadecimal digits".to_owned());
            }
            Ok(Value::Float(f64::from_bits(
                u64::from_str_radix(bits, 16)
                    .map_err(|error| format!("invalid float bits: {error}"))?,
            )))
        }
        "text" => Ok(Value::Text(
            payload
                .and_then(Json::as_str)
                .ok_or_else(|| "text requires string payload".to_owned())?
                .to_owned(),
        )),
        "blob" => Ok(Value::Blob(
            STANDARD
                .decode(
                    payload
                        .and_then(Json::as_str)
                        .ok_or_else(|| "blob requires base64 string payload".to_owned())?,
                )
                .map_err(|error| format!("invalid base64 blob: {error}"))?,
        )),
        "json" => {
            Ok(Value::Json(parse_json(payload.ok_or_else(|| {
                "json requires value payload".to_owned()
            })?)?))
        }
        other => Err(format!("unknown KV value tag: {other}")),
    }
}

pub fn encode_value(value: Value) -> Json {
    let (tag, payload) = match value {
        Value::Null => ("null", Json::Null),
        Value::Type(value) => ("type", Json::String(format!("{value:?}"))),
        Value::Uuid(value) => ("uuid", Json::String(value.to_string())),
        Value::Bool(value) => ("bool", Json::Bool(value)),
        Value::Integer(value) => ("integer", Json::String(value.to_string())),
        Value::Float(value) => ("float", Json::String(format!("{:016x}", value.to_bits()))),
        Value::Text(value) => ("text", Json::String(value)),
        Value::Blob(value) => ("blob", Json::String(STANDARD.encode(value))),
        Value::Json(value) => ("json", encode_json(value)),
    };
    let mut object = Map::new();
    object.insert("type".to_owned(), Json::String(tag.to_owned()));
    if tag != "null" {
        object.insert("value".to_owned(), payload);
    }
    Json::Object(object)
}

fn encode_json(value: JsonValue) -> Json {
    match value {
        JsonValue::Null => serde_json::json!({"type":"null"}),
        JsonValue::Bool(value) => serde_json::json!({"type":"bool", "value":value}),
        JsonValue::String(value) => serde_json::json!({"type":"string", "value":value}),
        JsonValue::Array(values) => {
            serde_json::json!({"type":"array", "value":values.into_iter().map(encode_json).collect::<Vec<_>>()})
        }
        JsonValue::Object(values) => {
            serde_json::json!({"type":"object", "value":values.into_iter().map(|(key, value)| (key, encode_json(value))).collect::<BTreeMap<_, _>>()})
        }
        JsonValue::Number(JsonNumber::I64(value)) => {
            serde_json::json!({"type":"i64", "value":value.to_string()})
        }
        JsonValue::Number(JsonNumber::U64(value)) => {
            serde_json::json!({"type":"u64", "value":value.to_string()})
        }
        JsonValue::Number(JsonNumber::F64(value)) => {
            serde_json::json!({"type":"f64", "value":format!("{:016x}", value.to_bits())})
        }
    }
}

fn parse_json(value: &Json) -> Result<JsonValue, String> {
    let object = value
        .as_object()
        .ok_or("JSON value must be a tagged object")?;
    if object.keys().any(|key| key != "type" && key != "value") {
        return Err("unknown JSON value fields".into());
    }
    let tag = object
        .get("type")
        .and_then(Json::as_str)
        .ok_or("missing JSON value tag")?;
    let payload = object.get("value");
    let string = || {
        payload
            .and_then(Json::as_str)
            .ok_or_else(|| "number or string requires string payload".to_owned())
    };
    match tag {
        "null" if payload.is_none() => Ok(JsonValue::Null),
        "bool" => Ok(JsonValue::Bool(
            payload
                .and_then(Json::as_bool)
                .ok_or("bool requires boolean payload")?,
        )),
        "string" => Ok(JsonValue::String(string()?.into())),
        "i64" => string()?
            .parse()
            .map(JsonNumber::I64)
            .map(JsonValue::Number)
            .map_err(|error| format!("invalid i64: {error}")),
        "u64" => string()?
            .parse()
            .map(JsonNumber::U64)
            .map(JsonValue::Number)
            .map_err(|error| format!("invalid u64: {error}")),
        "f64" => {
            let bits = string()?;
            if bits.len() != 16 {
                return Err("float requires 16 hexadecimal digits".into());
            }
            let bits =
                u64::from_str_radix(bits, 16).map_err(|error| format!("invalid float: {error}"))?;
            Ok(JsonValue::Number(JsonNumber::F64(f64::from_bits(bits))))
        }
        "array" => payload
            .and_then(Json::as_array)
            .ok_or("array requires array payload")?
            .iter()
            .map(parse_json)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        "object" => payload
            .and_then(Json::as_object)
            .ok_or("object requires object payload")?
            .iter()
            .map(|(key, value)| Ok((key.clone(), parse_json(value)?)))
            .collect::<Result<BTreeMap<_, _>, String>>()
            .map(JsonValue::Object),
        _ => Err(format!("invalid JSON value tag: {tag}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_value, parse_value};
    use value::{JsonNumber, JsonValue, Value, ValueType};

    #[test]
    fn all_types_round_trip_losslessly() {
        let values = [
            Value::Null,
            Value::Type(ValueType::Json),
            Value::Uuid(
                "00000000-0000-0000-0000-000000000000"
                    .parse()
                    .expect("valid UUID"),
            ),
            Value::Bool(true),
            Value::Integer(i64::MIN),
            Value::Float(f64::from_bits(0x7ff8000000000042)),
            Value::Float(-0.0),
            Value::Text("{json}".into()),
            Value::Blob(vec![0, 255]),
            Value::Json(JsonValue::Object(
                [(
                    "$number".into(),
                    JsonValue::Array(vec![
                        JsonValue::Number(JsonNumber::U64(u64::MAX)),
                        JsonValue::Number(JsonNumber::I64(i64::MIN)),
                        JsonValue::Number(JsonNumber::F64(f64::from_bits(0x7ff8000000000001))),
                    ]),
                )]
                .into(),
            )),
        ];
        for value in values {
            let encoded = encode_value(value.clone());
            let bytes = serde_json::to_vec(&encoded).expect("encode tagged JSON");
            let json = serde_json::from_slice(&bytes).expect("read tagged JSON");
            assert_eq!(
                encode_value(parse_value(&json).expect("decode tagged value")),
                encoded
            );
        }
    }

    #[test]
    fn rejects_untagged_or_invalid_values() {
        for json in [
            serde_json::json!("text"),
            serde_json::json!({"type":"integer","value":42}),
            serde_json::json!({"type":"json","value":{"a":1}}),
            serde_json::json!({"type":"null","value":"ignored"}),
            serde_json::json!({"type":"blob","value":"!"}),
        ] {
            assert!(parse_value(&json).is_err());
        }
    }
}
