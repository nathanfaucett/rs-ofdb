use ofdb_value::{JsonValue, Value};
use serde_json::Value as Json;
use std::collections::BTreeMap;
use wasm_bindgen::JsError;

pub(crate) fn parse_json(text: &str) -> Result<Json, JsError> {
    serde_json::from_str(text).map_err(|e| JsError::new(&e.to_string()))
}

pub(crate) fn write_json(value: &Json) -> Result<String, JsError> {
    serde_json::to_string(value).map_err(|e| JsError::new(&e.to_string()))
}

pub(crate) fn value_to_json(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Integer(i) => Json::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Text(s) => Json::String(s.clone()),
        Value::Json(j) => json_value_to_json(j),
        Value::Blob(b) => Json::String(base64::encode(b)),
        Value::Uuid(u) => Json::String(u.to_string()),
        Value::Type(_) => Json::Null,
    }
}

fn json_value_to_json(j: &JsonValue) -> Json {
    match j {
        JsonValue::Null => Json::Null,
        JsonValue::Bool(b) => Json::Bool(*b),
        JsonValue::Number(n) => json_number_to_json(n),
        JsonValue::String(s) => Json::String(s.clone()),
        JsonValue::Array(arr) => Json::Array(arr.iter().map(json_value_to_json).collect()),
        JsonValue::Object(obj) => Json::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_value_to_json(v)))
                .collect(),
        ),
    }
}

fn json_number_to_json(n: &ofdb_value::JsonNumber) -> Json {
    match n {
        ofdb_value::JsonNumber::I64(i) => Json::Number((*i).into()),
        ofdb_value::JsonNumber::U64(u) => Json::Number((*u).into()),
        ofdb_value::JsonNumber::F64(f) => serde_json::Number::from_f64(*f)
            .map(Json::Number)
            .unwrap_or(Json::Null),
    }
}

pub(crate) fn value_from_json(value: &Json) -> Value {
    match value {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        Json::String(s) => {
            if let Ok(bytes) = base64::decode(s) {
                Value::Blob(bytes)
            } else {
                Value::Text(s.clone())
            }
        }
        Json::Array(a) => {
            let json_vals: Vec<JsonValue> = a.iter().map(|v| json_from_value(v)).collect();
            Value::Json(JsonValue::Array(json_vals))
        }
        Json::Object(o) => {
            let mut json_map: BTreeMap<String, JsonValue> = BTreeMap::new();
            for (k, v) in o {
                json_map.insert(k.clone(), json_from_value(v));
            }
            Value::Json(JsonValue::Object(json_map))
        }
    }
}

fn json_from_value(v: &Json) -> JsonValue {
    match v {
        Json::Null => JsonValue::Null,
        Json::Bool(b) => JsonValue::Bool(*b),
        Json::Number(n) => JsonValue::Number(n.clone().into()),
        Json::String(s) => JsonValue::String(s.clone()),
        Json::Array(arr) => JsonValue::Array(arr.iter().map(json_from_value).collect()),
        Json::Object(obj) => {
            let mut map: BTreeMap<String, JsonValue> = BTreeMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_from_value(v));
            }
            JsonValue::Object(map)
        }
    }
}
