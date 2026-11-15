#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::{
    collections::BTreeMap,
    format,
    string::{String, ToString},
    vec::Vec,
};
#[cfg(feature = "std")]
use std::collections::BTreeMap;

use core::{fmt, hash::Hash};

use crate::json_number::JsonNumber;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "automerge", derive(autosurgeon::Hydrate))]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

#[cfg(feature = "automerge")]
impl autosurgeon::Reconcile for JsonValue {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        use autosurgeon::reconcile::MapReconciler;
        let mut map = reconciler.map()?;
        match self {
            Self::Null => {
                map.put("type", "null")?;
            }
            Self::Bool(value) => {
                map.put("type", "bool")?;
                map.put("value", *value)?;
            }
            Self::Number(value) => {
                map.put("type", "number")?;
                map.put("value", value.clone())?;
            }
            Self::String(value) => {
                map.put("type", "string")?;
                map.put("value", value.as_str())?;
            }
            Self::Array(values) => {
                map.put("type", "array")?;
                map.put("value", values.clone())?;
            }
            Self::Object(entries) => {
                map.put("type", "object")?;
                map.put("value", entries.clone())?;
            }
        }
        Ok(())
    }
}

impl From<()> for JsonValue {
    fn from((): ()) -> Self {
        Self::Null
    }
}

impl From<bool> for JsonValue {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl<T> From<T> for JsonValue
where
    T: Into<JsonNumber>,
{
    fn from(number: T) -> Self {
        Self::Number(number.into())
    }
}

impl From<String> for JsonValue {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for JsonValue {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl<K, V> FromIterator<(K, V)> for JsonValue
where
    K: Into<String>,
    V: Into<Self>,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self::Object(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl<T> FromIterator<T> for JsonValue
where
    T: Into<Self>,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::Array(iter.into_iter().map(Into::into).collect())
    }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Value> for JsonValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(b) => Self::Bool(b),
            serde_json::Value::Number(n) => Self::Number(n.into()),
            serde_json::Value::String(s) => Self::String(s),
            serde_json::Value::Array(arr) => Self::Array(arr.into_iter().map(Self::from).collect()),
            serde_json::Value::Object(obj) => {
                Self::Object(obj.into_iter().map(|(k, v)| (k, Self::from(v))).collect())
            }
        }
    }
}

#[cfg(feature = "serde_json")]
impl TryFrom<JsonValue> for serde_json::Value {
    type Error = &'static str;

    fn try_from(value: JsonValue) -> Result<Self, Self::Error> {
        Ok(match value {
            JsonValue::Null => Self::Null,
            JsonValue::Bool(value) => Self::Bool(value),
            JsonValue::String(value) => Self::String(value),
            JsonValue::Number(JsonNumber::I64(value)) => Self::Number(value.into()),
            JsonValue::Number(JsonNumber::U64(value)) => Self::Number(value.into()),
            JsonValue::Number(JsonNumber::F64(value)) => {
                Self::Number(serde_json::Number::from_f64(value).ok_or("non-finite JSON number")?)
            }
            JsonValue::Array(values) => Self::Array(
                values
                    .into_iter()
                    .map(Self::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            JsonValue::Object(values) => Self::Object(
                values
                    .into_iter()
                    .map(|(key, value)| Self::try_from(value).map(|value| (key, value)))
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
}

#[cfg(all(test, feature = "serde_json"))]
mod conversion_tests {
    use super::JsonValue;
    use crate::json_number::JsonNumber;

    #[test]
    fn serde_json_roundtrip_preserves_nested_values_and_rejects_nonfinite_numbers() {
        let value =
            serde_json::json!({"escaped\"": ["line\n", 18446744073709551615_u64, true, null]});
        assert_eq!(
            serde_json::Value::try_from(JsonValue::from(value.clone())).expect("JSON value"),
            value
        );
        assert!(
            serde_json::Value::try_from(JsonValue::Number(JsonNumber::F64(f64::INFINITY))).is_err()
        );
    }
}

impl fmt::Display for JsonValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Number(n) => write!(f, "{n}"),
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Array(arr) => {
                let items = arr
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{items}]")
            }
            Self::Object(obj) => {
                let items = obj
                    .iter()
                    .map(|(k, v)| format!("\"{k}\": {v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{{{items}}}")
            }
        }
    }
}
