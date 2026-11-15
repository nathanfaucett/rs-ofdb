#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::{boxed::Box, format};
#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use core::{
    cmp::Ordering,
    f64,
    hash::{Hash, Hasher},
};

use uuid::Uuid;

use crate::JsonValue;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "automerge",
    derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum Value {
    #[default]
    Null,
    Type(ValueType),
    Uuid(Uuid),
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(String),
    Json(JsonValue),
    Blob(Vec<u8>),
}

impl From<()> for Value {
    fn from((): ()) -> Self {
        Self::Null
    }
}

impl From<ValueType> for Value {
    fn from(value_type: ValueType) -> Self {
        Self::Type(value_type)
    }
}

impl From<Uuid> for Value {
    fn from(uuid: Uuid) -> Self {
        Self::Uuid(uuid)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Float(f)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Blob(bytes)
    }
}

impl From<JsonValue> for Value {
    fn from(json: JsonValue) -> Self {
        Self::Json(json)
    }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Value> for Value {
    fn from(json: serde_json::Value) -> Self {
        Self::Json(JsonValue::from(json))
    }
}

impl Value {
    #[must_use]
    pub const fn r#type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::Type(_) => ValueType::Type,
            Self::Uuid(_) => ValueType::Uuid,
            Self::Bool(_) => ValueType::Bool,
            Self::Integer(_) => ValueType::Integer,
            Self::Float(_) => ValueType::Float,
            Self::Text(_) => ValueType::Text,
            Self::Blob(_) => ValueType::Blob,
            Self::Json(_) => ValueType::Json,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Type(a), Self::Type(b)) => a == b,
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Integer(a), Self::Integer(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Blob(a), Self::Blob(b)) => a == b,
            (Self::Json(a), Self::Json(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.r#type().hash(state);

        match self {
            Self::Null => {}
            Self::Type(value_type) => value_type.hash(state),
            Self::Uuid(bytes) => bytes.hash(state),
            Self::Bool(value) => value.hash(state),
            Self::Integer(value) => value.hash(state),
            Self::Float(value) => value.to_bits().hash(state),
            Self::Text(text) => text.hash(state),
            Self::Json(value) => value.hash(state),
            Self::Blob(bytes) => bytes.hash(state),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Type(a), Self::Type(b)) => a.rank().cmp(&b.rank()),
            (Self::Uuid(a), Self::Uuid(b)) => a.cmp(b),
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Json(a), Self::Json(b)) => a.to_string().cmp(&b.to_string()),
            (Self::Blob(a), Self::Blob(b)) => a.cmp(b),
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => a.to_bits().cmp(&b.to_bits()),
            // Different variants — deterministic ordering by variant rank
            _ => self.r#type().rank().cmp(&other.r#type().rank()),
        }
    }
}

impl Value {
    #[must_use]
    pub const fn as_null(&self) -> Option<()> {
        match self {
            Self::Null => Some(()),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_type(&self) -> Option<&ValueType> {
        match self {
            Self::Type(value_type) => Some(value_type),
            _ => None,
        }
    }

    #[must_use]
    pub const fn to_type(&self) -> Option<ValueType> {
        self.as_type().copied()
    }

    #[must_use]
    pub const fn as_uuid(&self) -> Option<&Uuid> {
        match self {
            Self::Uuid(uuid) => Some(uuid),
            _ => None,
        }
    }

    #[must_use]
    pub const fn to_uuid(&self) -> Option<Uuid> {
        match self {
            Self::Uuid(uuid) => Some(*uuid),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    #[must_use]
    pub const fn to_bool(&self) -> Option<bool> {
        self.as_bool()
    }

    #[must_use]
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            Self::Float(f) => (f.is_finite() && *f >= (i64::MIN as f64) && *f <= (i64::MAX as f64))
                .then_some(*f as i64),
            _ => None,
        }
    }

    #[must_use]
    pub fn to_integer(&self) -> Option<i64> {
        self.as_integer()
    }

    #[must_use]
    pub const fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    #[must_use]
    pub const fn to_float(&self) -> Option<f64> {
        self.as_float()
    }

    #[must_use]
    pub const fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn to_text(&self) -> Option<String> {
        self.as_text().map(str::to_string)
    }

    #[must_use]
    pub const fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    #[must_use]
    pub fn to_blob(&self) -> Option<Vec<u8>> {
        self.as_blob().map(<[u8]>::to_vec)
    }

    #[must_use]
    pub const fn as_json(&self) -> Option<&JsonValue> {
        match self {
            Self::Json(j) => Some(j),
            _ => None,
        }
    }

    #[must_use]
    pub fn to_json(&self) -> Option<JsonValue> {
        self.as_json().cloned()
    }
}

#[derive(
    Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(
    feature = "automerge",
    derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    #[must_use]
    pub const fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}

impl<T, const N: usize> From<[T; N]> for Row
where
    T: Into<Value>,
{
    fn from(values: [T; N]) -> Self {
        Self {
            values: values.into_iter().map(Into::into).collect(),
        }
    }
}

impl<T> From<Vec<T>> for Row
where
    T: Into<Value>,
{
    fn from(values: Vec<T>) -> Self {
        Self {
            values: values.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "automerge",
    derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
#[repr(u8)]
pub enum ValueType {
    Null,
    Type,
    Uuid,
    Bool,
    Integer,
    Float,
    Text,
    Json,
    Blob,
}

impl ValueType {
    #[must_use]
    pub const fn rank(&self) -> u8 {
        *self as u8
    }
}

impl Ord for ValueType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank().cmp(&other.rank())
    }
}

impl PartialOrd for ValueType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "redb")]
impl redb::Value for Value {
    type SelfType<'a>
        = Self
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        postcard::from_bytes(data).expect("Failed to deserialize Value from bytes")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        postcard::to_allocvec(value).expect("Failed to serialize Value to bytes")
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new(core::any::type_name::<Self>())
    }
}

#[cfg(feature = "redb")]
impl redb::Key for Value {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        use redb::Value;
        let a_value = Self::from_bytes(a);
        let b_value = Self::from_bytes(b);
        a_value.cmp(&b_value)
    }
}

#[cfg(feature = "redb")]
impl redb::Value for Row {
    type SelfType<'a>
        = Self
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        postcard::from_bytes(data).expect("Failed to deserialize Value from bytes")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        postcard::to_allocvec(value).expect("Failed to serialize Value to bytes")
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new(core::any::type_name::<Self>())
    }
}

#[cfg(feature = "redb")]
impl redb::Key for Row {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        use redb::Value;
        let a_value = Self::from_bytes(a);
        let b_value = Self::from_bytes(b);
        a_value.cmp(&b_value)
    }
}
