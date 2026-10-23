#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::{boxed::Box, format, string::ToString};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use value::{Value, ValueType};

pub type ColumnSchemaIndex = u32;

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct ColumnSchema {
    pub name: String,
    pub r#type: ValueType,
    pub default: Value,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct IndexSchema {
    pub name: String,
    pub table_name: String,
    pub column_indices: Vec<ColumnSchemaIndex>,
    pub unique: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn validate_primary_key(&self) -> Result<(), &'static str> {
        let mut primary_keys = self
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .peekable();
        if primary_keys.peek().is_none() {
            return Err("Table requires a primary key");
        }
        if primary_keys.any(|column| column.r#type == ValueType::Null) {
            return Err("Primary key must have a non-null type");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "std"))]
    use alloc::{string::String, vec};
    #[cfg(feature = "std")]
    use std::{string::String, vec};

    use value::{Value, ValueType};

    use super::{ColumnSchema, TableSchema};

    fn column(value_type: ValueType, primary_key: bool) -> ColumnSchema {
        ColumnSchema {
            name: String::new(),
            r#type: value_type,
            default: Value::Null,
            primary_key,
        }
    }

    #[test]
    fn accepts_all_supported_non_null_primary_key_types() {
        for value_type in [
            ValueType::Type,
            ValueType::Uuid,
            ValueType::Bool,
            ValueType::Integer,
            ValueType::Float,
            ValueType::Text,
            ValueType::Json,
            ValueType::Blob,
        ] {
            let schema = TableSchema {
                name: String::new(),
                columns: vec![column(value_type, true)],
            };
            assert_eq!(schema.validate_primary_key(), Ok(()), "{value_type:?}");
        }
    }

    #[test]
    fn accepts_composite_primary_keys() {
        for columns in [
            vec![
                column(ValueType::Integer, true),
                column(ValueType::Text, true),
            ],
            vec![column(ValueType::Uuid, true), column(ValueType::Uuid, true)],
            vec![
                column(ValueType::Text, false),
                column(ValueType::Uuid, true),
                column(ValueType::Integer, true),
                column(ValueType::Blob, true),
            ],
        ] {
            let schema = TableSchema {
                name: String::new(),
                columns,
            };
            assert_eq!(schema.validate_primary_key(), Ok(()));
        }
    }

    #[test]
    fn rejects_null_primary_key_components() {
        for columns in [
            vec![column(ValueType::Null, true)],
            vec![column(ValueType::Null, true), column(ValueType::Uuid, true)],
            vec![column(ValueType::Uuid, true), column(ValueType::Null, true)],
        ] {
            let schema = TableSchema {
                name: String::new(),
                columns,
            };
            assert_eq!(
                schema.validate_primary_key(),
                Err("Primary key must have a non-null type")
            );
        }
    }

    #[test]
    fn rejects_tables_without_primary_keys() {
        for columns in [vec![], vec![column(ValueType::Text, false)]] {
            let schema = TableSchema {
                name: String::new(),
                columns,
            };
            assert_eq!(
                schema.validate_primary_key(),
                Err("Table requires a primary key")
            );
        }
    }

    #[test]
    fn allows_null_types_outside_primary_key() {
        let schema = TableSchema {
            name: String::new(),
            columns: vec![
                column(ValueType::Null, false),
                column(ValueType::Integer, true),
            ],
        };
        assert_eq!(schema.validate_primary_key(), Ok(()));
    }
}
