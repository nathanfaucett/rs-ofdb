use alloc::{string::String, vec::Vec};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    EngineResult, KernelTransaction, RowCodec, RowIdentity,
    index::update_row,
    schema::{active_user_row, catalog_table_for_storage, columns},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub id: Uuid,
    pub key: ChangeKey,
    pub value: Option<Vec<u8>>,
}

impl Change {
    pub fn row(id: Uuid, table: String, row: RowIdentity, value: Option<Vec<u8>>) -> Self {
        Self {
            id,
            key: ChangeKey::Row { table, row },
            value,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChangeKey {
    Row { table: String, row: RowIdentity },
}

pub(crate) async fn apply_local_change<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    changes: &mut Vec<Change>,
    change: Change,
) -> EngineResult<()> {
    let _ = materialize_change(transaction, codec, &change, true).await?;
    changes.push(change);
    Ok(())
}

pub(crate) async fn materialize_change<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    change: &Change,
    enforce_unique: bool,
) -> EngineResult<bool> {
    let ChangeKey::Row { table, row } = &change.key;
    let catalog = catalog_table_for_storage(table).is_some();
    if !catalog && columns(transaction, codec, table).await.is_err() {
        return Ok(true);
    }
    match &change.value {
        Some(value) => {
            let old = codec.get_row(transaction, table, row).await?;
            let Some(value) = codec
                .merge_row(transaction, table, row.clone(), value)
                .await?
            else {
                return Ok(true);
            };
            if !catalog && active_user_row(transaction, codec, table, row).await? {
                update_row(
                    transaction,
                    codec,
                    table,
                    old.as_ref(),
                    Some(&value),
                    enforce_unique,
                )
                .await?;
            }
        }
        None => {
            let old = codec.remove_row(transaction, table, row).await?;
            if !catalog && active_user_row(transaction, codec, table, row).await? {
                update_row(
                    transaction,
                    codec,
                    table,
                    old.as_ref(),
                    None,
                    enforce_unique,
                )
                .await?;
            }
        }
    }
    Ok(false)
}
