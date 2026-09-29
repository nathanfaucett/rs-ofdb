use alloc::vec::Vec;

use core::{fmt, future::Future};

use async_stream::stream;
use futures::{Stream, StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use value::Row;

use crate::{EngineResult, KernelTransaction};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum RowIdentity {
    User([u8; 16]),
    ScopedUser { table: [u8; 16], row: [u8; 16] },
    Catalog(Vec<u8>),
}

impl RowIdentity {
    pub fn user(id: Uuid) -> Self {
        Self::User(*id.as_bytes())
    }

    pub fn scoped_user(table: Uuid, row: Uuid) -> Self {
        Self::ScopedUser {
            table: *table.as_bytes(),
            row: *row.as_bytes(),
        }
    }

    pub fn catalog(key: Vec<u8>) -> Self {
        Self::Catalog(key)
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes.split_first()? {
            (0, payload) if payload.len() == 16 => {
                let mut id = [0; 16];
                id.copy_from_slice(payload);
                Some(Self::User(id))
            }
            (1, payload) => Some(Self::Catalog(payload.into())),
            (2, payload) if payload.len() == 32 => {
                let mut table = [0; 16];
                let mut row = [0; 16];
                table.copy_from_slice(&payload[..16]);
                row.copy_from_slice(&payload[16..]);
                Some(Self::ScopedUser { table, row })
            }
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::User(id) => {
                let mut bytes = Vec::with_capacity(17);
                bytes.push(0);
                bytes.extend_from_slice(id);
                bytes
            }
            Self::ScopedUser { table, row } => {
                let mut bytes = Vec::with_capacity(33);
                bytes.push(2);
                bytes.extend_from_slice(table);
                bytes.extend_from_slice(row);
                bytes
            }
            Self::Catalog(key) => {
                let mut bytes = Vec::with_capacity(key.len() + 1);
                bytes.push(1);
                bytes.extend_from_slice(key);
                bytes
            }
        }
    }
}

impl fmt::Display for RowIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::User(id) => write!(formatter, "{}", Uuid::from_bytes(*id)),
            Self::ScopedUser { row, .. } => write!(formatter, "{}", Uuid::from_bytes(*row)),
            Self::Catalog(key) => write!(formatter, "catalog key ({} bytes)", key.len()),
        }
    }
}

pub trait RowCodec<T>: Send + Sync
where
    T: KernelTransaction,
{
    fn ensure_table(
        &self,
        transaction: &mut T,
        table: &str,
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn drop_table(
        &self,
        transaction: &mut T,
        table: &str,
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn get_row(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn scan_rows(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(RowIdentity, Row)>> + Send;
    fn scan_row_states(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(RowIdentity, Row, bool)>> + Send {
        stream! {
            let rows = self.scan_rows(transaction, table);
            pin_mut!(rows);
            while let Some(row) = rows.next().await {
                let (id, value) = row?;
                yield Ok((id, value, false));
            }
        }
    }
    fn put_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        value: Row,
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn remove_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &RowIdentity,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn encode_row(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
        value: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + Send;
    fn conflicted_columns(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> impl Future<Output = EngineResult<Vec<usize>>> + Send;
    fn conflict_values(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> impl Future<Output = EngineResult<Vec<(usize, Vec<value::Value>)>>> + Send;
    fn encode_resolution(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
        value: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + Send;
    fn merge_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        value: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn delete_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &RowIdentity,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn row_is_deleted(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> impl Future<Output = EngineResult<bool>> + Send;
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::RowIdentity;

    #[test]
    fn identities_round_trip_without_collisions() {
        let id = Uuid::from_bytes([7; 16]);
        let user = RowIdentity::user(id);
        let catalog = RowIdentity::catalog(id.as_bytes().to_vec());
        let scoped = RowIdentity::scoped_user(Uuid::from_bytes([8; 16]), id);

        assert_eq!(
            RowIdentity::from_bytes(&user.to_bytes()),
            Some(user.clone())
        );
        assert_eq!(
            RowIdentity::from_bytes(&catalog.to_bytes()),
            Some(catalog.clone())
        );
        assert_ne!(user.to_bytes(), catalog.to_bytes());
        assert_ne!(user.to_bytes(), scoped.to_bytes());
        assert_eq!(RowIdentity::from_bytes(&scoped.to_bytes()), Some(scoped));
        assert_eq!(RowIdentity::from_bytes(&[0, 1]), None);
        assert_eq!(RowIdentity::from_bytes(&[2]), None);
    }
}
