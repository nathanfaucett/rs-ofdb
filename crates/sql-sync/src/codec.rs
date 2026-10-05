use alloc::vec::Vec;

use engine::{EngineResult, KernelTransaction, RowCodec, RowIdentity};
use value::Row;

use crate::SyncChangeId;

pub trait SyncRowCodec<T>: RowCodec<T>
where
    T: KernelTransaction,
{
    fn row_ids(
        &self,
        transaction: &T,
        table: &str,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Vec<RowIdentity>>> + Send;
    fn export_state(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn merge_state(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        state: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn export_metadata(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + Send;
    fn merge_metadata(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        metadata: &[u8],
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn change_inventory(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Vec<SyncChangeId>>> + Send;
    fn export_change(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
        id: &SyncChangeId,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn apply_change(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        id: &SyncChangeId,
        payload: &[u8],
    ) -> impl Future<Output = EngineResult<()>> + Send;
}
