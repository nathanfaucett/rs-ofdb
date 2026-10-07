use alloc::vec::Vec;

use engine::{EngineResult, KernelTransaction, RowCodec, Uuid};
use value::Row;

use crate::{StateDigest, SyncChangeId, SyncStateUnit};

pub trait SyncRowCodec<T>: RowCodec<T>
where
    T: KernelTransaction,
{
    fn row_ids<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl futures::Stream<Item = EngineResult<Uuid>> + Send + 'a;
    fn export_state(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn manifest_digest(&self, state: &[u8], metadata: &[u8]) -> EngineResult<StateDigest> {
        Ok(SyncStateUnit::digest_parts(state, metadata))
    }
    fn merge_state(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        state: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn export_metadata(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + Send;
    fn merge_metadata(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        metadata: &[u8],
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn change_inventory(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Vec<SyncChangeId>>> + Send;
    fn export_change(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        id: &SyncChangeId,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn apply_change(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        id: &SyncChangeId,
        payload: &[u8],
    ) -> impl Future<Output = EngineResult<()>> + Send;
}
