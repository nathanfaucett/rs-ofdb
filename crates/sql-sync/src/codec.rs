use alloc::{string::String, vec::Vec};

use engine::{EngineResult, KernelTransaction, RowCodec, Uuid};
use value::{Row, Value, ValueType};

use crate::{StateDigest, SyncChangeId, SyncStateUnit};

pub trait SyncRowCodec<T>: RowCodec<T>
where
    T: KernelTransaction,
{
    /// Inspects every historical operation, including overwritten/deleted properties.
    /// Codecs without complete-history validation must reject scoped sessions.
    fn inspect_history(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        payload: &[u8],
        metadata: &[u8],
        columns: &[(String, ValueType, Value, bool)],
    ) -> impl Future<Output = EngineResult<(Row, Vec<Uuid>)>> + Send {
        let _ = (transaction, table, row, payload, metadata, columns);
        async {
            Err(engine::EngineError::Unsupported(
                "Codec cannot inspect full scoped history",
            ))
        }
    }

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
    /// Returns unique change IDs in dependency order, not ID sort order.
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
