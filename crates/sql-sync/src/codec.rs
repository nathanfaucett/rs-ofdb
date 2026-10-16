use alloc::{string::String, vec::Vec};

use engine::{EngineResult, KernelTransaction, RowCodec, Uuid};
use value::{Row, Value, ValueType};

use crate::{StateDigest, SyncChangeId, SyncStateUnit, SyncTableView};

pub type InspectedHistory = (Row, Vec<Uuid>, Vec<(String, Value)>);

pub trait SyncRowCodec<T>: RowCodec<T>
where
    T: KernelTransaction,
{
    /// Observe every row, including tombstones and all losing conflict values,
    /// using the actual decoder column order/defaults from this immutable transaction.
    /// Conflict names refer to that same layout. Reject if it cannot be established,
    /// even for an empty table; stored positional fallback is not authoritative.
    /// Codecs without complete observations must reject instead of omitting evidence.
    fn observe_table_state(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Future<Output = EngineResult<SyncTableView>> + Send {
        let _ = (transaction, table);
        async {
            Err(engine::EngineError::Unsupported(
                "Codec cannot observe complete table state",
            ))
        }
    }

    /// Inspects every historical operation, including overwritten/deleted properties.
    /// Returns the merged row, schema references, and every historical column value
    /// (including layout defaults, overwritten/deleted and losing-conflict values).
    /// Codecs without complete-history validation must reject scoped sessions.
    fn inspect_history(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        payload: &[u8],
        metadata: &[u8],
        columns: &[(String, ValueType, Value, bool)],
    ) -> impl Future<Output = EngineResult<InspectedHistory>> + Send {
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
    /// Returns all known change IDs in dependency order, including snapshot-contained
    /// history, not just separately exportable deltas. Snapshot-only IDs may require
    /// full-state fallback from `Self::export_change`.
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
