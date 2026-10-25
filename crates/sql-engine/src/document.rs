use alloc::{string::String, vec::Vec};
use core::future::Future;

use platform::MaybeSend;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use value::{Row, Value, ValueType};

use crate::{EngineResult, KernelTransaction, RowCodec, Uuid};

/// Opaque native history identity, independent of physical storage-record labels.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DocumentChangeId(pub Vec<u8>);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DocumentDigest(pub [u8; 32]);

impl DocumentDigest {
    pub fn from_parts(state: &[u8], metadata: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update((state.len() as u64).to_le_bytes());
        hasher.update(state);
        hasher.update((metadata.len() as u64).to_le_bytes());
        hasher.update(metadata);
        Self(hasher.finalize().into())
    }
}

/// History and metadata observed together in the caller's transaction snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentSummary {
    pub digest: DocumentDigest,
    pub metadata: Vec<u8>,
    /// Actual native changes in dependency order, never physical record labels.
    pub changes: Vec<DocumentChangeId>,
}

#[derive(Debug)]
pub struct TableStateView {
    pub name: String,
    /// Decoder order in codec observations; scope-start order in candidate views.
    /// Rows are normalized by name; conflicts retain their actual column names.
    pub columns: Vec<String>,
    pub rows: Vec<RowStateView>,
}

#[derive(Debug)]
pub struct RowStateView {
    pub id: Uuid,
    pub row: Row,
    pub deleted: bool,
    pub conflicts: Vec<(String, Vec<Value>)>,
}

pub type InspectedHistory = (Row, Vec<Uuid>, Vec<(String, Value)>);

/// Transaction-bound row state, history and validation capabilities; no peer or protocol state.
pub trait RowReconciler<T>: RowCodec<T>
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
    ) -> impl Future<Output = EngineResult<TableStateView>> + MaybeSend {
        let _ = (transaction, table);
        async {
            Err(crate::EngineError::Unsupported(
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
    ) -> impl Future<Output = EngineResult<InspectedHistory>> + MaybeSend {
        let _ = (transaction, table, row, payload, metadata, columns);
        async {
            Err(crate::EngineError::Unsupported(
                "Codec cannot inspect full scoped history",
            ))
        }
    }

    /// Summarizes history and metadata from this transaction, bounded by the returned
    /// metadata and inventory size. Native implementations avoid snapshot serialization.
    fn history_summary(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Option<DocumentSummary>>> + MaybeSend {
        async move {
            let state = self
                .export_state(transaction, table, row, max_bytes)
                .await?;
            let metadata = self.export_metadata(transaction, table, row).await?;
            if state.is_none() && metadata.is_empty() {
                return Ok(None);
            }
            if metadata.len() > max_bytes {
                return Err(crate::EngineError::custom(
                    "History summary exceeds byte budget",
                ));
            }
            let changes = self
                .change_inventory(transaction, table, row, max_bytes - metadata.len())
                .await?;
            let digest = self.manifest_digest(state.as_deref().unwrap_or_default(), &metadata)?;
            Ok(Some(DocumentSummary {
                digest,
                metadata,
                changes,
            }))
        }
    }

    fn row_ids<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl futures::Stream<Item = EngineResult<Uuid>> + MaybeSend + 'a;
    fn export_state(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + MaybeSend;
    fn manifest_digest(&self, state: &[u8], metadata: &[u8]) -> EngineResult<DocumentDigest> {
        Ok(DocumentDigest::from_parts(state, metadata))
    }
    fn merge_state(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        state: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + MaybeSend;
    fn export_metadata(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + MaybeSend;
    fn merge_metadata(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        metadata: &[u8],
    ) -> impl Future<Output = EngineResult<()>> + MaybeSend;
    /// Returns all known change IDs in dependency order, including snapshot-contained
    /// history, not just separately exportable deltas. Snapshot-only IDs may require
    /// full-state fallback from `Self::export_change`.
    fn change_inventory(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Vec<DocumentChangeId>>> + MaybeSend;
    /// Exports one native change by its actual identity, including snapshot-contained history.
    /// Returns `None` when unavailable; the caller may recover with full state.
    fn export_change(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        id: &DocumentChangeId,
        max_bytes: usize,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + MaybeSend;
    /// Validates the advertised identity and every payload dependency before writing.
    /// Native payloads may contain several changes. Known history is an idempotent no-op;
    /// unresolved dependencies return `EngineError::SyncDependencyUnavailable`.
    /// Engine-managed rows must be applied through the engine's validated mutation scope.
    fn apply_change(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        id: &DocumentChangeId,
        payload: &[u8],
    ) -> impl Future<Output = EngineResult<()>> + MaybeSend;
}
