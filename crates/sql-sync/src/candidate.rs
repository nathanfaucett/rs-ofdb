use alloc::{collections::BTreeSet, string::String, vec::Vec};

use engine::{EngineError, EngineResult, KernelTransaction, Uuid};
use futures::{StreamExt, pin_mut};
use value::{Row, Value};

use crate::{SyncKey, SyncRowCodec};

/// The transaction being observed, not the authority of an original mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncValidationDirection {
    Incoming,
    Outgoing,
}

/// Read-only selected-table evidence from one owner transaction.
/// Incoming candidates have all merged before this view is built, but are not
/// committed yet. Outgoing candidates are actual source rows at export time.
/// This is not a session-wide snapshot or an authenticated mutation ledger.
#[derive(Debug)]
pub struct SyncCandidateView {
    pub direction: SyncValidationDirection,
    pub candidates: Vec<SyncKey>,
    pub tables: Vec<SyncTableView>,
}

#[derive(Debug)]
pub struct SyncTableView {
    pub name: String,
    /// Positional names for each row and named concurrent values below.
    pub columns: Vec<String>,
    pub rows: Vec<SyncRowView>,
}

#[derive(Debug)]
pub struct SyncRowView {
    pub id: Uuid,
    pub row: Row,
    pub deleted: bool,
    pub conflicts: Vec<(String, Vec<Value>)>,
}

pub(crate) type CandidateValidator<'a> =
    dyn Fn(&SyncCandidateView) -> Result<(), String> + Send + Sync + 'a;

pub(crate) async fn read_candidate_view<T, R>(
    codec: &R,
    transaction: &T,
    candidates: Vec<SyncKey>,
    direction: SyncValidationDirection,
    tables: Option<Vec<(String, Vec<String>)>>,
) -> EngineResult<Option<SyncCandidateView>>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    let Some(tables) = tables else {
        return Ok(None);
    };
    let candidates = candidates
        .into_iter()
        .filter(|key| {
            let SyncKey::Row { table, .. } = key;
            tables.iter().any(|(name, _)| name == table)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut evidence = Vec::new();
    for (name, columns) in tables {
        let mut rows = Vec::new();
        {
            let states = codec.scan_row_states(transaction, &name);
            pin_mut!(states);
            while let Some(state) = states.next().await {
                let (id, row, deleted) = state?;
                let conflicts = codec
                    .conflict_values(transaction, &name, &id)
                    .await?
                    .into_iter()
                    .map(|(index, values)| {
                        Ok((
                            columns
                                .get(index)
                                .ok_or(EngineError::InvalidQuery(
                                    "Conflict column outside fixed schema",
                                ))?
                                .clone(),
                            values,
                        ))
                    })
                    .collect::<EngineResult<Vec<_>>>()?;
                rows.push(SyncRowView {
                    id,
                    row,
                    deleted,
                    conflicts,
                });
            }
        }
        evidence.push(SyncTableView {
            name,
            columns,
            rows,
        });
    }
    Ok(Some(SyncCandidateView {
        direction,
        candidates,
        tables: evidence,
    }))
}
