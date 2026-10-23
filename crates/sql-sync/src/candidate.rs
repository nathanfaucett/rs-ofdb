use alloc::{collections::BTreeSet, string::String, vec::Vec};

use engine::{EngineError, EngineResult, KernelTransaction};
use value::Row;

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

pub use engine::{RowStateView as SyncRowView, TableStateView as SyncTableView};

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
        let mut observed = codec.observe_table_state(transaction, &name).await?;
        let actual = observed.columns;
        let expected_names = columns.iter().collect::<BTreeSet<_>>();
        let actual_names = actual.iter().collect::<BTreeSet<_>>();
        if observed.name != name
            || columns.is_empty()
            || expected_names.len() != columns.len()
            || actual_names.len() != actual.len()
            || expected_names != actual_names
        {
            return Err(EngineError::InvalidQuery(
                "Candidate column layout does not match fixed schema",
            ));
        }
        let positions = columns
            .iter()
            .map(|column| {
                actual
                    .iter()
                    .position(|name| name == column)
                    .expect("validated column sets define a complete bijection")
            })
            .collect::<Vec<_>>();
        for row in &mut observed.rows {
            if row.row.values.len() != actual.len() {
                return Err(EngineError::InvalidQuery(
                    "Candidate row does not match transaction column layout",
                ));
            }
            if row
                .conflicts
                .iter()
                .any(|(column, _)| !actual.contains(column))
            {
                return Err(EngineError::InvalidQuery(
                    "Conflict column outside fixed schema",
                ));
            }
            row.row = Row::new(
                positions
                    .iter()
                    .map(|index| row.row.values[*index].clone())
                    .collect(),
            );
        }
        evidence.push(SyncTableView {
            name,
            columns,
            rows: observed.rows,
        });
    }
    Ok(Some(SyncCandidateView {
        direction,
        candidates,
        tables: evidence,
    }))
}
