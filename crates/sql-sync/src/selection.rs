use alloc::{string::String, vec::Vec};

use serde::{Deserialize, Serialize};

use crate::SyncKey;

/// Discovery work, never authorization. Use All for new peers, reconnects and gaps.
/// Each endpoint supplies its own pending work; the session compares their union.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncSelection {
    All,
    Selected {
        tables: Vec<String>,
        rows: Vec<SyncKey>,
    },
}

impl SyncSelection {
    pub(crate) fn union(self, other: Self) -> Self {
        match (self, other) {
            (
                Self::Selected {
                    mut tables,
                    mut rows,
                },
                Self::Selected {
                    tables: right_tables,
                    rows: right_rows,
                },
            ) => {
                tables.extend(right_tables);
                rows.extend(right_rows);
                tables.sort_unstable();
                tables.dedup();
                rows.sort_unstable();
                rows.dedup();
                rows.retain(|key| {
                    let SyncKey::Row { table, .. } = key;
                    !tables.contains(table)
                });
                Self::Selected { tables, rows }
            }
            _ => Self::All,
        }
    }
}
