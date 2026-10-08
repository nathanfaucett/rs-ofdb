use serde::{Deserialize, Serialize};

/// Publication/apply direction for a scoped session, independent of turn role.
/// Controls still exchange real manifest and inventory metadata in both directions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncDirection {
    #[default]
    Bidirectional,
    SendOnly,
    ReceiveOnly,
}

impl SyncDirection {
    pub(crate) fn sends_history(self) -> bool {
        self != Self::ReceiveOnly
    }

    pub(crate) fn receives_history(self) -> bool {
        self != Self::SendOnly
    }

    pub(crate) fn complements(self, remote: Self) -> bool {
        matches!(
            (self, remote),
            (Self::Bidirectional, Self::Bidirectional)
                | (Self::SendOnly, Self::ReceiveOnly)
                | (Self::ReceiveOnly, Self::SendOnly)
        )
    }
}
