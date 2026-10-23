/// A native Automerge change, independent of physical snapshot/delta records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeChange {
    pub hash: [u8; 32],
    pub deps: Vec<[u8; 32]>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeSummary {
    pub hash: [u8; 32],
    pub deps: Vec<[u8; 32]>,
}

/// History and generation read from one transaction snapshot, in dependency order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvHistory {
    pub id: Vec<u8>,
    pub heads: Vec<[u8; 32]>,
    pub changes: Vec<ChangeSummary>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeApply {
    Applied,
    MissingDependencies,
}
