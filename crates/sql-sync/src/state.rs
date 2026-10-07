use alloc::{string::String, vec::Vec};

use engine::Uuid;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SyncChangeId(pub Vec<u8>);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum SyncKey {
    Row { table: String, row: Uuid },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct StateDigest(pub [u8; 32]);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncStateUnit {
    pub key: SyncKey,
    pub state: Vec<u8>,
    pub metadata: Vec<u8>,
    pub digest: StateDigest,
}

impl SyncStateUnit {
    pub fn new(key: SyncKey, state: Vec<u8>, metadata: Vec<u8>) -> Self {
        let digest = Self::digest_parts(&state, &metadata);
        Self {
            key,
            state,
            metadata,
            digest,
        }
    }

    pub fn verify_digest(&self) -> bool {
        Self::digest_parts(&self.state, &self.metadata) == self.digest
    }

    pub fn digest_parts(state: &[u8], metadata: &[u8]) -> StateDigest {
        let mut hasher = Sha256::new();
        hasher.update((state.len() as u64).to_le_bytes());
        hasher.update(state);
        hasher.update((metadata.len() as u64).to_le_bytes());
        hasher.update(metadata);
        StateDigest(hasher.finalize().into())
    }
}

#[cfg(test)]
mod tests {
    use alloc::{string::String, vec};

    use super::{SyncKey, SyncStateUnit};
    use engine::Uuid;

    #[test]
    fn digest_verification_does_not_require_mutating_state() {
        let mut unit = SyncStateUnit::new(
            SyncKey::Row {
                table: String::from("items"),
                row: Uuid::from_bytes([1; 16]),
            },
            vec![7; 1024],
            vec![9; 256],
        );

        assert!(unit.verify_digest());
        unit.state[0] ^= 1;
        assert!(!unit.verify_digest());
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncManifest {
    pub entries: Vec<(SyncKey, StateDigest)>,
}

impl SyncManifest {
    pub fn new(mut entries: Vec<(SyncKey, StateDigest)>) -> Self {
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        entries.dedup_by(|left, right| left.0 == right.0);
        Self { entries }
    }

    pub fn contains(&self, key: &SyncKey, digest: StateDigest) -> bool {
        self.entries
            .binary_search_by(|(candidate, _)| candidate.cmp(key))
            .is_ok_and(|index| self.entries[index].1 == digest)
    }
}
