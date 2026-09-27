use automerge::AutoCommit;
use btree::{BTreeError, BTreeResult};
use btree_automerge::{DocumentChangeKey, DocumentType, hash_heads};
use serde::{Deserialize, Serialize};

use crate::decode_document_id;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvSnapshot {
    pub key: DocumentChangeKey,
    pub payload: Vec<u8>,
}

impl KvSnapshot {
    pub(crate) fn from_document(id: Vec<u8>, mut document: AutoCommit) -> Self {
        let key = DocumentChangeKey::new_snapshot(id, hash_heads(document.get_heads()));
        let payload = document.save();
        Self { key, payload }
    }

    pub(crate) fn validate(&self) -> BTreeResult<AutoCommit> {
        decode_document_id(&self.key.id)?;
        if self.key.r#type != DocumentType::Snapshot {
            return Err(BTreeError::InvalidDocument);
        }
        let mut document = AutoCommit::load(&self.payload).map_err(BTreeError::custom)?;
        if document.save() != self.payload
            || self.key.change_hash != hash_heads(document.get_heads())
        {
            return Err(BTreeError::InvalidDocument);
        }
        crate::value_document::read_document(&document)?;
        Ok(document)
    }
}
