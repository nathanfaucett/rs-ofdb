#![allow(dead_code)]
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

use automerge::AutoCommit;
use btree::{
    BTree, BTreeError, BTreeRead, BTreeResult, BTreeTransaction, ChangeEvent, ChangeSignal,
    StorageKey,
};
use btree_automerge::{
    AutomergeBTreeTransaction, AutomergeChangeStore, DocumentChangeKey, hash_heads,
};
use futures::{StreamExt, pin_mut};
use uuid::Uuid;

use value::Value;

use crate::{
    ChangeSummary, KvHistory, NativeChange, decode_document_id, encode_document_id,
    value_document::{new_document, read_document, write_live, write_tombstone},
};

pub type TimestampProvider = fn() -> uuid::Timestamp;

/// A generation change also invalidates latest-generation selection for its key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum KvInvalidation {
    All,
    Generation { key: String, generation: Uuid },
}

#[derive(Clone)]
pub struct KvStore<B>
where
    B: BTree<Vec<u8>, Vec<u8>>,
{
    inner: B,
    timestamp_provider: TimestampProvider,
    change_signal: ChangeSignal,
}

impl<B> KvStore<B>
where
    B: BTree<Vec<u8>, Vec<u8>>,
{
    pub fn new(inner: B, timestamp_provider: TimestampProvider) -> Self {
        Self {
            inner,
            timestamp_provider,
            change_signal: ChangeSignal::default(),
        }
    }

    /// Get the change signal for subscribing to committed changes.
    pub fn change_signal(&self) -> &ChangeSignal {
        &self.change_signal
    }

    /// Interpret work from this backend's subscription using its physical table name.
    /// Unknown tables/keys and gaps require full comparison, never skipped work.
    pub fn committed_invalidations(
        &self,
        storage_table: &str,
        work: &btree::ChangeWork,
    ) -> Vec<KvInvalidation> {
        let mut result = BTreeSet::new();
        for invalidation in work.invalidations() {
            let btree::StorageInvalidation::Key { table, key } = invalidation else {
                return vec![KvInvalidation::All];
            };
            if table != storage_table {
                return vec![KvInvalidation::All];
            }
            let decoded =
                DocumentChangeKey::decode_ordered(key).and_then(|key| decode_document_id(key.id()));
            let Ok((key, generation)) = decoded else {
                return vec![KvInvalidation::All];
            };
            result.insert(KvInvalidation::Generation { key, generation });
        }
        result.into_iter().collect()
    }

    pub async fn transaction(&self) -> BTreeResult<KvTransaction<B::Transaction>> {
        let inner = self.inner.transaction().await?;
        Ok(KvTransaction {
            inner: AutomergeBTreeTransaction::new(
                AutomergeChangeStore::new(inner),
                Default::default(),
            ),
            timestamp_provider: self.timestamp_provider,
            change_signal: self.change_signal.clone(),
            modified_keys: BTreeSet::new(),
        })
    }
}

pub struct KvTransaction<T>
where
    T: BTreeTransaction<Vec<u8>, Vec<u8>>,
{
    inner: AutomergeBTreeTransaction<AutomergeChangeStore<T>>,
    timestamp_provider: TimestampProvider,
    change_signal: ChangeSignal,
    modified_keys: BTreeSet<Vec<u8>>,
}

impl<T> KvTransaction<T>
where
    T: BTreeTransaction<Vec<u8>, Vec<u8>>,
{
    pub async fn get(&self, key: &str, _now: i64) -> BTreeResult<Option<Value>> {
        let Some((_, document)) = self.latest(key).await? else {
            return Ok(None);
        };
        let state = read_document(&document)?;
        if state.tombstone {
            return Ok(None);
        }
        Ok(state.value)
    }

    pub async fn scan(&self, prefix: &str, _now: i64) -> BTreeResult<Vec<(String, Value)>> {
        let range = (
            Bound::Included(lower_id(prefix)),
            Bound::Included(upper_id(prefix)),
        );
        let documents = self.inner.range(range);
        pin_mut!(documents);
        let mut results = Vec::new();
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            decode_document_id(&id)?;
            let state = read_document(&document)?;
            if state.tombstone {
                continue;
            }
            let key = String::from_utf8(id[id.len() - 16..].to_vec())
                .map_err(|_| BTreeError::custom("invalid key encoding"))?;
            results.push((key, state.value.unwrap()));
        }
        Ok(results)
    }

    pub async fn scan_prefix(&self, prefix: &str, _now: i64) -> BTreeResult<Vec<(String, Value)>> {
        self.scan(prefix, 0).await
    }

    async fn scan_bounds(
        &self,
        lower: &[u8],
        upper: &[u8],
        _now: i64,
    ) -> BTreeResult<Vec<(String, Value)>> {
        let range = (
            Bound::Included(lower.to_vec()),
            Bound::Included(upper.to_vec()),
        );
        let documents = self.inner.range(range);
        pin_mut!(documents);
        let mut results = Vec::new();
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            decode_document_id(&id)?;
            let state = read_document(&document)?;
            if state.tombstone {
                continue;
            }
            let key = String::from_utf8(id[id.len() - 16..].to_vec())
                .map_err(|_| BTreeError::custom("invalid key encoding"))?;
            results.push((key, state.value.unwrap()));
        }
        Ok(results)
    }

    pub async fn scan_all(&self, _now: i64) -> BTreeResult<Vec<(String, Value)>> {
        let range = (Bound::<Vec<u8>>::Unbounded, Bound::Unbounded);
        let documents = self.inner.range(range);
        pin_mut!(documents);
        let mut results = Vec::new();
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            decode_document_id(&id)?;
            let state = read_document(&document)?;
            if state.tombstone {
                continue;
            }
            let key = String::from_utf8(id[id.len() - 16..].to_vec())
                .map_err(|_| BTreeError::custom("invalid key encoding"))?;
            results.push((key, state.value.unwrap()));
        }
        Ok(results)
    }

    pub async fn set(
        &mut self,
        key: &str,
        value: Value,
        expires_at: Option<i64>,
    ) -> BTreeResult<()> {
        let latest = self.latest(key).await?;
        let id = match latest {
            Some((id, document)) => {
                let state = read_document(&document)?;
                if !state.tombstone {
                    self.inner
                        .update(id.clone(), move |doc| write_live(doc, value, expires_at))
                        .await?;
                    self.modified_keys.insert(id);
                    return Ok(());
                }
                let next = Uuid::new_v7((self.timestamp_provider)());
                validate_generation(next)?;
                let (_, previous) = decode_document_id(&id)?;
                if next <= previous {
                    return Err(BTreeError::custom(
                        "Generated UUIDv7 does not exceed the current generation",
                    ));
                }
                self.remove_key_generations(key).await?;
                next
            }
            None => {
                let next = Uuid::new_v7((self.timestamp_provider)());
                validate_generation(next)?;
                next
            }
        };
        let encoded_id = encode_document_id(key, id);
        self.modified_keys.insert(encoded_id.clone());
        self.inner
            .insert(encoded_id, new_document(value, expires_at)?)
            .await
    }

    pub async fn delete(&mut self, key: &str) -> BTreeResult<()> {
        let Some((id, document)) = self.latest(key).await? else {
            return Ok(());
        };
        if read_document(&document)?.tombstone {
            return Ok(());
        }
        self.modified_keys.insert(id.clone());
        self.inner.update(id, write_tombstone).await?;
        Ok(())
    }

    pub async fn history(&self, key: &str) -> BTreeResult<KvHistory> {
        let documents = self.inner.range((
            Bound::Included(lower_id(key)),
            Bound::Included(upper_id(key)),
        ));
        pin_mut!(documents);
        let mut history = KvHistory {
            id: Vec::new(),
            heads: Vec::new(),
            changes: Vec::new(),
        };
        while let Some(item) = documents.next().await {
            let (id, mut document) = item?;
            decode_document_id(&id)?;
            let (_key, _generation) = decode_document_id(&id)?;
            let _state = read_document(&document)?;
            if history.id.is_empty() {
                history.id = id.clone();
                history.heads = document.get_heads().into_iter().map(|h| h.0).collect();
            }
            let summary = ChangeSummary {
                hash: document.get_heads().first().map(|h| h.0).unwrap_or([0; 32]),
                deps: Vec::new(),
            };
            history.changes.push(summary);
        }
        Ok(history)
    }

    pub async fn histories(&self, prefix: &str) -> BTreeResult<Vec<KvHistory>> {
        let range = (
            Bound::Included(lower_id(prefix)),
            Bound::Included(upper_id(prefix)),
        );
        let documents = self.inner.range(range);
        pin_mut!(documents);
        let mut histories = Vec::new();
        let mut current_key = None;
        let mut current_history = KvHistory::default();
        while let Some(item) = documents.next().await {
            let (id, mut document) = item?;
            decode_document_id(&id)?;
            let (key, _generation) = decode_document_id(&id)?;
            let _state = read_document(&document)?;
            if current_key.as_ref() != Some(&key) {
                if current_key.is_some() {
                    histories.push(current_history);
                }
                current_key = Some(key.clone());
                current_history = KvHistory {
                    id: id.clone(),
                    heads: document.get_heads().into_iter().map(|h| h.0).collect(),
                    changes: Vec::new(),
                };
            }
            let summary = ChangeSummary {
                hash: document.get_heads().first().map(|h| h.0).unwrap_or([0; 32]),
                deps: Vec::new(),
            };
            current_history.changes.push(summary);
        }
        if current_key.is_some() {
            histories.push(current_history);
        }
        Ok(histories)
    }

    pub async fn export_changes(
        &self,
        id: &[u8],
        known: &[[u8; 32]],
    ) -> BTreeResult<Vec<NativeChange>> {
        decode_document_id(id)?;
        let Some(mut document) = self.inner.get(&id.to_vec()).await? else {
            return Ok(Vec::new());
        };
        read_document(&document)?;
        let known = known.iter().collect::<BTreeSet<_>>();
        Ok(document
            .get_changes(&[])
            .into_iter()
            .filter(|change| !known.contains(&change.hash().0))
            .map(|change| NativeChange {
                hash: change.hash().0,
                deps: change.deps().iter().map(|hash| hash.0).collect(),
                payload: change.raw_bytes().to_vec(),
            })
            .collect())
    }

    /// Validate the entire candidate before writing. A gap leaves this transaction untouched.
    /// Other operation errors still require caller rollback of the enclosing batch.
    pub async fn import_changes(&mut self, id: &[u8], changes: &[NativeChange]) -> BTreeResult<()> {
        decode_document_id(id)?;
        let mut document = self
            .inner
            .get(&id.to_vec())
            .await?
            .map(|doc| read_document(&doc).map(|_| doc))
            .transpose()?
            .unwrap_or_else(|| new_document(Value::Null, None).expect("new document"));
        for change in changes {
            let change = automerge::Change::from_bytes(change.payload.clone())
                .map_err(|e| BTreeError::custom(format!("invalid change: {}", e)))?;
            document
                .apply_changes([change])
                .map_err(|e| BTreeError::custom(format!("apply change failed: {}", e)))?;
        }
        self.inner.insert(id.to_vec(), document).await
    }

    pub async fn export_snapshots(&self) -> BTreeResult<Vec<(DocumentChangeKey, Vec<u8>)>> {
        let documents = self.inner.range(..);
        pin_mut!(documents);
        let mut latest = BTreeMap::new();
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            let (key, _) = decode_document_id(&id)?;
            let snapshot = read_document(&document).map(|_| snapshot_parts(id, document));
            latest.insert(key, snapshot);
        }
        latest.into_values().collect()
    }

    pub async fn export_snapshot(
        &self,
        key: &str,
    ) -> BTreeResult<Option<(DocumentChangeKey, Vec<u8>)>> {
        self.latest(key)
            .await?
            .map(|(id, document)| {
                read_document(&document)?;
                Ok(snapshot_parts(id, document))
            })
            .transpose()
    }

    pub async fn import_snapshot(
        &mut self,
        id: &[u8; 16],
        snapshot: AutoCommit,
    ) -> BTreeResult<()> {
        self.inner.insert(id.to_vec(), snapshot).await
    }

    pub async fn commit(self) -> BTreeResult<()> {
        // Emit ChangeEvents for all modified keys before committing
        let mut events = Vec::new();
        for key in &self.modified_keys {
            if let Some(mut document) = self.inner.get(key).await?
                && let Ok(state) = read_document(&document)
            {
                if !state.tombstone {
                    // Get the latest changes for this document
                    let changes = document.get_changes(&[]);
                    if let Some(change) = changes.last() {
                        let storage_key = StorageKey::kv(key.clone());
                        events.push(ChangeEvent {
                            key: storage_key,
                            blob: change.raw_bytes().to_vec(),
                            generation: change.seq(),
                        });
                    }
                } else {
                    // For tombstones, emit a sentinel event with empty blob
                    let storage_key = StorageKey::kv(key.clone());
                    events.push(ChangeEvent {
                        key: storage_key,
                        blob: Vec::new(),
                        generation: 0,
                    });
                }
            }
        }

        // Commit the inner transaction
        self.inner.commit().await?;

        // Emit the change events
        if !events.is_empty() {
            self.change_signal.committed_with_events(events);
        }

        Ok(())
    }

    pub async fn rollback(self) -> BTreeResult<()> {
        self.inner.rollback().await
    }

    async fn latest(&self, key: &str) -> BTreeResult<Option<(Vec<u8>, AutoCommit)>> {
        let documents = self.inner.range((
            Bound::Included(lower_id(key)),
            Bound::Included(upper_id(key)),
        ));
        pin_mut!(documents);
        let mut latest = None;
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            decode_document_id(&id)?;
            latest = Some((id, document));
        }
        Ok(latest)
    }

    async fn remove_key_generations(&mut self, key: &str) -> BTreeResult<()> {
        let entries = self.inner.remove_range((
            Bound::Included(lower_id(key)),
            Bound::Included(upper_id(key)),
        ));
        pin_mut!(entries);
        while let Some(item) = entries.next().await {
            let (id, _) = item?;
            self.modified_keys.insert(id);
        }
        Ok(())
    }
}

fn history_parts(document: &AutoCommit) -> (Option<Value>, Option<i64>) {
    let Ok(state) = read_document(document) else {
        return (None, None);
    };
    if state.tombstone {
        return (None, state.expires_at);
    }
    (state.value, state.expires_at)
}

fn snapshot_parts(id: Vec<u8>, mut document: AutoCommit) -> (DocumentChangeKey, Vec<u8>) {
    let key = DocumentChangeKey::new_snapshot(id, hash_heads(document.get_heads()));
    (key, document.save())
}

fn validate_snapshot(document: &AutoCommit) -> BTreeResult<()> {
    let state = read_document(document)?;
    if state.tombstone {
        return Err(BTreeError::custom("cannot import tombstone as snapshot"));
    }
    Ok(())
}

fn validate_generation(generation: Uuid) -> BTreeResult<()> {
    if generation.is_nil() {
        return Err(BTreeError::custom("nil UUID is not a valid generation"));
    }
    Ok(())
}

fn logical_prefix(key: &str) -> Vec<u8> {
    let mut prefix = Vec::new();
    prefix.extend_from_slice(key.as_bytes());
    prefix.push(0);
    prefix
}

fn encoded_key_prefix(key: &str) -> Vec<u8> {
    let mut prefix = logical_prefix(key);
    prefix.extend_from_slice(&[0u8; 16]);
    prefix
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    if let Some(last) = end.last_mut() {
        *last = last.saturating_add(1);
    }
    end
}

fn lower_id(key: &str) -> Vec<u8> {
    encoded_key_prefix(key)
}

fn upper_id(key: &str) -> Vec<u8> {
    let mut upper = logical_prefix(key);
    upper.extend_from_slice(&[255u8; 16]);
    upper
}

#[cfg(test)]
mod tests {
    use super::*;
    use btree::InMemoryBTree;

    #[test]
    fn test_timestamp_provider() {
        let provider = || uuid::Timestamp::from_unix(uuid::NoContext, 0, 0);
        let store = KvStore::new(InMemoryBTree::new(), provider);
        let _tx = futures::executor::block_on(store.transaction()).unwrap();
    }
}
