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
    ChangeApply, ChangeSummary, KvHistory, NativeChange, decode_document_id, encode_document_id,
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
        let mut result = std::collections::BTreeSet::new();
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
    pub async fn get(&self, key: &str, now: i64) -> BTreeResult<Option<Value>> {
        let Some((_, document)) = self.latest(key).await? else {
            return Ok(None);
        };
        let state = read_document(&document)?;
        if state.tombstone || state.expires_at.is_some_and(|expiry| now >= expiry) {
            return Ok(None);
        }
        Ok(state.value)
    }

    pub async fn scan(
        &self,
        range: std::ops::Range<String>,
        now: i64,
    ) -> BTreeResult<Vec<(String, Value)>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        self.scan_bounds(
            (
                Bound::Included(lower_id(&range.start)),
                Bound::Excluded(logical_prefix(&range.end)),
            ),
            now,
        )
        .await
    }

    pub async fn scan_prefix(&self, prefix: &str, now: i64) -> BTreeResult<Vec<(String, Value)>> {
        let start = encoded_key_prefix(prefix);
        let end = prefix_end(start.clone());
        let bounds = match end {
            Some(end) => (Bound::Included(start), Bound::Excluded(end)),
            None => (Bound::Included(start), Bound::Unbounded),
        };
        self.scan_bounds(bounds, now).await
    }

    async fn scan_bounds(
        &self,
        bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        now: i64,
    ) -> BTreeResult<Vec<(String, Value)>> {
        let documents = self.inner.range(bounds);
        pin_mut!(documents);
        let mut latest = BTreeMap::<String, (Uuid, AutoCommit)>::new();
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            let (key, generation) = decode_document_id(&id)?;
            latest.insert(key, (generation, document));
        }
        latest
            .into_iter()
            .try_fold(Vec::new(), |mut visible, (key, (_, document))| {
                let state = read_document(&document)?;
                if !state.tombstone && !state.expires_at.is_some_and(|expiry| now >= expiry) {
                    visible.push((key, state.value.ok_or(BTreeError::InvalidDocument)?));
                }
                Ok(visible)
            })
    }

    pub async fn scan_all(&self, now: i64) -> BTreeResult<Vec<(String, Value)>> {
        let documents = self.inner.range(..);
        pin_mut!(documents);
        let mut latest = BTreeMap::<String, (Uuid, AutoCommit)>::new();
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            let (key, generation) = decode_document_id(&id)?;
            latest.insert(key, (generation, document));
        }
        latest
            .into_iter()
            .try_fold(Vec::new(), |mut visible, (key, (_, document))| {
                let state = read_document(&document)?;
                if !state.tombstone && !state.expires_at.is_some_and(|expiry| now >= expiry) {
                    visible.push((key, state.value.ok_or(BTreeError::InvalidDocument)?));
                }
                Ok(visible)
            })
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
                    self.modified_keys.insert(id.clone());
                    self.inner
                        .update(id, move |doc| write_live(doc, value, expires_at))
                        .await?;
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
        self.inner
            .insert(
                encode_document_id(key, id),
                new_document(value, expires_at)?,
            )
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

    /// Inspect only the latest generation of this logical key, without saving a snapshot.
    pub async fn history(&self, key: &str) -> BTreeResult<Option<KvHistory>> {
        self.latest(key)
            .await?
            .map(|(id, document)| history_parts(id, document))
            .transpose()
    }

    pub async fn histories(&self) -> BTreeResult<Vec<KvHistory>> {
        let documents = self.inner.range(..);
        pin_mut!(documents);
        let mut latest = BTreeMap::new();
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            let (key, _) = decode_document_id(&id)?;
            latest.insert(key, history_parts(id, document));
        }
        latest.into_values().collect()
    }

    /// Export native changes even when their only physical representation is a snapshot.
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
        let known = known.iter().collect::<std::collections::BTreeSet<_>>();
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
    pub async fn import_changes(
        &mut self,
        id: &[u8],
        changes: &[NativeChange],
    ) -> BTreeResult<ChangeApply> {
        let logical_key = decode_document_id(id)?.0;
        let before = self.history(&logical_key).await?;
        let result = async {
            let (key, generation) = decode_document_id(id)?;
            let parsed = changes
                .iter()
                .map(|claimed| {
                    let change = automerge::Change::from_bytes(claimed.payload.clone())
                        .map_err(BTreeError::custom)?;
                    if change.hash().0 != claimed.hash
                        || change.deps().iter().map(|hash| hash.0).collect::<Vec<_>>()
                            != claimed.deps
                    {
                        return Err(BTreeError::InvalidDocument);
                    }
                    Ok(change)
                })
                .collect::<BTreeResult<Vec<_>>>()?;
            let mut document = match self.latest(&key).await? {
                Some((current_id, document)) => {
                    let (_, current_generation) = decode_document_id(&current_id)?;
                    if generation < current_generation {
                        return Ok(ChangeApply::Applied);
                    }
                    if generation == current_generation {
                        document
                    } else {
                        AutoCommit::new()
                    }
                }
                None => AutoCommit::new(),
            };
            let mut available = document
                .get_changes(&[])
                .into_iter()
                .map(|change| change.hash())
                .collect::<std::collections::BTreeSet<_>>();
            for change in &parsed {
                if change.deps().iter().any(|dep| !available.contains(dep)) {
                    return Ok(ChangeApply::MissingDependencies);
                }
                available.insert(change.hash());
            }
            document.apply_changes(parsed).map_err(BTreeError::custom)?;
            read_document(&document)?;
            self.import_snapshot(snapshot_parts(id.to_vec(), document))
                .await?;
            Ok(ChangeApply::Applied)
        }
        .await;
        if result.is_ok() && self.history(&logical_key).await? != before {
            if let Some(after) = self.history(&logical_key).await? {
                self.modified_keys.insert(after.id);
            }
        }
        result
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
        snapshot: (DocumentChangeKey, Vec<u8>),
    ) -> BTreeResult<()> {
        let logical_key = decode_document_id(&snapshot.0.id)?.0;
        let before = self.history(&logical_key).await?;
        let result = async {
            let (key, payload) = snapshot;
            let mut incoming = validate_snapshot(&key, &payload)?;
            let (logical_key, generation) = decode_document_id(&key.id)?;
            if let Some((id, mut current)) = self.latest(&logical_key).await? {
                read_document(&current)?;
                let (_, current_generation) = decode_document_id(&id)?;
                if generation < current_generation {
                    return Ok(());
                }
                if generation == current_generation {
                    let current_heads = current.get_heads();
                    let incoming_heads = incoming.get_heads();
                    if incoming_heads
                        .iter()
                        .all(|head| current.get_change_by_hash(head).is_some())
                    {
                        return Ok(());
                    }
                    if current_heads
                        .iter()
                        .all(|head| incoming.get_change_by_hash(head).is_some())
                    {
                        self.inner.insert(id, incoming).await?;
                        return Ok(());
                    }
                    current.merge(&mut incoming).map_err(BTreeError::custom)?;
                    read_document(&current)?;
                    self.inner.insert(id, current).await?;
                    return Ok(());
                }
                self.remove_key_generations(&logical_key).await?;
            }
            self.inner.insert(key.id, incoming).await
        }
        .await;
        if result.is_ok() && self.history(&logical_key).await? != before {
            if let Some(after) = self.history(&logical_key).await? {
                self.modified_keys.insert(after.id);
            }
        }
        result
    }

    pub async fn commit(self) -> BTreeResult<()> {
        let mut events = Vec::new();
        for key in &self.modified_keys {
            if let Some(mut document) = self.inner.get(key).await? {
                read_document(&document)?;
                if let Some(change) = document.get_changes(&[]).last() {
                    events.push(ChangeEvent {
                        key: StorageKey::kv(key.clone()),
                        blob: change.raw_bytes().to_vec(),
                        generation: change.seq(),
                    });
                }
            }
        }
        self.inner.commit().await?;
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
        while let Some(entry) = entries.next().await {
            entry?;
        }
        Ok(())
    }
}

fn history_parts(id: Vec<u8>, mut document: AutoCommit) -> BTreeResult<KvHistory> {
    read_document(&document)?;
    Ok(KvHistory {
        id,
        heads: document
            .get_heads()
            .into_iter()
            .map(|hash| hash.0)
            .collect(),
        changes: document
            .get_changes(&[])
            .into_iter()
            .map(|change| ChangeSummary {
                hash: change.hash().0,
                deps: change.deps().iter().map(|hash| hash.0).collect(),
            })
            .collect(),
    })
}

fn snapshot_parts(id: Vec<u8>, mut document: AutoCommit) -> (DocumentChangeKey, Vec<u8>) {
    let key = DocumentChangeKey::new_snapshot(id, hash_heads(document.get_heads()));
    (key, document.save())
}

fn validate_snapshot(key: &DocumentChangeKey, payload: &[u8]) -> BTreeResult<AutoCommit> {
    decode_document_id(&key.id)?;
    if key.r#type != btree_automerge::DocumentType::Snapshot {
        return Err(BTreeError::InvalidDocument);
    }
    let mut document = AutoCommit::load(payload).map_err(BTreeError::custom)?;
    if document.save() != payload || key.change_hash != hash_heads(document.get_heads()) {
        return Err(BTreeError::InvalidDocument);
    }
    read_document(&document)?;
    Ok(document)
}

fn validate_generation(generation: Uuid) -> BTreeResult<()> {
    if generation.get_version_num() != 7 {
        return Err(BTreeError::custom("Generation UUID must use version 7"));
    }
    Ok(())
}

fn logical_prefix(key: &str) -> Vec<u8> {
    let mut bytes = encoded_key_prefix(key);
    bytes.extend_from_slice(&[0, 0]);
    bytes
}

fn encoded_key_prefix(key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(key.len());
    for byte in key.bytes() {
        if byte == 0 {
            bytes.extend_from_slice(&[0, 255]);
        } else {
            bytes.push(byte);
        }
    }
    bytes
}

fn prefix_end(mut prefix: Vec<u8>) -> Option<Vec<u8>> {
    let last_incrementable = prefix.iter().rposition(|byte| *byte < u8::MAX)?;
    prefix[last_incrementable] += 1;
    prefix.truncate(last_incrementable + 1);
    Some(prefix)
}

fn lower_id(key: &str) -> Vec<u8> {
    let mut id = logical_prefix(key);
    id.extend_from_slice(&[0; 16]);
    id
}

fn upper_id(key: &str) -> Vec<u8> {
    let mut id = logical_prefix(key);
    let mut generation = [255; 16];
    generation[6] = 0x7f;
    generation[8] = 0xbf;
    id.extend_from_slice(&generation);
    id
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
