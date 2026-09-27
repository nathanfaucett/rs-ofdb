use std::{collections::BTreeMap, ops::Bound, sync::Mutex};

use automerge::AutoCommit;
use btree::{BTree, BTreeError, BTreeRead, BTreeResult, BTreeTransaction};
use btree_automerge::{
    AutomergeBTreeTransaction, AutomergeChangeStore, DocumentChangeKey, hash_heads,
};
use futures::{StreamExt, pin_mut};
use uuid::Uuid;

use crate::{
    decode_document_id, encode_document_id,
    value_document::{new_document, read_document, write_live, write_tombstone},
};

pub struct KvStore<B>
where
    B: BTree<Vec<u8>, Vec<u8>>,
{
    inner: B,
}

impl<B> KvStore<B>
where
    B: BTree<Vec<u8>, Vec<u8>>,
{
    pub fn new(inner: B) -> Self {
        Self { inner }
    }

    pub async fn transaction(&self) -> BTreeResult<KvTransaction<B::Transaction>> {
        self.transaction_with_uuid_generator(Uuid::now_v7).await
    }

    pub async fn transaction_with_uuid_generator<F>(
        &self,
        generator: F,
    ) -> BTreeResult<KvTransaction<B::Transaction>>
    where
        F: FnMut() -> Uuid + Send + 'static,
    {
        let inner = self.inner.transaction().await?;
        Ok(KvTransaction {
            inner: AutomergeBTreeTransaction::new(
                AutomergeChangeStore::new(inner),
                Default::default(),
            ),
            uuid_generator: Mutex::new(Box::new(generator)),
        })
    }
}

pub struct KvTransaction<T>
where
    T: BTreeTransaction<Vec<u8>, Vec<u8>>,
{
    inner: AutomergeBTreeTransaction<AutomergeChangeStore<T>>,
    uuid_generator: Mutex<Box<dyn FnMut() -> Uuid + Send>>,
}

impl<T> KvTransaction<T>
where
    T: BTreeTransaction<Vec<u8>, Vec<u8>>,
{
    pub async fn get(&self, key: &str, now: i64) -> BTreeResult<Option<Vec<u8>>> {
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
    ) -> BTreeResult<Vec<(String, Vec<u8>)>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let bounds = (
            Bound::Included(lower_id(&range.start)),
            Bound::Excluded(logical_prefix(&range.end)),
        );
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

    pub async fn scan_all(&self, now: i64) -> BTreeResult<Vec<(String, Vec<u8>)>> {
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
        value: Vec<u8>,
        expires_at: Option<i64>,
    ) -> BTreeResult<()> {
        let latest = self.latest(key).await?;
        let id = match latest {
            Some((id, document)) => {
                let state = read_document(&document)?;
                if !state.tombstone {
                    self.inner
                        .update(id, move |doc| write_live(doc, value, expires_at))
                        .await?;
                    return Ok(());
                }
                let next = (self
                    .uuid_generator
                    .lock()
                    .expect("UUID generator lock poisoned"))();
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
                let next = (self
                    .uuid_generator
                    .lock()
                    .expect("UUID generator lock poisoned"))();
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
        self.inner.update(id, write_tombstone).await?;
        Ok(())
    }

    pub async fn export_snapshots(&self) -> BTreeResult<Vec<(DocumentChangeKey, Vec<u8>)>> {
        let documents = self.inner.range(..);
        pin_mut!(documents);
        let mut latest = BTreeMap::<String, (Vec<u8>, AutoCommit)>::new();
        while let Some(item) = documents.next().await {
            let (id, document) = item?;
            let (key, _) = decode_document_id(&id)?;
            latest.insert(key, (id, document));
        }
        latest
            .into_values()
            .map(|(id, document)| {
                read_document(&document)?;
                Ok(snapshot_parts(id, document))
            })
            .collect()
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
        let (key, payload) = snapshot;
        let mut incoming = validate_snapshot(&key, &payload)?;
        let (logical_key, generation) = decode_document_id(&key.id)?;
        if let Some((id, mut current)) = self.latest(&logical_key).await? {
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
                if !current_heads
                    .iter()
                    .all(|head| incoming.get_change_by_hash(head).is_some())
                {
                    return Err(BTreeError::InvalidDocument);
                }
            }
            self.remove_key_generations(&logical_key).await?;
        }
        self.inner.insert(key.id, incoming).await
    }

    pub async fn commit(self) -> BTreeResult<()> {
        self.inner.commit().await
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
    let mut bytes = Vec::with_capacity(key.len() + 2);
    for byte in key.bytes() {
        if byte == 0 {
            bytes.extend_from_slice(&[0, 255]);
        } else {
            bytes.push(byte);
        }
    }
    bytes.extend_from_slice(&[0, 0]);
    bytes
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
    use btree::{BTreeRead, InMemoryBTree};
    use btree_automerge::{DocumentChangeKey, DocumentType};
    use futures::{StreamExt, executor::block_on};
    use uuid::Uuid;

    use super::KvStore;

    fn uuid(timestamp: u128) -> Uuid {
        Uuid::from_u128((timestamp << 80) | (7 << 76) | (2 << 62))
    }

    #[test]
    fn transaction_reads_writes_deletes_and_recreates() {
        block_on(async {
            let backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
            let store = KvStore::new(backend);
            let mut counter = 1;
            let mut tx = store
                .transaction_with_uuid_generator(move || {
                    let id = uuid(counter);
                    counter += 1;
                    id
                })
                .await
                .unwrap();
            tx.set("k", vec![1], None).await.unwrap();
            assert_eq!(tx.get("k", 0).await.unwrap(), Some(vec![1]));
            tx.commit().await.unwrap();

            let mut tx = store.transaction().await.unwrap();
            tx.delete("k").await.unwrap();
            assert_eq!(tx.get("k", 0).await.unwrap(), None);
            tx.commit().await.unwrap();

            let mut tx = store
                .transaction_with_uuid_generator(|| uuid(2))
                .await
                .unwrap();
            tx.set("k", vec![2], None).await.unwrap();
            assert_eq!(tx.get("k", 0).await.unwrap(), Some(vec![2]));
            tx.rollback().await.unwrap();

            let tx = store.transaction().await.unwrap();
            assert_eq!(tx.get("k", 0).await.unwrap(), None);
            tx.rollback().await.unwrap();
        });
    }

    #[test]
    fn persistence_uses_document_change_keys_and_incrementals() {
        block_on(async {
            let backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
            let store = KvStore::new(backend.clone());
            let mut tx = store
                .transaction_with_uuid_generator(|| uuid(1))
                .await
                .unwrap();
            tx.set("key", vec![1], None).await.unwrap();
            tx.commit().await.unwrap();
            let mut tx = store.transaction().await.unwrap();
            tx.set("key", vec![2], Some(9)).await.unwrap();
            tx.commit().await.unwrap();

            let entries = backend.range(..).collect::<Vec<_>>().await;
            let keys = entries
                .into_iter()
                .map(|entry| {
                    let (key, _) = entry.unwrap();
                    DocumentChangeKey::decode_ordered(&key).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0].r#type(), DocumentType::Snapshot);
            assert_eq!(keys[1].r#type(), DocumentType::Incremental);
        });
    }

    #[test]
    fn import_rejects_divergent_histories_for_the_same_generation() {
        block_on(async {
            let base = KvStore::new(InMemoryBTree::<Vec<u8>, Vec<u8>>::new());
            let mut tx = base
                .transaction_with_uuid_generator(|| uuid(1))
                .await
                .unwrap();
            tx.set("key", vec![0], None).await.unwrap();
            let initial = tx.export_snapshot("key").await.unwrap().unwrap();
            tx.commit().await.unwrap();

            let left = KvStore::new(InMemoryBTree::<Vec<u8>, Vec<u8>>::new());
            let right = KvStore::new(InMemoryBTree::<Vec<u8>, Vec<u8>>::new());
            for store in [&left, &right] {
                let mut tx = store.transaction().await.unwrap();
                tx.import_snapshot(initial.clone()).await.unwrap();
                tx.commit().await.unwrap();
            }

            let mut tx = left.transaction().await.unwrap();
            tx.set("key", vec![1], None).await.unwrap();
            let left_branch = tx.export_snapshot("key").await.unwrap().unwrap();
            tx.commit().await.unwrap();

            let mut tx = right.transaction().await.unwrap();
            tx.set("key", vec![2], None).await.unwrap();
            let right_branch = tx.export_snapshot("key").await.unwrap().unwrap();
            tx.commit().await.unwrap();

            let destination = KvStore::new(InMemoryBTree::<Vec<u8>, Vec<u8>>::new());
            let mut tx = destination.transaction().await.unwrap();
            tx.import_snapshot(left_branch).await.unwrap();
            assert!(tx.import_snapshot(right_branch).await.is_err());
            tx.rollback().await.unwrap();
        });
    }

    #[test]
    fn expiry_is_exclusive_and_scan_respects_key_ranges() {
        block_on(async {
            let backend = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
            let store = KvStore::new(backend);
            let mut id = 1;
            let mut tx = store
                .transaction_with_uuid_generator(move || {
                    let next = uuid(id);
                    id += 1;
                    next
                })
                .await
                .unwrap();
            tx.set("a", vec![1], Some(10)).await.unwrap();
            tx.set("ab", vec![2], None).await.unwrap();
            tx.set("b", vec![3], None).await.unwrap();
            assert_eq!(tx.get("a", 9).await.unwrap(), Some(vec![1]));
            assert_eq!(tx.get("ab", 9).await.unwrap(), Some(vec![2]));
            assert_eq!(tx.get("a", 10).await.unwrap(), None);
            assert_eq!(
                tx.scan_all(9).await.unwrap(),
                vec![
                    ("a".into(), vec![1]),
                    ("ab".into(), vec![2]),
                    ("b".into(), vec![3])
                ]
            );
            assert_eq!(
                tx.scan("a".to_string().."b".to_string(), 9).await.unwrap(),
                vec![("a".into(), vec![1]), ("ab".into(), vec![2])]
            );
            assert!(
                tx.scan("a".to_string().."a".to_string(), 0)
                    .await
                    .unwrap()
                    .is_empty()
            );
            tx.rollback().await.unwrap();
        });
    }
}
