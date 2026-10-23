use std::{collections::VecDeque, io, sync::Arc};

use futures::{StreamExt, future::join_all, stream::FuturesUnordered};
use tokio::sync::watch;

const HISTORY_COMMITS: usize = 128;
const BATCH_KEYS: usize = 128;
const BATCH_BYTES: usize = 16 * 1024;

/// Physical invalidation only; logical identities are interpreted by the store.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageInvalidation {
    All,
    Table(String),
    Key { table: String, key: Vec<u8> },
}

#[derive(Debug, Default)]
struct History {
    revision: u64,
    batches: VecDeque<(u64, Vec<StorageInvalidation>)>,
}

/// Backend-owned notification of successfully committed storage mutations.
#[derive(Clone)]
pub struct ChangeSignal {
    history: watch::Sender<History>,
}

impl Default for ChangeSignal {
    fn default() -> Self {
        Self {
            history: watch::channel(History::default()).0,
        }
    }
}

impl ChangeSignal {
    pub fn subscribe(&self) -> ChangeSubscription {
        ChangeSubscription {
            sources: vec![Source {
                history: self.history.subscribe(),
                acknowledged: None,
            }],
            identity: Arc::new(()),
        }
    }

    /// Unknown writes require broad comparison. Publish synchronously before writer release.
    pub fn committed(&self) {
        self.committed_with([StorageInvalidation::All]);
    }

    /// Retains at most 128 commits, each at most 128 keys / 16 KiB of identity bytes.
    /// Oversized commits and lagging consumers select broad reconciliation instead.
    pub fn committed_with(&self, invalidations: impl IntoIterator<Item = StorageInvalidation>) {
        let mut batch = Vec::new();
        let mut bytes = 0;
        for invalidation in invalidations {
            bytes += match &invalidation {
                StorageInvalidation::All => 0,
                StorageInvalidation::Table(table) => table.len(),
                StorageInvalidation::Key { table, key } => table.len() + key.len(),
            };
            if invalidation == StorageInvalidation::All
                || batch.len() == BATCH_KEYS
                || bytes > BATCH_BYTES
            {
                batch = vec![StorageInvalidation::All];
                break;
            }
            batch.push(invalidation);
        }
        if batch.is_empty() {
            return;
        }
        batch.sort_unstable();
        batch.dedup();
        self.history.send_modify(|history| {
            history.revision = history
                .revision
                .checked_add(1)
                .expect("committed revision exhausted");
            history.batches.push_back((history.revision, batch));
            if history.batches.len() > HISTORY_COMMITS {
                history.batches.pop_front();
            }
        });
    }
}

#[derive(Debug)]
struct Source {
    history: watch::Receiver<History>,
    acknowledged: Option<u64>,
}

/// Selected invalidation and its opaque completion watermark. Drop on failure/cancellation;
/// only acknowledge after successful reconciliation. It owns no storage snapshot.
#[derive(Debug)]
pub struct ChangeWork {
    identity: Arc<()>,
    watermarks: Vec<u64>,
    invalidations: Vec<StorageInvalidation>,
}

impl ChangeWork {
    pub fn invalidations(&self) -> &[StorageInvalidation] {
        &self.invalidations
    }
}

/// Independent bounded-history consumer. Wakeup consumption does not acknowledge work.
#[derive(Debug)]
pub struct ChangeSubscription {
    sources: Vec<Source>,
    identity: Arc<()>,
}

/// Every clone is a new consumer requiring broad initial reconciliation.
impl Clone for ChangeSubscription {
    fn clone(&self) -> Self {
        let mut subscription = Self {
            sources: self
                .sources
                .iter()
                .map(|source| Source {
                    history: source.history.clone(),
                    acknowledged: None,
                })
                .collect(),
            identity: Arc::new(()),
        };
        subscription.consume();
        subscription
    }
}

impl ChangeSubscription {
    pub fn merge(mut self, other: Self) -> Self {
        self.sources.extend(other.sources);
        self.identity = Arc::new(());
        self
    }

    /// Capture work before opening the reconciliation read snapshot. Commits after selection
    /// remain pending even if visible in that snapshot. Re-selecting without ack retries work.
    pub fn select(&self) -> Option<ChangeWork> {
        let mut invalidations = Vec::new();
        let mut watermarks = Vec::new();
        for source in &self.sources {
            let history = source.history.borrow();
            watermarks.push(history.revision);
            match source.acknowledged {
                None => invalidations.push(StorageInvalidation::All),
                Some(ack) if ack < history.revision => {
                    if history
                        .batches
                        .front()
                        .is_none_or(|(revision, _)| ack < revision - 1)
                    {
                        invalidations.push(StorageInvalidation::All);
                    } else {
                        invalidations.extend(
                            history
                                .batches
                                .iter()
                                .filter(|(revision, _)| *revision > ack)
                                .flat_map(|(_, batch)| batch.iter().cloned()),
                        );
                    }
                }
                _ => {}
            }
        }
        if invalidations.is_empty() {
            return None;
        }
        if invalidations.contains(&StorageInvalidation::All) {
            invalidations = vec![StorageInvalidation::All];
        } else {
            invalidations.sort_unstable();
            invalidations.dedup();
        }
        Some(ChangeWork {
            identity: self.identity.clone(),
            watermarks,
            invalidations,
        })
    }

    /// Rejects tokens from another consumer or a subscription before merge.
    pub fn acknowledge(&mut self, work: ChangeWork) -> bool {
        if !Arc::ptr_eq(&self.identity, &work.identity) {
            return false;
        }
        for (source, revision) in self.sources.iter_mut().zip(work.watermarks) {
            source.acknowledged = Some(source.acknowledged.unwrap_or(0).max(revision));
        }
        true
    }

    /// Wait for coalesced wakeups. This does not select or acknowledge invalidation.
    pub async fn changed(&mut self) -> io::Result<()> {
        let changed = {
            let mut waiting = self
                .sources
                .iter_mut()
                .map(|source| source.history.changed())
                .collect::<FuturesUnordered<_>>();
            let mut changed = false;
            while let Some(result) = waiting.next().await {
                if result.is_ok() {
                    changed = true;
                    break;
                }
            }
            changed
        };
        if !changed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "database change subscription closed",
            ));
        }
        self.consume();
        Ok(())
    }

    #[doc(hidden)]
    pub fn consume(&mut self) {
        for source in &mut self.sources {
            source.history.borrow_and_update();
        }
    }

    #[doc(hidden)]
    pub async fn closed(&self) {
        join_all(
            self.sources
                .iter()
                .map(|source| source.history.clone())
                .map(|mut history| async move {
                    let _ = history.wait_for(|_| false).await;
                }),
        )
        .await;
    }
}
