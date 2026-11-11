use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt, future::join_all, stream::FuturesUnordered};
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

/// Unified key for both KV (table="") and SQL (table="users")
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct StorageKey {
    pub table: String,
    pub key: Vec<u8>,
}

impl StorageKey {
    /// Create a StorageKey for KV (empty table).
    pub fn kv(key: Vec<u8>) -> Self {
        Self {
            table: String::new(),
            key,
        }
    }

    /// Create a StorageKey for SQL (with table name).
    pub fn sql(table: String, key: Vec<u8>) -> Self {
        Self { table, key }
    }
}

/// A committed change with its incremental Automerge blob.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChangeEvent {
    /// Logical key: table (SQL) or empty (KV), plus row/document key
    pub key: StorageKey,
    /// Raw Automerge change bytes (change.raw_bytes())
    pub blob: Vec<u8>,
    /// Optional: generation/version for ordering
    pub generation: u64,
}

#[derive(Debug, Default)]
struct History {
    revision: u64,
    batches: VecDeque<(u64, Vec<StorageInvalidation>, Vec<ChangeEvent>)>,
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
                event_cursor: 0,
                event_index: 0,
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
            history
                .batches
                .push_back((history.revision, batch, Vec::new()));
            if history.batches.len() > HISTORY_COMMITS {
                history.batches.pop_front();
            }
        });
    }

    /// Called by storage backend on successful commit.
    /// Includes the incremental blob so consumers don't need to re-fetch.
    pub fn committed_with_events(&self, events: impl IntoIterator<Item = ChangeEvent>) {
        let events: Vec<ChangeEvent> = events.into_iter().collect();
        let mut invalidations = Vec::new();
        let mut bytes = 0;
        for event in &events {
            invalidations.push(StorageInvalidation::Key {
                table: event.key.table.clone(),
                key: event.key.key.clone(),
            });
            bytes += event.blob.len();
        }
        if invalidations.is_empty() {
            return;
        }
        if invalidations.len() > BATCH_KEYS || bytes > BATCH_BYTES {
            invalidations = vec![StorageInvalidation::All];
        } else {
            invalidations.sort_unstable();
            invalidations.dedup();
        }
        self.history.send_modify(|history| {
            history.revision = history
                .revision
                .checked_add(1)
                .expect("committed revision exhausted");
            history
                .batches
                .push_back((history.revision, invalidations, events));
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
    event_cursor: usize,
    event_index: usize,
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
                    acknowledged: source.acknowledged,
                    event_cursor: source.event_cursor,
                    event_index: source.event_index,
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
    #[deprecated(since = "0.1.0", note = "use events() stream instead")]
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
                        .is_none_or(|(revision, _, _)| ack < revision - 1)
                    {
                        invalidations.push(StorageInvalidation::All);
                    } else {
                        invalidations.extend(
                            history
                                .batches
                                .iter()
                                .filter(|(revision, _, _)| *revision > ack)
                                .flat_map(|(_, batch, _)| batch.iter().cloned()),
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
    #[deprecated(since = "0.1.0", note = "use events() stream instead")]
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
    #[deprecated(since = "0.1.0", note = "use events() stream instead")]
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

    /// Returns a stream of ChangeEvent. No select/acknowledge needed.
    /// Each event is delivered exactly once per subscription.
    pub fn events(&mut self) -> EventStream<'_> {
        EventStream {
            sources: &mut self.sources,
            delivered_any: false,
        }
    }

    #[doc(hidden)]
    pub fn consume(&mut self) {
        for source in &mut self.sources {
            source.history.borrow_and_update();
            // Update event cursor to current batch count so new events are delivered
            let _history = source.history.borrow();
            source.event_cursor = 0;
            source.event_index = 0;
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

/// Stream of ChangeEvent returned by ChangeSubscription::events().
pub struct EventStream<'a> {
    sources: &'a mut Vec<Source>,
    /// Tracks whether this stream has delivered any events in its lifetime.
    delivered_any: bool,
}

impl<'a> Stream for EventStream<'a> {
    type Item = ChangeEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Try to deliver events from each source
        for source in &mut *this.sources {
            let history = source.history.borrow();
            let batches = &history.batches;

            // Deliver events from current batch position
            while source.event_cursor < batches.len() {
                let batch = &batches[source.event_cursor];
                let (_, _invals, events) = batch;

                // Sort events by generation for consistent ordering
                let mut sorted_events: Vec<_> = events.iter().collect();
                sorted_events.sort_by_key(|e| e.generation);

                if source.event_index < sorted_events.len() {
                    // There are undelivered events in this batch
                    this.delivered_any = true;
                    let event = sorted_events[source.event_index].clone();
                    source.event_index += 1;
                    return Poll::Ready(Some(event));
                }

                // Finished this batch, move to next
                source.event_cursor += 1;
                source.event_index = 0;
            }
        }

        // No undelivered historical events.
        // If this stream has delivered events before, it means we've exhausted history - end the stream.
        // If this stream has NOT delivered any events (cursor already at end from previous stream),
        // wait for future changes.
        if this.delivered_any {
            // This stream delivered events and now there are no more - end (snapshot semantics)
            return Poll::Ready(None);
        }

        // This stream never delivered any events - wait for future changes.
        let mut waiting = this
            .sources
            .iter_mut()
            .map(|source| source.history.changed())
            .collect::<FuturesUnordered<_>>();

        // Poll each future to register waker
        loop {
            match Pin::new(&mut waiting).poll_next(cx) {
                Poll::Ready(Some(Ok(_))) => {
                    // New changes available, will be delivered on next poll
                    return Poll::Pending;
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(None);
                }
                Poll::Ready(None) => {
                    break;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(None)
    }
}
