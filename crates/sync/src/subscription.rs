#![allow(dead_code, unused_variables)]
use std::{future::Future, io, time::Duration};

#[cfg(feature = "std")]
use platform::sleep;

#[inline(never)]
fn sync_stage<F: Future>(make: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    Box::pin(make())
}

/// Sync-owned continuous reconciliation over coalesced committed changes.
///
/// Construct before starting reconciliation. This subscription alone does not retain storage.
/// Cloning starts observing at the current committed state, like a new subscription.
#[derive(Clone, Debug)]
pub struct ChangeSubscription {
    changes: btree::ChangeSubscription,
}

impl From<btree::ChangeSubscription> for ChangeSubscription {
    fn from(changes: btree::ChangeSubscription) -> Self {
        Self { changes }
    }
}

impl ChangeSubscription {
    /// Returns a stream of committed changes with their incremental blobs.
    /// Each event is delivered exactly once per subscription in generation order.
    /// This is the push-based replacement for `select()` + `acknowledge()` + `changed()`.
    pub fn events(&mut self) -> impl futures::Stream<Item = btree::ChangeEvent> + '_ {
        self.changes.events()
    }

    /// Combine stores into one lifecycle. Changes to either schedule one shared pass.
    /// The subscription closes only after all combined backends close.
    pub fn merge(self, other: Self) -> Self {
        Self {
            changes: self.changes.merge(other.changes),
        }
    }

    /// Wait for coalesced admission invalidation, not synchronization scheduling.
    /// Returns BrokenPipe after all backends close and pending changes are consumed.
    #[deprecated(since = "0.1.0", note = "Use events() stream instead")]
    pub async fn changed(&mut self) -> io::Result<()> {
        #[allow(deprecated)]
        self.changes.changed().await
    }

    /// Internal helper to avoid deprecated warning in run()
    #[allow(deprecated)]
    async fn changed_internal(&mut self) -> io::Result<()> {
        self.changes.changed().await
    }

    /// Run an initial pass, then reconcile on committed changes without idle polling.
    ///
    /// Changes during a pass remain pending. Failed passes retry indefinitely with
    /// exponential backoff from 100 ms to 30 s, retaining pending work without another
    /// commit. The callback supplies authorized transport and reports/logs errors.
    /// Dropping this future cancels its callback or retry timer; no task is spawned.
    /// All backends closing also cancels in-flight work. Uses browser timers on Wasm; native callers require a Tokio time runtime.
    #[deprecated(
        since = "0.1.0",
        note = "Use push_synchronize() with events() stream instead"
    )]
    pub async fn run<F, Fut, E>(mut self, mut synchronize: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let mut retry = Duration::from_millis(100);
        loop {
            self.changes.consume();
            let selected = self.select_deprecated();
            let failed = tokio::select! {
                biased;
                () = self.changes.closed() => return,
                result = async { sync_stage(&mut synchronize).await } => result.is_err(),
            };
            if failed {
                tokio::select! {
                    biased;
                    () = self.changes.closed() => return,
                    () = sleep(retry) => {},
                }
                retry = (retry * 2).min(Duration::from_secs(30));
            } else {
                if let Some(work) = selected {
                    self.acknowledge_deprecated(work);
                }
                retry = Duration::from_millis(100);
                if self.select_deprecated().is_none() && self.changed_internal().await.is_err() {
                    return;
                }
            }
        }
    }

    /// Select before opening a reconciliation snapshot; drop the token on failure.
    #[deprecated(since = "0.1.0", note = "Use events() stream instead")]
    pub fn select(&self) -> Option<btree::ChangeWork> {
        #[allow(deprecated)]
        self.changes.select()
    }

    /// Complete exactly the selected watermark, never commits arriving during the pass.
    #[deprecated(since = "0.1.0", note = "Use events() stream instead")]
    pub fn acknowledge(&mut self, work: btree::ChangeWork) -> bool {
        #[allow(deprecated)]
        self.changes.acknowledge(work)
    }

    // Internal helpers to avoid experimental attribute-on-expression
    #[allow(deprecated)]
    fn select_deprecated(&self) -> Option<btree::ChangeWork> {
        self.select()
    }

    #[allow(deprecated)]
    fn acknowledge_deprecated(&mut self, work: btree::ChangeWork) -> bool {
        self.acknowledge(work)
    }

    #[allow(deprecated)]
    async fn changed_deprecated(&mut self) -> io::Result<()> {
        self.changed().await
    }
}
