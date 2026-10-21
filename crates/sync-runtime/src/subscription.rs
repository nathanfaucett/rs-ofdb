use std::{future::Future, io, time::Duration};

use tokio::time::sleep;

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
    /// Combine stores into one lifecycle. Changes to either schedule one shared pass.
    /// The subscription closes only after all combined backends close.
    pub fn merge(self, other: Self) -> Self {
        Self {
            changes: self.changes.merge(other.changes),
        }
    }

    /// Wait for coalesced admission invalidation, not synchronization scheduling.
    /// Returns BrokenPipe after all backends close and pending changes are consumed.
    pub async fn changed(&mut self) -> io::Result<()> {
        self.changes.changed().await
    }

    /// Run an initial pass, then reconcile on committed changes without idle polling.
    ///
    /// Changes during a pass remain pending. Failed passes retry indefinitely with
    /// exponential backoff from 100 ms to 30 s, retaining pending work without another
    /// commit. The callback supplies authorized transport and reports/logs errors.
    /// Dropping this future cancels its callback or retry timer; no task is spawned.
    /// All backends closing also cancels in-flight work. Requires a Tokio time runtime.
    pub async fn run<F, Fut, E>(mut self, mut synchronize: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let mut retry = Duration::from_millis(100);
        loop {
            self.changes.consume();
            let failed = tokio::select! {
                biased;
                () = self.changes.closed() => return,
                result = sync_stage(&mut synchronize) => result.is_err(),
            };
            if failed {
                tokio::select! {
                    biased;
                    () = self.changes.closed() => return,
                    () = sleep(retry) => {},
                }
                retry = (retry * 2).min(Duration::from_secs(30));
            } else {
                retry = Duration::from_millis(100);
                if self.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}
