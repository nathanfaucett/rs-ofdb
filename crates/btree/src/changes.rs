use std::io;

use futures::{StreamExt, future::join_all, stream::FuturesUnordered};
use tokio::sync::watch;

/// Backend-owned notification of successfully committed storage mutations.
#[derive(Clone)]
pub struct ChangeSignal {
    revision: watch::Sender<u64>,
}

impl Default for ChangeSignal {
    fn default() -> Self {
        Self {
            revision: watch::channel(0).0,
        }
    }
}

impl ChangeSignal {
    pub fn subscribe(&self) -> ChangeSubscription {
        ChangeSubscription {
            revisions: vec![self.revision.subscribe()],
        }
    }

    /// Call synchronously after a successful changed commit, before releasing writer admission.
    pub fn committed(&self) {
        self.revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}

/// Generic coalesced notifications of committed storage changes.
///
/// Construct before observing mutations. The subscription does not retain storage.
#[derive(Debug)]
pub struct ChangeSubscription {
    revisions: Vec<watch::Receiver<u64>>,
}

/// A clone starts observing at the current committed state, like a new subscription.
impl Clone for ChangeSubscription {
    fn clone(&self) -> Self {
        let mut subscription = Self {
            revisions: self.revisions.clone(),
        };
        subscription.consume();
        subscription
    }
}

impl ChangeSubscription {
    /// Combine signals into one coalesced notification source.
    /// The subscription closes only after all combined backends close.
    pub fn merge(mut self, other: Self) -> Self {
        self.revisions.extend(other.revisions);
        self
    }

    /// Wait for committed changes, coalescing all currently pending notifications.
    /// Returns BrokenPipe after every backend closes and pending changes are consumed.
    pub async fn changed(&mut self) -> io::Result<()> {
        let changed = {
            let mut waiting = self
                .revisions
                .iter_mut()
                .map(|revision| revision.changed())
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

    /// Acknowledge currently committed notifications without waiting.
    #[doc(hidden)]
    pub fn consume(&mut self) {
        for revision in &mut self.revisions {
            revision.borrow_and_update();
        }
    }

    /// Wait until every combined signal has released its senders.
    #[doc(hidden)]
    pub async fn closed(&self) {
        join_all(
            self.revisions
                .iter()
                .cloned()
                .map(|mut revision| async move {
                    let _ = revision.wait_for(|_| false).await;
                }),
        )
        .await;
    }
}
