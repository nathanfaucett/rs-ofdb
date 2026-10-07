#![cfg(all(feature = "sync", feature = "in-memory"))]

use core::{
    future::{Future, poll_fn},
    task::Poll,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use ofdb_kv::{Database, SyncConfig, SyncRole, SyncTransport, Value};
use tokio::sync::mpsc;

struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}
impl SyncTransport for Channel {
    type Error = ();
    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.receiver.recv().await.ok_or(())
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        self.sender.send(frame).map_err(|_| ())
    }
}
struct PendingAbort {
    inner: Channel,
    denied: Arc<AtomicBool>,
    polls: Arc<AtomicUsize>,
}
impl SyncTransport for PendingAbort {
    type Error = ();
    async fn receive(&mut self) -> Result<Vec<u8>, ()> {
        self.inner.receive().await
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), ()> {
        if self.denied.load(Ordering::SeqCst) {
            return poll_fn(|_| {
                self.polls.fetch_add(1, Ordering::SeqCst);
                Poll::Pending
            })
            .await;
        }
        self.inner.send(frame).await
    }
}

#[tokio::test]
async fn pending_abort_does_not_block_local_denial_or_cancellation() {
    for role in [SyncRole::Initiator, SyncRole::Responder] {
        for stop_at in [1, 2] {
            for reason in ["denied", "canceled"] {
                let source = Database::in_memory();
                let target = Database::in_memory();
                for (key, value) in [("a", 1), ("b", 2)] {
                    source
                        .client()
                        .set(key, Value::Integer(value), None)
                        .await
                        .expect("source write");
                }
                let denied = Arc::new(AtomicBool::new(false));
                let polls = Arc::new(AtomicUsize::new(0));
                let (mut st, tt) = channels();
                let mut tt = PendingAbort {
                    inner: tt,
                    denied: denied.clone(),
                    polls: polls.clone(),
                };
                let mut calls = 0;
                let check = || {
                    calls += 1;
                    let stop = calls == stop_at;
                    denied.store(stop, Ordering::SeqCst);
                    core::future::ready(if stop {
                        Err(reason.to_string())
                    } else {
                        Ok(())
                    })
                };
                let config = SyncConfig {
                    max_snapshots_per_batch: 1,
                    ..SyncConfig::default()
                };
                let source_role = if role == SyncRole::Initiator {
                    SyncRole::Responder
                } else {
                    SyncRole::Initiator
                };
                let mut sender = Box::pin(source.synchronize(&mut st, source_role, config));
                let mut receiver =
                    Box::pin(target.synchronize_checked(&mut tt, role, config, check));
                let result = poll_fn(|cx| {
                    assert!(sender.as_mut().poll(cx).is_pending());
                    match receiver.as_mut().poll(cx) {
                        Poll::Ready(result) => Poll::Ready(result),
                        Poll::Pending => {
                            assert_eq!(
                                polls.load(Ordering::SeqCst),
                                0,
                                "local failure must not await Abort send"
                            );
                            Poll::Pending
                        }
                    }
                })
                .await;
                assert!(
                    matches!(result, Err(ofdb_kv::SyncError::PreApply(detail)) if detail == reason)
                );
                assert_eq!(polls.load(Ordering::SeqCst), 1);
                drop(receiver);
                drop(sender);
                assert_eq!(calls, stop_at);
                assert_eq!(
                    target.client().scan_all().await.expect("prior progress"),
                    if stop_at == 1 {
                        vec![]
                    } else {
                        vec![("a".into(), Value::Integer(1))]
                    }
                );
                exchange(&source, &target).await;
                assert_eq!(
                    source.export_snapshots().await.expect("source"),
                    target.export_snapshots().await.expect("retry")
                );
            }
        }
    }
}

fn channels() -> (Channel, Channel) {
    let (a, ar) = mpsc::unbounded_channel();
    let (b, br) = mpsc::unbounded_channel();
    (
        Channel {
            sender: a,
            receiver: br,
        },
        Channel {
            sender: b,
            receiver: ar,
        },
    )
}
async fn exchange(left: &Database, right: &Database) {
    let (mut a, mut b) = channels();
    let (a, b) = tokio::join!(
        left.synchronize(&mut a, SyncRole::Initiator, SyncConfig::default()),
        right.synchronize(&mut b, SyncRole::Responder, SyncConfig::default())
    );
    a.expect("initiator");
    b.expect("responder");
}
#[tokio::test]
async fn checks_buffered_batches_in_both_roles_and_retry_converges() {
    for role in [SyncRole::Initiator, SyncRole::Responder] {
        for allowed in [0, 1, 3] {
            let source = Database::in_memory();
            let target = Database::in_memory();
            for (key, value) in [("a", 1), ("b", 2), ("c", 3)] {
                source
                    .client()
                    .set(key, Value::Integer(value), None)
                    .await
                    .expect("offline write");
            }
            let config = SyncConfig {
                max_snapshots_per_batch: 1,
                ..SyncConfig::default()
            };
            let (mut st, mut tt) = channels();
            let mut calls = 0;
            let mut check = || {
                calls += 1;
                core::future::ready(if calls <= allowed {
                    Ok(())
                } else {
                    Err("caller denied/canceled".to_string())
                })
            };
            let source_role = if role == SyncRole::Initiator {
                SyncRole::Responder
            } else {
                SyncRole::Initiator
            };
            let (_, result) = tokio::join!(
                source.synchronize(&mut st, source_role, config),
                target.synchronize_checked(&mut tt, role, config, &mut check)
            );
            assert_eq!(calls, (allowed + 1).min(3));
            if allowed == 3 {
                result.expect("allow");
            } else {
                assert!(matches!(result, Err(ofdb_kv::SyncError::PreApply(_))));
            }
            let visible = target.client().scan_all().await.expect("partial state");
            assert_eq!(visible.len(), allowed);
            if allowed == 1 {
                assert_eq!(visible, vec![("a".into(), Value::Integer(1))]);
            }
            exchange(&source, &target).await;
            assert_eq!(
                source.export_snapshots().await.expect("source"),
                target.export_snapshots().await.expect("converged retry")
            );
        }
    }
}

#[tokio::test]
async fn decision_changes_after_authorization_and_cancellation_preserve_atomic_batches() {
    for cancel in [false, true] {
        let source = Database::in_memory();
        let target = Database::in_memory();
        source
            .client()
            .set("a", Value::Integer(1), None)
            .await
            .expect("first");
        source
            .client()
            .set("b", Value::Integer(2), None)
            .await
            .expect("second");
        let config = SyncConfig {
            max_snapshots_per_batch: 1,
            ..SyncConfig::default()
        };
        let (mut st, mut tt) = channels();
        let mut allowed = true;
        let mut calls = 0;
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let mut ready = Some(ready);
        let check = || {
            calls += 1;
            if calls == 2 {
                ready.take().expect("signal once").send(()).expect("waiter");
            }
            let decision = allowed;
            allowed = false;
            async move {
                if decision {
                    Ok(())
                } else if cancel {
                    core::future::pending().await
                } else {
                    Err("changed after authorization".into())
                }
            }
        };
        let mut future = Box::pin(async {
            tokio::join!(
                source.synchronize(&mut st, SyncRole::Initiator, config),
                target.synchronize_checked(&mut tt, SyncRole::Responder, config, check)
            )
        });
        if cancel {
            tokio::select! {
                _ = &mut future => panic!("canceled hook must remain pending"),
                result = waiting => result.expect("second check reached"),
            }
        } else {
            assert!(matches!(
                future.as_mut().await.1,
                Err(ofdb_kv::SyncError::PreApply(_))
            ));
        }
        drop(future);
        assert_eq!(calls, 2);
        assert_eq!(
            target.client().scan_all().await.expect("prior commit"),
            vec![("a".into(), Value::Integer(1))]
        );
        exchange(&source, &target).await;
        assert_eq!(
            target.client().scan_all().await.expect("retry"),
            vec![
                ("a".into(), Value::Integer(1)),
                ("b".into(), Value::Integer(2))
            ]
        );
    }
}

#[tokio::test]
async fn malformed_snapshot_rolls_back_the_entire_direct_atomic_import() {
    let source = Database::in_memory();
    let target = Database::in_memory();
    source
        .client()
        .set("a", Value::Integer(1), None)
        .await
        .expect("first");
    source
        .client()
        .set("b", Value::Integer(2), None)
        .await
        .expect("second");
    let mut snapshots = source.export_snapshots().await.expect("snapshots");
    snapshots[1].payload = vec![255];
    assert!(target.import_snapshots(snapshots).await.is_err());
    assert!(
        target
            .client()
            .scan_all()
            .await
            .expect("rollback")
            .is_empty()
    );
}

#[tokio::test]
async fn facade_only_offline_sync_preserves_generations_tombstones_and_expiry() {
    let left = Database::in_memory();
    let right = Database::in_memory();
    left.client()
        .set("shared", Value::Integer(1), None)
        .await
        .expect("seed");
    exchange(&left, &right).await;
    left.client()
        .delete("shared")
        .await
        .expect("offline delete");
    right
        .client()
        .set("shared", Value::Integer(2), None)
        .await
        .expect("offline update");
    right
        .client()
        .set("expired", Value::Integer(3), Some(0))
        .await
        .expect("expiry");
    exchange(&left, &right).await;
    for db in [&left, &right] {
        assert_eq!(db.client().get("shared").await.expect("tombstone"), None);
        assert_eq!(db.client().get("expired").await.expect("expired"), None);
    }
    right
        .client()
        .set("shared", Value::Integer(4), None)
        .await
        .expect("new generation");
    exchange(&left, &right).await;
    assert_eq!(
        left.client().get("shared").await.expect("recreated"),
        Some(Value::Integer(4))
    );
    assert_eq!(
        left.export_snapshots().await.expect("left snapshot"),
        right.export_snapshots().await.expect("right snapshot")
    );
}
