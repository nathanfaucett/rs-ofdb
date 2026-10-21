use futures::FutureExt;

use crate::ChangeSignal;

#[tokio::test]
async fn notifications_are_coalesced_and_cancel_safe() {
    let signal = ChangeSignal::default();
    let mut changes = signal.subscribe();
    assert!(changes.changed().now_or_never().is_none());
    signal.committed();
    signal.committed();
    changes.changed().await.expect("coalesced notification");
    assert!(changes.changed().now_or_never().is_none());
    signal.committed();
    let mut clone = changes.clone();
    assert!(clone.changed().now_or_never().is_none(), "fresh clone");
    changes
        .changed()
        .await
        .expect("original pending notification");
    signal.committed();
    changes.consume();
    assert!(changes.changed().now_or_never().is_none());
}

#[tokio::test]
async fn merged_notifications_close_only_after_all_senders_close() {
    let a = ChangeSignal::default();
    let b = ChangeSignal::default();
    let mut changes = a.subscribe().merge(b.subscribe());
    a.committed();
    b.committed();
    changes.changed().await.expect("merged invalidation");
    assert!(changes.changed().now_or_never().is_none());
    drop(a);
    assert!(changes.closed().now_or_never().is_none());
    b.committed();
    drop(b);
    changes.closed().await;
    changes.changed().await.expect("final pending notification");
    assert_eq!(
        changes.changed().await.expect_err("closed").kind(),
        std::io::ErrorKind::BrokenPipe
    );
}
