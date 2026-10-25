use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use btree::ChangeSignal;
use futures::FutureExt;
use tokio::{task::yield_now, time::advance};

use crate::ChangeSubscription;

fn subscribe(signal: &ChangeSignal) -> ChangeSubscription {
    signal.subscribe().into()
}

#[test]
fn large_callback_state_stays_out_of_the_runner_future() {
    let signal = ChangeSignal::default();
    let runner = subscribe(&signal).run(|| async {
        let state = [0_u8; 128 * 1024];
        pending::<()>().await;
        core::hint::black_box(&state);
        Ok::<(), ()>(())
    });
    assert!(
        core::mem::size_of_val(&runner) < 4096,
        "runner must not embed its callback future"
    );
}

#[tokio::test(start_paused = true)]
async fn initial_pass_coalesces_changes_and_never_polls_idle_storage() {
    let signal = ChangeSignal::default();
    let subscription = subscribe(&signal);
    signal.committed();
    signal.committed();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let runner = tokio::spawn(subscription.run(move || {
        count.fetch_add(1, Ordering::SeqCst);
        async { Ok::<(), ()>(()) }
    }));
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    advance(Duration::from_secs(3600)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    for _ in 0..3 {
        signal.committed();
    }
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    advance(Duration::from_secs(3600)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(signal);
    runner.await.expect("closed runner");
}

#[tokio::test(start_paused = true)]
async fn a_commit_during_a_pass_is_not_lost() {
    let signal = ChangeSignal::default();
    let mut subscription = subscribe(&signal);
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let barrier = release.clone();
    let runner = tokio::spawn(subscription.clone().run(move || {
        let barrier = barrier.clone();
        started.send(()).expect("started");
        async move {
            barrier.notified().await;
            Ok::<(), ()>(())
        }
    }));
    starts.recv().await.expect("initial pass");
    signal.committed();
    signal.committed();
    release.notify_one();
    starts
        .recv()
        .await
        .expect("pending changes schedule second pass");
    release.notify_one();
    yield_now().await;
    assert!(starts.try_recv().is_err());
    assert!(
        subscription
            .changed()
            .now_or_never()
            .expect("admission wakeup")
            .is_ok()
    );
    runner.abort();
    assert!(runner.await.expect_err("cancelled").is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn failed_work_retries_without_new_commits_and_backoff_is_capped() {
    let signal = ChangeSignal::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let runner = tokio::spawn(subscribe(&signal).run(move || {
        count.fetch_add(1, Ordering::SeqCst);
        async { Err::<(), ()>(()) }
    }));
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    advance(Duration::from_millis(99)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    signal.committed();
    yield_now().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "changes cannot bypass backoff"
    );
    advance(Duration::from_millis(1)).await;
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let mut delay = Duration::from_millis(200);
    for expected in 3..=14 {
        advance(delay).await;
        yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), expected);
        delay = (delay * 2).min(Duration::from_secs(30));
    }
    drop(signal);
    runner.await.expect("closed while retry pending");
}

#[tokio::test(start_paused = true)]
async fn successful_retry_returns_to_idle_and_resets_backoff() {
    let signal = ChangeSignal::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let runner = tokio::spawn(subscribe(&signal).run(move || {
        let attempt = count.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt.is_multiple_of(2) {
                Err(())
            } else {
                Ok(())
            }
        }
    }));
    yield_now().await;
    advance(Duration::from_millis(100)).await;
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    advance(Duration::from_secs(3600)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    signal.committed();
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    advance(Duration::from_millis(100)).await;
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    drop(signal);
    runner.await.expect("closed runner");
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn dropping_runner_cancels_inflight_work_and_retry_timer() {
    let signal = ChangeSignal::default();
    let drops = Arc::new(AtomicUsize::new(0));
    let count = drops.clone();
    let mut runner = Box::pin(subscribe(&signal).run(move || {
        let guard = Dropped(count.clone());
        async move {
            let _guard = guard;
            pending::<Result<(), ()>>().await
        }
    }));
    assert!(runner.as_mut().now_or_never().is_none());
    drop(runner);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut runner = Box::pin(subscribe(&signal).run(move || {
        count.fetch_add(1, Ordering::SeqCst);
        async { Err::<(), ()>(()) }
    }));
    assert!(runner.as_mut().now_or_never().is_none());
    drop(runner);
    signal.committed();
    advance(Duration::from_secs(3600)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn merged_stores_share_one_runner_and_close_only_together() {
    let sql = ChangeSignal::default();
    let kv = ChangeSignal::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let runner = tokio::spawn(subscribe(&sql).merge(subscribe(&kv)).run(move || {
        count.fetch_add(1, Ordering::SeqCst);
        async { Ok::<(), ()>(()) }
    }));
    yield_now().await;
    sql.committed();
    kv.committed();
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(sql);
    yield_now().await;
    assert!(!runner.is_finished());
    kv.committed();
    yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    drop(kv);
    runner.await.expect("both backends closed");
}

#[tokio::test]
async fn backend_close_cancels_inflight_work() {
    let signal = ChangeSignal::default();
    let drops = Arc::new(AtomicUsize::new(0));
    let count = drops.clone();
    let runner = tokio::spawn(subscribe(&signal).run(move || {
        let guard = Dropped(count.clone());
        async move {
            let _guard = guard;
            pending::<Result<(), ()>>().await
        }
    }));
    yield_now().await;
    drop(signal);
    runner.await.expect("close cancels work");
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalidation_is_coalesced_cancel_safe_and_opaque() {
    let a = ChangeSignal::default();
    let b = ChangeSignal::default();
    let mut changes = subscribe(&a).merge(subscribe(&b));
    assert!(changes.changed().now_or_never().is_none());
    a.committed();
    b.committed();
    changes.changed().await.expect("one coalesced invalidation");
    assert!(changes.changed().now_or_never().is_none());
    a.committed();
    let mut clone = changes.clone();
    assert!(clone.changed().now_or_never().is_none(), "fresh clone");
    changes
        .changed()
        .await
        .expect("original retains pending update");
    b.committed();
    drop(a);
    drop(b);
    changes.changed().await.expect("final pending notification");
    assert_eq!(
        changes.changed().await.expect_err("closed").kind(),
        std::io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn closed_backend_never_constructs_a_transport_callback() {
    let signal = ChangeSignal::default();
    let changes = subscribe(&signal);
    drop(signal);
    let mut calls = 0;
    changes
        .run(|| {
            calls += 1;
            async { Ok::<(), ()>(()) }
        })
        .await;
    assert_eq!(calls, 0, "closed backend must not construct transport");
}
