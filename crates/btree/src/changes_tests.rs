use futures::FutureExt;

use crate::changes::ChangeSignal;

#[tokio::test]
async fn notifications_are_coalesced_and_cancel_safe() {
    let signal = ChangeSignal::default();
    let mut changes = signal.subscribe();
    #[allow(deprecated)]
    let _ = changes.changed().now_or_never().is_none();
    signal.committed();
    signal.committed();
    #[allow(deprecated)]
    changes.changed().await.expect("coalesced notification");
    #[allow(deprecated)]
    let _ = changes.changed().now_or_never().is_none();
    signal.committed();
    let mut clone = changes.clone();
    #[allow(deprecated)]
    let _ = clone.changed().now_or_never().is_none();
    #[allow(deprecated)]
    changes
        .changed()
        .await
        .expect("original pending notification");
    signal.committed();
    changes.consume();
    #[allow(deprecated)]
    let _ = changes.changed().now_or_never().is_none();
}

#[tokio::test]
async fn merged_notifications_close_only_after_all_senders_close() {
    let a = ChangeSignal::default();
    let b = ChangeSignal::default();
    let mut changes = a.subscribe().merge(b.subscribe());
    a.committed();
    b.committed();
    #[allow(deprecated)]
    changes.changed().await.expect("merged invalidation");
    #[allow(deprecated)]
    let _ = changes.changed().now_or_never().is_none();
    drop(a);
    assert!(changes.closed().now_or_never().is_none());
    b.committed();
    drop(b);
    changes.closed().await;
    #[allow(deprecated)]
    changes.changed().await.expect("final pending notification");
    #[allow(deprecated)]
    let _ = changes.changed().await.expect_err("closed").kind();
}

#[cfg(test)]
mod change_event_tests {
    use crate::changes::{ChangeEvent, ChangeSignal, StorageKey};
    use futures::StreamExt;

    #[tokio::test]
    async fn committed_with_events_single_event() {
        let signal = ChangeSignal::default();
        let mut changes = signal.subscribe();
        let event = ChangeEvent {
            key: StorageKey {
                table: "test".into(),
                key: b"key1".to_vec(),
            },
            blob: vec![1, 2, 3],
            generation: 1,
        };
        signal.committed_with_events([event.clone()]);
        #[allow(deprecated)]
        changes.changed().await.expect("notification received");
        let mut stream = changes.events();
        let e = StreamExt::next(&mut stream).await.unwrap();
        assert_eq!(e.key.table, "test");
        assert_eq!(e.key.key, b"key1");
        assert_eq!(e.blob, vec![1, 2, 3]);
        assert_eq!(e.generation, 1);
        assert!(StreamExt::next(&mut stream).await.is_none());
    }

    #[tokio::test]
    async fn committed_with_events_multiple_events() {
        let signal = ChangeSignal::default();
        let mut changes = signal.subscribe();
        let events = vec![
            ChangeEvent {
                key: StorageKey {
                    table: "test".into(),
                    key: b"key1".to_vec(),
                },
                blob: vec![1, 2, 3],
                generation: 1,
            },
            ChangeEvent {
                key: StorageKey {
                    table: "test".into(),
                    key: b"key2".to_vec(),
                },
                blob: vec![4, 5, 6],
                generation: 2,
            },
        ];
        signal.committed_with_events(events.clone());
        #[allow(deprecated)]
        changes.changed().await.expect("notification received");
        let mut stream = changes.events();
        let e1 = StreamExt::next(&mut stream).await.unwrap();
        let e2 = StreamExt::next(&mut stream).await.unwrap();
        assert_eq!(e1.generation, 1);
        assert_eq!(e2.generation, 2);
        assert!(StreamExt::next(&mut stream).await.is_none());
    }

    #[tokio::test]
    async fn committed_with_events_large_blob_fallback() {
        let signal = ChangeSignal::default();
        let mut changes = signal.subscribe();
        let large_blob = vec![0u8; 1024 * 1024]; // 1MB
        let event = ChangeEvent {
            key: StorageKey {
                table: "test".into(),
                key: b"key1".to_vec(),
            },
            blob: large_blob.clone(),
            generation: 1,
        };
        signal.committed_with_events([event]);
        #[allow(deprecated)]
        changes.changed().await.expect("notification received");
        let mut stream = changes.events();
        let e = StreamExt::next(&mut stream).await.unwrap();
        assert_eq!(e.blob.len(), 1024 * 1024);
        assert!(StreamExt::next(&mut stream).await.is_none());
    }

    #[tokio::test]
    async fn events_stream_yields_multiple_events_in_order() {
        let signal = ChangeSignal::default();
        let mut changes = signal.subscribe();
        let events = vec![
            ChangeEvent {
                key: StorageKey {
                    table: "test".into(),
                    key: b"key1".to_vec(),
                },
                blob: vec![1],
                generation: 2,
            },
            ChangeEvent {
                key: StorageKey {
                    table: "test".into(),
                    key: b"key2".to_vec(),
                },
                blob: vec![2],
                generation: 1,
            },
            ChangeEvent {
                key: StorageKey {
                    table: "test".into(),
                    key: b"key3".to_vec(),
                },
                blob: vec![3],
                generation: 3,
            },
        ];
        signal.committed_with_events(events);
        #[allow(deprecated)]
        changes.changed().await.expect("notification received");
        let mut stream = changes.events();
        let e1 = StreamExt::next(&mut stream).await.unwrap();
        let e2 = StreamExt::next(&mut stream).await.unwrap();
        let e3 = StreamExt::next(&mut stream).await.unwrap();
        assert_eq!(e1.generation, 1);
        assert_eq!(e2.generation, 2);
        assert_eq!(e3.generation, 3);
        assert!(StreamExt::next(&mut stream).await.is_none());
    }
}

#[cfg(test)]
mod change_event_tests2 {
    use crate::changes::{ChangeEvent, ChangeSignal, StorageKey};
    use futures::StreamExt;

    #[tokio::test]
    async fn committed_with_events_large_blob_fallback() {
        let signal = ChangeSignal::default();
        let mut changes = signal.subscribe();
        let large_blob = vec![0u8; 1024 * 1024]; // 1MB
        let event = ChangeEvent {
            key: StorageKey {
                table: "test".into(),
                key: b"key1".to_vec(),
            },
            blob: large_blob.clone(),
            generation: 1,
        };
        signal.committed_with_events([event]);
        #[allow(deprecated)]
        changes.changed().await.expect("notification received");
        let mut stream = changes.events();
        let e = StreamExt::next(&mut stream).await.unwrap();
        assert_eq!(e.blob.len(), 1024 * 1024);
        assert!(StreamExt::next(&mut stream).await.is_none());
    }

    #[tokio::test]
    async fn events_stream_yields_multiple_events_in_order() {
        let signal = ChangeSignal::default();
        let mut changes = signal.subscribe();
        let events = vec![
            ChangeEvent {
                key: StorageKey {
                    table: "test".into(),
                    key: b"key1".to_vec(),
                },
                blob: vec![1],
                generation: 2,
            },
            ChangeEvent {
                key: StorageKey {
                    table: "test".into(),
                    key: b"key2".to_vec(),
                },
                blob: vec![2],
                generation: 1,
            },
            ChangeEvent {
                key: StorageKey {
                    table: "test".into(),
                    key: b"key3".to_vec(),
                },
                blob: vec![3],
                generation: 3,
            },
        ];
        signal.committed_with_events(events);
        #[allow(deprecated)]
        changes.changed().await.expect("notification received");
        let mut stream = changes.events();
        let e1 = StreamExt::next(&mut stream).await.unwrap();
        let e2 = StreamExt::next(&mut stream).await.unwrap();
        let e3 = StreamExt::next(&mut stream).await.unwrap();
        assert_eq!(e1.generation, 1);
        assert_eq!(e2.generation, 2);
        assert_eq!(e3.generation, 3);
        assert!(StreamExt::next(&mut stream).await.is_none());
    }
}
