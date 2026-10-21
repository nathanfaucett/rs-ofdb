#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    struct Channel {
        sender: mpsc::UnboundedSender<Vec<u8>>,
        receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    }
    impl ofdb_sql::SyncTransport for Channel {
        type Error = &'static str;
        async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
            self.sender.send(frame).map_err(|_| "closed")
        }
        async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
            self.receiver.recv().await.ok_or("closed")
        }
    }
    impl ofdb_kv::SyncTransport for Channel {
        type Error = &'static str;
        async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
            self.sender.send(frame).map_err(|_| "closed")
        }
        async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
            self.receiver.recv().await.ok_or("closed")
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
    #[tokio::test]
    async fn sql_and_kv_share_one_public_continuous_runner() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let dir = tempfile::tempdir().expect("directory");
        let sql = ofdb_sql::Database::open(dir.path().join("continuous-sql.redb")).expect("SQL");
        let kv = ofdb_kv::Database::open(dir.path().join("continuous-kv.redb")).expect("KV");
        let changes: ofdb_sql::ChangeSubscription = sql
            .sync_changes()
            .expect("SQL lifecycle")
            .merge(kv.sync_changes().expect("KV lifecycle"));
        let changes: ofdb_kv::ChangeSubscription = changes;
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let runner = tokio::spawn(changes.run(move || {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok::<(), ()>(()) }
        }));
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        sql.client()
            .execute_sql("CREATE TABLE items (id UUID PRIMARY KEY)", None)
            .await
            .expect("SQL commit");
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        kv.client()
            .set("key", ofdb_kv::Value::Integer(1), None)
            .await
            .expect("KV commit");
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        runner.abort();
        assert!(runner.await.expect_err("stopped").is_cancelled());
    }

    #[tokio::test]
    async fn persisted_sql_success_is_not_rolled_back_by_subsequent_kv_denial() {
        let dir = tempfile::tempdir().expect("directory");
        let sql_source = ofdb_sql::Database::in_memory();
        let sql_path = dir.path().join("sql.redb");
        let sql_target = ofdb_sql::Database::open(&sql_path).expect("SQL open");
        sql_source
            .client()
            .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", None)
            .await
            .expect("schema");
        sql_source.client().execute_sql("INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", None).await.expect("row");
        let (mut a, mut b) = channels();
        let config = ofdb_sql::SessionConfig::default();
        let stages = dir.path().join("stages");
        let (a, b) = tokio::join!(
            sql_source.synchronize_checked(&mut a, &config, ofdb_sql::SyncRole::Initiator, || {
                core::future::ready(Ok(()))
            }),
            sql_target.synchronize_with_stage_directory_checked(
                &mut b,
                &config,
                ofdb_sql::SyncRole::Responder,
                &stages,
                || core::future::ready(Ok(()))
            )
        );
        a.expect("SQL initiator");
        b.expect("SQL responder");
        let sql_state = sql_source.export_sync_state().await.expect("SQL state");
        assert_eq!(
            sql_target.export_sync_state().await.expect("SQL applied"),
            sql_state
        );
        assert_eq!(std::fs::read_dir(stages).expect("stage cleanup").count(), 0);

        let kv_source = ofdb_kv::Database::in_memory();
        let kv_path = dir.path().join("kv.redb");
        let kv_target = ofdb_kv::Database::open_with_table(&kv_path, "kv").expect("KV open");
        let mut tx = kv_source.client().transaction().await.expect("transaction");
        tx.set("key", ofdb_kv::Value::Integer(42), None)
            .await
            .expect("typed write");
        tx.commit().await.expect("commit");
        let (mut a, mut b) = channels();
        let (a, b) = tokio::join!(
            kv_source.synchronize_checked(
                &mut a,
                ofdb_kv::SyncRole::Initiator,
                ofdb_kv::SyncConfig::default(),
                || core::future::ready(Ok(()))
            ),
            kv_target.synchronize_checked(
                &mut b,
                ofdb_kv::SyncRole::Responder,
                ofdb_kv::SyncConfig::default(),
                || core::future::ready(Err("caller denied".into()))
            )
        );
        assert!(a.is_err());
        assert!(matches!(b, Err(ofdb_kv::SyncError::PreApply(_))));
        drop(kv_target);
        drop(sql_target);
        let kv_target = ofdb_kv::Database::open_with_table(kv_path, "kv").expect("KV reopen");
        assert_eq!(kv_target.client().get("key").await.expect("denied"), None);
        let sql_target = ofdb_sql::Database::open(sql_path).expect("SQL reopen");
        assert_eq!(
            sql_target
                .export_sync_state()
                .await
                .expect("SQL persists independently"),
            sql_state
        );
    }
}
