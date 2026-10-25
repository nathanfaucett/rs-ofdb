#![cfg(not(target_arch = "wasm32"))]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_clients_and_transactions_remain_spawnable() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<ofdb_kv::Database>();
    send_sync::<ofdb_kv::Client>();
    send_sync::<ofdb_sql::Database>();
    send_sync::<ofdb_sql::Client>();
    let kv = ofdb_kv::Database::in_memory().client();
    let sql = ofdb_sql::Database::in_memory().client();
    tokio::spawn(async move {
        kv.set("spawned", value::Value::Bool(true), None)
            .await
            .expect("spawned KV write");
        let mut transaction = kv.transaction().await.expect("spawned KV transaction");
        transaction
            .set("committed", value::Value::Bool(true), None)
            .await
            .expect("spawned transaction write");
        transaction.commit().await.expect("spawned KV commit");
        assert_eq!(
            kv.get("committed").await.expect("spawned read"),
            Some(value::Value::Bool(true))
        );
        sql.translate_and_execute(
            "CREATE TABLE people (name TEXT PRIMARY KEY)",
            &ofdb_sql::SqlTranslator,
        )
        .await
        .expect("spawned SQL DDL");
        let mut transaction = sql.transaction().await.expect("spawned SQL transaction");
        transaction
            .translate_and_execute(
                "INSERT INTO people VALUES ('Ada')",
                &ofdb_sql::SqlTranslator,
            )
            .await
            .expect("spawned SQL write");
        transaction.commit().await.expect("spawned SQL commit");
        assert_eq!(
            sql.translate_and_execute("SELECT name FROM people", &ofdb_sql::SqlTranslator)
                .await
                .expect("spawned SQL read")[0]
                .rows,
            vec![ofdb_sql::Row::from(["Ada"])]
        );
    })
    .await
    .expect("native spawned queries retain Send");
}

#[cfg(feature = "sync")]
mod sync {
    struct Closed;
    impl ofdb_kv::SyncTransport for Closed {
        type Error = &'static str;
        async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
            Err("closed")
        }
        async fn send(&mut self, _: Vec<u8>) -> Result<(), Self::Error> {
            Err("closed")
        }
    }
    impl ofdb_sql::SyncTransport for Closed {
        type Error = &'static str;
        async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
            Err("closed")
        }
        async fn send(&mut self, _: Vec<u8>) -> Result<(), Self::Error> {
            Err("closed")
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_sync_work_remains_spawnable() {
        let kv = ofdb_kv::Database::in_memory();
        let sql = ofdb_sql::Database::in_memory();
        tokio::spawn(async move {
            assert!(
                kv.synchronize(
                    &mut Closed,
                    ofdb_kv::SyncRole::Initiator,
                    ofdb_kv::SyncConfig::default()
                )
                .await
                .is_err()
            );
            assert!(
                sql.synchronize(
                    &mut Closed,
                    &ofdb_sql::SessionConfig::default(),
                    ofdb_sql::SyncRole::Initiator
                )
                .await
                .is_err()
            );
        })
        .await
        .expect("native sync retains Send");
    }
}

#[cfg(feature = "sync")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_continuous_runner_retains_send_retry_and_shutdown() {
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures::{StreamExt, channel::mpsc};

    fn send_sync<T: Send + Sync>() {}
    send_sync::<ofdb_sql::ChangeSubscription>();
    send_sync::<ofdb_kv::ChangeSubscription>();
    let database = ofdb_sql::Database::in_memory();
    let client = database.client();
    let changes = database.sync_changes().expect("native SQL subscription");
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (started, mut starts) = mpsc::unbounded();
    let runner = tokio::spawn(changes.run(move || {
        let attempt = count.fetch_add(1, Ordering::SeqCst);
        started.unbounded_send(attempt).expect("notify caller");
        async move {
            match attempt {
                0 => Err(()),
                1 => Ok(()),
                _ => pending::<Result<(), ()>>().await,
            }
        }
    }));
    assert_eq!(starts.next().await, Some(0));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), starts.next())
            .await
            .expect("native timer retry"),
        Some(1)
    );
    client
        .execute_sql("CREATE TABLE runner (name TEXT PRIMARY KEY)", None)
        .await
        .expect("commit");
    assert_eq!(starts.next().await, Some(2));
    drop(database);
    assert!(!runner.is_finished(), "query handle keeps backend alive");
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("backend close interrupts callback")
        .expect("spawned runner remains Send");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}
