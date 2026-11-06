#![cfg(all(feature = "sql", feature = "in-memory"))]

use ofdb_sql::{Client, Database, Migration, Row};

fn plan() -> [Migration; 2] {
    [
        Migration::new(
            1,
            "m1",
            "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER, pending INTEGER); INSERT INTO items VALUES (1,10,11)",
        ),
        Migration::new(
            2,
            "m2",
            "UPDATE items SET amount = pending; UPDATE items SET pending = 0",
        ),
    ]
}

async fn assert_applied_once(client: &Client) {
    let rows = client
        .execute_sql("SELECT amount FROM items", None)
        .await
        .expect("read data");
    assert_eq!(rows[0].rows, vec![Row::from([11_i64])]);
    let history = client
        .execute_sql(
            "SELECT version FROM _ofdb_migrations ORDER BY version",
            None,
        )
        .await
        .expect("read history");
    assert_eq!(
        history[0].rows,
        vec![Row::from([1_i64]), Row::from([2_i64])]
    );
    let repeated = client
        .migrate(&plan())
        .await
        .expect("explicit follow-up reads history");
    assert_eq!(repeated.current_version, 2);
    assert!(repeated.applied_versions.is_empty());
}

async fn concurrent_calls(client: &Client) {
    let plan = plan();
    client.migrate(&plan[..1]).await.expect("initialize");
    let (first, second) = futures::join!(client.migrate(&plan), client.migrate(&plan));
    let applied: usize = [&first, &second]
        .into_iter()
        .filter_map(|result| result.as_ref().ok())
        .map(|result| result.applied_versions.len())
        .sum();
    assert_eq!(
        applied, 1,
        "only one call can apply the transformation: {first:?}, {second:?}"
    );
    assert!(first.is_ok() || second.is_ok());
    assert_applied_once(client).await;
    client
        .execute_sql("INSERT INTO items VALUES (2,20,21)", None)
        .await
        .expect("transactions released after race");
}

#[test]
fn embedded_concurrent_calls_apply_each_version_once() {
    futures::executor::block_on(concurrent_calls(&Database::in_memory().client()));
}

#[cfg(all(feature = "remote", feature = "server"))]
mod remote {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use ofdb_sql::{Error, ErrorKind, MigrationPhase, Query, QueryResult, Statement, Transaction};
    use protocol::{QueryExecutor, QueryServiceError, QueryTransaction};
    use tokio::sync::{Notify, oneshot};

    use super::{Client, Database, assert_applied_once, concurrent_calls, plan};

    #[derive(Default)]
    struct Control {
        stage: AtomicU8,
        executes: AtomicUsize,
        malformed_at: AtomicUsize,
        extra_result: AtomicBool,
        reject_after_commit: AtomicBool,
        reject_begin: AtomicBool,
        reached: Notify,
        release: Notify,
        cleaned: Notify,
        commit_finished: Notify,
    }

    struct ControlledExecutor {
        client: Client,
        control: Arc<Control>,
    }

    struct ControlledTransaction {
        transaction: Option<Transaction>,
        control: Arc<Control>,
    }

    fn service_error(error: Error) -> QueryServiceError {
        match error.kind {
            ErrorKind::Rejected => QueryServiceError::Rejected(error.message),
            ErrorKind::Storage => QueryServiceError::Storage(error.message),
            _ => QueryServiceError::Internal(error.message),
        }
    }

    impl QueryExecutor for ControlledExecutor {
        async fn execute(
            &self,
            statements: Vec<Statement>,
        ) -> Result<Vec<QueryResult>, QueryServiceError> {
            self.client.execute(statements).await.map_err(service_error)
        }

        async fn transaction(&self) -> Result<Box<dyn QueryTransaction>, QueryServiceError> {
            if self.control.reject_begin.load(Ordering::SeqCst) {
                return Err(QueryServiceError::Rejected("begin disabled".into()));
            }
            Ok(Box::new(ControlledTransaction {
                transaction: Some(self.client.transaction().await.map_err(service_error)?),
                control: Arc::clone(&self.control),
            }))
        }
    }

    impl QueryTransaction for ControlledTransaction {
        fn execute(
            &mut self,
            statements: Vec<Statement>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<QueryResult>, QueryServiceError>> + Send + '_>>
        {
            Box::pin(async move {
                let writes = statements
                    .iter()
                    .any(|statement| matches!(statement, Statement::Query(Query::Update(_))));
                let result = self
                    .transaction
                    .as_mut()
                    .expect("active transaction")
                    .execute(statements)
                    .await
                    .map_err(service_error)?;
                if writes
                    && self
                        .control
                        .stage
                        .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    self.control.reached.notify_one();
                    self.control.release.notified().await;
                }
                let number = self.control.executes.fetch_add(1, Ordering::SeqCst) + 1;
                if number == self.control.malformed_at.load(Ordering::SeqCst) {
                    if self.control.extra_result.load(Ordering::SeqCst) {
                        let mut result = result;
                        result.push(result[0].clone());
                        return Ok(result);
                    }
                    return Ok(Vec::new());
                }
                Ok(result)
            })
        }

        fn commit(
            mut self: Box<Self>,
        ) -> Pin<Box<dyn Future<Output = Result<(), QueryServiceError>> + Send>> {
            Box::pin(async move {
                self.transaction
                    .take()
                    .expect("active commit")
                    .commit()
                    .await
                    .map_err(service_error)?;
                if self.control.reject_after_commit.load(Ordering::SeqCst) {
                    self.control.reject_begin.store(true, Ordering::SeqCst);
                }
                if self
                    .control
                    .stage
                    .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.control.reached.notify_one();
                    self.control.release.notified().await;
                    self.control.commit_finished.notify_one();
                }
                Ok(())
            })
        }

        fn rollback(
            mut self: Box<Self>,
        ) -> Pin<Box<dyn Future<Output = Result<(), QueryServiceError>> + Send>> {
            Box::pin(async move {
                self.transaction
                    .take()
                    .expect("active rollback")
                    .rollback()
                    .await
                    .map_err(service_error)?;
                self.control.cleaned.notify_one();
                Ok(())
            })
        }
    }

    async fn start() -> (
        Client,
        Arc<Control>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve address");
        let address = listener.local_addr().expect("address");
        drop(listener);
        let control = Arc::new(Control::default());
        let executor = ControlledExecutor {
            client: Database::in_memory().client(),
            control: Arc::clone(&control),
        };
        let (shutdown, signal) = oneshot::channel();
        let server = tokio::spawn(async move {
            ofdb_sql::Server::tcp(address, executor)
                .serve(async {
                    let _ = signal.await;
                })
                .await
                .expect("serve SQL");
        });
        let client = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(client) = Client::connect(format!("http://{address}")).await {
                    break client;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("server starts");
        (client, control, shutdown, server)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_final_commit_returns_success_without_another_begin() {
        let (client, control, shutdown, server) = start().await;
        client.migrate(&plan()[..1]).await.expect("initialize");
        control.reject_after_commit.store(true, Ordering::SeqCst);
        let result = client
            .migrate(&plan())
            .await
            .expect("final commit completes migration");
        assert_eq!(result.current_version, 2);
        assert_eq!(result.applied_versions, vec![2]);
        assert_eq!(
            client
                .transaction()
                .await
                .err()
                .expect("next begin is disabled")
                .kind,
            ErrorKind::Rejected
        );
        control.reject_begin.store(false, Ordering::SeqCst);
        assert_applied_once(&client).await;
        drop(client);
        shutdown.send(()).expect("shutdown");
        server.await.expect("join server");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_malformed_history_results_return_protocol_errors() {
        for step in 1..=3 {
            let (client, control, shutdown, server) = start().await;
            if step > 1 {
                client
                    .migrate(&plan()[..1])
                    .await
                    .expect("initialize history");
            }
            control.executes.store(0, Ordering::SeqCst);
            control.malformed_at.store(step, Ordering::SeqCst);
            let error = client
                .migrate(&if step == 1 {
                    vec![]
                } else {
                    plan()[..1].to_vec()
                })
                .await
                .expect_err("empty response must not panic");
            assert_eq!(error.phase, MigrationPhase::History);
            assert_eq!(error.source.kind, ErrorKind::Protocol);
            drop(client);
            shutdown.send(()).expect("shutdown");
            server.await.expect("join server");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_record_read_failures_preserve_protocol_source_and_active_version() {
        let (client, control, shutdown, server) = start().await;
        client.migrate(&plan()[..1]).await.expect("initialize");
        for step in 5..=7 {
            control.executes.store(0, Ordering::SeqCst);
            control.malformed_at.store(step, Ordering::SeqCst);
            let error = client
                .migrate(&plan())
                .await
                .expect_err("post-execution response missing");
            assert_eq!(error.version, Some(2));
            assert_eq!(error.phase, MigrationPhase::Record);
            assert_eq!(error.source.kind, ErrorKind::Protocol);
            assert_eq!(
                error.source.message,
                "transaction execute result count mismatch"
            );
            assert!(!error.commit_outcome_unknown());
            let rows = client
                .execute_sql("SELECT amount FROM items", None)
                .await
                .expect("rolled back data");
            assert_eq!(rows[0].rows, vec![super::Row::from([10_i64])]);
            let history = client
                .execute_sql("SELECT version FROM _ofdb_migrations", None)
                .await
                .expect("no false record");
            assert_eq!(history[0].rows, vec![super::Row::from([1_i64])]);
        }
        control.malformed_at.store(0, Ordering::SeqCst);
        assert_eq!(
            client
                .migrate(&plan())
                .await
                .expect("resume")
                .applied_versions,
            vec![2]
        );
        drop(client);
        shutdown.send(()).expect("shutdown");
        server.await.expect("join server");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_transaction_rejects_extra_results() {
        let (client, control, shutdown, server) = start().await;
        control.malformed_at.store(1, Ordering::SeqCst);
        control.extra_result.store(true, Ordering::SeqCst);
        let mut tx = client.transaction().await.expect("begin");
        let error = tx
            .execute_sql("SELECT name FROM tables", None)
            .await
            .expect_err("extra result");
        assert_eq!(error.kind, ErrorKind::Protocol);
        tx.rollback().await.expect("rollback malformed response");
        drop(client);
        shutdown.send(()).expect("shutdown");
        server.await.expect("join server");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_concurrent_calls_apply_each_version_once() {
        let (client, _, shutdown, server) = start().await;
        concurrent_calls(&client).await;
        drop(client);
        shutdown.send(()).expect("shutdown");
        server.await.expect("join server");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_stale_migration_cannot_commit_its_staged_transformation() {
        let (client, control, shutdown, server) = start().await;
        client.migrate(&plan()[..1]).await.expect("initialize");
        control.stage.store(1, Ordering::SeqCst);
        let losing_client = client.clone();
        let loser = tokio::spawn(async move { losing_client.migrate(&plan()).await });
        tokio::time::timeout(Duration::from_secs(5), control.reached.notified())
            .await
            .expect("loser staged transformation");
        assert_eq!(
            client
                .migrate(&plan())
                .await
                .expect("winner commits")
                .applied_versions,
            vec![2]
        );
        control.release.notify_one();
        let error = loser
            .await
            .expect("join loser")
            .expect_err("stale revision rejects commit");
        assert_eq!(error.version, Some(2));
        assert_eq!(error.phase, MigrationPhase::Commit);
        assert_eq!(error.source.kind, ErrorKind::Storage);
        assert!(error.commit_outcome_unknown());
        assert_applied_once(&client).await;
        client
            .execute_sql("INSERT INTO items VALUES (2,20,21)", None)
            .await
            .expect("failed transaction released");
        drop(client);
        shutdown.send(()).expect("shutdown");
        server.await.expect("join server");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_deadline_before_commit_rolls_back_staged_transformation() {
        let (client, control, shutdown, server) = start().await;
        client.migrate(&plan()[..1]).await.expect("initialize");
        control.stage.store(1, Ordering::SeqCst);
        let timed = client.clone().with_deadline(Duration::from_millis(500));
        let pending = tokio::spawn(async move { timed.migrate(&plan()).await });
        tokio::time::timeout(Duration::from_secs(5), control.reached.notified())
            .await
            .expect("staged update reached");
        let error = pending
            .await
            .expect("join migration")
            .expect_err("response exceeded deadline");
        assert_eq!(error.version, Some(2));
        assert_eq!(error.phase, MigrationPhase::Execute);
        assert_eq!(error.source.kind, ErrorKind::Timeout);
        assert!(!error.commit_outcome_unknown());
        control.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), control.cleaned.notified())
            .await
            .expect("server rolled back closed stream");
        let rows = client
            .execute_sql("SELECT amount FROM items", None)
            .await
            .expect("uncommitted change absent");
        assert_eq!(rows[0].rows, vec![ofdb_sql::Row::from([10_i64])]);
        assert_eq!(
            client
                .migrate(&plan())
                .await
                .expect("explicit retry")
                .applied_versions,
            vec![2]
        );
        assert_applied_once(&client).await;
        drop(client);
        shutdown.send(()).expect("shutdown");
        server.await.expect("join server");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_lost_commit_response_is_uncertain_and_follow_up_skips_transformation() {
        let (client, control, shutdown, server) = start().await;
        client.migrate(&plan()[..1]).await.expect("initialize");
        control.stage.store(2, Ordering::SeqCst);
        let timed = client.clone().with_deadline(Duration::from_millis(500));
        let pending = tokio::spawn(async move { timed.migrate(&plan()).await });
        tokio::time::timeout(Duration::from_secs(5), control.reached.notified())
            .await
            .expect("commit completed before response");
        let error = pending
            .await
            .expect("join migration")
            .expect_err("completion response withheld");
        assert_eq!(error.version, Some(2));
        assert_eq!(error.phase, MigrationPhase::Commit);
        assert_eq!(error.source.kind, ErrorKind::Timeout);
        assert!(error.commit_outcome_unknown());
        assert!(!error.to_string().contains("UPDATE"));
        assert_applied_once(&client).await;
        control.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), control.commit_finished.notified())
            .await
            .expect("commit handler released");
        drop(client);
        shutdown.send(()).expect("shutdown");
        server.await.expect("join server");
    }
}
