use core::{future::Future, time::Duration};

use futures::{
    FutureExt,
    future::{Either, select},
};
use ofdb_kv::Value;

use ofdb_sql::{QueryParams, SqlTranslator, Statement, Translator};
use query::TranslateError;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

struct SlowTranslation;

impl Translator for SlowTranslation {
    async fn translate_with_params(
        &self,
        sql: &str,
        params: Option<&QueryParams>,
    ) -> Result<Vec<Statement>, TranslateError> {
        // Translation may do synchronous CPU work before yielding to the browser.
        let start = js_sys::Date::now();
        while js_sys::Date::now() - start < 250.0 {
            core::hint::spin_loop();
        }
        platform::sleep(Duration::from_millis(200)).await;
        SqlTranslator.translate_with_params(sql, params).await
    }
}

#[wasm_bindgen_test]
async fn deadlines_sql_translation_cpu_and_pending_work_share_one_budget() {
    let client = ofdb_sql::Database::in_memory().client();
    let timed = client.clone().with_deadline(Duration::from_millis(350));
    let error = timed
        .translate_and_execute(
            "CREATE TABLE expired (id TEXT PRIMARY KEY)",
            &SlowTranslation,
        )
        .await
        .expect_err("translation CPU time must not postpone the deadline timer");
    assert_eq!(error.kind, ofdb_sql::QueryErrorKind::Timeout);
    client
        .execute_sql("CREATE TABLE expired (id TEXT PRIMARY KEY)", None)
        .await
        .expect("timed-out translation never executed its DDL");
}

#[wasm_bindgen(module = "/src/deadline_helpers.js")]
extern "C" {
    type Completion;
    #[wasm_bindgen(js_name = holdNextCompletion)]
    fn hold_next_completion(mode: &str) -> Completion;
    #[wasm_bindgen(method, getter)]
    fn completed(this: &Completion) -> js_sys::Promise;
    #[wasm_bindgen(method)]
    fn release(this: &Completion);
}

struct HeldCompletion(Completion);

impl HeldCompletion {
    fn new(mode: &str) -> Self {
        Self(hold_next_completion(mode))
    }

    async fn completed(&self) {
        platform::timeout(Duration::from_secs(5), JsFuture::from(self.0.completed()))
            .await
            .expect("native transaction completed")
            .expect("native completion");
    }
}

impl Drop for HeldCompletion {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn name() -> String {
    format!("deadlines-{}", uuid::Uuid::now_v7())
}

async fn kv_pending<T>(operation: impl Future<Output = Result<T, ofdb_kv::Error>>) {
    let completion = HeldCompletion::new("readonly");
    assert!(matches!(operation.await, Err(ofdb_kv::Error::Timeout)));
    completion.completed().await;
}

#[wasm_bindgen_test]
async fn deadlines_kv_public_pending_queries_and_clone_settings() {
    let db = ofdb_kv::Database::open_indexeddb(&name())
        .await
        .expect("open KV");
    let base = db.client();
    base.set("kept", Value::Integer(7), None)
        .await
        .expect("seed");
    let short = base.clone().with_deadline(Duration::from_millis(20));
    let long = short.clone().with_deadline(Duration::from_secs(5));
    drop(db);
    kv_pending(short.get("kept")).await;
    kv_pending(short.set("canceled", Value::Bool(true), None)).await;
    kv_pending(short.delete("kept")).await;
    kv_pending(short.scan("a", "z")).await;
    kv_pending(short.scan_prefix("k")).await;
    kv_pending(short.scan_all()).await;
    kv_pending(short.transaction()).await;
    assert_eq!(
        base.get("canceled").await.expect("before persistence"),
        None
    );
    assert_eq!(
        base.get("kept").await.expect("delete never began"),
        Some(Value::Integer(7))
    );
    for client in [&base, &long] {
        let completion = HeldCompletion::new("readonly");
        let (result, ()) = futures::join!(client.get("kept"), async {
            completion.completed().await;
            platform::sleep(Duration::from_millis(60)).await;
            drop(completion);
        });
        assert_eq!(
            result.expect("clone has its own budget"),
            Some(Value::Integer(7))
        );
    }
    kv_pending(short.clone().get("kept")).await;
}

async fn sql_pending<T>(operation: impl Future<Output = Result<T, ofdb_sql::QueryError>>) {
    let completion = HeldCompletion::new("readonly");
    assert!(
        matches!(operation.await, Err(error) if error.kind == ofdb_sql::QueryErrorKind::Timeout)
    );
    completion.completed().await;
}

#[derive(Debug)]
struct SelectedRow;

impl ofdb_sql::FromRow for SelectedRow {
    fn from_row(_: &ofdb_sql::Row, _: &[&str]) -> Result<Self, ofdb_sql::FromRowError> {
        Ok(Self)
    }
}

#[wasm_bindgen_test]
async fn deadlines_sql_public_pending_queries_and_clone_settings() {
    let db = ofdb_sql::Database::open_indexeddb(&name())
        .await
        .expect("open SQL");
    let base = db.client();
    base.execute_sql(
        "CREATE TABLE items (id TEXT PRIMARY KEY); INSERT INTO items VALUES ('kept')",
        None,
    )
    .await
    .expect("seed");
    let short = base.clone().with_deadline(Duration::from_millis(20));
    let long = short.clone().with_deadline(Duration::from_secs(5));
    drop(db);
    let statements = SqlTranslator
        .translate("SELECT id FROM items")
        .await
        .expect("typed query");
    sql_pending(short.execute(statements)).await;
    sql_pending(short.execute_sql("INSERT INTO items VALUES ('canceled')", None)).await;
    sql_pending(short.translate_and_execute("SELECT id FROM items", &SqlTranslator)).await;
    sql_pending(short.translate_and_execute_with_params(
        "SELECT id FROM items",
        None,
        &SqlTranslator,
    ))
    .await;
    sql_pending(
        short.translate_and_select::<_, SelectedRow>("SELECT id FROM items", &SqlTranslator),
    )
    .await;
    let key = ofdb_sql::Row::from(["kept"]);
    sql_pending(short.row_conflicts("items", &key)).await;
    sql_pending(short.row_conflicts_batch("items", core::slice::from_ref(&key))).await;
    sql_pending(short.row_conflict_values("items", &key)).await;
    sql_pending(short.resolve_row("items", &key, vec![("id".into(), Value::from("kept"))])).await;
    sql_pending(short.transaction()).await;
    for client in [&base, &long] {
        let completion = HeldCompletion::new("readonly");
        let (result, ()) =
            futures::join!(client.execute_sql("SELECT id FROM items", None), async {
                completion.completed().await;
                platform::sleep(Duration::from_millis(60)).await;
                drop(completion);
            });
        assert_eq!(
            result.expect("independent clone budget")[0].rows,
            vec![ofdb_sql::Row::from(["kept"])]
        );
    }
    sql_pending(short.clone().execute_sql("SELECT id FROM items", None)).await;
}

#[wasm_bindgen_test]
async fn deadlines_timed_transactions_have_per_operation_not_lifetime_budgets() {
    let kv = ofdb_kv::Database::open_indexeddb(&name())
        .await
        .expect("KV")
        .client()
        .with_deadline(Duration::from_millis(100));
    let mut tx = kv.transaction().await.expect("timed KV begin");
    platform::sleep(Duration::from_millis(150)).await;
    tx.set("kept", Value::Integer(7), None)
        .await
        .expect("new write budget");
    assert_eq!(tx.get("kept").await.expect("read"), Some(Value::Integer(7)));
    assert_eq!(tx.scan("a", "z").await.expect("range").len(), 1);
    assert_eq!(tx.scan_prefix("k").await.expect("prefix").len(), 1);
    assert_eq!(tx.scan_all().await.expect("all").len(), 1);
    tx.delete("kept").await.expect("delete");
    tx.set("kept", Value::Integer(8), None)
        .await
        .expect("restage");
    platform::sleep(Duration::from_millis(150)).await;
    tx.commit().await.expect("new commit budget");
    let mut tx = kv.transaction().await.expect("rollback begin");
    tx.delete("kept").await.expect("staged delete");
    platform::sleep(Duration::from_millis(150)).await;
    tx.rollback().await.expect("new rollback budget");
    assert_eq!(
        kv.get("kept").await.expect("rollback preserved value"),
        Some(Value::Integer(8))
    );

    let sql = ofdb_sql::Database::open_indexeddb(&name())
        .await
        .expect("SQL")
        .client()
        .with_deadline(Duration::from_millis(100));
    sql.execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY)", None)
        .await
        .expect("DDL");
    let mut tx = sql.transaction().await.expect("timed SQL begin");
    platform::sleep(Duration::from_millis(150)).await;
    tx.execute_sql("INSERT INTO items VALUES ('kept')", None)
        .await
        .expect("new SQL budget");
    let statements = SqlTranslator
        .translate("SELECT id FROM items")
        .await
        .expect("typed select");
    assert_eq!(
        tx.execute(statements).await.expect("typed tx")[0].rows,
        vec![ofdb_sql::Row::from(["kept"])]
    );
    tx.translate_and_execute("SELECT id FROM items", &SqlTranslator)
        .await
        .expect("translated tx");
    tx.translate_and_execute_with_params("SELECT id FROM items", None, &SqlTranslator)
        .await
        .expect("parameter tx");
    let key = ofdb_sql::Row::from(["kept"]);
    assert!(
        tx.row_conflicts("items", &key)
            .await
            .expect("conflicts")
            .is_empty()
    );
    assert!(
        tx.row_conflict_values("items", &key)
            .await
            .expect("values")
            .is_empty()
    );
    platform::sleep(Duration::from_millis(150)).await;
    tx.commit().await.expect("new SQL commit budget");
    let mut tx = sql.transaction().await.expect("SQL rollback begin");
    tx.execute_sql("DELETE FROM items", None)
        .await
        .expect("staged SQL delete");
    platform::sleep(Duration::from_millis(150)).await;
    tx.rollback().await.expect("new SQL rollback budget");
    assert_eq!(
        sql.execute_sql("SELECT id FROM items", None)
            .await
            .expect("rollback preserved row")[0]
            .rows,
        vec![ofdb_sql::Row::from(["kept"])]
    );
}

struct DelayedTranslation;

impl Translator for DelayedTranslation {
    async fn translate_with_params(
        &self,
        sql: &str,
        params: Option<&QueryParams>,
    ) -> Result<Vec<Statement>, TranslateError> {
        platform::sleep(Duration::from_millis(200)).await;
        SqlTranslator.translate_with_params(sql, params).await
    }
}

#[wasm_bindgen_test]
async fn deadlines_sql_translation_and_persistence_do_not_restart_the_budget() {
    for parameterized in [false, true] {
        let client = ofdb_sql::Database::open_indexeddb(&name())
            .await
            .expect("SQL")
            .client()
            .with_deadline(Duration::from_millis(300));
        let completion = HeldCompletion::new("readwrite");
        let query = async {
            if parameterized {
                client
                    .translate_and_execute_with_params(
                        "CREATE TABLE items (id TEXT PRIMARY KEY)",
                        None,
                        &DelayedTranslation,
                    )
                    .await
            } else {
                client
                    .translate_and_execute(
                        "CREATE TABLE items (id TEXT PRIMARY KEY)",
                        &DelayedTranslation,
                    )
                    .await
            }
        };
        let (result, ()) = futures::join!(query, async {
            completion.completed().await;
            platform::sleep(Duration::from_millis(200)).await;
            drop(completion);
        });
        assert!(matches!(result, Err(error) if error.kind == ofdb_sql::QueryErrorKind::Timeout));
        client
            .execute_sql("SELECT id FROM items", None)
            .await
            .expect("issued DDL committed despite timeout");
    }
}

#[wasm_bindgen(module = "/src/kv_sync_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = afterNextWrite)]
    fn after_next_write() -> js_sys::Promise;
}

async fn abandon_issued<T>(operation: impl Future<Output = T>) {
    let issued = JsFuture::from(after_next_write());
    match platform::timeout(
        Duration::from_secs(5),
        select(Box::pin(operation), Box::pin(issued)),
    )
    .await
    .expect("reach issued native write")
    {
        Either::Right((result, pending)) => {
            result.expect("native write was issued");
            drop(pending);
        }
        Either::Left(_) => panic!("caller must still be pending at the native write"),
    }
}

#[wasm_bindgen_test]
async fn deadlines_kv_issued_query_and_transaction_survive_timeout_and_cancellation() {
    for explicit in [false, true] {
        for cancel in [false, true] {
            let name = name();
            let db = ofdb_kv::Database::open_indexeddb(&name).await.expect("KV");
            let client = db.client();
            let timed = client.clone().with_deadline(Duration::from_millis(100));
            #[cfg(feature = "sync")]
            let mut changes = {
                let mut changes = db.sync_changes().expect("KV subscription");
                let initial = changes.select().expect("initial");
                assert!(changes.acknowledge(initial));
                changes
            };
            let mut tx = if explicit {
                let mut tx = timed.transaction().await.expect("begin");
                tx.set("a", Value::Integer(1), None).await.expect("stage a");
                tx.set("b", Value::Integer(2), None).await.expect("stage b");
                Some(tx)
            } else {
                None
            };
            let completion = HeldCompletion::new("readwrite");
            let operation = async {
                if let Some(tx) = tx.take() {
                    tx.commit().await
                } else {
                    timed.set("a", Value::Integer(1), None).await
                }
            };
            if cancel {
                abandon_issued(operation).await;
            } else {
                assert!(matches!(operation.await, Err(ofdb_kv::Error::Timeout)));
            }
            completion.completed().await;
            #[cfg(feature = "sync")]
            assert!(
                changes.select().is_none(),
                "no publication before observed completion"
            );
            drop(completion);
            #[cfg(feature = "sync")]
            {
                platform::timeout(Duration::from_secs(5), changes.changed())
                    .await
                    .expect("completion observed independently of caller")
                    .expect("committed wakeup");
                assert!(changes.select().is_some(), "commit remains discoverable");
            }
            let reopened = ofdb_kv::Database::open_indexeddb(&name)
                .await
                .expect("independent reopen")
                .client();
            let mut expected = vec![("a".into(), Value::Integer(1))];
            if explicit {
                expected.push(("b".into(), Value::Integer(2)));
            }
            assert_eq!(
                reopened.scan_all().await.expect("persisted whole write"),
                expected
            );
            assert_eq!(
                client.scan_all().await.expect("live shared client"),
                expected
            );
        }
    }
}

#[wasm_bindgen_test]
async fn deadlines_sql_issued_query_and_transaction_survive_timeout_and_cancellation() {
    for explicit in [false, true] {
        for cancel in [false, true] {
            let name = name();
            let db = ofdb_sql::Database::open_indexeddb(&name)
                .await
                .expect("SQL");
            let client = db.client();
            client
                .execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY)", None)
                .await
                .expect("DDL");
            #[cfg(feature = "sync")]
            let mut changes = {
                let mut changes = db.sync_changes().expect("SQL subscription");
                let initial = changes.select().expect("initial work");
                assert!(changes.acknowledge(initial));
                changes
            };
            let timed = client.clone().with_deadline(Duration::from_millis(100));
            let mut tx = if explicit {
                let mut tx = timed.transaction().await.expect("begin");
                tx.execute_sql(
                    "INSERT INTO items VALUES ('a'); INSERT INTO items VALUES ('b')",
                    None,
                )
                .await
                .expect("stage two rows");
                Some(tx)
            } else {
                None
            };
            let completion = HeldCompletion::new("readwrite");
            let operation = async {
                if let Some(tx) = tx.take() {
                    tx.commit().await
                } else {
                    timed
                        .execute_sql(
                            "INSERT INTO items VALUES ('a'); INSERT INTO items VALUES ('b')",
                            None,
                        )
                        .await
                        .map(|_| ())
                }
            };
            if cancel {
                abandon_issued(operation).await;
            } else {
                assert!(
                    matches!(operation.await, Err(error) if error.kind == ofdb_sql::QueryErrorKind::Timeout)
                );
            }
            completion.completed().await;
            #[cfg(feature = "sync")]
            assert!(
                changes.select().is_none(),
                "held completion must not publish early"
            );
            drop(completion);
            #[cfg(feature = "sync")]
            platform::timeout(Duration::from_secs(10), changes.changed())
                .await
                .expect("SQL commit publication survives timeout/cancellation")
                .expect("committed wakeup");
            let reopened = ofdb_sql::Database::open_indexeddb(&name)
                .await
                .expect("independent reopen")
                .client();
            let expected = vec![ofdb_sql::Row::from(["a"]), ofdb_sql::Row::from(["b"])];
            assert_eq!(
                reopened
                    .execute_sql("SELECT id FROM items ORDER BY id", None)
                    .await
                    .expect("persisted atomic batch")[0]
                    .rows,
                expected
            );
            assert_eq!(
                client
                    .execute_sql("SELECT id FROM items ORDER BY id", None)
                    .await
                    .expect("live shared client")[0]
                    .rows,
                expected
            );
        }
    }
}

#[wasm_bindgen_test]
async fn deadlines_cancellation_before_persistence_does_not_write() {
    let kv = ofdb_kv::Database::open_indexeddb(&name())
        .await
        .expect("KV")
        .client();
    let completion = HeldCompletion::new("readonly");
    assert!(
        kv.set("canceled", Value::Bool(true), None)
            .now_or_never()
            .is_none()
    );
    completion.completed().await;
    drop(completion);
    assert_eq!(kv.get("canceled").await.expect("no issued commit"), None);
    let mut tx = kv.transaction().await.expect("staged KV");
    tx.set("canceled", Value::Bool(true), None)
        .await
        .expect("stage KV");
    drop(tx);
    assert!(kv.scan_all().await.expect("drop staging").is_empty());

    let sql = ofdb_sql::Database::open_indexeddb(&name())
        .await
        .expect("SQL")
        .client();
    sql.execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY)", None)
        .await
        .expect("DDL");
    let completion = HeldCompletion::new("readonly");
    assert!(
        sql.execute_sql("INSERT INTO items VALUES ('canceled')", None)
            .now_or_never()
            .is_none()
    );
    completion.completed().await;
    drop(completion);
    let mut tx = sql.transaction().await.expect("staged SQL");
    tx.execute_sql("INSERT INTO items VALUES ('canceled')", None)
        .await
        .expect("stage SQL");
    drop(tx);
    assert!(
        sql.execute_sql("SELECT id FROM items", None)
            .await
            .expect("no issued commit")[0]
            .rows
            .is_empty()
    );
}

#[wasm_bindgen_test]
async fn deadlines_browser_timer_edges_without_tokio() {
    assert_eq!(
        platform::timeout(Duration::ZERO, async { 42 }).await,
        Ok(42)
    );
    assert!(
        platform::timeout(Duration::from_nanos(1), core::future::pending::<()>())
            .await
            .is_err()
    );
    assert!(
        platform::timeout(Duration::from_millis(10), platform::sleep(Duration::MAX))
            .await
            .is_err(),
        "long browser delays must not wrap to immediate completion"
    );
}

struct PendingTranslation;

impl Translator for PendingTranslation {
    async fn translate_with_params(
        &self,
        _: &str,
        _: Option<&QueryParams>,
    ) -> Result<Vec<Statement>, TranslateError> {
        core::future::pending().await
    }
}

#[wasm_bindgen_test]
async fn deadlines_sql_transaction_translation_times_out_before_execution() {
    let client = ofdb_sql::Database::open_indexeddb(&name())
        .await
        .expect("SQL")
        .client();
    client
        .execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY)", None)
        .await
        .expect("DDL");
    let timed = client.clone().with_deadline(Duration::from_millis(100));
    for parameterized in [false, true] {
        let mut tx = timed.transaction().await.expect("timed begin");
        tx.execute_sql("INSERT INTO items VALUES ('staged')", None)
            .await
            .expect("staged write");
        let result = if parameterized {
            tx.translate_and_execute_with_params(
                "INSERT INTO items VALUES ('never')",
                None,
                &PendingTranslation,
            )
            .await
        } else {
            tx.translate_and_execute("INSERT INTO items VALUES ('never')", &PendingTranslation)
                .await
        };
        assert!(matches!(result, Err(error) if error.kind == ofdb_sql::QueryErrorKind::Timeout));
        tx.rollback()
            .await
            .expect("explicitly discard staged writes after timeout");
        assert!(
            client
                .execute_sql("SELECT id FROM items", None)
                .await
                .expect("nothing persisted")[0]
                .rows
                .is_empty()
        );
    }
}
