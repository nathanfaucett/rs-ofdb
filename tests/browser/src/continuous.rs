use std::{
    cell::{Cell, RefCell},
    future::{Future, pending},
    rc::Rc,
    time::Duration,
};

use futures::{
    FutureExt, StreamExt,
    channel::{mpsc, oneshot},
    future::{Either, join, select},
};

use ofdb_kv::{Database, Value};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn continuous_closed_backend_does_not_start_transport() {
    let database = Database::open_indexeddb(&format!("runner-{}", uuid::Uuid::now_v7()))
        .await
        .expect("open persistent KV");
    let changes = database.sync_changes().expect("subscribe");
    let calls = Rc::new(Cell::new(0));
    drop(database);
    changes
        .run(|| {
            calls.set(calls.get() + 1);
            async { Ok::<(), ()>(()) }
        })
        .await;
    assert_eq!(
        calls.get(),
        0,
        "closed storage must not start caller transport"
    );
}

async fn database() -> Database {
    Database::open_indexeddb(&format!("runner-{}", uuid::Uuid::now_v7()))
        .await
        .expect("open persistent KV")
}

async fn pause(millis: u64) {
    platform::sleep(Duration::from_millis(millis)).await;
}

async fn during(runner: impl Future<Output = ()>, scenario: impl Future<Output = ()>) {
    match select(Box::pin(runner), Box::pin(scenario)).await {
        Either::Left(_) => panic!("runner stopped before caller cancellation"),
        Either::Right(((), runner)) => drop(runner),
    }
}

#[wasm_bindgen_test]
async fn continuous_initial_coalescing_and_selected_ack_retain_later_commits() {
    let database = database().await;
    let client = database.client();
    client
        .set("seed", Value::Integer(0), None)
        .await
        .expect("seed");
    let changes = database.sync_changes().expect("runner subscription");
    let mut independent = database.sync_changes().expect("independent peer");
    let selected = independent.select().expect("initial work");
    let snapshots = Rc::new(RefCell::new(Vec::new()));
    let (started, start) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    let mut first = Some((started, gate));
    let runner = changes.run(|| {
        let first = first.take();
        let client = client.clone();
        let snapshots = snapshots.clone();
        async move {
            let snapshot = client.scan_all().await.expect("snapshot");
            snapshots.borrow_mut().push(snapshot);
            if let Some((started, gate)) = first {
                started.send(()).expect("started");
                gate.await.expect("caller release");
            }
            Ok::<(), ()>(())
        }
    });
    assert!(
        snapshots.borrow().is_empty(),
        "construction must not spawn work"
    );
    during(runner, async {
        start.await.expect("initial reconciliation read");
        for key in ["a", "b", "c"] {
            client
                .set(key, Value::Bool(true), None)
                .await
                .expect("later commit");
        }
        assert_eq!(snapshots.borrow().len(), 1, "in-flight commits coalesce");
        assert!(independent.acknowledge(selected));
        assert!(
            independent.select().is_some(),
            "old ack retains later commits"
        );
        release.send(()).expect("finish initial pass");
        pause(30).await;
        {
            let reads = snapshots.borrow();
            assert_eq!(reads.len(), 2, "runner ack must also retain later commits");
            assert_eq!(reads[0], vec![("seed".into(), Value::Integer(0))]);
            assert_eq!(reads[1].len(), 4, "second pass observes all three commits");
        }
        pause(150).await;
        assert_eq!(
            snapshots.borrow().len(),
            2,
            "successful runner remains idle"
        );
        assert!(
            independent.select().is_some(),
            "runner cannot ack another peer"
        );
    })
    .await;
}

#[wasm_bindgen_test]
async fn continuous_real_browser_backoff_reaches_30_seconds_and_resets() {
    let database = database().await;
    let client = database.client();
    client
        .set("seed", Value::Bool(true), None)
        .await
        .expect("seed");
    let changes = database.sync_changes().expect("subscribe");
    let calls = Rc::new(Cell::new(0));
    let (starts, mut attempts) = mpsc::unbounded();
    let runner = changes.run(|| {
        let attempt = calls.get();
        calls.set(attempt + 1);
        starts.unbounded_send(js_sys::Date::now()).expect("attempt");
        async move {
            if attempt == 11 || attempt == 13 {
                Ok(())
            } else {
                Err(())
            }
        }
    });
    during(runner, async {
        let mut previous = attempts.next().await.expect("initial failed pass");
        client
            .set("during-backoff", Value::Bool(true), None)
            .await
            .expect("commit");
        pause(30).await;
        assert_eq!(calls.get(), 1, "a commit cannot bypass retry backoff");
        for delay in [
            100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 30000, 30000,
        ] {
            let now = platform::timeout(Duration::from_millis(delay + 10000), attempts.next())
                .await
                .expect("retry must not exceed the cap")
                .expect("retry without a new commit");
            assert!(
                now - previous >= delay as f64 - 2.0,
                "retry fired early: {} < {delay}",
                now - previous
            );
            previous = now;
        }
        pause(250).await;
        assert_eq!(calls.get(), 12, "successful retry returns to idle");
        client
            .set("reset", Value::Bool(true), None)
            .await
            .expect("new work");
        let failed = attempts.next().await.expect("new failed pass");
        let retried = platform::timeout(Duration::from_secs(2), attempts.next())
            .await
            .expect("success resets retry to 100 ms")
            .expect("retry");
        assert!(retried - failed >= 98.0, "reset retry cannot be immediate");
        pause(250).await;
        assert_eq!(calls.get(), 14, "reset retry also settles");
    })
    .await;
}

struct Dropped(Rc<Cell<usize>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[wasm_bindgen_test]
async fn continuous_caller_drop_cancels_inflight_local_callback() {
    let database = database().await;
    let drops = Rc::new(Cell::new(0));
    let calls = Rc::new(Cell::new(0));
    let runner = database.sync_changes().expect("subscribe").run(|| {
        calls.set(calls.get() + 1);
        let guard = Dropped(drops.clone());
        async move {
            let _guard = guard;
            pending::<Result<(), ()>>().await
        }
    });
    during(runner, async {
        pause(20).await;
        assert_eq!(calls.get(), 1);
        assert_eq!(drops.get(), 0, "callback is in flight");
    })
    .await;
    assert_eq!(drops.get(), 1, "dropping caller future drops callback");
    database
        .client()
        .set("after-cancel", Value::Bool(true), None)
        .await
        .expect("commit");
    pause(150).await;
    assert_eq!(
        calls.get(),
        1,
        "no autonomous replication after cancellation"
    );
}

#[wasm_bindgen_test]
async fn continuous_caller_drop_cancels_retry_and_reconnects_broadly() {
    let database = database().await;
    let mut changes = database.sync_changes().expect("subscribe");
    let initial = changes.select().expect("initial work");
    assert!(changes.acknowledge(initial));
    database
        .client()
        .set("pending", Value::Bool(true), None)
        .await
        .expect("commit");
    let calls = Rc::new(Cell::new(0));
    let mut runner = Box::pin(changes.run(|| {
        calls.set(calls.get() + 1);
        async { Err::<(), ()>(()) }
    }));
    assert!(runner.as_mut().now_or_never().is_none());
    assert_eq!(calls.get(), 1);
    drop(runner);
    pause(250).await;
    assert_eq!(calls.get(), 1, "cancelled retry cannot invoke transport");
    let fresh = database.sync_changes().expect("reconnect broadly");
    assert!(
        fresh.select().is_some(),
        "cancellation does not erase stored work"
    );
    assert_eq!(
        database.client().get("pending").await.expect("read"),
        Some(Value::Bool(true))
    );
}

#[wasm_bindgen_test]
async fn continuous_merged_backend_close_cancels_callback_and_retry() {
    for retry in [false, true] {
        let sql = ofdb_sql::Database::open_indexeddb(&format!("runner-{}", uuid::Uuid::now_v7()))
            .await
            .expect("open SQL");
        let kv = database().await;
        let client = kv.client();
        let changes = sql
            .sync_changes()
            .expect("SQL subscription")
            .merge(kv.sync_changes().expect("KV subscription"));
        let calls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let runner = changes.run(|| {
            calls.set(calls.get() + 1);
            let guard = Dropped(drops.clone());
            async move {
                let _guard = guard;
                if retry {
                    Err(())
                } else {
                    pending::<Result<(), ()>>().await
                }
            }
        });
        platform::timeout(
            Duration::from_secs(2),
            join(runner, async {
                pause(10).await;
                assert_eq!(calls.get(), 1);
                drop(sql);
                drop(kv);
                pause(20).await;
                assert_eq!(
                    calls.get(),
                    1,
                    "one live backend preserves the shared runner"
                );
                assert_eq!(
                    drops.get(),
                    usize::from(retry),
                    "pending callback remains alive"
                );
                drop(client);
            }),
        )
        .await
        .expect("last backend close must interrupt callback or timer");
        assert_eq!(drops.get(), 1);
        pause(150).await;
        assert_eq!(calls.get(), 1, "closure cannot leave a retry behind");
    }
}

#[wasm_bindgen_test]
async fn continuous_merged_sql_kv_broad_sessions_converge_and_settle() {
    let sql = ofdb_sql::Database::open_indexeddb(&format!("runner-{}", uuid::Uuid::now_v7()))
        .await
        .expect("open SQL");
    let sql_peer =
        ofdb_sql::Database::open_indexeddb(&format!("runner-peer-{}", uuid::Uuid::now_v7()))
            .await
            .expect("open SQL peer");
    let kv = database().await;
    let kv_peer = database().await;
    sql.client()
        .execute_sql("CREATE TABLE items (name TEXT PRIMARY KEY)", None)
        .await
        .expect("DDL");
    kv.client()
        .set("seed", Value::Integer(1), None)
        .await
        .expect("seed");
    let changes = sql
        .sync_changes()
        .expect("SQL subscription")
        .merge(kv.sync_changes().expect("same shared KV subscription type"));
    let (release, gate) = oneshot::channel();
    let mut gate = Some(gate);
    let (done, mut completed) = mpsc::unbounded();
    let calls = Rc::new(Cell::new(0));
    let runner = changes.run(|| {
        calls.set(calls.get() + 1);
        let gate = gate.take();
        let done = done.clone();
        let (sql, sql_peer, kv, kv_peer) = (&sql, &sql_peer, &kv, &kv_peer);
        async move {
            if let Some(gate) = gate {
                gate.await.expect("caller release");
            }
            super::sql_sync_support::broad(sql, sql_peer).await;
            super::kv_sync::exchange(kv, kv_peer).await;
            done.unbounded_send(()).expect("session completed");
            Ok::<(), ()>(())
        }
    });
    during(runner, async {
        sql.client()
            .execute_sql("INSERT INTO items VALUES ('later')", None)
            .await
            .expect("SQL commit");
        kv.client()
            .set("later", Value::Integer(2), None)
            .await
            .expect("KV commit");
        assert_eq!(calls.get(), 1, "commits merge during initial callback");
        release.send(()).expect("release exchange");
        for _ in 0..2 {
            platform::timeout(Duration::from_secs(10), completed.next())
                .await
                .expect("broad session")
                .expect("completion");
        }
        assert_eq!(
            sql_peer
                .client()
                .execute_sql("SELECT name FROM items", None)
                .await
                .expect("replicated SQL")[0]
                .rows,
            vec![ofdb_sql::Row::from(["later"])]
        );
        assert_eq!(
            kv_peer.client().scan_all().await.expect("replicated KV"),
            vec![
                ("later".into(), Value::Integer(2)),
                ("seed".into(), Value::Integer(1))
            ]
        );
        pause(150).await;
        assert_eq!(
            calls.get(),
            2,
            "broad replay settles without notification echoes"
        );
    })
    .await;
}
