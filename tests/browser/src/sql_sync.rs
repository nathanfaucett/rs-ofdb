use std::{cell::Cell, rc::Rc, time::Duration};

use futures::{
    FutureExt,
    future::{Either, join, select},
};
use ofdb_sql::{
    ChangeSubscription, Database, Row, SessionConfig, SqlInvalidation, SyncDirection, SyncError,
    SyncMessage, SyncRole, Value,
};
use wasm_bindgen_test::wasm_bindgen_test;

use crate::sql_sync_support::{broad, channels, transcript, verify_fixture};

#[wasm_bindgen_test]
async fn sql_sync_committed_work_survives_shared_client_ownership() {
    let database = Database::open_indexeddb(&format!("sql-sync-{}", uuid::Uuid::now_v7()))
        .await
        .expect("open");
    let client = database.client();
    client
        .execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT)", None)
        .await
        .expect("schema");
    let mut changes = database.sync_changes().expect("browser SQL subscriptions");
    let initial = changes.select().expect("initial broad work");
    assert_eq!(
        database
            .sync_invalidations(&initial)
            .await
            .expect("map initial"),
        vec![SqlInvalidation::All]
    );
    assert!(changes.acknowledge(initial));
    let mut independent = database.sync_changes().expect("independent consumer");
    assert!(independent.acknowledge(independent.select().expect("initial")));
    client
        .clone()
        .execute_sql("INSERT INTO items VALUES ('a', 'first')", None)
        .await
        .expect("commit");
    let work = changes.select().expect("committed row");
    assert!(
        matches!(database.sync_invalidations(&work).await.expect("map").as_slice(), [SqlInvalidation::Row { table, .. }] if table == "items")
    );
    client
        .execute_sql("INSERT INTO items VALUES ('b', 'later')", None)
        .await
        .expect("later commit");
    assert!(changes.acknowledge(work));
    assert!(changes.select().is_some(), "later commits retained");
    assert!(
        independent.select().is_some(),
        "independent acknowledgement"
    );
    while let Some(work) = changes.select() {
        assert!(changes.acknowledge(work));
    }
    client
        .transaction()
        .await
        .expect("read only")
        .commit()
        .await
        .expect("commit");
    client
        .execute_sql("DELETE FROM items WHERE id = 'missing'", None)
        .await
        .expect("byte no-op");
    let mut tx = client.transaction().await.expect("rollback");
    tx.execute_sql("INSERT INTO items VALUES ('discard', 'no')", None)
        .await
        .expect("stage");
    tx.rollback().await.expect("rollback");
    assert!(changes.select().is_none(), "no-op and rollback silent");
    drop(database);
    client
        .execute_sql("INSERT INTO items VALUES ('c', 'owned')", None)
        .await
        .expect("client owns storage");
    assert!(changes.select().is_some());
    assert_eq!(
        client
            .execute_sql("SELECT label FROM items WHERE id = 'c'", None)
            .await
            .expect("read")[0]
            .rows,
        vec![Row::from(["owned"])]
    );
}

async fn database() -> Database {
    Database::open_indexeddb(&format!("sql-sync-{}", uuid::Uuid::now_v7()))
        .await
        .expect("open replica")
}
fn acknowledge(changes: &mut ChangeSubscription) {
    while let Some(work) = changes.select() {
        assert!(changes.acknowledge(work));
    }
}
fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("sql_native_fixture.json")).expect("native fixture")
}
#[wasm_bindgen_test]
async fn sql_sync_native_payloads_history_metadata_and_replay_silence() {
    let db = database().await;
    let mut changes = db.sync_changes().expect("subscription");
    acknowledge(&mut changes);
    verify_fixture(&db, &fixture()).await;
    assert!(changes.select().is_some(), "incoming fanout");
    assert_eq!(
        db.client()
            .execute_sql(
                "SELECT label, count FROM items WHERE label = 'updated'",
                None
            )
            .await
            .expect("derived index")[0]
            .rows,
        vec![Row::new(vec![
            Value::from("updated"),
            Value::Integer(i64::MAX)
        ])]
    );
    assert!(
        db.client()
            .execute_sql("SELECT * FROM items WHERE id = 'deleted'", None)
            .await
            .expect("Tombstone")[0]
            .rows
            .is_empty()
    );
    let peer = database().await;
    broad(&db, &peer).await;
    acknowledge(&mut changes);
    broad(&db, &peer).await;
    assert!(changes.select().is_none(), "settled replay silent");
    assert_eq!(
        db.export_sync_state().await.expect("history"),
        peer.export_sync_state().await.expect("history")
    );
}

#[wasm_bindgen_test]
async fn sql_sync_selected_union_retains_later_commits_and_independent_peers() {
    let left = database().await;
    let right = database().await;
    left.client().execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT); INSERT INTO items VALUES ('seed', 'seed')", None).await.expect("seed");
    let mut lc = left.sync_changes().expect("left subscription");
    let mut rc = right.sync_changes().expect("right subscription");
    // Initial broad selection bootstraps complete catalog and data.
    selected(&left, &mut lc, &right, &mut rc).await;
    acknowledge(&mut lc);
    acknowledge(&mut rc);
    let mut other = right.sync_changes().expect("independent peer");
    acknowledge(&mut other);
    left.client()
        .execute_sql("UPDATE items SET label = 'left' WHERE id = 'seed'", None)
        .await
        .expect("offline left");
    right
        .client()
        .execute_sql("INSERT INTO items VALUES ('selected', 'right')", None)
        .await
        .expect("offline right");
    let lw = lc.select().expect("left work");
    let rw = rc.select().expect("right work");
    let li = left.sync_invalidations(&lw).await.expect("map");
    let ri = right.sync_invalidations(&rw).await.expect("map");
    assert!(
        li.iter()
            .chain(&ri)
            .all(|i| matches!(i, SqlInvalidation::Row { table, .. } if table == "items"))
    );
    right
        .client()
        .execute_sql("INSERT INTO items VALUES ('later', 'retained')", None)
        .await
        .expect("after selection");
    let config = SessionConfig::default();
    let (mut a, mut b) = channels();
    let (ar, br) = join(
        left.synchronize_selected_checked(&mut a, &config, SyncRole::Initiator, &li, || async {
            Ok(())
        }),
        right.synchronize_selected_checked(&mut b, &config, SyncRole::Responder, &ri, || async {
            Ok(())
        }),
    )
    .await;
    ar.expect("selected initiator");
    br.expect("selected responder");
    assert!(lc.acknowledge(lw));
    assert!(rc.acknowledge(rw));
    assert!(
        left.client()
            .execute_sql("SELECT * FROM items WHERE id = 'later'", None)
            .await
            .expect("selection excludes later")[0]
            .rows
            .is_empty()
    );
    assert!(
        rc.select().is_some(),
        "later commit survives acknowledgement"
    );
    assert!(other.select().is_some(), "other consumer independent");
    assert!(a.sent.borrow().iter().all(|frame| !matches!(
        postcard::from_bytes::<SyncMessage>(frame).expect("frame"),
        SyncMessage::Selection {
            selection: sql_sync::SyncSelection::All,
            ..
        }
    )));
    selected(&left, &mut lc, &right, &mut rc).await;
    assert_eq!(
        left.client()
            .execute_sql("SELECT label FROM items WHERE id = 'later'", None)
            .await
            .expect("next pass")[0]
            .rows,
        vec![Row::from(["retained"])]
    );
}
async fn selected(
    left: &Database,
    lc: &mut ChangeSubscription,
    right: &Database,
    rc: &mut ChangeSubscription,
) {
    let lw = lc.select();
    let rw = rc.select();
    let li = if let Some(work) = &lw {
        left.sync_invalidations(work).await.expect("map left")
    } else {
        vec![]
    };
    let ri = if let Some(work) = &rw {
        right.sync_invalidations(work).await.expect("map right")
    } else {
        vec![]
    };
    let (mut a, mut b) = channels();
    let config = SessionConfig::default();
    let (ar, br) = join(
        left.synchronize_selected_checked(&mut a, &config, SyncRole::Initiator, &li, || async {
            Ok(())
        }),
        right.synchronize_selected_checked(&mut b, &config, SyncRole::Responder, &ri, || async {
            Ok(())
        }),
    )
    .await;
    ar.expect("selected left");
    br.expect("selected right");
    if let Some(work) = lw {
        assert!(lc.acknowledge(work));
    }
    if let Some(work) = rw {
        assert!(rc.acknowledge(work));
    }
}

#[wasm_bindgen_test]
async fn sql_sync_scoped_direction_history_and_candidate_validation() {
    let left = database().await;
    let right = database().await;
    left.client().execute_sql("CREATE TABLE allowed (id TEXT PRIMARY KEY, label TEXT); CREATE INDEX allowed_label ON allowed (label); CREATE TABLE private (id TEXT PRIMARY KEY)", None).await.expect("schema");
    broad(&left, &right).await;
    left.client()
        .execute_sql(
            "INSERT INTO allowed VALUES ('one', 'safe'); INSERT INTO private VALUES ('secret')",
            None,
        )
        .await
        .expect("offline");
    let send = SessionConfig {
        direction: SyncDirection::SendOnly,
        ..SessionConfig::default()
    };
    let receive = SessionConfig {
        direction: SyncDirection::ReceiveOnly,
        ..SessionConfig::default()
    };
    let (mut a, mut b) = channels();
    let validated = Rc::new(Cell::new(false));
    let observed = validated.clone();
    let (ar, br) = join(
        left.synchronize_tables_validated_checked(
            &mut a,
            &send,
            SyncRole::Initiator,
            &["allowed"],
            |_, _, _| Ok(()),
            || async { Ok(()) },
        ),
        right.synchronize_tables_candidate_validated_checked(
            &mut b,
            &receive,
            SyncRole::Responder,
            &["allowed"],
            |_, _, _| Ok(()),
            move |_| {
                observed.set(true);
                Ok(())
            },
            || async { Ok(()) },
        ),
    )
    .await;
    ar.expect("scoped sender");
    br.expect("scoped receiver");
    assert!(validated.get());
    assert_eq!(
        right
            .client()
            .execute_sql("SELECT label FROM allowed WHERE label = 'safe'", None)
            .await
            .expect("scoped index")[0]
            .rows,
        vec![Row::from(["safe"])]
    );
    assert!(
        right
            .client()
            .execute_sql("SELECT * FROM private", None)
            .await
            .expect("private excluded")[0]
            .rows
            .is_empty()
    );
    let before = right
        .export_sync_state()
        .await
        .expect("before denied candidate");
    left.client()
        .execute_sql("UPDATE allowed SET label = 'forbidden'", None)
        .await
        .expect("offline forbidden");
    let (mut a, mut b) = channels();
    let br = select(
        Box::pin(left.synchronize_tables_checked(
            &mut a,
            &send,
            SyncRole::Initiator,
            &["allowed"],
            || async { Ok(()) },
        )),
        Box::pin(right.synchronize_tables_candidate_validated_checked(
            &mut b,
            &receive,
            SyncRole::Responder,
            &["allowed"],
            |_, _, _| Ok(()),
            |_| Err("candidate denied".into()),
            || async { Ok(()) },
        )),
    )
    .await;
    let br = match br {
        Either::Left((r, _)) | Either::Right((r, _)) => r,
    };
    assert!(br.is_err());
    assert_eq!(
        right.export_sync_state().await.expect("denied atomic"),
        before
    );
    let (mut a, mut b) = channels();
    let br = select(
        Box::pin(left.synchronize_tables_checked(
            &mut a,
            &send,
            SyncRole::Initiator,
            &["allowed"],
            || async { Ok(()) },
        )),
        Box::pin(right.synchronize_tables_validated_checked(
            &mut b,
            &receive,
            SyncRole::Responder,
            &["allowed"],
            |_, _, _| Err("history denied".into()),
            || async { Ok(()) },
        )),
    )
    .await;
    let br = match br {
        Either::Left((r, _)) | Either::Right((r, _)) => r,
    };
    assert!(br.is_err());
    assert_eq!(
        right
            .export_sync_state()
            .await
            .expect("history denial atomic"),
        before
    );
}

#[wasm_bindgen::prelude::wasm_bindgen(module = "/src/kv_sync_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = afterNextWrite)]
    fn after_next_write() -> js_sys::Promise;
}
#[wasm_bindgen::prelude::wasm_bindgen(module = "/src/indexeddb_helpers.js")]
extern "C" {
    #[wasm_bindgen(js_name = failNextCommit)]
    fn fail_next_commit(mode: &str);
}

#[wasm_bindgen_test]
async fn sql_sync_checked_denial_cancellation_prior_batches_and_retry() {
    for cancel in [false, true] {
        for boundary in [1, 2] {
            let db = database().await;
            let mut changes = db.sync_changes().expect("subscribe");
            acknowledge(&mut changes);
            let fixture = fixture();
            let mut transport = transcript(&fixture, "initial_initiator");
            let calls = Rc::new(Cell::new(0));
            let observed = calls.clone();
            let (reached, waiting) = futures::channel::oneshot::channel();
            let mut reached = Some(reached);
            let check = move || {
                observed.set(observed.get() + 1);
                let denied = observed.get() == boundary;
                if denied && let Some(reached) = reached.take() {
                    let _ = reached.send(());
                }
                async move {
                    if denied && cancel {
                        futures::future::pending().await
                    } else if denied {
                        Err("authorization revoked".into())
                    } else {
                        Ok(())
                    }
                }
            };
            let config = SessionConfig::default();
            let session = Box::pin(db.synchronize_checked(
                &mut transport,
                &config,
                SyncRole::Responder,
                check,
            ));
            if cancel {
                match select(session, waiting).await {
                    Either::Right((result, pending)) => {
                        result.expect("reached apply boundary");
                        drop(pending);
                    }
                    Either::Left(_) => panic!("must cancel while checked apply is pending"),
                }
            } else {
                assert!(matches!(session.await, Err(SyncError::PreApply(_))));
            }
            assert_eq!(calls.get(), boundary);
            if boundary == 1 {
                assert!(
                    db.export_sync_state()
                        .await
                        .expect("no partial catalog")
                        .is_empty()
                );
                assert!(changes.select().is_none(), "denied catalog silent");
                assert!(
                    db.client()
                        .execute_sql("SELECT * FROM items", None)
                        .await
                        .is_err()
                );
            } else {
                assert!(
                    changes.select().is_some(),
                    "earlier complete catalog survives"
                );
                assert!(
                    db.client()
                        .execute_sql("SELECT * FROM items WHERE label = 'native'", None)
                        .await
                        .expect("complete schema and index, no data")[0]
                        .rows
                        .is_empty()
                );
                assert!(db.client().execute_sql("INSERT INTO items VALUES ('probe', 'probe', 1); INSERT INTO items VALUES ('probe2', 'probe', 2)", None).await.is_err(), "unique constraint installed atomically");
            }
            let mut retry = transcript(&fixture, "initial_initiator");
            db.synchronize_checked(&mut retry, &config, SyncRole::Responder, || async {
                Ok(())
            })
            .await
            .expect("retry converges");
            let expected: Vec<ofdb_sql::SyncStateUnit> =
                serde_json::from_value(fixture["initial_state"].clone()).expect("native history");
            assert_eq!(db.export_sync_state().await.expect("all history"), expected);
        }
    }
}

#[wasm_bindgen_test]
async fn sql_sync_issued_local_and_incoming_commits_publish_after_caller_cancellation() {
    let db = database().await;
    let fixture = fixture();
    let config = SessionConfig::default();
    // Commit only the complete catalog, so the next replay issues a data commit.
    let mut input = transcript(&fixture, "initial_initiator");
    let mut checks = 0;
    assert!(matches!(
        db.synchronize_checked(&mut input, &config, SyncRole::Responder, || {
            checks += 1;
            let deny = checks == 2;
            async move {
                if deny {
                    Err("pause after catalog".into())
                } else {
                    Ok(())
                }
            }
        })
        .await,
        Err(SyncError::PreApply(_))
    ));
    let mut changes = db.sync_changes().expect("consumer");
    let mut other = db.sync_changes().expect("other consumer");
    acknowledge(&mut changes);
    acknowledge(&mut other);
    let mut input = transcript(&fixture, "initial_initiator");
    let issued = wasm_bindgen_futures::JsFuture::from(after_next_write());
    let session = Box::pin(db.synchronize_checked(
        &mut input,
        &config,
        SyncRole::Responder,
        || async { Ok(()) },
    ));
    match select(session, Box::pin(issued)).await {
        Either::Right((result, pending)) => {
            result.expect("native data commit issued");
            assert!(changes.select().is_none(), "not request success");
            drop(pending);
        }
        Either::Left(_) => panic!("session must be canceled during issued commit"),
    }
    platform::timeout(Duration::from_secs(10), changes.changed())
        .await
        .expect("completion independent of caller")
        .expect("wake");
    assert!(other.select().is_some());
    let expected: Vec<ofdb_sql::SyncStateUnit> =
        serde_json::from_value(fixture["initial_state"].clone()).expect("native state");
    assert_eq!(
        db.export_sync_state()
            .await
            .expect("complete persisted data"),
        expected
    );
    acknowledge(&mut changes);
    acknowledge(&mut other);
    let client = db.client();
    let mut tx = client.transaction().await.expect("stage local");
    tx.execute_sql("UPDATE items SET label = 'local' WHERE id = 'live'", None)
        .await
        .expect("stage");
    assert!(
        tx.commit().now_or_never().is_none(),
        "issue and drop caller"
    );
    platform::timeout(Duration::from_secs(10), changes.changed())
        .await
        .expect("local completion")
        .expect("wake");
    assert!(other.select().is_some());
    assert_eq!(
        client
            .execute_sql("SELECT id FROM items WHERE label = 'local'", None)
            .await
            .expect("updated index")[0]
            .rows,
        vec![Row::from(["live"])]
    );
}

#[wasm_bindgen_test]
async fn sql_sync_failed_malformed_and_bounded_incoming_stages_are_atomic_and_silent() {
    for fault in ["abort", "throw", "malformed", "budget", "interrupted"] {
        let db = database().await;
        let mut changes = db.sync_changes().expect("subscribe");
        acknowledge(&mut changes);
        let fixture = fixture();
        let mut input = transcript(&fixture, "initial_initiator");
        let mut config = SessionConfig::default();
        match fault {
            "abort" | "throw" => fail_next_commit(fault),
            "budget" => config.max_session_bytes = 64,
            "interrupted" => {
                let done = input
                    .incoming
                    .iter()
                    .position(|f| {
                        matches!(
                            postcard::from_bytes::<SyncMessage>(f),
                            Ok(SyncMessage::Done)
                        )
                    })
                    .expect("phase end");
                input.incoming.truncate(done);
            }
            "malformed" => {
                let frame = input
                    .incoming
                    .iter_mut()
                    .find(|f| {
                        matches!(
                            postcard::from_bytes::<SyncMessage>(f),
                            Ok(SyncMessage::State(_))
                        )
                    })
                    .expect("catalog batch");
                let mut message: SyncMessage = postcard::from_bytes(frame).expect("decode");
                if let SyncMessage::State(units) = &mut message {
                    units.iter_mut().rev().find(|unit| matches!(&unit.key, ofdb_sql::SyncKey::Row { table, .. } if table == engine::ENGINE_INDEX_FIELDS_STORAGE)).expect("last catalog dependency").state = vec![0xff];
                }
                *frame = postcard::to_allocvec(&message).expect("corrupt catalog payload");
            }
            _ => unreachable!(),
        }
        assert!(
            db.synchronize_checked(&mut input, &config, SyncRole::Responder, || async {
                Ok(())
            })
            .await
            .is_err(),
            "reject {fault}"
        );
        assert!(
            db.export_sync_state()
                .await
                .expect("no partial catalog history")
                .is_empty(),
            "{fault}"
        );
        assert!(changes.select().is_none(), "failed apply silent: {fault}");
        assert!(
            db.client()
                .execute_sql("SELECT * FROM items", None)
                .await
                .is_err()
        );
        let mut retry = transcript(&fixture, "initial_initiator");
        db.synchronize_checked(
            &mut retry,
            &SessionConfig::default(),
            SyncRole::Responder,
            || async { Ok(()) },
        )
        .await
        .expect("fresh retry");
        assert!(changes.select().is_some());
    }
    let db = database().await;
    let fixture = fixture();
    let mut input = transcript(&fixture, "initial_initiator");
    let config = SessionConfig::default();
    let factory = sql_sync::MemorySyncStageFactory::new(1);
    assert!(matches!(
        db.synchronize_with_stage_factory_checked(
            &mut input,
            &config,
            SyncRole::Responder,
            &factory,
            || async { Ok(()) }
        )
        .await,
        Err(SyncError::Staging(_))
    ));
    assert!(
        db.export_sync_state()
            .await
            .expect("bounded stage cannot partially apply")
            .is_empty()
    );
}

#[wasm_bindgen_test]
async fn sql_sync_overflow_and_external_open_gaps_require_broad_recovery() {
    let name = format!("sql-sync-gaps-{}", uuid::Uuid::now_v7());
    let db = Database::open_indexeddb(&name).await.expect("open");
    let client = db.client();
    client.execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY, count INTEGER); INSERT INTO items VALUES ('counter', 0)", None).await.expect("seed");
    let peer = database().await;
    broad(&db, &peer).await;
    let mut changes = db.sync_changes().expect("lagged");
    acknowledge(&mut changes);
    for count in 1..=129 {
        client
            .execute_sql(&format!("UPDATE items SET count = {count}"), None)
            .await
            .expect("commit history");
    }
    let work = changes.select().expect("gap");
    assert_eq!(
        db.sync_invalidations(&work).await.expect("gap fallback"),
        vec![SqlInvalidation::All]
    );
    assert!(changes.acknowledge(work));
    let mut remote = peer.sync_changes().expect("peer");
    selected(&db, &mut changes, &peer, &mut remote).await;
    acknowledge(&mut changes);
    Database::open_indexeddb(&name)
        .await
        .expect("independent external writer")
        .client()
        .execute_sql("UPDATE items SET count = 999", None)
        .await
        .expect("external write");
    assert!(changes.select().is_none(), "no automatic cross-open wakeup");
    broad(&db, &peer).await;
    assert_eq!(
        peer.client()
            .execute_sql("SELECT count FROM items", None)
            .await
            .expect("explicit broad recovery")[0]
            .rows,
        vec![Row::new(vec![Value::Integer(999)])]
    );
    acknowledge(&mut changes);
    let mut tx = client.transaction().await.expect("large invalidation");
    for i in 0..129 {
        tx.execute_sql(
            &format!("INSERT INTO items VALUES ('batch-{i}', {i})"),
            None,
        )
        .await
        .expect("stage");
    }
    tx.commit().await.expect("oversized commit");
    let work = changes.select().expect("overflow work");
    assert_eq!(
        db.sync_invalidations(&work)
            .await
            .expect("overflow fallback"),
        vec![SqlInvalidation::All]
    );
}

struct MissingDependency {
    inner: crate::sql_sync_support::Channel,
    omitted: bool,
}
impl ofdb_sql::SyncTransport for MissingDependency {
    type Error = &'static str;
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.inner.receive().await
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        let mut message: SyncMessage = postcard::from_bytes(&frame).expect("current protocol");
        if !self.omitted
            && let SyncMessage::Changes(changes) = &mut message
        {
            let selected: Vec<_> = changes
                .iter()
                .enumerate()
                .filter(|(_, c)| c.table == "items")
                .map(|(i, _)| i)
                .collect();
            if selected.len() >= 2 {
                changes.remove(selected[0]);
                self.omitted = true;
            }
        }
        self.inner
            .send(postcard::to_allocvec(&message).expect("frame"))
            .await
    }
}
#[wasm_bindgen_test]
async fn sql_sync_missing_dependencies_recover_through_native_snapshots() {
    let left = database().await;
    let right = database().await;
    left.client().execute_sql("CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT); CREATE INDEX items_label ON items (label); INSERT INTO items VALUES ('one', 'seed')", None).await.expect("seed");
    broad(&left, &right).await;
    left.client()
        .execute_sql("UPDATE items SET label = 'dependency'", None)
        .await
        .expect("first change");
    left.client()
        .execute_sql("UPDATE items SET label = 'dependent'", None)
        .await
        .expect("second change");
    let (a, mut b) = channels();
    let mut a = MissingDependency {
        inner: a,
        omitted: false,
    };
    let config = SessionConfig::default();
    let (ar, br) = join(
        left.synchronize_checked(&mut a, &config, SyncRole::Initiator, || async { Ok(()) }),
        right.synchronize_checked(&mut b, &config, SyncRole::Responder, || async { Ok(()) }),
    )
    .await;
    ar.expect("source recovery");
    br.expect("target recovery");
    assert!(a.omitted);
    assert!(b.sent.borrow().iter().any(|frame| matches!(postcard::from_bytes::<SyncMessage>(frame).expect("frame"), SyncMessage::RequestSnapshots(requests) if !requests.is_empty())));
    assert_eq!(
        right
            .client()
            .execute_sql("SELECT id FROM items WHERE label = 'dependent'", None)
            .await
            .expect("recovered index")[0]
            .rows,
        vec![Row::from(["one"])]
    );
    assert_eq!(
        left.export_sync_state()
            .await
            .expect("source native history"),
        right
            .export_sync_state()
            .await
            .expect("complete native history recovered")
    );
}

#[wasm_bindgen_test]
async fn sql_sync_offline_conflicts_resolution_tombstones_and_catalog_lifecycle() {
    let left = database().await;
    let right = database().await;
    let id = "018f0f8e-7b6d-7c4a-8f12-123456789abc";
    left.client().execute_sql(&format!("CREATE TABLE items (id UUID PRIMARY KEY, label TEXT); CREATE INDEX items_label ON items (label); INSERT INTO items VALUES (CAST('{id}' AS UUID), 'seed'); CREATE TABLE recreated (id TEXT PRIMARY KEY, label TEXT); INSERT INTO recreated VALUES ('old', 'seed')"), None).await.expect("seed");
    broad(&left, &right).await;
    left.client().execute_sql("UPDATE items SET label = 'left'; DROP TABLE recreated; CREATE TABLE recreated (id TEXT PRIMARY KEY, count INTEGER); CREATE INDEX recreated_count ON recreated (count); INSERT INTO recreated VALUES ('new', 9)", None).await.expect("offline left");
    right
        .client()
        .execute_sql(
            "UPDATE items SET label = 'right'; UPDATE recreated SET label = 'stale'",
            None,
        )
        .await
        .expect("offline right");
    let stale = right.export_sync_state().await.expect("stale replica");
    broad(&left, &right).await;
    let key = Row::new(vec![Value::Uuid(uuid::Uuid::parse_str(id).expect("UUID"))]);
    let conflicts = right
        .client()
        .row_conflict_values("items", &key)
        .await
        .expect("conflicts");
    assert_eq!(conflicts.len(), 1);
    assert!(
        conflicts[0].1.contains(&Value::from("left"))
            && conflicts[0].1.contains(&Value::from("right"))
    );
    let mut changes = right.sync_changes().expect("resolution notifications");
    acknowledge(&mut changes);
    right
        .client()
        .resolve_row(
            "items",
            &key,
            vec![("label".into(), Value::from("resolved"))],
        )
        .await
        .expect("explicit resolution");
    assert!(changes.select().is_some(), "Resolution publishes");
    broad(&right, &left).await;
    assert!(
        left.client()
            .row_conflicts("items", &key)
            .await
            .expect("resolved")
            .is_empty()
    );
    assert_eq!(
        left.client()
            .execute_sql("SELECT id FROM recreated WHERE count = 9", None)
            .await
            .expect("recreated schema and index")[0]
            .rows,
        vec![Row::from(["new"])]
    );
    left.client()
        .execute_sql("DELETE FROM items", None)
        .await
        .expect("Tombstone");
    broad(&left, &right).await;
    for unit in stale {
        right.apply_sync_state(unit).await.expect("stale replay");
    }
    assert!(
        right
            .client()
            .execute_sql("SELECT * FROM items", None)
            .await
            .expect("cannot revive Tombstone")[0]
            .rows
            .is_empty()
    );
    assert_eq!(
        right
            .client()
            .execute_sql("SELECT id FROM recreated WHERE count = 9", None)
            .await
            .expect("stale schema cannot revive")[0]
            .rows,
        vec![Row::from(["new"])]
    );
    broad(&left, &right).await;
    acknowledge(&mut changes);
    broad(&left, &right).await;
    assert!(changes.select().is_none(), "settled replay");
}

#[wasm_bindgen_test]
async fn sql_sync_scoped_composite_uuid_key_is_distinct_from_row_identity() {
    let left = database().await;
    let right = database().await;
    left.client().execute_sql("CREATE TABLE memberships (tenant UUID, code INTEGER, label TEXT, PRIMARY KEY (tenant, code)); CREATE INDEX memberships_label ON memberships (label)", None).await.expect("composite schema");
    broad(&left, &right).await;
    left.client().execute_sql("INSERT INTO memberships VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 7, 'composite')", None).await.expect("composite row");
    let (mut a, mut b) = channels();
    let config = SessionConfig::default();
    let (ar, br) = join(
        left.synchronize_tables_checked(
            &mut a,
            &config,
            SyncRole::Initiator,
            &["memberships"],
            || async { Ok(()) },
        ),
        right.synchronize_tables_checked(
            &mut b,
            &config,
            SyncRole::Responder,
            &["memberships"],
            || async { Ok(()) },
        ),
    )
    .await;
    ar.expect("scoped composite sender");
    br.expect("scoped composite receiver");
    assert_eq!(
        right
            .client()
            .execute_sql(
                "SELECT code FROM memberships WHERE label = 'composite'",
                None
            )
            .await
            .expect("composite derived index")[0]
            .rows,
        vec![Row::new(vec![Value::Integer(7)])]
    );
}

#[wasm_bindgen_test]
async fn sql_sync_malformed_data_rolls_back_whole_row_batch_and_derived_indexes() {
    let db = database().await;
    let fixture = fixture();
    let mut input = transcript(&fixture, "initial_initiator");
    let mut corrupted = None;
    for frame in &mut input.incoming {
        let mut message: SyncMessage = postcard::from_bytes(frame).expect("native message");
        if let SyncMessage::State(units) = &mut message {
            let unit = units.iter_mut().rev().find(|unit| matches!(&unit.key, ofdb_sql::SyncKey::Row { table, .. } if table == "items")).expect("last data row");
            *unit =
                ofdb_sql::SyncStateUnit::new(unit.key.clone(), vec![0xff], unit.metadata.clone());
            corrupted = Some((unit.key.clone(), unit.digest));
            *frame = postcard::to_allocvec(&message).expect("malformed document with valid digest");
            break;
        }
    }
    let (key, digest) = corrupted.expect("corrupted data row");
    for frame in &mut input.incoming {
        let mut message: SyncMessage = postcard::from_bytes(frame).expect("message");
        if let SyncMessage::Manifest(manifest) = &mut message {
            manifest
                .entries
                .iter_mut()
                .find(|(candidate, _)| candidate == &key)
                .expect("advertised row")
                .1 = digest;
            *frame = postcard::to_allocvec(&message).expect("consistent advertised digest");
        }
    }
    assert!(
        db.synchronize_checked(
            &mut input,
            &SessionConfig::default(),
            SyncRole::Responder,
            || async { Ok(()) }
        )
        .await
        .is_err()
    );
    assert!(
        db.client()
            .execute_sql("SELECT * FROM items WHERE label = 'native'", None)
            .await
            .expect("earlier complete catalog remains, data index rolled back")[0]
            .rows
            .is_empty()
    );
    assert!(
        db.client()
            .execute_sql("SELECT * FROM items", None)
            .await
            .expect("no earlier row in failed data batch persisted")[0]
            .rows
            .is_empty()
    );
    assert!(
        db.export_sync_state()
            .await
            .expect("history atomic")
            .iter()
            .all(
                |unit| matches!(&unit.key, ofdb_sql::SyncKey::Row { table, .. } if table != "items")
            )
    );
    let mut retry = transcript(&fixture, "initial_initiator");
    db.synchronize_checked(
        &mut retry,
        &SessionConfig::default(),
        SyncRole::Responder,
        || async { Ok(()) },
    )
    .await
    .expect("retry converges");
    let expected: Vec<ofdb_sql::SyncStateUnit> =
        serde_json::from_value(fixture["initial_state"].clone()).expect("native state");
    assert_eq!(
        db.export_sync_state().await.expect("native history"),
        expected
    );
}
