#![cfg(all(
    feature = "redb",
    feature = "in-memory",
    feature = "native-stage",
    feature = "sql"
))]

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use ofdb_sql::{Database, Row, SessionConfig, SyncRole, SyncTransport, Uuid, Value};
use tokio::sync::mpsc;

struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    pending_send: Option<Arc<AtomicBool>>,
    sent: Option<Arc<Mutex<Vec<Vec<u8>>>>>,
}
impl SyncTransport for Channel {
    type Error = &'static str;
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        if self
            .pending_send
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
        {
            return core::future::pending().await;
        }
        if let Some(sent) = &self.sent {
            sent.lock().expect("sent frames").push(frame.clone());
        }
        self.sender.send(frame).map_err(|_| "closed")
    }
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.recv().await.ok_or("closed")
    }
}
fn pair() -> (Channel, Channel) {
    let (a, ar) = mpsc::unbounded_channel();
    let (b, br) = mpsc::unbounded_channel();
    (
        Channel {
            sender: a,
            receiver: br,
            pending_send: None,
            sent: None,
        },
        Channel {
            sender: b,
            receiver: ar,
            pending_send: None,
            sent: None,
        },
    )
}
async fn schema(database: &Database) {
    database.client().execute_sql("CREATE TABLE public_rows (id UUID PRIMARY KEY, name TEXT); CREATE UNIQUE INDEX public_names ON public_rows (name); CREATE TABLE local_secrets (id UUID PRIMARY KEY, secret TEXT)", None).await.expect("schema");
}
#[tokio::test]
async fn scoped_native_denial_with_pending_abort_cleans_stages_in_both_roles() {
    for role in [SyncRole::Initiator, SyncRole::Responder] {
        let directory = tempfile::tempdir().expect("databases");
        let left = Database::open(directory.path().join("left.redb")).expect("left");
        let right = Database::in_memory();
        schema(&left).await;
        schema(&right).await;
        right.client().execute_sql("INSERT INTO public_rows VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", None).await.expect("peer row");
        let before = left.export_sync_state().await.expect("before denial");
        let (mut lt, mut rt) = pair();
        let denied = Arc::new(AtomicBool::new(false));
        lt.pending_send = Some(denied.clone());
        let config = SessionConfig::default();
        let other = if role == SyncRole::Initiator {
            SyncRole::Responder
        } else {
            SyncRole::Initiator
        };
        let stages = directory.path().join("stages");
        let (l, _) = tokio::join!(
            async {
                let result = left
                    .synchronize_tables_with_stage_directory_checked(
                        &mut lt,
                        &config,
                        role,
                        &stages,
                        &["public_rows"],
                        || {
                            denied.store(true, Ordering::SeqCst);
                            async { Err("cancelled".into()) }
                        },
                    )
                    .await;
                drop(lt);
                result
            },
            async {
                let result = right
                    .synchronize_tables_checked(
                        &mut rt,
                        &config,
                        other,
                        &["public_rows"],
                        || async { Ok(()) },
                    )
                    .await;
                drop(rt);
                result
            }
        );
        assert!(matches!(l, Err(ofdb_sql::SyncError::PreApply(reason)) if reason == "cancelled"));
        assert_eq!(
            left.export_sync_state().await.expect("denied state"),
            before
        );
        assert_eq!(std::fs::read_dir(stages).expect("stages").count(), 0);
    }
}

async fn exchange(
    left: &Database,
    right: &Database,
    left_tables: &[&str],
    right_tables: &[&str],
) -> (Result<(), String>, Result<(), String>) {
    let (mut lt, mut rt) = pair();
    let config = SessionConfig {
        max_units_per_frame: 1,
        ..SessionConfig::default()
    };
    tokio::join!(
        async {
            let result = left
                .synchronize_tables_checked(
                    &mut lt,
                    &config,
                    SyncRole::Initiator,
                    left_tables,
                    || async { Ok(()) },
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string());
            drop(lt);
            result
        },
        async {
            let result = right
                .synchronize_tables_checked(
                    &mut rt,
                    &config,
                    SyncRole::Responder,
                    right_tables,
                    || async { Ok(()) },
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string());
            drop(rt);
            result
        }
    )
}

#[tokio::test]
async fn scoped_native_sessions_preserve_local_tables_and_sync_full_selected_history() {
    let directory = tempfile::tempdir().expect("databases");
    let path = directory.path().join("left.redb");
    let left = Database::open(&path).expect("left");
    let right = Database::in_memory();
    schema(&left).await;
    schema(&right).await;
    left.client().execute_sql("INSERT INTO public_rows VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada'); INSERT INTO local_secrets VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'never transfer')", None).await.expect("rows");
    let config = SessionConfig::default();
    let (mut lt, mut rt) = pair();
    let (l, r) = tokio::join!(
        left.synchronize_tables_with_stage_directory_validated_checked(
            &mut lt,
            &config,
            SyncRole::Initiator,
            directory.path().join("stages"),
            &["public_rows"],
            |table, column, _| {
                assert_eq!(table, "public_rows");
                assert!(["id", "name"].contains(&column));
                Ok(())
            },
            || async { Ok(()) }
        ),
        right.synchronize_tables_validated_checked(
            &mut rt,
            &config,
            SyncRole::Responder,
            &["public_rows"],
            |_, _, _| Ok(()),
            || async { Ok(()) }
        )
    );
    l.expect("left scoped sync");
    r.expect("right scoped sync");
    assert!(
        right
            .client()
            .execute_sql("SELECT * FROM local_secrets", None)
            .await
            .expect("local table retained")[0]
            .rows
            .is_empty()
    );
    assert_eq!(
        right
            .client()
            .execute_sql("SELECT * FROM public_rows", None)
            .await
            .expect("selected rows")[0]
            .rows
            .len(),
        1
    );
    assert_eq!(
        std::fs::read_dir(directory.path().join("stages"))
            .expect("stages")
            .count(),
        0
    );
    drop(left);
    let left = Database::open(&path).expect("reopen");
    assert_eq!(
        left.client()
            .execute_sql("SELECT * FROM local_secrets", None)
            .await
            .expect("local secrets persisted")[0]
            .rows
            .len(),
        1
    );
}

#[tokio::test]
async fn scope_is_explicit_and_remote_schema_cannot_widen_it() {
    let left = Database::in_memory();
    let right = Database::in_memory();
    schema(&left).await;
    schema(&right).await;
    for scope in [
        vec![],
        vec!["public_rows", "public_rows"],
        vec!["missing"],
        vec!["__engine_tables"],
    ] {
        let (l, r) = exchange(&left, &right, &scope, &["public_rows"]).await;
        assert!(l.is_err());
        assert!(r.is_err());
    }
    let (l, r) = exchange(&left, &right, &["public_rows"], &["local_secrets"]).await;
    assert!(l.expect_err("scope mismatch").contains("scopes differ"));
    assert!(r.expect_err("scope mismatch").contains("scopes differ"));
    left.client()
        .execute_sql("ALTER TABLE public_rows ADD COLUMN secret TEXT", None)
        .await
        .expect("widen source schema");
    let before = right.export_sync_state().await.expect("receiver before");
    let (l, r) = exchange(&left, &right, &["public_rows"], &["public_rows"]).await;
    assert!(l.is_err() && r.is_err());
    assert_eq!(
        right.export_sync_state().await.expect("receiver unchanged"),
        before
    );
}

#[tokio::test]
async fn multi_table_scope_keeps_excluded_indexes_and_rows_local() {
    let left = Database::in_memory();
    let right = Database::in_memory();
    for database in [&left, &right] {
        schema(database).await;
        database.client().execute_sql("CREATE TABLE grants (id UUID PRIMARY KEY, token_hash TEXT, consumed_at INTEGER); CREATE UNIQUE INDEX grant_hashes ON grants (token_hash); CREATE UNIQUE INDEX local_verifiers ON local_secrets (secret)", None).await.expect("extra schemas");
    }
    left.client().execute_sql("INSERT INTO public_rows VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada'); INSERT INTO grants VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'lookup hash', NULL); INSERT INTO local_secrets VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'verifier')", None).await.expect("records");
    let (l, r) = exchange(
        &left,
        &right,
        &["public_rows", "grants"],
        &["grants", "public_rows"],
    )
    .await;
    l.expect("left");
    r.expect("right");
    assert_eq!(
        right
            .client()
            .execute_sql("SELECT * FROM public_rows; SELECT * FROM grants", None)
            .await
            .expect("selected rows")
            .iter()
            .map(|r| r.rows.len())
            .collect::<Vec<_>>(),
        vec![1, 1]
    );
    assert!(
        right
            .client()
            .execute_sql("SELECT * FROM local_secrets", None)
            .await
            .expect("excluded rows")[0]
            .rows
            .is_empty()
    );
    right.client().execute_sql("INSERT INTO local_secrets VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abf' AS UUID), 'verifier')", None).await.expect("local-only index remains usable");
    let (l, r) = exchange(
        &right,
        &left,
        &["grants", "public_rows"],
        &["public_rows", "grants"],
    )
    .await;
    l.expect("reverse left");
    r.expect("reverse right");
    assert_eq!(
        left.client()
            .execute_sql("SELECT * FROM local_secrets", None)
            .await
            .expect("only own secret")[0]
            .rows
            .len(),
        1
    );
}

#[tokio::test]
async fn overwritten_secret_history_is_rejected_not_sanitized() {
    let left = Database::in_memory();
    let right = Database::in_memory();
    schema(&left).await;
    schema(&right).await;
    left.client().execute_sql("ALTER TABLE public_rows ADD COLUMN secret TEXT; INSERT INTO public_rows VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', 'old verifier'); UPDATE public_rows SET secret = NULL", None).await.expect("historical mixed schema");
    let before = right.export_sync_state().await.expect("receiver before");
    let (l, r) = exchange(&left, &right, &["public_rows"], &["public_rows"]).await;
    assert!(l.is_err() && r.is_err());
    assert_eq!(
        right.export_sync_state().await.expect("receiver unchanged"),
        before
    );
}

const STABLE_ID: &str = "886313e1-3b8a-5372-9b90-0c9aee199e5d";

async fn stable_schema(database: &Database) {
    database.client().execute_sql("CREATE TABLE stable_resources (id UUID PRIMARY KEY, resource_key TEXT, selected BOOLEAN); CREATE UNIQUE INDEX stable_resource_keys ON stable_resources(resource_key)", None).await.expect("stable schema");
}

#[tokio::test]
async fn uuidv5_same_key_partition_conflicts_resolve_and_retry_after_native_reopen() {
    let directory = tempfile::tempdir().expect("databases");
    let path = directory.path().join("stable.redb");
    let left = Database::open(&path).expect("left");
    let right = Database::in_memory();
    for database in [&left, &right] {
        stable_schema(database).await;
    }
    left.client().execute_sql(&format!("INSERT INTO stable_resources VALUES (CAST('{STABLE_ID}' AS UUID), 'resource', true)"), None).await.expect("explicit deterministic UUIDv5 primary key");
    right.client().execute_sql(&format!("INSERT INTO stable_resources VALUES (CAST('{STABLE_ID}' AS UUID), 'resource', false)"), None).await.expect("same deterministic UUIDv5 on independent partition");
    drop(left);
    let left = Database::open(&path).expect("reopen explicit stable key");
    let (l, r) = exchange(&left, &right, &["stable_resources"], &["stable_resources"]).await;
    l.expect("left stable sync");
    r.expect("right stable sync");
    let id = Uuid::parse_str(STABLE_ID).expect("stable UUIDv5");
    let key = Row::new(vec![Value::Uuid(id)]);
    for database in [&left, &right] {
        let rows = database
            .client()
            .execute_sql("SELECT * FROM stable_resources", None)
            .await
            .expect("one stable row")[0]
            .rows
            .clone();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Value::Uuid(id));
        let conflicts = database
            .client()
            .row_conflict_values("stable_resources", &key)
            .await
            .expect("same-key conflicts");
        let values = &conflicts
            .iter()
            .find(|(column, _)| column == "selected")
            .expect("concurrent selection values")
            .1;
        assert!(values.contains(&Value::Bool(true)) && values.contains(&Value::Bool(false)));
    }
    left.client()
        .resolve_row(
            "stable_resources",
            &key,
            vec![("selected".into(), Value::Bool(false))],
        )
        .await
        .expect("explicit restrictive resolution");
    drop(left);
    let left = Database::open(&path).expect("reopen resolution");
    let (l, r) = exchange(&left, &right, &["stable_resources"], &["stable_resources"]).await;
    l.expect("left resolution delta");
    r.expect("right resolution delta");
    for database in [&left, &right] {
        assert!(
            !database
                .client()
                .row_conflicts("stable_resources", &key)
                .await
                .expect("partial resolution preserves other concurrent assignments")
                .contains(&"selected".to_string())
        );
        assert_eq!(
            database
                .client()
                .execute_sql("SELECT selected FROM stable_resources", None)
                .await
                .expect("deselected")[0]
                .rows,
            vec![Row::new(vec![Value::Bool(false)])]
        );
    }
    right
        .client()
        .execute_sql("UPDATE stable_resources SET selected = true", None)
        .await
        .expect("retry/reselection uses same key, not replacement identity");
    let (l, r) = exchange(&right, &left, &["stable_resources"], &["stable_resources"]).await;
    l.expect("reverse retry sync");
    r.expect("reverse retry sync");
    assert_eq!(
        left.client()
            .execute_sql("SELECT id, selected FROM stable_resources", None)
            .await
            .expect("stable reselected row")[0]
            .rows,
        vec![Row::new(vec![Value::Uuid(id), Value::Bool(true)])]
    );
}

#[tokio::test]
async fn uuidv5_tombstones_win_partition_updates_and_stale_replay() {
    let directory = tempfile::tempdir().expect("databases");
    let path = directory.path().join("tombstone.redb");
    let left = Database::open(&path).expect("left");
    let right = Database::in_memory();
    for database in [&left, &right] {
        stable_schema(database).await;
    }
    let insert = format!(
        "INSERT INTO stable_resources VALUES (CAST('{STABLE_ID}' AS UUID), 'resource', true)"
    );
    left.client()
        .execute_sql(&insert, None)
        .await
        .expect("stable row");
    let config = SessionConfig::default();
    let (mut lt, mut rt) = pair();
    let (l, r) = tokio::join!(
        left.synchronize(&mut lt, &config, SyncRole::Initiator),
        right.synchronize(&mut rt, &config, SyncRole::Responder),
    );
    l.expect("whole-resource UUIDv5 state sync");
    r.expect("whole-resource UUIDv5 state sync");
    let stale = right.export_sync_state().await.expect("stale live state");
    left.client()
        .execute_sql("DELETE FROM stable_resources", None)
        .await
        .expect("tombstone");
    right
        .client()
        .execute_sql("UPDATE stable_resources SET selected = false", None)
        .await
        .expect("concurrent offline update");
    let (l, r) = exchange(&left, &right, &["stable_resources"], &["stable_resources"]).await;
    l.expect("delete/update left");
    r.expect("delete/update right");
    for database in [&left, &right] {
        assert!(
            database
                .client()
                .execute_sql("SELECT * FROM stable_resources", None)
                .await
                .expect("deleted")[0]
                .rows
                .is_empty()
        );
        for unit in &stale {
            database
                .apply_sync_state(unit.clone())
                .await
                .expect("stale state reconciles without resurrection");
        }
        assert!(
            database
                .client()
                .execute_sql("SELECT * FROM stable_resources", None)
                .await
                .expect("still deleted")[0]
                .rows
                .is_empty()
        );
        assert!(
            database.client().execute_sql(&insert, None).await.is_err(),
            "exact tombstoned key cannot be reinserted"
        );
    }
    drop(left);
    let left = Database::open(&path).expect("reopen tombstone");
    assert!(
        left.client()
            .execute_sql("SELECT * FROM stable_resources", None)
            .await
            .expect("durable tombstone")[0]
            .rows
            .is_empty()
    );
    let (l, r) = exchange(&right, &left, &["stable_resources"], &["stable_resources"]).await;
    l.expect("repeat reverse left");
    r.expect("repeat reverse right");
}

#[tokio::test]
async fn uuid_identity_rejects_invalid_ids_and_keeps_generated_ids_v7() {
    let database = Database::in_memory();
    stable_schema(&database).await;
    for id in [
        "00000000-0000-0000-0000-000000000000",
        "018f0f8e-7b6d-4c4a-8f12-123456789abc",
        "886313e1-3b8a-5372-0b90-0c9aee199e5d",
        "018f0f8e-7b6d-7c4a-cf12-123456789abc",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "not-a-uuid",
    ] {
        assert!(database.client().execute_sql(&format!("INSERT INTO stable_resources VALUES (CAST('{id}' AS UUID), 'invalid', true)"), None).await.is_err(), "invalid identity {id}");
    }
    assert!(
        database
            .client()
            .execute_sql(
                "INSERT INTO stable_resources VALUES ('not a UUID Value', 'invalid type', true)",
                None
            )
            .await
            .is_err()
    );
    database
        .client()
        .execute(vec![ofdb_sql::Statement::Query(ofdb_sql::Query::Insert(
            ofdb_sql::QueryInsert {
                table: "stable_resources".into(),
                row: Row::new(vec![Value::from("generated"), Value::Bool(true)]),
                returning: None,
            },
        ))])
        .await
        .expect("omitted ID is still owner-generated v7");
    let generated = database
        .client()
        .execute_sql("SELECT id FROM stable_resources", None)
        .await
        .expect("generated ID")[0]
        .rows[0]
        .values[0]
        .to_uuid()
        .expect("UUID");
    assert_eq!(generated.as_bytes()[6] >> 4, 7);
    assert_eq!(generated.as_bytes()[8] & 0xc0, 0x80);
    let before = database
        .export_sync_state()
        .await
        .expect("before invalid imports");

    assert_eq!(
        database
            .export_sync_state()
            .await
            .expect("unchanged imports"),
        before
    );
    assert!(
        database
            .client()
            .execute_sql(
                &format!("UPDATE stable_resources SET id = CAST('{STABLE_ID}' AS UUID)"),
                None
            )
            .await
            .is_err(),
        "immutable primary key"
    );
    assert_eq!(
        database
            .export_sync_state()
            .await
            .expect("unchanged primary key"),
        before
    );
}

#[tokio::test]
async fn incoming_historical_value_denial_cleans_native_stages_without_abort_progress() {
    for role in [SyncRole::Initiator, SyncRole::Responder] {
        for incremental in [false, true] {
            let directory = tempfile::tempdir().expect("databases");
            let path = directory.path().join("receiver.redb");
            let source = Database::in_memory();
            let receiver = Database::open(&path).expect("receiver");
            schema(&source).await;
            schema(&receiver).await;
            source.client().execute_sql("INSERT INTO public_rows VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'safe')", None).await.expect("initial row");
            if incremental {
                let (l, r) = exchange(&source, &receiver, &["public_rows"], &["public_rows"]).await;
                l.expect("initial source");
                r.expect("initial receiver");
            }
            source.client().execute_sql("UPDATE public_rows SET name = 'forbidden historical text'; UPDATE public_rows SET name = 'safe overwritten value'", None).await.expect("same-type forbidden history");
            let before = receiver.export_sync_state().await.expect("before denial");
            let denied = Arc::new(AtomicBool::new(false));
            let (mut lt, mut rt) = pair();
            rt.pending_send = Some(denied.clone());
            let config = SessionConfig {
                max_units_per_frame: 1,
                ..SessionConfig::default()
            };
            let other = if role == SyncRole::Initiator {
                SyncRole::Responder
            } else {
                SyncRole::Initiator
            };
            let stages = directory.path().join("stages");
            let (_, r) = tokio::join!(
                async {
                    let result = source
                        .synchronize_tables_checked(
                            &mut lt,
                            &config,
                            other,
                            &["public_rows"],
                            || async { Ok(()) },
                        )
                        .await;
                    drop(lt);
                    result
                },
                async {
                    let result = receiver
                        .synchronize_tables_with_stage_directory_validated_checked(
                            &mut rt,
                            &config,
                            role,
                            &stages,
                            &["public_rows"],
                            |table, column, value| {
                                if value == &Value::from("forbidden historical text") {
                                    assert_eq!((table, column), ("public_rows", "name"));
                                    denied.store(true, Ordering::SeqCst);
                                    Err("historical value denied".into())
                                } else {
                                    Ok(())
                                }
                            },
                            || async { Ok(()) },
                        )
                        .await;
                    drop(rt);
                    result
                }
            );
            assert!(
                r.expect_err("historical value rejected before mutation")
                    .to_string()
                    .contains("historical value denied")
            );
            assert!(denied.load(Ordering::SeqCst));
            assert_eq!(
                receiver.export_sync_state().await.expect("unchanged"),
                before
            );
            assert_eq!(
                std::fs::read_dir(&stages).expect("created stages").count(),
                0
            );
            drop(receiver);
            let receiver = Database::open(&path).expect("reopen denied receiver");
            assert_eq!(
                receiver
                    .export_sync_state()
                    .await
                    .expect("unchanged after reopen"),
                before
            );
        }
    }
}

#[tokio::test]
async fn historical_schema_default_is_validated_before_any_transport_io() {
    let source = Database::in_memory();
    source.client().execute_sql("CREATE TABLE public_rows (id UUID PRIMARY KEY, name TEXT DEFAULT 'forbidden historical text')", None).await.expect("schema default");
    let (mut lt, rt) = pair();
    drop(rt);
    let sent = Arc::new(Mutex::new(Vec::new()));
    lt.sent = Some(sent.clone());
    let config = SessionConfig::default();
    let forbidden = Value::from("forbidden historical text");
    let error = source
        .synchronize_tables_validated_checked(
            &mut lt,
            &config,
            SyncRole::Initiator,
            &["public_rows"],
            |table, column, value| {
                if value == &forbidden {
                    assert_eq!((table, column), ("public_rows", "name"));
                    Err("default value denied".into())
                } else {
                    Ok(())
                }
            },
            || async { Ok(()) },
        )
        .await
        .expect_err("forbidden default");
    assert!(error.to_string().contains("default value denied"));
    assert!(sent.lock().expect("frames").is_empty());
}

#[tokio::test]
async fn historical_value_denial_prevents_disclosure_after_overwrite_delete_and_conflict() {
    for role in [SyncRole::Initiator, SyncRole::Responder] {
        for variant in 0..3 {
            let directory = tempfile::tempdir().expect("databases");
            let path = directory.path().join("source.redb");
            let source = Database::open(&path).expect("source");
            let peer = Database::in_memory();
            schema(&source).await;
            schema(&peer).await;
            source.client().execute_sql("INSERT INTO public_rows VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'safe')", None).await.expect("initial row");
            if variant == 2 {
                let (l, r) = exchange(&source, &peer, &["public_rows"], &["public_rows"]).await;
                l.expect("initial source");
                r.expect("initial peer");
                peer.client()
                    .execute_sql("UPDATE public_rows SET name = 'safe conflict'", None)
                    .await
                    .expect("concurrent safe value");
            }
            source
                .client()
                .execute_sql(
                    "UPDATE public_rows SET name = 'forbidden historical text'",
                    None,
                )
                .await
                .expect("forbidden value is valid SQL Text");
            if variant == 2 {
                let (l, r) = exchange(&source, &peer, &["public_rows"], &["public_rows"]).await;
                l.expect("structural-only source");
                r.expect("structural-only peer");
                let key = Row::new(vec![Value::Uuid(
                    Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").expect("UUID"),
                )]);
                assert!(
                    source
                        .client()
                        .row_conflict_values("public_rows", &key)
                        .await
                        .expect("conflicts")[0]
                        .1
                        .contains(&Value::from("forbidden historical text"))
                );
                source
                    .client()
                    .resolve_row(
                        "public_rows",
                        &key,
                        vec![("name".into(), Value::from("safe conflict"))],
                    )
                    .await
                    .expect("resolve away forbidden conflict");
            } else {
                source
                    .client()
                    .execute_sql(
                        if variant == 0 {
                            "UPDATE public_rows SET name = 'safe'"
                        } else {
                            "DELETE FROM public_rows"
                        },
                        None,
                    )
                    .await
                    .expect("hide forbidden value");
            }
            drop(source);
            let source = Database::open(&path).expect("reopen history");
            let peer = Database::in_memory();
            schema(&peer).await;
            let peer_before = peer.export_sync_state().await.expect("peer before");
            let before = source.export_sync_state().await.expect("source before");
            let rejected = Arc::new(AtomicBool::new(false));
            let stages = directory.path().join("stages");
            let (mut lt, mut rt) = pair();
            let sent = Arc::new(Mutex::new(Vec::new()));
            lt.sent = Some(sent.clone());
            let config = SessionConfig::default();
            let other = if role == SyncRole::Initiator {
                SyncRole::Responder
            } else {
                SyncRole::Initiator
            };
            let (l, _) = tokio::join!(
                async {
                    let result = source
                        .synchronize_tables_with_stage_directory_validated_checked(
                            &mut lt,
                            &config,
                            role,
                            &stages,
                            &["public_rows"],
                            |table, column, value| {
                                if value == &Value::from("forbidden historical text") {
                                    assert_eq!((table, column), ("public_rows", "name"));
                                    rejected.store(true, Ordering::SeqCst);
                                    Err("historical value denied".into())
                                } else {
                                    Ok(())
                                }
                            },
                            || async { Ok(()) },
                        )
                        .await;
                    drop(lt);
                    result
                },
                async {
                    let result = peer
                        .synchronize_tables_validated_checked(
                            &mut rt,
                            &config,
                            other,
                            &["public_rows"],
                            |_, _, _| Ok(()),
                            || async { Ok(()) },
                        )
                        .await;
                    drop(rt);
                    result
                }
            );
            assert!(
                l.expect_err("historical Text must reject")
                    .to_string()
                    .contains("historical value denied")
            );
            assert!(rejected.load(Ordering::SeqCst));
            assert!(
                sent.lock().expect("frames").iter().all(|frame| !frame
                    .windows(b"forbidden historical text".len())
                    .any(|bytes| bytes == b"forbidden historical text")),
                "no forbidden bytes disclosed"
            );
            assert_eq!(
                peer.export_sync_state().await.expect("no peer mutation"),
                peer_before
            );
            assert_eq!(
                source
                    .export_sync_state()
                    .await
                    .expect("no source mutation"),
                before
            );
            if stages.exists() {
                assert_eq!(std::fs::read_dir(&stages).expect("stages").count(), 0);
            }
        }
    }
}

#[tokio::test]
async fn scoped_conflicts_resolution_and_tombstones_survive_incremental_reopen() {
    let directory = tempfile::tempdir().expect("databases");
    let path = directory.path().join("left.redb");
    let left = Database::open(&path).expect("left");
    let right = Database::in_memory();
    schema(&left).await;
    schema(&right).await;
    left.client().execute_sql("INSERT INTO public_rows VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", None).await.expect("row");
    let (l, r) = exchange(&left, &right, &["public_rows"], &["public_rows"]).await;
    l.expect("bootstrap left");
    r.expect("bootstrap right");
    left.client()
        .execute_sql("UPDATE public_rows SET name = 'alice'", None)
        .await
        .expect("left offline");
    right
        .client()
        .execute_sql(
            "UPDATE public_rows SET name = 'first'; UPDATE public_rows SET name = 'bob'",
            None,
        )
        .await
        .expect("right sequential offline");
    let (l, r) = exchange(&left, &right, &["public_rows"], &["public_rows"]).await;
    l.expect("incremental left");
    r.expect("incremental right");
    let key = Row::new(vec![Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").expect("UUID"),
    )]);
    let values = left
        .client()
        .row_conflict_values("public_rows", &key)
        .await
        .expect("conflicts");
    assert!(values[0].1.contains(&Value::Text("alice".into())));
    assert!(values[0].1.contains(&Value::Text("bob".into())));
    left.client()
        .resolve_row(
            "public_rows",
            &key,
            vec![("name".into(), Value::Text("alice".into()))],
        )
        .await
        .expect("explicit resolution");
    drop(left);
    let left = Database::open(&path).expect("reopen resolution");
    let (l, r) = exchange(&left, &right, &["public_rows"], &["public_rows"]).await;
    l.expect("resolved left");
    r.expect("resolved right");
    assert!(
        right
            .client()
            .row_conflicts("public_rows", &key)
            .await
            .expect("resolved")
            .is_empty()
    );
    left.client()
        .execute_sql("DELETE FROM public_rows", None)
        .await
        .expect("delete");
    let (l, r) = exchange(&right, &left, &["public_rows"], &["public_rows"]).await;
    l.expect("tombstone right");
    r.expect("tombstone left");
    assert!(
        right
            .client()
            .execute_sql("SELECT * FROM public_rows", None)
            .await
            .expect("deleted")[0]
            .rows
            .is_empty()
    );
}
