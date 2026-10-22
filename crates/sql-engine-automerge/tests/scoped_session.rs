use core::fmt;
use std::{cell::Cell, rc::Rc};

use automerge::{ActorId, AutoCommit, ROOT, ReadDoc, ScalarValue, transaction::Transactable};
use engine::{Engine, InMemoryKernel};
use engine_automerge::AutomergeRowCodec;
use futures::{StreamExt, channel::mpsc, executor::block_on};
use sql_translator::SqlTranslator;
use sync::{
    MemorySyncStageFactory, SessionConfig, SyncDirection, SyncKey, SyncMessage, SyncRole,
    SyncSnapshotRequest, SyncStateUnit, SyncTransport, SyncValidationDirection,
    export_sync_state_for, sync_manifest_for,
    synchronize_tables_with_stage_factory_candidate_validated_checked,
};
use value::Value;

#[derive(Debug)]
struct Closed;
impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("closed")
    }
}
struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    attack: u8,
    injected: Rc<Cell<bool>>,
    mutation: Option<Engine<InMemoryKernel, AutomergeRowCodec>>,
    advertised_changes: Vec<sync::SyncChangeId>,
    receive_only: bool,
    done_count: usize,
}
impl SyncTransport for Channel {
    type Error = Closed;
    async fn receive(&mut self) -> Result<Vec<u8>, Closed> {
        self.receiver.next().await.ok_or(Closed)
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Closed> {
        let mut output = frame;
        if let Ok(mut message) = postcard::from_bytes::<SyncMessage>(&output) {
            if self.receive_only {
                assert!(
                    !matches!(message, SyncMessage::State(_) | SyncMessage::Changes(_)),
                    "receiver must never publish any history, including catalogs and recovery"
                );
            }
            if matches!(message, SyncMessage::Done) {
                self.done_count += 1;
                if (((17..=19).contains(&self.attack) || self.attack == 24) && self.done_count == 1)
                    || (self.attack == 20 && self.done_count == 2)
                {
                    message = if self.attack == 19 {
                        SyncMessage::Changes(vec![sync::SyncIncrementalChange {
                            table: "people".into(),
                            row: uuid::Uuid::nil(),
                            id: sync::SyncChangeId(vec![0; 32]),
                            payload: vec![1, 2, 3],
                        }])
                    } else {
                        SyncMessage::State(vec![SyncStateUnit::new(
                            SyncKey::Row {
                                table: if self.attack == 18 {
                                    "tables"
                                } else if self.attack == 24 {
                                    "index_fields"
                                } else {
                                    "people"
                                }
                                .into(),
                                row: uuid::Uuid::nil(),
                            },
                            vec![1, 2, 3],
                            Vec::new(),
                        )])
                    };
                    self.injected.set(true);
                }
            }
            if (((self.attack == 12 || self.attack == 14)
                && matches!(message, SyncMessage::Inventory(_)))
                || ((self.attack == 13 || self.attack == 16)
                    && matches!(message, SyncMessage::RequestSnapshots(_))))
                && !self.injected.get()
            {
                let sql = if self.attack == 16 {
                    "UPDATE people SET name = 'uncorrelated late value'"
                } else {
                    "UPDATE people SET name = 'forbidden historical text'; UPDATE people SET name = 'safe late overwrite'"
                };
                self.mutation
                    .as_ref()
                    .expect("late source mutation")
                    .translate_and_execute(sql, &SqlTranslator)
                    .await
                    .expect("change after preflight");
                self.injected.set(true);
            }
            if self.attack == 23 {
                match &message {
                    SyncMessage::Inventory(inventories) if self.receive_only => {
                        assert!(inventories.iter().any(|inventory| inventory.table == "people" && inventory.row == uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").expect("fixture UUID") && !inventory.changes.is_empty()), "retry must advertise actual retained application change IDs");
                        self.injected.set(true);
                    }
                    SyncMessage::State(units) => assert!(units.iter().all(|unit| !matches!(&unit.key, SyncKey::Row { table, .. } if table == "people")), "completed retry must not resend application snapshot"),
                    SyncMessage::Changes(changes) => assert!(changes.iter().all(|change| change.table != "people"), "completed retry must not resend application deltas"),
                    _ => {}
                }
            }
            match &mut message {
                SyncMessage::State(units)
                    if self.attack <= 2
                        || self.attack == 6
                        || (7..=9).contains(&self.attack)
                        || self.attack == 11 =>
                {
                    for unit in units {
                        let SyncKey::Row { table, row } = &unit.key;
                        if ((self.attack <= 1
                            || self.attack == 6
                            || (7..=9).contains(&self.attack)
                            || self.attack == 11)
                            && table == "people")
                            || (self.attack == 2 && table == "index_fields")
                        {
                            let mut document =
                                AutoCommit::load(&unit.state).expect("fixture state");
                            if (7..=9).contains(&self.attack) || self.attack == 11 {
                                let mut forbidden = document.fork();
                                if self.attack == 9 {
                                    forbidden.set_actor(ActorId::from(vec![0; 32]));
                                    document.set_actor(ActorId::from(vec![255; 32]));
                                }
                                forbidden
                                    .put(
                                        ROOT,
                                        "name",
                                        ScalarValue::Bytes(
                                            postcard::to_allocvec(&Value::from(
                                                "forbidden historical text",
                                            ))
                                            .expect("encoded Text"),
                                        ),
                                    )
                                    .expect("forbidden known Text");
                                forbidden.get_heads();
                                if self.attack == 9 {
                                    document
                                        .put(
                                            ROOT,
                                            "name",
                                            ScalarValue::Bytes(
                                                postcard::to_allocvec(&Value::from(
                                                    "safe concurrent value",
                                                ))
                                                .expect("encoded Text"),
                                            ),
                                        )
                                        .expect("safe concurrent value");
                                    document.merge(&mut forbidden).expect("conflict");
                                    assert_eq!(
                                        document.get_all(ROOT, "name").expect("conflicts").len(),
                                        2
                                    );
                                    let selected = document
                                        .get(ROOT, "name")
                                        .expect("visible value")
                                        .expect("name exists")
                                        .0;
                                    assert_eq!(
                                        selected.to_bytes().map(|bytes| postcard::from_bytes::<
                                            Value,
                                        >(
                                            bytes
                                        )
                                        .expect("decoded visible value")),
                                        Some(Value::from("safe concurrent value")),
                                        "forbidden value is a losing conflict"
                                    );
                                } else {
                                    document = forbidden;
                                    if self.attack == 8 {
                                        document
                                            .delete(ROOT, "name")
                                            .expect("delete forbidden property");
                                    } else {
                                        document
                                            .put(
                                                ROOT,
                                                "name",
                                                ScalarValue::Bytes(
                                                    postcard::to_allocvec(&Value::from(
                                                        "safe overwritten value",
                                                    ))
                                                    .expect("encoded Text"),
                                                ),
                                            )
                                            .expect("overwrite forbidden property");
                                    }
                                }
                            } else if self.attack == 0 || self.attack == 6 {
                                document
                                    .put(ROOT, "secret_hash", ScalarValue::Bytes(vec![42]))
                                    .expect("hidden secret");
                                document.get_heads();
                                document
                                    .delete(ROOT, "secret_hash")
                                    .expect("hide unknown property");
                            } else if self.attack == 1 {
                                unit.key = SyncKey::Row {
                                    table: "local_secrets".into(),
                                    row: *row,
                                };
                            } else {
                                document
                                    .put(
                                        ROOT,
                                        "column_id",
                                        ScalarValue::Bytes(
                                            postcard::to_allocvec(&Value::Uuid(uuid::Uuid::nil()))
                                                .expect("value"),
                                        ),
                                    )
                                    .expect("invalid index reference");
                            }
                            *unit = SyncStateUnit::new(
                                unit.key.clone(),
                                document.save(),
                                unit.metadata.clone(),
                            );
                            self.injected.set(true);
                        }
                    }
                }
                SyncMessage::RequestSnapshots(requests)
                    if self.attack == 3 || self.attack == 21 =>
                {
                    requests.push(SyncSnapshotRequest {
                        table: if self.attack == 21 {
                            "people"
                        } else {
                            "local_secrets"
                        }
                        .into(),
                        row: uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abd")
                            .expect("UUID"),
                    });
                    self.injected.set(true);
                }
                SyncMessage::Inventory(inventory) if self.attack == 5 => {
                    inventory.push(sync::SyncRowInventory {
                        table: "people".into(),
                        row: uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc")
                            .expect("UUID"),
                        changes: self.advertised_changes.clone(),
                    });
                    self.injected.set(true);
                }
                SyncMessage::Changes(changes) if self.attack == 10 => {
                    for change in changes.iter_mut().filter(|c| c.table == "people") {
                        let mut document = AutoCommit::new();
                        document
                            .put(
                                ROOT,
                                "name",
                                ScalarValue::Bytes(
                                    postcard::to_allocvec(&Value::from(
                                        "forbidden historical text",
                                    ))
                                    .expect("encoded Text"),
                                ),
                            )
                            .expect("valid forbidden delta");
                        let changes = document.get_changes(&[]);
                        let forbidden = changes.last().expect("one real change");
                        change.payload = forbidden.raw_bytes().to_vec();
                        change.id = sync::SyncChangeId(forbidden.hash().0.to_vec());
                        self.injected.set(true);
                    }
                }
                SyncMessage::Changes(changes) if self.attack == 4 => {
                    for change in changes.iter_mut().filter(|c| c.table == "people") {
                        change.table = "local_secrets".into();
                        self.injected.set(true);
                    }
                }
                _ => {}
            }
            output = postcard::to_allocvec(&message).expect("frame");
        }
        if (12..=14).contains(&self.attack) {
            assert!(
                !output
                    .windows(b"forbidden historical text".len())
                    .any(|bytes| bytes == b"forbidden historical text"),
                "late forbidden value must not be disclosed"
            );
        }
        self.sender.unbounded_send(output).map_err(|_| Closed)
    }
}
async fn exchange(
    left: &Engine<InMemoryKernel, AutomergeRowCodec>,
    right: &Engine<InMemoryKernel, AutomergeRowCodec>,
    attack: u8,
) -> (bool, bool, bool) {
    let (l, r, injected) = exchange_as(left, right, attack, SyncRole::Initiator).await;
    (l.is_ok(), r.is_ok(), injected)
}

async fn exchange_as(
    left: &Engine<InMemoryKernel, AutomergeRowCodec>,
    right: &Engine<InMemoryKernel, AutomergeRowCodec>,
    attack: u8,
    role: SyncRole,
) -> (Result<(), String>, Result<(), String>, bool) {
    exchange_with_directions(left, right, attack, role, None).await
}

async fn exchange_with_directions(
    left: &Engine<InMemoryKernel, AutomergeRowCodec>,
    right: &Engine<InMemoryKernel, AutomergeRowCodec>,
    attack: u8,
    role: SyncRole,
    directions: Option<(SyncDirection, SyncDirection)>,
) -> (Result<(), String>, Result<(), String>, bool) {
    let (a, ar) = mpsc::unbounded();
    let (b, br) = mpsc::unbounded();
    let injected = Rc::new(Cell::new(false));
    let mut advertised_changes = Vec::new();
    if attack == 5 || attack == 6 || attack == 11 || attack == 13 || attack == 15 || attack == 16 {
        let units = export_sync_state_for(left).await.expect("source history");
        let unit = units
            .iter()
            .find(|unit| matches!(&unit.key, SyncKey::Row { table, .. } if table == "people"))
            .expect("source row");
        let mut document = AutoCommit::load(&unit.state).expect("source document");
        let changes = document.get_changes(&[]);
        advertised_changes = changes
            .iter()
            .take(changes.len() - 1)
            .map(|change| sync::SyncChangeId(change.hash().0.to_vec()))
            .collect();
        assert!(
            !advertised_changes.is_empty(),
            "spoof prerequisite hashes but not the last delta to force recovery"
        );
    }
    let mut lt = Channel {
        sender: a,
        receiver: br,
        attack: if attack == 5 || (17..=20).contains(&attack) || attack == 24 {
            255
        } else {
            attack
        },
        injected: injected.clone(),
        advertised_changes: Vec::new(),
        receive_only: directions.is_some_and(|(left, _)| left == SyncDirection::ReceiveOnly),
        done_count: 0,
        mutation: if (12..=14).contains(&attack) || attack == 16 {
            Some(left.clone())
        } else {
            None
        },
    };
    let mut rt = Channel {
        sender: b,
        receiver: ar,
        attack: if attack == 5
            || attack == 6
            || attack == 11
            || attack == 13
            || attack == 15
            || attack == 16
        {
            5
        } else if (17..=20).contains(&attack) || attack == 23 || attack == 24 {
            attack
        } else {
            255
        },
        receive_only: directions.is_some_and(|(_, right)| right == SyncDirection::ReceiveOnly),
        done_count: 0,
        injected: if attack == 6 || attack == 11 || attack == 13 || attack == 16 {
            Rc::new(Cell::new(false))
        } else {
            injected.clone()
        },
        mutation: None,
        advertised_changes,
    };
    let config = SessionConfig {
        direction: directions.map_or(SyncDirection::Bidirectional, |(left, _)| left),
        max_units_per_frame: 1,
        ..SessionConfig::default()
    };
    let receiver_config = SessionConfig {
        direction: directions.map_or(SyncDirection::Bidirectional, |(_, right)| right),
        ..config.clone()
    };
    let factory = MemorySyncStageFactory::new(config.max_session_bytes);
    let other = if role == SyncRole::Initiator {
        SyncRole::Responder
    } else {
        SyncRole::Initiator
    };
    let validate = |table: &str, column: &str, value: &Value| {
        if value == &Value::from("forbidden historical text") {
            assert_eq!((table, column), ("people", "name"));
            Err("historical value denied".into())
        } else {
            Ok(())
        }
    };
    let (l, r) = futures::join!(
        async {
            let r = synchronize_tables_with_stage_factory_candidate_validated_checked(
                left,
                &mut lt,
                &config,
                role,
                &factory,
                &["people"],
                validate,
                |view| {
                    if attack == 16 && view.direction == SyncValidationDirection::Outgoing {
                        let row = &view.tables[0].rows[0].row;
                        if row.values.get(1) != Some(&Value::from("Grace")) {
                            return Err("candidate recovery export denied".into());
                        }
                    }
                    Ok(())
                },
                || async { Ok(()) },
            )
            .await;
            drop(lt);
            r
        },
        async {
            let r =
                synchronize_tables_with_stage_factory_candidate_validated_checked(
                    right,
                    &mut rt,
                    &receiver_config,
                    other,
                    &factory,
                    &["people"],
                    validate,
                    |view| {
                        if attack == 15 && view.direction == SyncValidationDirection::Incoming {
                            assert_eq!(view.candidates.len(), 1);
                            assert!(view.tables[0].rows.iter().any(
                                    |row| row.row.values.get(1) == Some(&Value::from("Grace"))
                                ));
                            return Err("candidate recovery import denied".into());
                        }
                        Ok(())
                    },
                    || async { Ok(()) },
                )
                .await;
            drop(rt);
            r
        },
    );
    (
        l.map(|_| ()).map_err(|e| e.to_string()),
        r.map(|_| ()).map_err(|e| e.to_string()),
        injected.get(),
    )
}
#[test]
fn directional_reverse_payloads_requests_and_mismatches_fail_closed_in_both_roles() {
    block_on(async {
        for role in [SyncRole::Initiator, SyncRole::Responder] {
            for attack in [17, 18, 19, 20, 21, 24] {
                let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                for engine in [&left, &right] {
                    engine
                        .translate_and_execute(
                            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                            &SqlTranslator,
                        )
                        .await
                        .expect("schema");
                }
                right.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'receiver-only')", &SqlTranslator).await.expect("retained receiver history");
                let before = sync_manifest_for(&left).await.expect("source before");
                let (l, r, injected) = exchange_with_directions(
                    &left,
                    &right,
                    attack,
                    role,
                    Some((SyncDirection::SendOnly, SyncDirection::ReceiveOnly)),
                )
                .await;
                assert!(injected, "attack {attack}, {role:?} reaches wire");
                let error = if attack == 21 {
                    r.expect_err("reverse recovery denied")
                } else {
                    l.expect_err("reverse apply denied")
                };
                assert!(
                    error.contains(if attack == 21 {
                        "Reverse recovery publication is forbidden"
                    } else {
                        "Reverse history apply is forbidden"
                    }),
                    "{attack}, {role:?}: {error}"
                );
                assert_eq!(
                    sync_manifest_for(&left).await.expect("source unchanged"),
                    before
                );
            }
            for directions in [
                (SyncDirection::SendOnly, SyncDirection::SendOnly),
                (SyncDirection::ReceiveOnly, SyncDirection::ReceiveOnly),
                (SyncDirection::Bidirectional, SyncDirection::ReceiveOnly),
                (SyncDirection::SendOnly, SyncDirection::Bidirectional),
            ] {
                let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                for engine in [&left, &right] {
                    engine
                        .translate_and_execute(
                            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                            &SqlTranslator,
                        )
                        .await
                        .expect("schema");
                }
                let (l, r, _) =
                    exchange_with_directions(&left, &right, 255, role, Some(directions)).await;
                for result in [l, r] {
                    assert!(
                        result
                            .expect_err("mismatch")
                            .contains("incompatible scoped sync directions")
                    );
                }
            }
        }
    });
}

#[test]
fn directional_forward_recovery_and_historical_candidate_validation_remain_enforced() {
    block_on(async {
        for role in [SyncRole::Initiator, SyncRole::Responder] {
            for attack in [5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 23] {
                let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                for engine in [&left, &right] {
                    engine
                        .translate_and_execute(
                            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                            &SqlTranslator,
                        )
                        .await
                        .expect("schema");
                }
                let directions = Some((SyncDirection::SendOnly, SyncDirection::ReceiveOnly));
                left.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", &SqlTranslator).await.expect("source");
                if attack == 10 || attack == 12 || attack == 23 {
                    let (l, r, _) =
                        exchange_with_directions(&left, &right, 255, role, directions).await;
                    l.expect("initial source");
                    r.expect("initial receiver");
                }
                if attack == 14 {
                    right.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'independent safe value')", &SqlTranslator).await.expect("independent history");
                }
                if attack == 5 || (10..=16).contains(&attack) {
                    left.translate_and_execute("UPDATE people SET name = 'Grace'", &SqlTranslator)
                        .await
                        .expect("dependent delta");
                }
                let (l, r, injected) =
                    exchange_with_directions(&left, &right, attack, role, directions).await;
                assert!(injected, "{attack}, {role:?} reaches path");
                if attack == 23 {
                    l.expect("exact retry source");
                    r.expect("exact retry receiver");
                } else if attack == 5 {
                    l.expect("forward recovery source");
                    r.expect("forward recovery receiver");
                    assert_eq!(
                        right
                            .translate_and_execute("SELECT name FROM people", &SqlTranslator)
                            .await
                            .expect("recovered row")[0]
                            .rows,
                        vec![value::Row::new(vec![Value::from("Grace")])]
                    );
                } else {
                    let error = if (12..=14).contains(&attack) || attack == 16 {
                        l.expect_err("export validation")
                    } else {
                        r.expect_err("import validation")
                    };
                    assert!(
                        error.contains(if attack == 15 {
                            "candidate recovery import denied"
                        } else if attack == 16 {
                            "candidate recovery export denied"
                        } else {
                            "historical value denied"
                        }),
                        "{attack}, {role:?}: {error}"
                    );
                    if attack == 10 || attack == 12 {
                        assert_eq!(
                            right
                                .translate_and_execute("SELECT name FROM people", &SqlTranslator)
                                .await
                                .expect("prior commit retained")[0]
                                .rows,
                            vec![value::Row::new(vec![Value::from("Ada")])]
                        );
                    } else if attack != 14 {
                        assert!(
                            right
                                .translate_and_execute("SELECT * FROM people", &SqlTranslator)
                                .await
                                .expect("denied candidate absent")[0]
                                .rows
                                .is_empty()
                        );
                    }
                }
            }
        }
    });
}

#[test]
fn candidate_recovery_validation_denies_actual_import_and_late_export_in_both_roles() {
    block_on(async {
        for role in [SyncRole::Initiator, SyncRole::Responder] {
            for attack in [15, 16] {
                let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                for engine in [&left, &right] {
                    engine
                        .translate_and_execute(
                            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                            &SqlTranslator,
                        )
                        .await
                        .expect("schema");
                }
                left.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada'); UPDATE people SET name = 'Grace'", &SqlTranslator).await.expect("snapshot-only prerequisites plus delta");
                right.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abf' AS UUID), 'retained')", &SqlTranslator).await.expect("earlier local row");
                let (l, r, injected) = exchange_as(&left, &right, attack, role).await;
                assert!(injected, "recovery was forced");
                if attack == 15 {
                    assert!(
                        r.expect_err("candidate import denial")
                            .contains("candidate recovery import denied")
                    );
                } else {
                    assert!(
                        l.expect_err("late actual export denial")
                            .contains("candidate recovery export denied")
                    );
                }
                assert_eq!(
                    right
                        .translate_and_execute("SELECT name FROM people", &SqlTranslator)
                        .await
                        .expect("recovery rollback preserves prior row")[0]
                        .rows,
                    vec![value::Row::new(vec![Value::from("retained")])]
                );
            }
        }
    });
}

#[test]
fn outgoing_late_incremental_fallback_and_recovery_values_are_validated_before_disclosure() {
    block_on(async {
        for role in [SyncRole::Initiator, SyncRole::Responder] {
            for attack in [12, 13, 14] {
                let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                for engine in [&left, &right] {
                    engine
                        .translate_and_execute(
                            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                            &SqlTranslator,
                        )
                        .await
                        .expect("schemas");
                }
                left.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", &SqlTranslator).await.expect("valid source");
                if attack == 12 {
                    assert_eq!(exchange(&left, &right, 255).await, (true, true, false));
                } else if attack == 14 {
                    right.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'independent safe value')", &SqlTranslator).await.expect("independent snapshot-only prerequisites");
                }
                left.translate_and_execute("UPDATE people SET name = 'Grace'", &SqlTranslator)
                    .await
                    .expect("valid initial update");
                let before = sync_manifest_for(&right).await.expect("before");
                let (l, _r, injected) = exchange_as(&left, &right, attack, role).await;
                assert!(injected, "late mutation must run");
                assert!(
                    l.expect_err("contaminated exporter rejects")
                        .contains("historical value denied")
                );
                if attack == 12 || attack == 14 {
                    assert_eq!(
                        sync_manifest_for(&right)
                            .await
                            .expect("no changes disclosed"),
                        before
                    );
                } else {
                    assert!(
                        right
                            .translate_and_execute("SELECT * FROM people", &SqlTranslator)
                            .await
                            .expect("no recovered row")[0]
                            .rows
                            .is_empty()
                    );
                }
            }
        }
    });
}

#[test]
fn historical_value_validation_rejects_state_delta_and_recovery_in_both_roles() {
    block_on(async {
        for role in [SyncRole::Initiator, SyncRole::Responder] {
            for attack in 7..=11 {
                let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
                for engine in [&left, &right] {
                    engine
                        .translate_and_execute(
                            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                            &SqlTranslator,
                        )
                        .await
                        .expect("schemas");
                }
                left.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", &SqlTranslator).await.expect("valid source");
                if attack == 10 {
                    assert_eq!(exchange(&left, &right, 255).await, (true, true, false));
                }
                if attack >= 10 {
                    left.translate_and_execute("UPDATE people SET name = 'Grace'", &SqlTranslator)
                        .await
                        .expect("real incremental update");
                }
                if attack == 11 {
                    right.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abf' AS UUID), 'retained local value')", &SqlTranslator).await.expect("prior valid row");
                }
                let before = sync_manifest_for(&right).await.expect("before");
                let (_l, r, injected) = exchange_as(&left, &right, attack, role).await;
                assert!(injected, "attack {attack}, role {role:?} must reach wire");

                assert!(
                    r.expect_err("forbidden historical Text rejects")
                        .contains("historical value denied"),
                    "attack {attack}, role {role:?} must hit value callback, not incidental integrity failure"
                );
                if attack == 11 {
                    assert_eq!(
                        right
                            .translate_and_execute("SELECT name FROM people", &SqlTranslator)
                            .await
                            .expect("prior row retained")[0]
                            .rows,
                        vec![value::Row::new(vec![Value::from("retained local value")])]
                    );
                    assert_ne!(
                        sync_manifest_for(&right)
                            .await
                            .expect("earlier authorized catalog commits"),
                        before
                    );
                } else {
                    assert_eq!(
                        sync_manifest_for(&right)
                            .await
                            .expect("invalid import absent"),
                        before
                    );
                }
            }
        }
    });
}

#[test]
fn hostile_state_catalog_incremental_and_recovery_cannot_escape_scope() {
    block_on(async {
        for attack in 0..7 {
            let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
            let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
            for engine in [&left, &right] {
                engine.translate_and_execute("CREATE TABLE people (id UUID PRIMARY KEY, name TEXT); CREATE UNIQUE INDEX names ON people(name); CREATE TABLE local_secrets (id UUID PRIMARY KEY, secret TEXT)", &SqlTranslator).await.expect("schemas");
            }
            left.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada'); INSERT INTO local_secrets VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'never transfer')", &SqlTranslator).await.expect("rows");
            if attack == 4 {
                assert_eq!(exchange(&left, &right, 255).await, (true, true, false));
                left.translate_and_execute("UPDATE people SET name = 'Grace'", &SqlTranslator)
                    .await
                    .expect("incremental change");
            }
            if attack == 5 || attack == 6 {
                left.translate_and_execute("UPDATE people SET name = 'Grace'", &SqlTranslator)
                    .await
                    .expect("change depending on missing snapshot");
            }
            let before = sync_manifest_for(&right).await.expect("before");
            let (l, r, injected) = exchange(&left, &right, attack).await;
            assert!(injected, "attack {attack} reached required wire path");
            if attack == 5 {
                assert!(l && r, "selected missing dependency recovers full history");
                assert_eq!(
                    right
                        .translate_and_execute("SELECT name FROM people", &SqlTranslator)
                        .await
                        .expect("recovered row")[0]
                        .rows,
                    vec![value::Row::new(vec![Value::from("Grace")])]
                );
            } else {
                assert!(!l && !r, "attack {attack} rejects");
            }
            if attack != 3 && attack != 5 && attack != 6 {
                assert_eq!(
                    sync_manifest_for(&right).await.expect("unchanged target"),
                    before
                );
            }
            if attack == 6 {
                assert!(
                    right
                        .translate_and_execute("SELECT * FROM people", &SqlTranslator)
                        .await
                        .expect("rejected recovery did not import row")[0]
                        .rows
                        .is_empty()
                );
            }
            assert!(
                right
                    .translate_and_execute("SELECT * FROM local_secrets", &SqlTranslator)
                    .await
                    .expect("local secrets")[0]
                    .rows
                    .is_empty()
            );
        }
    });
}
