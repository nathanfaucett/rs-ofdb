use core::fmt;
use std::{cell::Cell, rc::Rc};

use automerge::{ActorId, AutoCommit, ROOT, ReadDoc, ScalarValue, transaction::Transactable};
use engine::{Engine, InMemoryKernel};
use engine_automerge::AutomergeRowCodec;
use futures::{StreamExt, channel::mpsc, executor::block_on};
use sql_translator::SqlTranslator;
use sync::{
    MemorySyncStageFactory, SessionConfig, SyncKey, SyncMessage, SyncRole, SyncSnapshotRequest,
    SyncStateUnit, SyncTransport, sync_manifest_for,
    synchronize_tables_with_stage_factory_validated_checked,
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
}
impl SyncTransport for Channel {
    type Error = Closed;
    async fn receive(&mut self) -> Result<Vec<u8>, Closed> {
        self.receiver.next().await.ok_or(Closed)
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Closed> {
        let mut output = frame;
        if let Ok(mut message) = postcard::from_bytes::<SyncMessage>(&output) {
            if ((self.attack == 12 && matches!(message, SyncMessage::Inventory(_)))
                || (self.attack == 13 && matches!(message, SyncMessage::RequestSnapshots(_))))
                && !self.injected.get()
            {
                self.mutation.as_ref().expect("late source mutation").translate_and_execute(
                    "UPDATE people SET name = 'forbidden historical text'; UPDATE people SET name = 'safe late overwrite'", &SqlTranslator,
                ).await.expect("change after preflight");
                self.injected.set(true);
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
                            || (self.attack == 2 && table == "__engine_index_fields")
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
                SyncMessage::RequestSnapshots(requests) if self.attack == 3 => {
                    requests.push(SyncSnapshotRequest {
                        table: "local_secrets".into(),
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
                        changes: Vec::new(),
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
        if self.attack == 12 || self.attack == 13 {
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
    let (a, ar) = mpsc::unbounded();
    let (b, br) = mpsc::unbounded();
    let injected = Rc::new(Cell::new(false));
    let mut lt = Channel {
        sender: a,
        receiver: br,
        attack: if attack == 5 { 255 } else { attack },
        injected: injected.clone(),
        mutation: if attack == 12 || attack == 13 {
            Some(left.clone())
        } else {
            None
        },
    };
    let mut rt = Channel {
        sender: b,
        receiver: ar,
        attack: if attack == 5 || attack == 6 || attack == 11 || attack == 13 {
            5
        } else {
            255
        },
        injected: if attack == 6 || attack == 11 || attack == 13 {
            Rc::new(Cell::new(false))
        } else {
            injected.clone()
        },
        mutation: None,
    };
    let config = SessionConfig {
        max_units_per_frame: 1,
        ..SessionConfig::default()
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
            let r = synchronize_tables_with_stage_factory_validated_checked(
                left,
                &mut lt,
                &config,
                role,
                &factory,
                &["people"],
                validate,
                || async { Ok(()) },
            )
            .await;
            drop(lt);
            r
        },
        async {
            let r = synchronize_tables_with_stage_factory_validated_checked(
                right,
                &mut rt,
                &config,
                other,
                &factory,
                &["people"],
                validate,
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
fn outgoing_late_incremental_and_recovery_values_are_validated_before_disclosure() {
    block_on(async {
        for role in [SyncRole::Initiator, SyncRole::Responder] {
            for attack in [12, 13] {
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
                if attack == 12 {
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
