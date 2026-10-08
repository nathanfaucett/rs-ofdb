use core::fmt;
use std::{cell::Cell, rc::Rc};

use automerge::{AutoCommit, ROOT, ScalarValue, transaction::Transactable};
use engine::{Engine, InMemoryKernel};
use engine_automerge::AutomergeRowCodec;
use futures::{StreamExt, channel::mpsc, executor::block_on};
use sql_translator::SqlTranslator;
use sync::{
    MemorySyncStageFactory, SessionConfig, SyncKey, SyncMessage, SyncRole, SyncSnapshotRequest,
    SyncStateUnit, SyncTransport, sync_manifest_for, synchronize_tables_with_stage_factory_checked,
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
}
impl SyncTransport for Channel {
    type Error = Closed;
    async fn receive(&mut self) -> Result<Vec<u8>, Closed> {
        self.receiver.next().await.ok_or(Closed)
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Closed> {
        let mut output = frame;
        if let Ok(mut message) = postcard::from_bytes::<SyncMessage>(&output) {
            match &mut message {
                SyncMessage::State(units) if self.attack <= 2 || self.attack == 6 => {
                    for unit in units {
                        let SyncKey::Row { table, row } = &unit.key;
                        if ((self.attack <= 1 || self.attack == 6) && table == "people")
                            || (self.attack == 2 && table == "__engine_index_fields")
                        {
                            let mut document =
                                AutoCommit::load(&unit.state).expect("fixture state");
                            if self.attack == 0 || self.attack == 6 {
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
        self.sender.unbounded_send(output).map_err(|_| Closed)
    }
}
async fn exchange(
    left: &Engine<InMemoryKernel, AutomergeRowCodec>,
    right: &Engine<InMemoryKernel, AutomergeRowCodec>,
    attack: u8,
) -> (bool, bool, bool) {
    let (a, ar) = mpsc::unbounded();
    let (b, br) = mpsc::unbounded();
    let injected = Rc::new(Cell::new(false));
    let mut lt = Channel {
        sender: a,
        receiver: br,
        attack: if attack == 5 { 255 } else { attack },
        injected: injected.clone(),
    };
    let mut rt = Channel {
        sender: b,
        receiver: ar,
        attack: if attack == 5 || attack == 6 { 5 } else { 255 },
        injected: if attack == 6 {
            Rc::new(Cell::new(false))
        } else {
            injected.clone()
        },
    };
    let config = SessionConfig {
        max_units_per_frame: 1,
        ..SessionConfig::default()
    };
    let factory = MemorySyncStageFactory::new(config.max_session_bytes);
    let (l, r) = futures::join!(
        async {
            let r = synchronize_tables_with_stage_factory_checked(
                left,
                &mut lt,
                &config,
                SyncRole::Initiator,
                &factory,
                &["people"],
                || async { Ok(()) },
            )
            .await;
            drop(lt);
            r
        },
        async {
            let r = synchronize_tables_with_stage_factory_checked(
                right,
                &mut rt,
                &config,
                SyncRole::Responder,
                &factory,
                &["people"],
                || async { Ok(()) },
            )
            .await;
            drop(rt);
            r
        },
    );
    (l.is_ok(), r.is_ok(), injected.get())
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
