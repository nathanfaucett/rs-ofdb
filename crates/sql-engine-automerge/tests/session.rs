use core::fmt;
use std::{cell::RefCell, rc::Rc};

use engine::{Engine, InMemoryKernel, RowCodec};
use engine_automerge::AutomergeRowCodec;
use futures::{StreamExt, channel::mpsc, executor::block_on};
use query::{DataDefinition, Query, QueryInsert, Statement};
use schema::{ColumnSchema, IndexSchema, TableSchema};
use sql_translator::SqlTranslator;
use sync::{
    SessionConfig, SyncKey, SyncMessage, SyncRole, SyncRowCodec, SyncTransport, sync_manifest_for,
    synchronize,
};
use value::{Row, Value, ValueType};

#[derive(Debug)]
struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("channel closed")
    }
}

struct ChannelTransport {
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    sender: mpsc::UnboundedSender<Vec<u8>>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
}

impl SyncTransport for ChannelTransport {
    type Error = Closed;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.next().await.ok_or(Closed)
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sent
            .borrow_mut()
            .push(postcard::from_bytes(&frame).unwrap());
        self.sender.unbounded_send(frame).map_err(|_| Closed)
    }
}

fn transport_pair() -> (ChannelTransport, ChannelTransport) {
    let (left_sender, right_receiver) = mpsc::unbounded();
    let (right_sender, left_receiver) = mpsc::unbounded();
    (
        ChannelTransport {
            receiver: left_receiver,
            sender: left_sender,
            sent: Rc::new(RefCell::new(Vec::new())),
        },
        ChannelTransport {
            receiver: right_receiver,
            sender: right_sender,
            sent: Rc::new(RefCell::new(Vec::new())),
        },
    )
}

fn table(name: &str) -> TableSchema {
    TableSchema {
        name: name.into(),
        columns: vec![ColumnSchema {
            name: "id".into(),
            r#type: ValueType::Uuid,
            default: Value::Null,
            primary_key: true,
        }],
    }
}

fn row_uuid(value: u128) -> uuid::Uuid {
    let mut bytes = value.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

fn people_table() -> TableSchema {
    TableSchema {
        name: "people".into(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                r#type: ValueType::Uuid,
                default: Value::Null,
                primary_key: true,
            },
            ColumnSchema {
                name: "name".into(),
                r#type: ValueType::Text,
                default: Value::Null,
                primary_key: false,
            },
        ],
    }
}

async fn sync(
    left: &Engine<InMemoryKernel, AutomergeRowCodec>,
    right: &Engine<InMemoryKernel, AutomergeRowCodec>,
    config: &SessionConfig,
) -> Vec<SyncMessage> {
    let (mut left_transport, mut right_transport) = transport_pair();
    let (left_result, right_result) = futures::join!(
        synchronize(left, &mut left_transport, config, SyncRole::Initiator),
        synchronize(right, &mut right_transport, config, SyncRole::Responder),
    );
    left_result.unwrap();
    right_result.unwrap();
    left_transport.sent.borrow().clone()
}

#[test]
fn synchronizes_catalog_state_and_converges() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let users = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Uuid,
                    default: Value::Null,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "email".into(),
                    r#type: ValueType::Text,
                    default: Value::Null,
                    primary_key: false,
                },
            ],
        };
        left.create_table(users).await.unwrap();
        left.execute(vec![query::Statement::DataDefinition(
            query::DataDefinition::CreateIndex {
                schema: schema::IndexSchema {
                    name: "users_by_email".into(),
                    table_name: "users".into(),
                    column_indices: vec![1],
                    unique: true,
                },
                if_not_exists: false,
            },
        )])
        .await
        .unwrap();
        left.execute(vec![Statement::Query(Query::Insert(QueryInsert {
            table: "users".into(),
            row: Row::new(vec![
                Value::Uuid(row_uuid(1)),
                Value::from("ada@example.test"),
            ]),
            returning: None,
        }))])
        .await
        .unwrap();

        let frames = sync(&left, &right, &SessionConfig::default()).await;

        assert_eq!(
            right.table_schema("users").await.unwrap(),
            TableSchema {
                name: "users".into(),
                columns: vec![
                    ColumnSchema {
                        name: "id".into(),
                        r#type: ValueType::Uuid,
                        default: Value::Null,
                        primary_key: true,
                    },
                    ColumnSchema {
                        name: "email".into(),
                        r#type: ValueType::Text,
                        default: Value::Null,
                        primary_key: false,
                    },
                ],
            }
        );
        assert_eq!(
            right.index_schema("users_by_email").await.unwrap(),
            schema::IndexSchema {
                name: "users_by_email".into(),
                table_name: "users".into(),
                column_indices: vec![1],
                unique: true,
            }
        );
        assert!(
            right
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "users".into(),
                    row: Row::new(vec![
                        Value::Uuid(row_uuid(2)),
                        Value::from("ada@example.test"),
                    ]),
                    returning: None,
                }))])
                .await
                .is_err()
        );
        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::State(_)))
        );
        assert_eq!(
            sync::sync_manifest_for(&left).await.unwrap(),
            sync::sync_manifest_for(&right).await.unwrap()
        );
    });
}

#[test]
fn oversized_single_row_fails_sync_and_preserves_source_data() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        left.create_table(people_table()).await.unwrap();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let oversized_value = (0..sync::MAX_MESSAGE_BYTES * 3)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                char::from(b'a' + (seed % 26) as u8)
            })
            .collect::<String>();
        left.execute(vec![Statement::Query(Query::Insert(QueryInsert {
            table: "people".into(),
            row: Row::new(vec![Value::Uuid(row_uuid(1)), Value::from(oversized_value)]),
            returning: None,
        }))])
        .await
        .unwrap();

        let (mut left_transport, mut right_transport) = transport_pair();
        let config = SessionConfig::default();
        let (left_result, right_result) = futures::join!(
            synchronize(&left, &mut left_transport, &config, SyncRole::Initiator),
            synchronize(&right, &mut right_transport, &config, SyncRole::Responder),
        );
        assert!(left_result.is_err());
        assert!(right_result.is_err());

        let source_rows = left
            .translate_and_execute("SELECT * FROM people;", &SqlTranslator)
            .await
            .unwrap();
        assert_eq!(source_rows[0].rows.len(), 1);
        assert!(right.table_names().await.unwrap().is_empty());
    });
}

#[test]
fn oversized_incremental_fails_sync_and_preserves_both_versions() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let translator = SqlTranslator;
        left.create_table(people_table()).await.unwrap();
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let oversized_value = (0..sync::MAX_MESSAGE_BYTES * 3)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                char::from(b'a' + (seed % 26) as u8)
            })
            .collect::<String>();
        let update = format!(
            "UPDATE people SET name = '{oversized_value}' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);"
        );
        left.translate_and_execute(&update, &translator)
            .await
            .unwrap();

        let (mut left_transport, mut right_transport) = transport_pair();
        let config = SessionConfig::default();
        let (left_result, right_result) = futures::join!(
            synchronize(&left, &mut left_transport, &config, SyncRole::Initiator),
            synchronize(&right, &mut right_transport, &config, SyncRole::Responder),
        );
        assert!(left_result.is_err());
        assert!(right_result.is_err());

        let source_rows = left
            .translate_and_execute("SELECT * FROM people;", &translator)
            .await
            .unwrap();
        assert_eq!(source_rows[0].rows.len(), 1);
        let destination_rows = right
            .translate_and_execute("SELECT * FROM people;", &translator)
            .await
            .unwrap();
        assert_eq!(destination_rows[0].rows.len(), 1);
        assert_eq!(destination_rows[0].rows[0].values[1], Value::from("Ada"));
    });
}

#[test]
fn realtime_update_sends_one_incremental_without_snapshot() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        left.create_table(people_table()).await.unwrap();
        let translator = SqlTranslator;
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();

        let _ = sync(&left, &right, &SessionConfig::default()).await;
        left.translate_and_execute(
            "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();

        let (mut left_transport, mut right_transport) = transport_pair();
        let config = SessionConfig::default();
        let (left_result, right_result) = futures::join!(
            synchronize(&left, &mut left_transport, &config, SyncRole::Initiator),
            synchronize(&right, &mut right_transport, &config, SyncRole::Responder),
        );
        let left_result = left_result.unwrap();
        right_result.unwrap();
        let frames = left_transport.sent.borrow().clone();
        assert_eq!(left_result.sent_changes, 1);
        assert_eq!(left_result.sent_snapshots, 0);
        assert!(
            frames
                .iter()
                .any(|frame| matches!(frame, SyncMessage::Changes(changes) if changes.len() == 1))
        );
        assert!(
            frames
                .iter()
                .all(|frame| !matches!(frame, SyncMessage::State(_)))
        );
        assert_eq!(
            sync::sync_manifest_for(&left).await.unwrap(),
            sync::sync_manifest_for(&right).await.unwrap()
        );
    });
}

#[test]
fn propagates_row_deletion_without_resurrecting_the_row() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let translator = SqlTranslator;
        left.create_table(people_table()).await.unwrap();
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        left.translate_and_execute(
            "DELETE FROM people WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        let rows = right
            .translate_and_execute("SELECT * FROM people;", &translator)
            .await
            .unwrap();
        assert!(rows.iter().all(|result| result.rows.is_empty()));
    });
}

#[test]
fn independently_created_same_name_schema_rows_converge() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        for engine in [&left, &right] {
            engine.create_table(people_table()).await.unwrap();
            engine
                .execute(vec![Statement::DataDefinition(
                    DataDefinition::CreateIndex {
                        schema: IndexSchema {
                            name: "people_by_name".into(),
                            table_name: "people".into(),
                            column_indices: vec![1],
                            unique: false,
                        },
                        if_not_exists: false,
                    },
                )])
                .await
                .unwrap();
        }

        let left_before_sync = sync_manifest_for(&left).await.unwrap();
        let right_before_sync = sync_manifest_for(&right).await.unwrap();
        let table_rows = |manifest: &sync::SyncManifest, table: &str| {
            manifest
                .entries
                .iter()
                .filter_map(|(key, _)| match key {
                    SyncKey::Row {
                        table: row_table,
                        row,
                    } if row_table == table => Some(*row),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_ne!(
            table_rows(&left_before_sync, engine::ENGINE_TABLES_STORAGE),
            table_rows(&right_before_sync, engine::ENGINE_TABLES_STORAGE),
            "same-name schema rows have independent UUIDv7 primary keys"
        );
        assert_ne!(
            table_rows(&left_before_sync, engine::ENGINE_TABLE_FIELDS_STORAGE),
            table_rows(&right_before_sync, engine::ENGINE_TABLE_FIELDS_STORAGE),
            "same-name field rows have independent UUIDv7 primary keys"
        );
        assert_ne!(
            table_rows(&left_before_sync, engine::ENGINE_INDICES_STORAGE),
            table_rows(&right_before_sync, engine::ENGINE_INDICES_STORAGE),
            "same-name index rows have independent UUIDv7 primary keys"
        );
        assert_ne!(
            table_rows(&left_before_sync, engine::ENGINE_INDEX_FIELDS_STORAGE),
            table_rows(&right_before_sync, engine::ENGINE_INDEX_FIELDS_STORAGE),
            "same-name index-field rows have independent UUIDv7 primary keys"
        );

        sync(&left, &right, &SessionConfig::default()).await;

        assert_eq!(left.table_schema("people").await.unwrap(), people_table());
        assert_eq!(right.table_schema("people").await.unwrap(), people_table());
        let index = IndexSchema {
            name: "people_by_name".into(),
            table_name: "people".into(),
            column_indices: vec![1],
            unique: false,
        };
        assert_eq!(left.index_schema("people_by_name").await.unwrap(), index);
        assert_eq!(right.index_schema("people_by_name").await.unwrap(), index);
        let left_keys = sync_manifest_for(&left)
            .await
            .unwrap()
            .entries
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        let right_keys = sync_manifest_for(&right)
            .await
            .unwrap()
            .entries
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(left_keys, right_keys);
    });
}

#[test]
fn synchronizes_concurrent_branches() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let translator = SqlTranslator;
        left.create_table(people_table()).await.unwrap();
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        left.translate_and_execute(
            "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();
        right.translate_and_execute(
            "UPDATE people SET name = 'Lin' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;

        assert_eq!(
            sync::sync_manifest_for(&left).await.unwrap(),
            sync::sync_manifest_for(&right).await.unwrap()
        );
    });
}

#[test]
fn duplicate_incremental_frames_are_idempotent() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let translator = SqlTranslator;
        left.create_table(people_table()).await.unwrap();
        left.translate_and_execute(
            "INSERT INTO people (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada');",
            &translator,
        )
        .await
        .unwrap();
        sync(&left, &right, &SessionConfig::default()).await;
        left.translate_and_execute(
            "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID);",
            &translator,
        )
        .await
        .unwrap();

        let (mut left_transport, mut right_transport) = transport_pair();
        let config = SessionConfig::default();
        let (left_result, right_result) = futures::join!(
            synchronize(&left, &mut left_transport, &config, SyncRole::Initiator),
            synchronize(&right, &mut right_transport, &config, SyncRole::Responder),
        );
        left_result.unwrap();
        right_result.unwrap();
        let change = left_transport
            .sent
            .borrow()
            .iter()
            .find_map(|message| match message {
                SyncMessage::Changes(changes) => changes.first().cloned(),
                _ => None,
            })
            .unwrap();
        let table = change.table;
        let row = change.row;
        let id = change.id;
        let payload = change.payload;
        let table_for_apply = table.clone();
        right
            .mutate_transaction(&table, row, move |codec, transaction, _| {
                Box::pin(async move {
                    codec
                        .apply_change(transaction, &table_for_apply, row, &id, &payload)
                        .await?;
                    let value = codec.get_row(transaction, &table_for_apply, &row).await?;
                    Ok(((), value))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            sync::sync_manifest_for(&right).await.unwrap(),
            sync::sync_manifest_for(&left).await.unwrap()
        );
    });
}

#[test]
fn batches_state_units_without_checkpoint_or_envelope_messages() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        for name in ["one", "two", "three"] {
            left.create_table(table(name)).await.unwrap();
        }

        let frames = sync(
            &left,
            &right,
            &SessionConfig {
                max_units_per_frame: 2,
                ..SessionConfig::default()
            },
        )
        .await;
        let batches = frames
            .iter()
            .filter_map(|frame| match frame {
                SyncMessage::State(units) => Some(units.len()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(batches.len() > 1);
        assert!(batches.iter().all(|size| *size <= 2));
        assert!(frames.iter().all(|frame| {
            matches!(
                frame,
                SyncMessage::Hello(_)
                    | SyncMessage::Manifest(_)
                    | SyncMessage::Inventory(_)
                    | SyncMessage::State(_)
                    | SyncMessage::Changes(_)
                    | SyncMessage::RequestSnapshots(_)
                    | SyncMessage::Done
            )
        }));
    });
}
