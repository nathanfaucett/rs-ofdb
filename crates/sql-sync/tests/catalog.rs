mod support;

use core::fmt;

use engine::{
    ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE, ENGINE_TABLES_STORAGE, Engine,
    InMemoryKernel, Kernel, KernelTransaction, Uuid,
};
use engine_redb::{RedbKernel, redb};
use futures::{
    FutureExt, StreamExt,
    channel::{mpsc, oneshot},
    executor::block_on,
    future::{Either, select},
};
use support::TestCodec;
use sync::{
    MAX_APPLY_BATCH_BYTES, SessionConfig, SyncChangeId, SyncIncrementalChange, SyncKey,
    SyncMessage, SyncRole, SyncStateUnit, SyncTransport, apply_sync_state_batch_for,
    export_sync_state_for, sync_manifest_for, synchronize,
};
use value::{Row, Value, ValueType};

#[derive(Debug)]
struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("connection closed")
    }
}

struct Transport {
    incoming: mpsc::UnboundedReceiver<Vec<u8>>,
    outgoing: mpsc::UnboundedSender<Vec<u8>>,
    interrupt_after_state: bool,
    inject_change: bool,
    reverse_states: bool,
    buffered: Vec<Vec<u8>>,
}

impl SyncTransport for Transport {
    type Error = Closed;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.incoming.next().await.ok_or(Closed)
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        let message: SyncMessage = postcard::from_bytes(&frame).expect("decode frame");
        if self.reverse_states && matches!(message, SyncMessage::State(_)) {
            self.buffered.push(frame);
            return Ok(());
        }
        if matches!(message, SyncMessage::Done) {
            for state in self.buffered.drain(..).rev() {
                self.outgoing.unbounded_send(state).map_err(|_| Closed)?;
            }
            if self.inject_change {
                self.outgoing
                    .unbounded_send(
                        postcard::to_allocvec(&SyncMessage::Changes(vec![SyncIncrementalChange {
                            table: "people".into(),
                            row: row_uuid(7),
                            id: SyncChangeId(vec![1]),
                            payload: vec![1],
                        }]))
                        .expect("encode dependent change"),
                    )
                    .map_err(|_| Closed)?;
                self.inject_change = false;
            }
        }
        let is_state = matches!(message, SyncMessage::State(_));
        self.outgoing.unbounded_send(frame).map_err(|_| Closed)?;
        if is_state && self.interrupt_after_state {
            return Err(Closed);
        }
        Ok(())
    }
}

fn row_uuid(value: u128) -> Uuid {
    let mut bytes = value.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn transport_pair(interrupt: bool) -> (Transport, Transport) {
    let (left_sender, right_receiver) = mpsc::unbounded();
    let (right_sender, left_receiver) = mpsc::unbounded();
    let transport = |incoming, outgoing, interrupt_after_state| Transport {
        incoming,
        outgoing,
        interrupt_after_state,
        inject_change: false,
        reverse_states: false,
        buffered: Vec::new(),
    };
    (
        transport(left_receiver, left_sender, interrupt),
        transport(right_receiver, right_sender, false),
    )
}

struct StalledReceiveTransport {
    receives: usize,
    ready: Option<oneshot::Sender<()>>,
}

impl SyncTransport for StalledReceiveTransport {
    type Error = Closed;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receives += 1;
        let message = match self.receives {
            1 => SyncMessage::Hello(sync::SyncHello {
                protocol_version: sync::PROTOCOL_VERSION,
                manifest: sync::SyncManifest::default(),
            }),
            2 => SyncMessage::Manifest(sync::SyncManifest::default()),
            3 => SyncMessage::Inventory(Vec::new()),
            _ => {
                self.ready
                    .take()
                    .expect("signal once when sync waits for peer data")
                    .send(())
                    .expect("writer test is waiting");
                return futures::future::pending().await;
            }
        };
        postcard::to_allocvec(&message).map_err(|_| Closed)
    }

    async fn send(&mut self, _frame: Vec<u8>) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[test]
fn rejects_non_uuidv7_row_addresses_before_applying_sync_state() {
    block_on(async {
        let engine = destination();
        let row = Uuid::from_bytes([0x11; 16]);
        let unit = SyncStateUnit::new(
            SyncKey::Row {
                table: String::from("people"),
                row,
            },
            vec![1],
            Vec::new(),
        );
        let error = apply_sync_state_batch_for(&engine, vec![unit])
            .await
            .expect_err("non-UUIDv7 row address must be rejected");
        assert!(error.to_string().contains("UUIDv7"));
    });
}

#[test]
fn waiting_for_network_frames_does_not_hold_the_engine_write_transaction() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let engine = Engine::new(kernel.clone(), TestCodec);
        let (ready, waiting) = oneshot::channel();
        let mut transport = StalledReceiveTransport {
            receives: 0,
            ready: Some(ready),
        };
        let config = SessionConfig::default();
        let sync = synchronize(&engine, &mut transport, &config, SyncRole::Initiator);
        let writer = async {
            waiting.await.expect("sync reached peer receive");
            let mut transaction = kernel
                .transaction()
                .await
                .expect("unrelated writer can begin during network receive");
            transaction
                .ensure_table("unrelated")
                .await
                .expect("create unrelated table");
            transaction
                .put_bytes("unrelated", vec![1], vec![2])
                .await
                .expect("write unrelated data");
            transaction.commit().await.expect("commit unrelated writer");
        };
        match select(sync.boxed(), writer.boxed()).await {
            Either::Right(((), _sync)) => {}
            Either::Left((result, _writer)) => panic!("sync ended before writer: {result:?}"),
        }
        let transaction = kernel.transaction().await.expect("read committed data");
        assert_eq!(
            transaction
                .get_bytes("unrelated", &[1])
                .await
                .expect("read committed value"),
            Some(vec![2])
        );
    });
}

#[test]
fn waiting_for_network_frames_does_not_hold_the_redb_write_transaction() {
    block_on(async {
        let path = std::env::temp_dir().join(format!(
            "ofdb-sync-writer-progress-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after Unix epoch")
                .as_nanos()
        ));
        let database = std::sync::Arc::new(
            redb::Database::create(&path).expect("create temporary Redb database"),
        );
        let sync_kernel = RedbKernel::new(std::sync::Arc::clone(&database));
        let writer_kernel = RedbKernel::new(std::sync::Arc::clone(&database));
        let engine = Engine::new(sync_kernel, TestCodec);
        let (ready, waiting) = oneshot::channel();
        let mut transport = StalledReceiveTransport {
            receives: 0,
            ready: Some(ready),
        };
        let config = SessionConfig::default();
        let sync = synchronize(&engine, &mut transport, &config, SyncRole::Initiator);
        let writer = async {
            waiting.await.expect("sync reached peer receive");
            let mut transaction = writer_kernel
                .transaction()
                .await
                .expect("unrelated Redb writer can begin during network receive");
            transaction
                .ensure_table("unrelated")
                .await
                .expect("create unrelated table");
            transaction
                .put_bytes("unrelated", vec![1], vec![2])
                .await
                .expect("write unrelated data");
            transaction.commit().await.expect("commit unrelated writer");
        };
        match select(sync.boxed(), writer.boxed()).await {
            Either::Right(((), _sync)) => {}
            Either::Left((result, _writer)) => panic!("sync ended before writer: {result:?}"),
        }
        let transaction = writer_kernel
            .transaction()
            .await
            .expect("read committed Redb data");
        assert_eq!(
            transaction
                .get_bytes("unrelated", &[1])
                .await
                .expect("read committed value"),
            Some(vec![2])
        );
        transaction
            .rollback()
            .await
            .expect("finish read transaction");
        drop(engine);
        drop(writer_kernel);
        drop(database);
        std::fs::remove_file(path).expect("remove temporary Redb database");
    });
}

#[test]
fn repeated_state_delivery_is_idempotent_and_manifest_canonical() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let units = export_sync_state_for(&source)
            .await
            .expect("export source state");

        apply_sync_state_batch_for(&destination, units.clone())
            .await
            .expect("apply initial state");
        let first_manifest = sync_manifest_for(&destination)
            .await
            .expect("build initial manifest");
        let first_state = export_sync_state_for(&destination)
            .await
            .expect("export initial destination state");

        apply_sync_state_batch_for(&destination, units)
            .await
            .expect("repeat state delivery");
        let repeated_manifest = sync_manifest_for(&destination)
            .await
            .expect("build repeated manifest");
        let repeated_state = export_sync_state_for(&destination)
            .await
            .expect("export repeated destination state");

        assert_eq!(repeated_manifest, first_manifest);
        assert_eq!(
            repeated_state
                .iter()
                .map(|unit| (&unit.key, unit.digest))
                .collect::<Vec<_>>(),
            first_state
                .iter()
                .map(|unit| (&unit.key, unit.digest))
                .collect::<Vec<_>>()
        );
    });
}

#[test]
fn manifest_matches_exported_state_digests() {
    block_on(async {
        let source = source().await;
        let expected = export_sync_state_for(&source)
            .await
            .expect("export source state")
            .into_iter()
            .map(|unit| (unit.key, unit.digest))
            .collect::<Vec<_>>();
        let manifest = sync_manifest_for(&source)
            .await
            .expect("build manifest without retaining state snapshots");

        assert_eq!(manifest.entries, expected);
    });
}

fn destination() -> Engine<InMemoryKernel, TestCodec> {
    Engine::new(InMemoryKernel::new(), TestCodec)
}

async fn source() -> Engine<InMemoryKernel, TestCodec> {
    source_with_rows(1).await
}

async fn source_with_rows(user_rows: usize) -> Engine<InMemoryKernel, TestCodec> {
    let kernel = InMemoryKernel::new();
    let mut transaction = kernel.transaction().await.expect("source transaction");
    transaction
        .ensure_table("people")
        .await
        .expect("source data storage");
    let table = Uuid::now_v7();
    let field = Uuid::now_v7();
    let payload_field = Uuid::now_v7();
    for (storage, id, value) in [
        (
            ENGINE_TABLES_STORAGE,
            table,
            Row::new(vec![Value::from("people")]),
        ),
        (
            ENGINE_TABLE_FIELDS_STORAGE,
            field,
            Row::new(vec![
                Value::Uuid(table),
                Value::from("people"),
                Value::from("id"),
                Value::from(ValueType::Uuid),
                Value::Null,
                Value::Integer(0),
                Value::Bool(true),
            ]),
        ),
        (
            ENGINE_TABLE_FIELDS_STORAGE,
            payload_field,
            Row::new(vec![
                Value::Uuid(table),
                Value::from("people"),
                Value::from("payload"),
                Value::from(ValueType::Text),
                Value::Null,
                Value::Integer(1),
                Value::Bool(false),
            ]),
        ),
    ] {
        transaction
            .ensure_table(storage)
            .await
            .expect("source storage");
        transaction
            .put_bytes(
                storage,
                id.as_bytes().to_vec(),
                postcard::to_allocvec(&value).expect("encode row"),
            )
            .await
            .expect("source row");
    }
    for index in 0..user_rows {
        let user = row_uuid(index as u128 + 4);
        transaction
            .put_bytes(
                "people",
                user.as_bytes().to_vec(),
                postcard::to_allocvec(&Row::new(vec![Value::Uuid(user), Value::from("")]))
                    .expect("encode user row"),
            )
            .await
            .expect("source user row");
    }
    transaction.commit().await.expect("commit source");
    Engine::new(kernel, TestCodec)
}

#[test]
fn interrupted_catalog_frames_leave_no_partial_table_and_retry() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let config = SessionConfig {
            max_units_per_frame: 1,
            ..SessionConfig::default()
        };
        let (mut left, mut right) = transport_pair(true);
        let (sent, received) = futures::join!(
            async {
                let result = synchronize(&source, &mut left, &config, SyncRole::Initiator).await;
                drop(left);
                result
            },
            synchronize(&destination, &mut right, &config, SyncRole::Responder)
        );
        assert!(sent.is_err());
        assert!(received.is_err());
        assert!(
            destination
                .table_names()
                .await
                .expect("destination catalog")
                .is_empty()
        );

        let (mut left, mut right) = transport_pair(false);
        left.reverse_states = true;
        let (sent, received) = futures::join!(
            synchronize(&source, &mut left, &config, SyncRole::Initiator),
            synchronize(&destination, &mut right, &config, SyncRole::Responder)
        );
        sent.expect("retry sender");
        received.expect("retry receiver");
        assert_eq!(
            destination.table_names().await.expect("recovered catalog"),
            vec!["people"]
        );
        assert_eq!(
            destination
                .table_schema("people")
                .await
                .expect("recovered schema")
                .columns
                .len(),
            2
        );
        assert!(
            export_sync_state_for(&destination)
                .await
                .expect("recovered state")
                .iter()
                .any(|unit| matches!(&unit.key, SyncKey::Row { table, .. } if table == "people"))
        );
    });
}

#[test]
fn session_payload_limit_rejects_before_catalog_commit() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let sender_config = SessionConfig::default();
        let receiver_config = SessionConfig {
            max_session_bytes: 1,
            ..SessionConfig::default()
        };
        let (mut left, mut right) = transport_pair(false);
        let (sent, received) = futures::join!(
            synchronize(&source, &mut left, &sender_config, SyncRole::Initiator),
            synchronize(
                &destination,
                &mut right,
                &receiver_config,
                SyncRole::Responder
            )
        );
        assert!(sent.is_err());
        assert!(received.is_err());
        assert!(
            destination
                .table_names()
                .await
                .expect("destination catalog")
                .is_empty()
        );
        assert_eq!(
            source.table_names().await.expect("source catalog"),
            vec!["people"]
        );
    });
}

#[test]
fn syncs_four_thousand_rows_with_default_session_budget() {
    block_on(async {
        let source = source_with_rows(4_096).await;
        let destination = destination();
        let config = SessionConfig::default();
        let (mut left, mut right) = transport_pair(false);
        let (sent, received) = futures::join!(
            synchronize(&source, &mut left, &config, SyncRole::Initiator),
            synchronize(&destination, &mut right, &config, SyncRole::Responder)
        );

        sent.expect("large source sync completes");
        received.expect("large destination sync completes");
        assert_eq!(
            destination
                .table_names()
                .await
                .expect("destination catalog"),
            vec!["people"]
        );
    });
}

#[test]
fn cumulative_inbound_budget_aborts_multiframe_catalog_before_apply() {
    block_on(async {
        let source = source_with_rows(2048).await;
        let destination = destination();
        let sender_config = SessionConfig::default();
        let receiver_config = SessionConfig {
            max_session_bytes: 8 * 1024,
            ..SessionConfig::default()
        };
        let (mut left, mut right) = transport_pair(false);
        let (sent, received) = futures::join!(
            synchronize(&source, &mut left, &sender_config, SyncRole::Initiator),
            synchronize(
                &destination,
                &mut right,
                &receiver_config,
                SyncRole::Responder
            )
        );

        assert!(sent.is_err(), "sender observes the receiver abort");
        let error = received.expect_err("cumulative budget aborts receiver");
        assert!(
            error.to_string().contains("sync session payload size"),
            "unexpected error: {error}"
        );
        assert!(
            destination
                .table_names()
                .await
                .expect("destination catalog")
                .is_empty(),
            "the receiver must not apply a partial catalog"
        );
        assert_eq!(
            source.table_names().await.expect("source catalog"),
            vec!["people"]
        );
    });
}

#[test]
fn outbound_payload_limit_aborts_peer_and_preserves_both_catalogs() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let sender_config = SessionConfig {
            max_session_bytes: 1,
            ..SessionConfig::default()
        };
        let receiver_config = SessionConfig::default();
        let (mut left, mut right) = transport_pair(false);
        let (sent, received) = futures::join!(
            synchronize(&source, &mut left, &sender_config, SyncRole::Initiator),
            synchronize(
                &destination,
                &mut right,
                &receiver_config,
                SyncRole::Responder
            )
        );
        assert!(sent.is_err());
        assert!(received.is_err());
        assert_eq!(
            source.table_names().await.expect("source catalog"),
            vec!["people"]
        );
        assert!(
            destination
                .table_names()
                .await
                .expect("destination catalog")
                .is_empty()
        );
    });
}

#[test]
fn dependent_incremental_failure_keeps_completed_catalog_batch() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let config = SessionConfig::default();
        let (mut left, mut right) = transport_pair(false);
        left.inject_change = true;
        let (_sent, received) = futures::join!(
            synchronize(&source, &mut left, &config, SyncRole::Initiator),
            synchronize(&destination, &mut right, &config, SyncRole::Responder)
        );
        assert!(received.is_err(), "unavailable recovery must fail");
        assert_eq!(
            destination
                .table_names()
                .await
                .expect("destination catalog"),
            vec!["people"]
        );
    });
}

#[test]
fn failed_data_batch_rolls_back_all_rows() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let source_units = export_sync_state_for(&source).await.expect("source state");
        let catalog = source_units
            .into_iter()
            .filter(|unit| matches!(&unit.key, SyncKey::Row { table, .. } if table != "people"))
            .collect();
        apply_sync_state_batch_for(&destination, catalog)
            .await
            .expect("apply catalog first");

        let earlier = row_uuid(1);
        let failing = row_uuid(2);
        let batch = vec![
            SyncStateUnit::new(
                SyncKey::Row {
                    table: String::from("people"),
                    row: earlier,
                },
                postcard::to_allocvec(&Row::new(vec![Value::from("committed")]))
                    .expect("encode earlier row"),
                Vec::new(),
            ),
            SyncStateUnit::new(
                SyncKey::Row {
                    table: String::from("people"),
                    row: failing,
                },
                vec![0xff],
                Vec::new(),
            ),
        ];

        assert!(
            apply_sync_state_batch_for(&destination, batch)
                .await
                .is_err()
        );
        let applied = export_sync_state_for(&destination)
            .await
            .expect("destination state");
        assert!(!applied.iter().any(|unit| matches!(
            &unit.key,
            SyncKey::Row { table, row } if table == "people" && row == &earlier
        )));
        assert!(!applied.iter().any(|unit| matches!(
            &unit.key,
            SyncKey::Row { table, row } if table == "people" && row == &failing
        )));
    });
}

#[test]
fn prior_data_batches_remain_committed_when_a_later_batch_fails() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let source_units = export_sync_state_for(&source).await.expect("source state");
        let catalog = source_units
            .into_iter()
            .filter(|unit| matches!(&unit.key, SyncKey::Row { table, .. } if table != "people"))
            .collect();
        apply_sync_state_batch_for(&destination, catalog)
            .await
            .expect("apply catalog first");

        let large_value = "x".repeat(MAX_APPLY_BATCH_BYTES / 4);
        let mut batch = (1..=5)
            .map(|identity| {
                SyncStateUnit::new(
                    SyncKey::Row {
                        table: String::from("people"),
                        row: row_uuid(identity as u128),
                    },
                    postcard::to_allocvec(&Row::new(vec![
                        Value::Uuid(row_uuid(identity as u128)),
                        Value::from(large_value.clone()),
                    ]))
                    .expect("encode row"),
                    Vec::new(),
                )
            })
            .collect::<Vec<_>>();
        let failed = row_uuid(99);
        batch.push(SyncStateUnit::new(
            SyncKey::Row {
                table: String::from("people"),
                row: failed,
            },
            vec![0xff],
            Vec::new(),
        ));

        assert!(
            apply_sync_state_batch_for(&destination, batch)
                .await
                .is_err()
        );
        let applied = export_sync_state_for(&destination)
            .await
            .expect("destination state");
        for identity in 1..=3 {
            assert!(applied.iter().any(|unit| matches!(
                &unit.key,
                SyncKey::Row { table, row }
                    if table == "people" && row == &row_uuid(identity as u128)
            )));
        }
        for identity in 4..=5 {
            assert!(!applied.iter().any(|unit| matches!(
                &unit.key,
                SyncKey::Row { table, row }
                    if table == "people" && row == &row_uuid(identity as u128)
            )));
        }
    });
}

#[test]
fn oversized_data_row_is_rejected_before_apply() {
    block_on(async {
        let engine = source().await;
        let row = row_uuid(9);
        let unit = SyncStateUnit::new(
            SyncKey::Row {
                table: String::from("people"),
                row,
            },
            vec![0; MAX_APPLY_BATCH_BYTES + 1],
            Vec::new(),
        );
        let error = apply_sync_state_batch_for(&engine, vec![unit])
            .await
            .expect_err("oversized row batch must be rejected");
        assert!(error.to_string().contains("Sync row batch"));
        assert!(
            export_sync_state_for(&engine)
                .await
                .expect("export state")
                .iter()
                .all(|unit| !matches!(
                    &unit.key,
                    SyncKey::Row { table, row: candidate }
                        if table == "people" && candidate == &row
                ))
        );
    });
}

#[test]
fn invalid_complete_catalog_definition_is_rejected_atomically() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let mut units = export_sync_state_for(&source).await.expect("export state");
        let field = units
            .iter_mut()
            .find(|unit| {
                matches!(&unit.key, SyncKey::Row { table, .. } if table == ENGINE_TABLE_FIELDS_STORAGE)
                    && postcard::from_bytes::<Row>(&unit.state)
                        .ok()
                        .and_then(|row| row.values.get(2).and_then(Value::as_text).map(String::from))
                        .as_deref()
                        == Some("id")
            })
            .expect("primary-key table field");
        let mut value: Row = postcard::from_bytes(&field.state).expect("decode field row");
        value.values[1] = Value::from("orphan");
        *field = SyncStateUnit::new(
            field.key.clone(),
            postcard::to_allocvec(&value).expect("encode field row"),
            field.metadata.clone(),
        );
        let error = apply_sync_state_batch_for(&destination, units)
            .await
            .expect_err("orphan field must fail");
        assert!(
            error
                .to_string()
                .contains("table people has no UUID primary key"),
            "{error}"
        );
        assert!(
            destination
                .table_names()
                .await
                .expect("destination catalog")
                .is_empty()
        );
    });
}

#[test]
fn incomplete_table_and_index_definitions_are_rejected_without_commit() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let table_only = export_sync_state_for(&source).await.expect("export state")
            .into_iter().filter(|unit| matches!(&unit.key, SyncKey::Row { table, .. } if table == ENGINE_TABLES_STORAGE)).collect();
        let error = apply_sync_state_batch_for(&destination, table_only)
            .await
            .expect_err("missing table fields");
        assert!(
            error.to_string().contains("table people has no fields"),
            "{error}"
        );
        assert!(
            destination
                .table_names()
                .await
                .expect("destination catalog")
                .is_empty()
        );

        let kernel = InMemoryKernel::new();
        let mut transaction = kernel.transaction().await.expect("index transaction");
        let index_id = Uuid::now_v7();
        transaction
            .ensure_table(ENGINE_INDICES_STORAGE)
            .await
            .expect("index storage");
        transaction
            .put_bytes(
                ENGINE_INDICES_STORAGE,
                index_id.as_bytes().to_vec(),
                postcard::to_allocvec(&Row::new(vec![
                    Value::from("by_id"),
                    Value::from("people"),
                    Value::Bool(false),
                    Value::Uuid(Uuid::now_v7()),
                ]))
                .expect("index value"),
            )
            .await
            .expect("index row");
        transaction.commit().await.expect("commit index");
        let mut units = export_sync_state_for(&source).await.expect("export table");
        units.extend(
            export_sync_state_for(&Engine::new(kernel, TestCodec))
                .await
                .expect("export index"),
        );
        let error = apply_sync_state_batch_for(&destination, units)
            .await
            .expect_err("missing index fields");
        assert!(
            error.to_string().contains("index") && error.to_string().contains("no fields"),
            "{error}"
        );
        assert!(
            destination
                .table_names()
                .await
                .expect("destination catalog")
                .is_empty()
        );
    });
}
