mod support;

use core::fmt;

use engine::{
    ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE, ENGINE_TABLES_STORAGE, Engine,
    InMemoryKernel, Kernel, KernelTransaction, RowIdentity,
};
use futures::{StreamExt, channel::mpsc, executor::block_on};
use support::TestCodec;
use sync::{
    SessionConfig, SyncChangeId, SyncIncrementalChange, SyncKey, SyncMessage, SyncRole,
    SyncStateUnit, SyncTransport, apply_sync_state_batch_for, export_sync_state_for, synchronize,
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
                            row: RowIdentity::User([7; 16]),
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

fn destination() -> Engine<InMemoryKernel, TestCodec> {
    Engine::new(InMemoryKernel::new(), TestCodec)
}

async fn source() -> Engine<InMemoryKernel, TestCodec> {
    let kernel = InMemoryKernel::new();
    let mut transaction = kernel.transaction().await.expect("source transaction");
    transaction
        .ensure_table("people")
        .await
        .expect("source data storage");
    let table = RowIdentity::catalog(vec![1; 16]);
    let mut field = vec![1; 16];
    field.extend([2; 16]);
    for (storage, id, value) in [
        (
            ENGINE_TABLES_STORAGE,
            table,
            Row::new(vec![Value::from("people")]),
        ),
        (
            ENGINE_TABLE_FIELDS_STORAGE,
            RowIdentity::catalog(field),
            Row::new(vec![
                Value::from("id"),
                Value::from(ValueType::Uuid),
                Value::Null,
                Value::Integer(0),
                Value::Bool(true),
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
                id.to_bytes(),
                postcard::to_allocvec(&value).expect("encode row"),
            )
            .await
            .expect("source row");
    }
    let user = RowIdentity::ScopedUser {
        table: [1; 16],
        row: [4; 16],
    };
    transaction
        .put_bytes(
            "people",
            user.to_bytes(),
            postcard::to_allocvec(&Row::new(vec![Value::Uuid(uuid::Uuid::from_bytes(
                [4; 16],
            ))]))
            .expect("encode user row"),
        )
        .await
        .expect("source user row");
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
            1
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
fn dependent_incremental_failure_does_not_commit_catalog_snapshots() {
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
fn invalid_complete_catalog_definition_is_rejected_atomically() {
    block_on(async {
        let source = source().await;
        let destination = destination();
        let mut units = export_sync_state_for(&source).await.expect("export state");
        let field = units.iter_mut().find(|unit| matches!(&unit.key, SyncKey::Row { table, .. } if table == ENGINE_TABLE_FIELDS_STORAGE)).expect("table field");
        let SyncKey::Row {
            row: RowIdentity::Catalog(ref mut id),
            ..
        } = field.key
        else {
            panic!("catalog identity")
        };
        id[0] ^= 1;
        *field = SyncStateUnit::new(
            field.key.clone(),
            field.state.clone(),
            field.metadata.clone(),
        );
        let error = apply_sync_state_batch_for(&destination, units)
            .await
            .expect_err("orphan field must fail");
        assert!(error.to_string().contains("has no parent"), "{error}");
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
        let mut index_id = vec![1; 16];
        index_id.extend([3; 16]);
        transaction
            .ensure_table(ENGINE_INDICES_STORAGE)
            .await
            .expect("index storage");
        transaction
            .put_bytes(
                ENGINE_INDICES_STORAGE,
                RowIdentity::catalog(index_id).to_bytes(),
                postcard::to_allocvec(&Row::new(vec![
                    Value::from("by_id"),
                    Value::from("people"),
                    Value::Bool(false),
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
