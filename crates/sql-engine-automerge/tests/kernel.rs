use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use btree_automerge::DocumentChangeKey;
use engine::{Engine, InMemoryKernel, Kernel, KernelTransaction, RowCodec};
use engine_automerge::{AutomergeRowCodec, RowMetadata};
use engine_redb::RedbKernel;

use futures::{StreamExt, executor::block_on};
use query::{
    AlterTableOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr,
    QueryExprValue, QueryFrom, QueryInsert, QuerySelect, QueryUpdate, QueryUpdateAssignment,
    Statement,
};
use schema::{ColumnSchema, IndexSchema, TableSchema};
use sync::{
    SyncIncrementalChange, SyncKey, SyncRowCodec, SyncStateUnit, apply_incremental_changes_for,
    apply_sync_state_batch_for, export_sync_state_for, sync_manifest_for,
};
use uuid::Uuid;
use value::{Row, Value, ValueType};

fn row_uuid(value: u128) -> Uuid {
    let mut bytes = value.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn uuid_value(value: u128) -> Value {
    Value::Uuid(row_uuid(value))
}

static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

fn database_path() -> PathBuf {
    let id = NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("engine-automerge-{}-{id}.redb", std::process::id()))
}

fn people_schema() -> TableSchema {
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
            ColumnSchema {
                name: "city".into(),
                r#type: ValueType::Text,
                default: Value::Null,
                primary_key: false,
            },
        ],
    }
}

fn replica(path: &PathBuf) -> Engine<RedbKernel, AutomergeRowCodec> {
    Engine::new(
        RedbKernel::new(Arc::new(redb::Database::create(path).unwrap())),
        AutomergeRowCodec::new(),
    )
}

fn offline_timestamp() -> uuid::Timestamp {
    uuid::Timestamp::from_unix_time(1_700_000_000, 0, 0, 0)
}

fn online_timestamp() -> uuid::Timestamp {
    static NEXT_MILLIS: AtomicU64 = AtomicU64::new(0);
    let millis = NEXT_MILLIS.fetch_add(1, Ordering::Relaxed);
    uuid::Timestamp::from_unix_time(
        1_700_000_010 + millis / 1_000,
        (millis % 1_000) as u32 * 1_000_000,
        0,
        0,
    )
}

fn replica_with_timestamp(
    path: &PathBuf,
    timestamp_provider: engine::TimestampProvider,
) -> Engine<RedbKernel, AutomergeRowCodec> {
    Engine::with_timestamp_provider(
        RedbKernel::new(Arc::new(redb::Database::create(path).unwrap())),
        AutomergeRowCodec::new(),
        timestamp_provider,
    )
}

fn id(value: u128) -> Option<QueryExpr> {
    Some(QueryExpr::Equals(
        Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
            "people".into(),
            "id".into(),
        )))),
        Box::new(QueryExpr::Value(QueryExprValue::Value(uuid_value(value)))),
    ))
}

async fn table_rows<K: Kernel>(
    engine: &Engine<K, AutomergeRowCodec>,
    table: &str,
    columns: &[&str],
) -> Vec<Row> {
    engine
        .execute(vec![Statement::Query(Query::Select(QuerySelect {
            from: QueryFrom {
                table: table.into(),
                joins: vec![],
            },
            projection: columns
                .iter()
                .map(|column| QueryColumn::new(table.into(), (*column).into()))
                .collect(),
            ..Default::default()
        }))])
        .await
        .unwrap()[0]
        .rows
        .clone()
}

async fn people_rows<K: Kernel>(
    engine: &Engine<K, AutomergeRowCodec>,
    columns: &[&str],
) -> Vec<Row> {
    table_rows(engine, "people", columns).await
}

async fn sync<K: Kernel>(
    source: &Engine<K, AutomergeRowCodec>,
    destination: &Engine<K, AutomergeRowCodec>,
) -> engine::EngineResult<()> {
    apply_sync_state_batch_for(destination, export_sync_state_for(source).await?).await
}

async fn live_table_definition_ids(engine: &Engine<RedbKernel, AutomergeRowCodec>) -> Vec<Uuid> {
    engine
        .read_transaction(|codec, transaction| {
            Box::pin(async move {
                let rows = codec.scan_row_states(transaction, engine::ENGINE_TABLES_STORAGE);
                futures::pin_mut!(rows);
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await {
                    let (id, value, deleted) = row?;
                    if !deleted && value.values.first().and_then(Value::as_text) == Some("people") {
                        ids.push(id);
                    }
                }
                Ok(ids)
            })
        })
        .await
        .expect("read live table definitions")
}

async fn schema_row_ids(engine: &Engine<RedbKernel, AutomergeRowCodec>) -> Vec<(String, Uuid)> {
    let mut rows = export_sync_state_for(engine)
        .await
        .expect("export schema rows")
        .into_iter()
        .filter_map(|unit| match unit.key {
            SyncKey::Row { table, row }
                if matches!(
                    table.as_str(),
                    engine::ENGINE_TABLES_STORAGE
                        | engine::ENGINE_TABLE_FIELDS_STORAGE
                        | engine::ENGINE_INDICES_STORAGE
                        | engine::ENGINE_INDEX_FIELDS_STORAGE
                ) =>
            {
                Some((table, row))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn manifest_digest_is_independent_of_automerge_snapshot_encoding() {
    let source_path = database_path();
    let compressed_path = database_path();
    let uncompressed_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let compressed = replica(&compressed_path);
        let uncompressed = replica(&uncompressed_path);
        source.create_table(people_schema()).await.unwrap();
        let row_id = row_uuid(41);
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    Value::Uuid(row_id),
                    Value::from("compressible".repeat(1024)),
                    Value::Null,
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        let source_state = export_sync_state_for(&source).await.unwrap();
        let schema_states: Vec<_> = source_state
            .iter()
            .filter(|unit| {
                matches!(&unit.key, SyncKey::Row { table, .. } if table.starts_with("__engine_"))
            })
            .cloned()
            .collect();
        let row_state = source_state
            .into_iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: "people".into(),
                        row: row_id,
                    }
            })
            .unwrap();
        let mut document = automerge::AutoCommit::load(&row_state.state).unwrap();
        let compressed_state = document.save();
        let uncompressed_state = document.save_nocompress();
        assert_ne!(compressed_state, uncompressed_state);
        let mut compressed_document = automerge::AutoCommit::load(&compressed_state).unwrap();
        let mut uncompressed_document = automerge::AutoCommit::load(&uncompressed_state).unwrap();
        assert_eq!(
            compressed_document.get_heads(),
            uncompressed_document.get_heads()
        );

        let codec = AutomergeRowCodec::new();
        let compressed_digest = <AutomergeRowCodec as SyncRowCodec<
            <RedbKernel as Kernel>::Transaction,
        >>::manifest_digest(
            &codec, &compressed_state, &row_state.metadata
        )
        .unwrap();
        let uncompressed_digest = <AutomergeRowCodec as SyncRowCodec<
            <RedbKernel as Kernel>::Transaction,
        >>::manifest_digest(
            &codec, &uncompressed_state, &row_state.metadata
        )
        .unwrap();
        assert_eq!(compressed_digest, uncompressed_digest);
        assert_ne!(
            SyncStateUnit::digest_parts(&compressed_state, &row_state.metadata),
            SyncStateUnit::digest_parts(&uncompressed_state, &row_state.metadata)
        );

        apply_sync_state_batch_for(&compressed, schema_states.clone())
            .await
            .unwrap();
        apply_sync_state_batch_for(&uncompressed, schema_states.clone())
            .await
            .unwrap();
        let compressed_unit = SyncStateUnit::new(
            row_state.key.clone(),
            compressed_state,
            row_state.metadata.clone(),
        );
        let uncompressed_unit =
            SyncStateUnit::new(row_state.key, uncompressed_state, row_state.metadata);
        assert!(compressed_unit.verify_digest());
        assert!(uncompressed_unit.verify_digest());
        apply_sync_state_batch_for(&compressed, vec![compressed_unit.clone()])
            .await
            .unwrap();
        apply_sync_state_batch_for(&uncompressed, vec![uncompressed_unit.clone()])
            .await
            .unwrap();
        let expected = sync_manifest_for(&compressed).await.unwrap();
        assert_eq!(expected, sync_manifest_for(&uncompressed).await.unwrap());
        apply_sync_state_batch_for(&compressed, vec![compressed_unit.clone()])
            .await
            .unwrap();
        apply_sync_state_batch_for(&uncompressed, vec![uncompressed_unit.clone()])
            .await
            .unwrap();
        assert_eq!(expected, sync_manifest_for(&compressed).await.unwrap());
        assert_eq!(expected, sync_manifest_for(&uncompressed).await.unwrap());
    });
    for path in [source_path, compressed_path, uncompressed_path] {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn deleted_unique_winner_blocks_a_stale_contender() {
    let winner_path = database_path();
    let stale_path = database_path();
    block_on(async {
        let winner = replica(&winner_path);
        let stale = replica(&stale_path);
        winner.create_table(people_schema()).await.unwrap();
        winner
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_city".into(),
                        table_name: "people".into(),
                        column_indices: vec![2],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        sync(&winner, &stale).await.unwrap();

        winner
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(2),
                    Value::from("Grace"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        winner
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                predicate: id(2),
                returning: None,
            }))])
            .await
            .unwrap();
        stale
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();

        sync(&stale, &winner).await.unwrap();
        assert!(
            people_rows(&winner, &["id", "name", "city"])
                .await
                .is_empty()
        );
        assert_eq!(
            winner
                .index_lookup("people_city", &Row::new(vec![Value::from("London")]))
                .await
                .unwrap(),
            None
        );
    });
    drop(winner_path);
    drop(stale_path);
}

#[test]
fn local_unique_replacement_must_exceed_deleted_owner() {
    let path = database_path();
    block_on(async {
        let database = replica(&path);
        database.create_table(people_schema()).await.unwrap();
        database
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_city".into(),
                        table_name: "people".into(),
                        column_indices: vec![2],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        database
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(2),
                    Value::from("Grace"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        database
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                predicate: id(2),
                returning: None,
            }))])
            .await
            .unwrap();

        let too_small = database
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await;
        assert!(too_small.is_err());

        database
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(3),
                    Value::from("Lin"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        assert_eq!(
            people_rows(&database, &["id", "name", "city"]).await,
            vec![Row::new(vec![
                uuid_value(3),
                Value::from("Lin"),
                Value::from("London"),
            ])]
        );
    });
    drop(path);
}

#[test]
fn incompatible_losing_schema_does_not_keep_its_unique_index() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica_with_timestamp(&source_path, offline_timestamp);
        let destination = replica_with_timestamp(&destination_path, online_timestamp);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_city_unique".into(),
                        table_name: "people".into(),
                        column_indices: vec![2],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        let mut winning_schema = people_schema();
        winning_schema.columns.pop();
        destination.create_table(winning_schema).await.unwrap();

        sync(&source, &destination)
            .await
            .expect("incompatible schema reconciliation should complete");
        assert!(
            destination
                .index_lookup("people_city_unique", &Row::new(vec![Value::from("London")]))
                .await
                .is_err(),
            "index owned by the losing incompatible schema remained active"
        );
        let index_rows = destination
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let rows = codec.scan_row_states(transaction, engine::ENGINE_INDICES_STORAGE);
                    futures::pin_mut!(rows);
                    let mut states = Vec::new();
                    while let Some(row) = rows.next().await {
                        states.push(row?);
                    }
                    Ok(states)
                })
            })
            .await
            .unwrap();
        assert!(
            index_rows.iter().all(|(_, _, deleted)| *deleted),
            "index definition from the losing schema was not tombstoned"
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn incompatible_same_name_schema_applies_defaults_for_new_fields() {
    let left_path = database_path();
    let right_path = database_path();
    block_on(async {
        let left = replica(&left_path);
        let right = replica(&right_path);
        let mut old_schema = people_schema();
        old_schema.columns.truncate(2);
        left.create_table(old_schema)
            .await
            .expect("create old schema");

        let mut new_schema = people_schema();
        new_schema.columns.insert(
            1,
            ColumnSchema {
                name: "age".into(),
                r#type: ValueType::Integer,
                default: Value::Integer(42),
                primary_key: false,
            },
        );
        right
            .create_table(new_schema)
            .await
            .expect("create new schema");
        let winner = live_table_definition_ids(&left).await[0]
            .max(live_table_definition_ids(&right).await[0]);

        left.execute(vec![Statement::Query(Query::Insert(QueryInsert {
            table: "people".into(),
            row: Row::new(vec![uuid_value(1), Value::from("Ada")]),
            returning: None,
        }))])
        .await
        .expect("insert row using old schema");
        sync(&left, &right)
            .await
            .expect("sync old row to new schema");
        sync(&right, &left).await.expect("sync winning schema back");

        assert_eq!(live_table_definition_ids(&left).await, vec![winner]);
        assert_eq!(live_table_definition_ids(&right).await, vec![winner]);
        let expected = vec![Row::new(vec![
            uuid_value(1),
            Value::from("Ada"),
            Value::Integer(42),
        ])];
        assert_eq!(people_rows(&left, &["id", "name", "age"]).await, expected);
        assert_eq!(people_rows(&right, &["id", "name", "age"]).await, expected);
    });
    std::fs::remove_file(left_path).expect("remove left database");
    std::fs::remove_file(right_path).expect("remove right database");
}

#[test]
fn incompatible_same_name_schema_drops_fields_missing_from_winner() {
    let old_path = database_path();
    let new_path = database_path();
    block_on(async {
        let old_replica = replica(&old_path);
        let new_replica = replica(&new_path);
        let old_schema = people_schema();
        let mut new_schema = people_schema();
        new_schema.columns.truncate(2);
        old_replica
            .create_table(old_schema)
            .await
            .expect("create schema with legacy field");
        new_replica
            .create_table(new_schema)
            .await
            .expect("create winning schema without legacy field");
        let winner = live_table_definition_ids(&new_replica).await[0];
        assert!(winner > live_table_definition_ids(&old_replica).await[0]);

        old_replica
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("legacy value"),
                ]),
                returning: None,
            }))])
            .await
            .expect("insert using old schema");
        sync(&old_replica, &new_replica)
            .await
            .expect("sync row to winning schema");
        sync(&new_replica, &old_replica)
            .await
            .expect("sync winning schema back");

        let expected = vec![Row::new(vec![uuid_value(1), Value::from("Ada")])];
        assert_eq!(live_table_definition_ids(&old_replica).await, vec![winner]);
        assert_eq!(live_table_definition_ids(&new_replica).await, vec![winner]);
        assert_eq!(people_rows(&old_replica, &["id", "name"]).await, expected);
        assert_eq!(people_rows(&new_replica, &["id", "name"]).await, expected);
        assert_eq!(
            old_replica
                .table_schema("people")
                .await
                .unwrap()
                .columns
                .len(),
            2
        );
        assert_eq!(
            new_replica
                .table_schema("people")
                .await
                .unwrap()
                .columns
                .len(),
            2
        );
    });
    std::fs::remove_file(old_path).expect("remove old database");
    std::fs::remove_file(new_path).expect("remove new database");
}

#[test]
fn incompatible_same_name_schema_tombstones_rows_with_invalid_values() {
    let left_path = database_path();
    let right_path = database_path();
    block_on(async {
        let left = replica(&left_path);
        let right = replica(&right_path);
        let mut text_schema = people_schema();
        text_schema.columns.truncate(2);
        let integer_schema = TableSchema {
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
                    r#type: ValueType::Integer,
                    default: Value::Null,
                    primary_key: false,
                },
            ],
        };
        left.create_table(text_schema)
            .await
            .expect("create text schema");
        right
            .create_table(integer_schema)
            .await
            .expect("create integer schema");
        let left_definition = live_table_definition_ids(&left).await[0];
        let right_definition = live_table_definition_ids(&right).await[0];

        left.execute(vec![Statement::Query(Query::Insert(QueryInsert {
            table: "people".into(),
            row: Row::new(vec![uuid_value(1), Value::from("not an integer")]),
            returning: None,
        }))])
        .await
        .expect("insert text value");
        right
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(2), Value::Integer(20)]),
                returning: None,
            }))])
            .await
            .expect("insert integer value");

        sync(&left, &right).await.expect("sync left to right");
        sync(&right, &left).await.expect("sync right to left");

        let winning_definition = live_table_definition_ids(&left).await[0];
        assert_eq!(winning_definition, left_definition.max(right_definition));
        let (expected, invalid_row) = if winning_definition == left_definition {
            (vec![Row::new(vec![Value::from("not an integer")])], 2)
        } else {
            (vec![Row::new(vec![Value::Integer(20)])], 1)
        };
        assert_eq!(table_rows(&left, "people", &["name"]).await, expected);
        assert_eq!(table_rows(&right, "people", &["name"]).await, expected);
        assert!(
            left.read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .row_is_deleted(transaction, "people", &row_uuid(invalid_row))
                        .await
                })
            })
            .await
            .expect("read invalid row tombstone")
        );
    });
    std::fs::remove_file(left_path).expect("remove left database");
    std::fs::remove_file(right_path).expect("remove right database");
}

#[test]
fn replicated_row_with_primary_key_mismatching_its_address_is_tombstoned() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source
            .create_table(people_schema())
            .await
            .expect("create source schema");
        sync(&source, &destination)
            .await
            .expect("bootstrap destination schema");

        let row_id = row_uuid(4);
        let malformed = Row::new(vec![uuid_value(5), Value::from("mismatched"), Value::Null]);
        let key = ("people".into(), row_id);
        source
            .mutate_rows(&[key], move |codec, transaction| {
                Box::pin(async move {
                    codec
                        .put_row(transaction, "people", row_id, malformed)
                        .await?;
                    Ok(((), Vec::new()))
                })
            })
            .await
            .expect("store malformed row for replication");
        let state = export_sync_state_for(&source)
            .await
            .expect("export malformed row")
            .into_iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: "people".into(),
                        row: row_id,
                    }
            })
            .expect("malformed row state exists");
        apply_sync_state_batch_for(&destination, vec![state])
            .await
            .expect("apply malformed row for reconciliation");

        assert!(
            people_rows(&destination, &["id", "name", "city"])
                .await
                .is_empty()
        );
        assert!(
            destination
                .read_transaction(move |codec, transaction| {
                    Box::pin(
                        async move { codec.row_is_deleted(transaction, "people", &row_id).await },
                    )
                })
                .await
                .expect("read invalid row tombstone")
        );
    });
    std::fs::remove_file(source_path).expect("remove source database");
    std::fs::remove_file(destination_path).expect("remove destination database");
}

#[test]
fn same_name_unique_index_uses_one_complete_winning_definition() {
    let left_path = database_path();
    let right_path = database_path();
    block_on(async {
        let left = replica_with_timestamp(&left_path, offline_timestamp);
        let right = replica_with_timestamp(&right_path, online_timestamp);
        left.create_table(people_schema())
            .await
            .expect("create left table");
        right
            .create_table(people_schema())
            .await
            .expect("create right table");
        for (engine, unique, column) in [(&left, true, 1), (&right, false, 2)] {
            engine
                .execute(vec![Statement::DataDefinition(
                    DataDefinition::CreateIndex {
                        schema: IndexSchema {
                            name: "people_by_name".into(),
                            table_name: "people".into(),
                            column_indices: vec![column],
                            unique,
                        },
                        if_not_exists: false,
                    },
                )])
                .await
                .expect("create index definition");
        }

        sync(&left, &right).await.expect("sync left definition");
        sync(&right, &left).await.expect("sync winning definition");

        let expected = IndexSchema {
            name: "people_by_name".into(),
            table_name: "people".into(),
            column_indices: vec![2],
            unique: false,
        };
        assert_eq!(left.index_schema("people_by_name").await.unwrap(), expected);
        assert_eq!(
            right.index_schema("people_by_name").await.unwrap(),
            expected
        );
        let row = Row::new(vec![
            uuid_value(3),
            Value::from("Ada"),
            Value::from("London"),
        ]);
        right
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: row.clone(),
                returning: None,
            }))])
            .await
            .expect("insert row for winning index");
        sync(&right, &left)
            .await
            .expect("sync row through winning index");
        let lookup = Row::new(vec![Value::from("London")]);
        assert_eq!(
            left.index_lookup("people_by_name", &lookup).await.unwrap(),
            Some(row.clone())
        );
        assert_eq!(
            right.index_lookup("people_by_name", &lookup).await.unwrap(),
            Some(row)
        );
        let ids = schema_row_ids(&left).await;
        let index_ids: Vec<_> = ids
            .iter()
            .filter_map(|(table, id)| (table == engine::ENGINE_INDICES_STORAGE).then_some(*id))
            .collect();
        assert_eq!(index_ids.len(), 2);
        let states = export_sync_state_for(&left)
            .await
            .expect("export index state");
        let index_states: Vec<_> = states
            .iter()
            .filter(|unit| matches!(&unit.key, SyncKey::Row { table, .. } if table == engine::ENGINE_INDICES_STORAGE))
            .collect();
        let winning_id = *index_ids.iter().max().expect("index definitions exist");
        for unit in index_states {
            let SyncKey::Row { row, .. } = &unit.key;
            let deleted = if unit.metadata.is_empty() {
                false
            } else {
                postcard::from_bytes::<RowMetadata>(&unit.metadata)
                    .expect("decode index metadata")
                    .deleted
            };
            assert_eq!(*row != winning_id, deleted);
        }
        let losing_id = *index_ids.iter().min().expect("index definitions exist");
        let losing_fields_are_deleted = left
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let rows =
                        codec.scan_row_states(transaction, engine::ENGINE_INDEX_FIELDS_STORAGE);
                    futures::pin_mut!(rows);
                    let mut losing_fields = 0;
                    while let Some(entry) = rows.next().await {
                        let (_, field, deleted) = entry?;
                        if field.values.first().and_then(Value::as_uuid).copied() == Some(losing_id)
                        {
                            losing_fields += 1;
                            if !deleted {
                                return Ok(false);
                            }
                        }
                    }
                    Ok(losing_fields > 0)
                })
            })
            .await
            .expect("read losing index fields");
        assert!(
            losing_fields_are_deleted,
            "losing index fields remained live"
        );
        let late_field_id = row_uuid(1000);
        let late_field = Row::new(vec![
            Value::Uuid(losing_id),
            Value::Integer(0),
            Value::from("name"),
            uuid_value(1001),
        ]);
        let mutation_value = late_field.clone();
        left.mutate_rows(
            &[(engine::ENGINE_INDEX_FIELDS_STORAGE.into(), late_field_id)],
            move |codec, transaction| {
                Box::pin(async move {
                    codec
                        .put_row(
                            transaction,
                            engine::ENGINE_INDEX_FIELDS_STORAGE,
                            late_field_id,
                            mutation_value.clone(),
                        )
                        .await?;
                    Ok((
                        (),
                        vec![engine::RowMutation {
                            table: engine::ENGINE_INDEX_FIELDS_STORAGE.into(),
                            row: late_field_id,
                            old: None,
                            new: Some(mutation_value),
                        }],
                    ))
                })
            },
        )
        .await
        .expect("apply field arriving after index loser tombstone");
        assert!(
            left.read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .row_is_deleted(
                            transaction,
                            engine::ENGINE_INDEX_FIELDS_STORAGE,
                            &late_field_id,
                        )
                        .await
                })
            })
            .await
            .expect("read late index-field tombstone"),
            "late field restored a deleted index definition"
        );
    });
    std::fs::remove_file(left_path).expect("remove left database");
    std::fs::remove_file(right_path).expect("remove right database");
}

#[test]
fn independently_created_same_name_tables_share_rows_after_sync() {
    let left_path = database_path();
    let right_path = database_path();
    block_on(async {
        let left = replica(&left_path);
        let right = replica(&right_path);
        left.create_table(people_schema())
            .await
            .expect("create left table");
        let mut reordered_schema = people_schema();
        reordered_schema.columns.swap(1, 2);
        right
            .create_table(reordered_schema)
            .await
            .expect("create right table with reordered fields");
        for (engine, name_column, city_column) in [(&left, 1, 2), (&right, 2, 1)] {
            engine
                .execute(vec![Statement::DataDefinition(
                    DataDefinition::CreateIndex {
                        schema: IndexSchema {
                            name: "people_by_name".into(),
                            table_name: "people".into(),
                            column_indices: vec![name_column, city_column],
                            unique: true,
                        },
                        if_not_exists: false,
                    },
                )])
                .await
                .expect("create same-name compound index");
        }
        assert_ne!(schema_row_ids(&left).await, schema_row_ids(&right).await);
        let left_definition = live_table_definition_ids(&left).await[0];
        let right_definition = live_table_definition_ids(&right).await[0];
        let winning_definition = left_definition.max(right_definition);
        for (engine, row) in [
            (
                &left,
                Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
            ),
            (
                &right,
                Row::new(vec![
                    uuid_value(2),
                    Value::from("Paris"),
                    Value::from("Grace"),
                ]),
            ),
        ] {
            engine
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "people".into(),
                    row,
                    returning: None,
                }))])
                .await
                .expect("insert local row");
        }

        for _ in 0..3 {
            sync(&left, &right).await.expect("sync left to right");
            sync(&right, &left).await.expect("sync right to left");
        }
        let expected = vec![
            Row::new(vec![uuid_value(1), Value::from("Ada")]),
            Row::new(vec![uuid_value(2), Value::from("Grace")]),
        ];
        assert_eq!(people_rows(&left, &["id", "name"]).await, expected);
        assert_eq!(people_rows(&right, &["id", "name"]).await, expected);
        assert_eq!(
            live_table_definition_ids(&left).await,
            vec![winning_definition]
        );
        assert_eq!(
            live_table_definition_ids(&right).await,
            vec![winning_definition]
        );
        let expected_row = |id, name: &str, city: &str| {
            Some(if winning_definition == right_definition {
                Row::new(vec![uuid_value(id), Value::from(city), Value::from(name)])
            } else {
                Row::new(vec![uuid_value(id), Value::from(name), Value::from(city)])
            })
        };
        assert_eq!(
            left.index_lookup(
                "people_by_name",
                &Row::new(vec![Value::from("Ada"), Value::from("London")]),
            )
            .await
            .expect("left compound index lookup"),
            expected_row(1, "Ada", "London")
        );
        assert_eq!(
            right
                .index_lookup(
                    "people_by_name",
                    &Row::new(vec![Value::from("Grace"), Value::from("Paris")]),
                )
                .await
                .expect("right compound index lookup"),
            expected_row(2, "Grace", "Paris")
        );

        let left_manifest = sync_manifest_for(&left).await.expect("left manifest");
        let right_manifest = sync_manifest_for(&right).await.expect("right manifest");
        let left_states = export_sync_state_for(&left).await.expect("left state");
        let right_states = export_sync_state_for(&right).await.expect("right state");
        let mut index_definition_states: Vec<_> = left_states
            .iter()
            .filter_map(|unit| match &unit.key {
                SyncKey::Row { table, row } if table == engine::ENGINE_INDICES_STORAGE => {
                    Some((*row, unit))
                }
                _ => None,
            })
            .collect();
        index_definition_states.sort_by_key(|(id, _)| *id);
        assert_eq!(index_definition_states.len(), 2);
        let winning_index_definition = index_definition_states[1].0;
        for (id, unit) in index_definition_states {
            let deleted = if unit.metadata.is_empty() {
                false
            } else {
                postcard::from_bytes::<RowMetadata>(&unit.metadata)
                    .expect("decode index metadata")
                    .deleted
            };
            assert_eq!(
                deleted,
                id != winning_index_definition,
                "index definition winner did not follow full UUID ordering"
            );
        }

        for (left, right) in left_states.iter().zip(&right_states) {
            if matches!(&left.key, SyncKey::Row { table, .. } if table.starts_with("__engine_")) {
                let mut left_doc = automerge::AutoCommit::load(&left.state).expect("load left");
                let mut right_doc = automerge::AutoCommit::load(&right.state).expect("load right");
                assert_eq!(
                    left_doc.get_heads(),
                    right_doc.get_heads(),
                    "{:?}",
                    left.key
                );
            }
        }
        assert_eq!(left_manifest.entries, right_manifest.entries);

        let losing_definition = left_definition.min(right_definition);
        let losing_state = left_states
            .iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: engine::ENGINE_TABLES_STORAGE.into(),
                        row: losing_definition,
                    }
            })
            .expect("losing schema definition remains in sync state");
        let losing_metadata: RowMetadata =
            postcard::from_bytes(&losing_state.metadata).expect("decode losing schema metadata");
        assert!(losing_metadata.deleted, "losing schema was not tombstoned");
        assert!(
            losing_metadata.drop_events.is_empty(),
            "losing schema tombstone was treated as a table drop"
        );
    });
    std::fs::remove_file(left_path).expect("remove left database");
    std::fs::remove_file(right_path).expect("remove right database");
}

async fn update<K: Kernel>(engine: &Engine<K, AutomergeRowCodec>, column: &str, value: Value) {
    engine
        .execute(vec![Statement::Query(Query::Update(QueryUpdate {
            from: QueryFrom {
                table: "people".into(),
                joins: vec![],
            },
            assignments: vec![QueryUpdateAssignment {
                column: QueryColumn::new("people".into(), column.into()),
                value: QueryExprValue::Value(value),
            }],
            predicate: id(1),
            returning: None,
        }))])
        .await
        .unwrap();
}

async fn add_column(engine: &Engine<RedbKernel, AutomergeRowCodec>, name: &str, default: Value) {
    engine
        .execute(vec![Statement::DataDefinition(
            DataDefinition::AlterTable {
                table_name: "people".into(),
                operations: vec![AlterTableOperation::AddColumn(ColumnSchema {
                    name: name.into(),
                    r#type: ValueType::Text,
                    default,
                    primary_key: false,
                })],
                if_exists: false,
            },
        )])
        .await
        .unwrap();
}

#[test]
fn malformed_incremental_batch_does_not_partially_mutate_state() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        update(&source, "city", Value::from("Paris")).await;
        update(&source, "name", Value::from("Grace")).await;
        let table = "people";
        let row = row_uuid(1);
        let keys = source
            .read_transaction(|codec, transaction| {
                Box::pin(codec.change_inventory(transaction, table, row, usize::MAX))
            })
            .await
            .unwrap();
        assert_eq!(keys.len(), 2);
        let key = keys[0].clone();
        let export_row = row;
        let first_payload = source
            .read_transaction(move |codec, transaction| {
                Box::pin(async move {
                    codec
                        .export_change(transaction, table, export_row, &key, usize::MAX)
                        .await
                })
            })
            .await
            .unwrap()
            .unwrap();
        let before = sync_manifest_for(&destination).await.unwrap();

        let result = apply_incremental_changes_for(
            &destination,
            &[
                SyncIncrementalChange {
                    table: table.into(),
                    row,
                    id: keys[0].clone(),
                    payload: first_payload,
                },
                SyncIncrementalChange {
                    table: table.into(),
                    row,
                    id: keys[1].clone(),
                    payload: vec![1, 2, 3],
                },
            ],
        )
        .await;
        assert!(result.is_err());
        assert_eq!(sync_manifest_for(&destination).await.unwrap(), before);
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn realtime_row_updates_export_one_incremental_change_after_bootstrap() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();

        sync(&source, &destination).await.unwrap();
        let table = "people";
        let row = row_uuid(1);
        assert!(
            source
                .read_transaction(|codec, transaction| {
                    Box::pin(codec.change_inventory(transaction, table, row, usize::MAX))
                })
                .await
                .unwrap()
                .is_empty()
        );

        update(&source, "city", Value::from("Paris")).await;
        assert!(
            source
                .read_transaction(|codec, transaction| {
                    Box::pin(codec.change_inventory(transaction, table, row, 1))
                })
                .await
                .is_err()
        );
        let inventory = source
            .read_transaction(|codec, transaction| {
                Box::pin(codec.change_inventory(transaction, table, row, usize::MAX))
            })
            .await
            .unwrap();
        assert_eq!(inventory.len(), 1);
        let key = inventory[0].clone();
        let export_key = key.clone();
        let export_row = row;
        let limited_key = key.clone();
        let limited_row = row;
        assert!(
            source
                .read_transaction(move |codec, transaction| {
                    Box::pin(async move {
                        codec
                            .export_change(transaction, table, limited_row, &limited_key, 1)
                            .await
                    })
                })
                .await
                .is_err()
        );
        let payload = source
            .read_transaction(move |codec, transaction| {
                Box::pin(async move {
                    codec
                        .export_change(transaction, table, export_row, &export_key, usize::MAX)
                        .await
                })
            })
            .await
            .unwrap()
            .unwrap();
        let full_state = export_sync_state_for(&source)
            .await
            .unwrap()
            .into_iter()
            .find_map(|unit| match unit.key {
                sync::SyncKey::Row {
                    table: unit_table,
                    row: unit_row,
                } if unit_table == table && unit_row == row => Some(unit.state),
                _ => None,
            })
            .unwrap();
        assert_ne!(payload, full_state);

        let change = SyncIncrementalChange {
            table: table.into(),
            row,
            id: key,
            payload,
        };
        apply_incremental_changes_for(&destination, core::slice::from_ref(&change))
            .await
            .unwrap();
        apply_incremental_changes_for(&destination, core::slice::from_ref(&change))
            .await
            .unwrap();
        assert_eq!(
            sync_manifest_for(&source).await.unwrap(),
            sync_manifest_for(&destination).await.unwrap()
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn metadata_first_tombstone_keeps_stale_row_snapshot() {
    let source_path = database_path();
    let dest_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let dest = replica(&dest_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        let row = row_uuid(1);
        assert!(
            source
                .read_transaction(|codec, transaction| {
                    Box::pin(codec.export_state(transaction, "people", row, 1))
                })
                .await
                .is_err()
        );
        let state = source
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .export_state(transaction, "people", row, usize::MAX)
                        .await
                })
            })
            .await
            .unwrap()
            .unwrap();
        let metadata = postcard::to_allocvec(&RowMetadata {
            version: 1,
            deleted: true,
            drop_events: Vec::new(),
            observed_drops: Vec::new(),
        })
        .unwrap();
        dest.create_table(people_schema()).await.unwrap();
        let rows = [("people".into(), row)];
        dest.mutate_rows(&rows, |codec, transaction| {
            Box::pin(async move {
                codec
                    .merge_metadata(transaction, "people", row, &metadata)
                    .await?;
                assert!(
                    codec
                        .merge_state(transaction, "people", row, &state)
                        .await?
                        .is_none()
                );
                assert_eq!(
                    codec
                        .export_state(transaction, "people", row, usize::MAX)
                        .await?,
                    Some(state)
                );
                assert!(codec.get_row(transaction, "people", &row).await?.is_none());
                Ok(((), Vec::new()))
            })
        })
        .await
        .unwrap();
        let row = rows[0].1;
        let retained = dest
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    Ok((
                        codec
                            .export_state(transaction, "people", row, usize::MAX)
                            .await?,
                        codec.get_row(transaction, "people", &row).await?,
                    ))
                })
            })
            .await
            .unwrap();
        assert!(retained.0.is_some());
        assert!(retained.1.is_none());
        let exported = export_sync_state_for(&dest)
            .await
            .expect("export metadata-only tombstone");
        let tombstone = exported
            .iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: "people".into(),
                        row,
                    }
            })
            .expect("metadata-only row tombstone remains in sync state");
        assert_eq!(
            tombstone.state,
            retained.0.expect("stale snapshot retained")
        );
        assert!(
            tombstone.verify_digest(),
            "state and metadata digest mismatch"
        );
        assert!(
            postcard::from_bytes::<RowMetadata>(&tombstone.metadata)
                .expect("decode tombstone metadata")
                .deleted
        );
        let manifest = sync_manifest_for(&dest)
            .await
            .expect("build manifest with metadata-only tombstone");
        let manifest_entry = manifest
            .entries
            .iter()
            .find(|(key, _)| key == &tombstone.key)
            .expect("manifest includes metadata-only tombstone");
        assert_ne!(
            manifest_entry.1, tombstone.digest,
            "canonical manifest digest is distinct from the state-byte integrity digest"
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(dest_path).unwrap();
}

#[test]
fn row_identity_stream_yields_catalog_rows() {
    let path = database_path();
    let engine = replica(&path);
    block_on(async {
        engine.create_table(people_schema()).await.unwrap();
        let result = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let rows = codec.row_ids(transaction, engine::ENGINE_TABLES_STORAGE);
                    futures::pin_mut!(rows);
                    let mut count = 0;
                    while rows.next().await.transpose()?.is_some() {
                        count += 1;
                    }
                    Ok(count)
                })
            })
            .await;
        assert_eq!(result.expect("enumerate catalog rows"), 1);
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn catalog_relationship_scan_returns_only_fields_for_requested_table() {
    let path = database_path();
    let engine = replica(&path);
    block_on(async {
        engine.create_table(people_schema()).await.unwrap();
        let mut second_schema = people_schema();
        second_schema.name = "animals".into();
        engine.create_table(second_schema).await.unwrap();

        let field_count = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let fields =
                        codec.scan_row_states(transaction, engine::ENGINE_TABLE_FIELDS_STORAGE);
                    futures::pin_mut!(fields);
                    let mut count = 0;
                    while let Some(field) = fields.next().await {
                        let (_, value, deleted) = field?;
                        if !deleted
                            && value.values.get(1).and_then(Value::as_text) == Some("people")
                        {
                            count += 1;
                        }
                    }
                    Ok(count)
                })
            })
            .await
            .unwrap();
        assert_eq!(field_count, 3);
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn internal_rows_share_automerge_conflict_and_resolution_behavior() {
    let source_path = database_path();
    let left_path = database_path();
    let right_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let left = replica(&left_path);
        let right = replica(&right_path);
        source.create_table(people_schema()).await.unwrap();
        let (field_id, base_field) = source
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let fields =
                        codec.scan_row_states(transaction, engine::ENGINE_TABLE_FIELDS_STORAGE);
                    futures::pin_mut!(fields);
                    while let Some(entry) = fields.next().await {
                        let (id, row, deleted) = entry?;
                        if !deleted && row.values.get(2).and_then(Value::as_text) == Some("name") {
                            return Ok((id, row));
                        }
                    }
                    Err(engine::EngineError::custom("name field row is missing"))
                })
            })
            .await
            .unwrap();
        sync(&source, &left).await.unwrap();
        sync(&source, &right).await.unwrap();

        for (engine, value) in [(&left, "left default"), (&right, "right default")] {
            let row_id = field_id;
            let mut changed = base_field.clone();
            changed.values[4] = Value::from(value);
            engine
                .mutate_transaction(
                    engine::ENGINE_TABLE_FIELDS_STORAGE,
                    row_id,
                    move |codec, transaction, _| {
                        Box::pin(async move {
                            let change = codec
                                .encode_row(
                                    transaction,
                                    engine::ENGINE_TABLE_FIELDS_STORAGE,
                                    &row_id,
                                    &changed,
                                    &[4],
                                )
                                .await?;
                            let value = codec
                                .merge_row(
                                    transaction,
                                    engine::ENGINE_TABLE_FIELDS_STORAGE,
                                    row_id,
                                    &change,
                                )
                                .await?
                                .ok_or(engine::EngineError::custom(
                                    "internal field update was not materialized",
                                ))?;
                            Ok(((), Some(value)))
                        })
                    },
                )
                .await
                .unwrap();
        }
        sync(&left, &right).await.unwrap();
        sync(&right, &left).await.unwrap();

        let conflicts = left
            .read_transaction(|codec, transaction| {
                let row = field_id;
                Box::pin(async move {
                    codec
                        .conflicted_columns(transaction, engine::ENGINE_TABLE_FIELDS_STORAGE, &row)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(conflicts, vec![4]);
        let values = left
            .read_transaction(|codec, transaction| {
                let row = field_id;
                Box::pin(async move {
                    codec
                        .conflict_values(transaction, engine::ENGINE_TABLE_FIELDS_STORAGE, &row)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].0, 4);
        assert!(values[0].1.contains(&Value::from("left default")));
        assert!(values[0].1.contains(&Value::from("right default")));

        let row_id = field_id;
        let mut resolved = base_field.clone();
        resolved.values[4] = Value::from("resolved default");
        let resolution_row = resolved.clone();
        left.mutate_transaction(
            engine::ENGINE_TABLE_FIELDS_STORAGE,
            row_id,
            move |codec, transaction, _| {
                Box::pin(async move {
                    let change = codec
                        .encode_resolution(
                            transaction,
                            engine::ENGINE_TABLE_FIELDS_STORAGE,
                            &row_id,
                            &resolution_row,
                            &[4],
                        )
                        .await?;
                    let value = codec
                        .merge_row(
                            transaction,
                            engine::ENGINE_TABLE_FIELDS_STORAGE,
                            row_id,
                            &change,
                        )
                        .await?
                        .ok_or(engine::EngineError::custom(
                            "internal field resolution was not materialized",
                        ))?;
                    Ok(((), Some(value)))
                })
            },
        )
        .await
        .unwrap();
        sync(&left, &right).await.unwrap();
        for engine in [&left, &right] {
            let row = field_id;
            let (conflicts, value) = engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move {
                        Ok((
                            codec
                                .conflicted_columns(
                                    transaction,
                                    engine::ENGINE_TABLE_FIELDS_STORAGE,
                                    &row,
                                )
                                .await?,
                            codec
                                .get_row(transaction, engine::ENGINE_TABLE_FIELDS_STORAGE, &row)
                                .await?,
                        ))
                    })
                })
                .await
                .unwrap();
            assert!(conflicts.is_empty());
            assert_eq!(value.unwrap().values[4], Value::from("resolved default"));
        }
    });
    for path in [source_path, left_path, right_path] {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn built_in_catalog_layout_decodes_without_catalog_field_rows() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = engine::ENGINE_TABLES_STORAGE;

        let identity = Uuid::now_v7();
        let expected = Row::new(vec![Value::from("people")]);
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        reconciler
            .put_row(&mut transaction, table, identity, expected.clone())
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, table, &identity)
                .await
                .unwrap(),
            Some(expected)
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn logical_rows_persist_in_one_kernel_transaction() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = "table-100";
        let row_id = Uuid::now_v7();
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                table,
                row_id,
                Row::new(vec![uuid_value(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, table, &row_id)
                .await
                .unwrap(),
            Some(Row::new(vec![uuid_value(1), Value::from("Ada")]))
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn redb_kernel_pairs_with_a_non_automerge_reconciler() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = "table-101";
        let row_id = Uuid::now_v7();
        let row = Row::new(vec![uuid_value(1), Value::from("Ada")]);
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        reconciler
            .put_row(&mut transaction, table, row_id, row.clone())
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, table, &row_id)
                .await
                .unwrap(),
            Some(row)
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn removing_a_logical_row_writes_a_tombstone() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = "table-102";
        let row_id = Uuid::now_v7();
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                table,
                row_id,
                Row::new(vec![uuid_value(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        assert!(
            reconciler
                .remove_row(&mut transaction, table, &row_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            reconciler
                .put_row(
                    &mut transaction,
                    table,
                    row_id,
                    Row::new(vec![uuid_value(2), Value::from("Grace")]),
                )
                .await
                .is_err()
        );
        reconciler
            .merge_metadata(
                &mut transaction,
                table,
                row_id,
                &postcard::to_allocvec(&RowMetadata {
                    version: 1,
                    deleted: false,
                    drop_events: Vec::new(),
                    observed_drops: Vec::new(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert!(
            reconciler
                .get_row(&transaction, table, &row_id)
                .await
                .unwrap()
                .is_none()
        );
        let metadata_key =
            DocumentChangeKey::new_metadata(row_id.as_bytes().to_vec()).encode_ordered();
        let metadata = transaction
            .get_bytes(table, &metadata_key)
            .await
            .unwrap()
            .map(|bytes| postcard::from_bytes::<RowMetadata>(&bytes).unwrap())
            .unwrap();
        assert_eq!(metadata.version, 1);
        assert!(metadata.deleted);
        {
            let row_ids = reconciler.row_ids(&transaction, table);
            futures::pin_mut!(row_ids);
            assert_eq!(row_ids.next().await.unwrap().unwrap(), row_id);
            assert!(row_ids.next().await.is_none());
        }
        {
            let rows = reconciler.scan_rows(&transaction, table);
            futures::pin_mut!(rows);
            assert!(rows.next().await.is_none());
        }
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn engine_persists_a_logical_row_through_the_public_transaction_seam() {
    let path = database_path();
    let database = Arc::new(redb::Database::create(&path).unwrap());
    let schema = TableSchema {
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
    };
    block_on(async {
        let engine = Engine::new(RedbKernel::new(database.clone()), AutomergeRowCodec::new());
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateTable {
                    schema,
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        let engine = Engine::new(RedbKernel::new(database), AutomergeRowCodec::new());
        let result = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("people".into(), "name".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(result[0].rows, vec![Row::new(vec![Value::from("Ada")])]);
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn received_automerge_incremental_change_materializes_a_row() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let schema = TableSchema {
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
        };
        let source = Engine::new(
            RedbKernel::new(Arc::new(redb::Database::create(&source_path).unwrap())),
            AutomergeRowCodec::new(),
        );
        let destination = Engine::new(
            RedbKernel::new(Arc::new(redb::Database::create(&destination_path).unwrap())),
            AutomergeRowCodec::new(),
        );
        source.create_table(schema.clone()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        sync(&source, &destination).await.unwrap();
        let results = destination
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("people".into(), "name".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(results[0].rows, vec![Row::new(vec![Value::from("Ada")])]);
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn in_memory_table_drop_and_recreation_converge() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let right = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        left.create_table(people_schema()).await.unwrap();
        left.execute(vec![Statement::Query(Query::Insert(QueryInsert {
            table: "people".into(),
            row: Row::new(vec![
                uuid_value(1),
                Value::from("Ada"),
                Value::from("London"),
            ]),
            returning: None,
        }))])
        .await
        .unwrap();
        sync(&left, &right).await.unwrap();

        left.execute(vec![Statement::DataDefinition(DataDefinition::DropTable {
            table_name: "people".into(),
            if_exists: false,
        })])
        .await
        .unwrap();
        sync(&left, &right).await.unwrap();
        right.create_table(people_schema()).await.unwrap();
        right
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(2),
                    Value::from("Grace"),
                    Value::from("Paris"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&right, &left).await.unwrap();
        sync(&left, &right).await.unwrap();

        let expected = vec![Row::new(vec![
            uuid_value(2),
            Value::from("Grace"),
            Value::from("Paris"),
        ])];
        assert_eq!(people_rows(&left, &["id", "name", "city"]).await, expected);
        assert_eq!(people_rows(&right, &["id", "name", "city"]).await, expected);
        assert_eq!(
            sync_manifest_for(&left).await.unwrap(),
            sync_manifest_for(&right).await.unwrap()
        );
    });
}

#[test]
fn in_memory_recreation_rejects_stale_offline_rows_and_converges() {
    block_on(async {
        let offline = Engine::with_timestamp_provider(
            InMemoryKernel::new(),
            AutomergeRowCodec::new(),
            offline_timestamp,
        );
        let online = Engine::with_timestamp_provider(
            InMemoryKernel::new(),
            AutomergeRowCodec::new(),
            online_timestamp,
        );
        offline.create_table(people_schema()).await.unwrap();
        offline
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("initial"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        online.create_table(people_schema()).await.unwrap();
        sync(&offline, &online).await.unwrap();

        update(&offline, "name", Value::from("stale update")).await;
        offline
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(2),
                    Value::from("stale insert"),
                    Value::Null,
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        online.drop_table("people").await.unwrap();
        online.create_table(people_schema()).await.unwrap();
        online
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(3), Value::from("fresh"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();

        sync(&offline, &online).await.unwrap();
        sync(&online, &offline).await.unwrap();
        let expected = vec![Row::new(vec![uuid_value(3), Value::from("fresh")])];
        assert_eq!(people_rows(&online, &["id", "name"]).await, expected);
        assert_eq!(people_rows(&offline, &["id", "name"]).await, expected);
        for engine in [&online, &offline] {
            for stale_row in [row_uuid(1), row_uuid(2)] {
                assert!(
                    engine
                        .read_transaction(move |codec, transaction| {
                            Box::pin(async move {
                                codec
                                    .row_is_deleted(transaction, "people", &stale_row)
                                    .await
                            })
                        })
                        .await
                        .unwrap(),
                    "stale in-memory row was not permanently tombstoned"
                );
            }
        }
        assert_eq!(
            sync_manifest_for(&online).await.unwrap(),
            sync_manifest_for(&offline).await.unwrap()
        );
    });
}

#[test]
fn failed_table_lifecycle_batch_rolls_back_complete_state() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        engine.create_table(people_schema()).await.unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_by_city".into(),
                        table_name: "people".into(),
                        column_indices: vec![2],
                        unique: false,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        let original = Row::new(vec![
            uuid_value(1),
            Value::from("Ada"),
            Value::from("London"),
        ]);
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: original.clone(),
                returning: None,
            }))])
            .await
            .unwrap();
        let before = sync_manifest_for(&engine).await.unwrap();

        let result = engine
            .execute(vec![
                Statement::DataDefinition(DataDefinition::DropTable {
                    table_name: "people".into(),
                    if_exists: false,
                }),
                Statement::DataDefinition(DataDefinition::CreateTable {
                    schema: people_schema(),
                    if_not_exists: false,
                }),
                Statement::DataDefinition(DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_by_city".into(),
                        table_name: "people".into(),
                        column_indices: vec![2],
                        unique: false,
                    },
                    if_not_exists: false,
                }),
                Statement::Query(Query::Insert(QueryInsert {
                    table: "people".into(),
                    row: Row::new(vec![
                        uuid_value(2),
                        Value::from("Grace"),
                        Value::from("Paris"),
                    ]),
                    returning: None,
                })),
                Statement::DataDefinition(DataDefinition::DropTable {
                    table_name: "missing".into(),
                    if_exists: false,
                }),
            ])
            .await;
        assert!(result.is_err());

        assert_eq!(sync_manifest_for(&engine).await.unwrap(), before);
        assert_eq!(
            engine.table_schema("people").await.unwrap(),
            people_schema()
        );
        assert_eq!(
            people_rows(&engine, &["id", "name", "city"]).await,
            vec![original.clone()]
        );
        assert_eq!(
            engine
                .index_lookup("people_by_city", &Row::new(vec![Value::from("London")]))
                .await
                .unwrap(),
            Some(original)
        );
        assert!(
            engine
                .index_lookup("people_by_city", &Row::new(vec![Value::from("Paris")]))
                .await
                .unwrap()
                .is_none()
        );
        drop(engine);
        let reopened = Engine::new(
            RedbKernel::new(Arc::new(redb::Database::open(&path).unwrap())),
            AutomergeRowCodec::new(),
        );
        assert_eq!(sync_manifest_for(&reopened).await.unwrap(), before);
        assert_eq!(
            people_rows(&reopened, &["id", "name", "city"]).await,
            vec![Row::new(vec![
                uuid_value(1),
                Value::from("Ada"),
                Value::from("London"),
            ])]
        );
        assert_eq!(
            reopened
                .index_lookup("people_by_city", &Row::new(vec![Value::from("London")]))
                .await
                .unwrap()
                .unwrap()
                .values[0],
            uuid_value(1)
        );
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn rollback_discards_catalog_and_logical_row_changes() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = "table-103";
        let row_id = Uuid::now_v7();
        let mut transaction = kernel.transaction().await.unwrap();
        transaction
            .ensure_table(engine::ENGINE_TABLES_STORAGE)
            .await
            .unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        let catalog_id = Uuid::now_v7();
        reconciler
            .put_row(
                &mut transaction,
                engine::ENGINE_TABLES_STORAGE,
                catalog_id,
                Row::new(vec![Value::from("people")]),
            )
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                table,
                row_id,
                Row::new(vec![uuid_value(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        transaction.rollback().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert!(
            reconciler
                .get_row(&transaction, engine::ENGINE_TABLES_STORAGE, &catalog_id)
                .await
                .unwrap()
                .is_none()
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn catalog_batch_validation_fails_without_partial_mutation() {
    let path = database_path();
    let engine = replica(&path);
    block_on(async {
        let result = apply_sync_state_batch_for(
            &engine,
            vec![
                SyncStateUnit::new(
                    SyncKey::Row {
                        table: engine::ENGINE_TABLES_STORAGE.into(),
                        row: uuid::Uuid::now_v7(),
                    },
                    postcard::to_allocvec(&Row::new(vec![Value::Bool(false)])).unwrap(),
                    vec![],
                ),
                SyncStateUnit::new(
                    SyncKey::Row {
                        table: engine::ENGINE_TABLES_STORAGE.into(),
                        row: uuid::Uuid::now_v7(),
                    },
                    vec![],
                    vec![],
                ),
            ],
        )
        .await;
        assert!(result.is_err());
        assert!(engine.table_names().await.unwrap().is_empty());
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn bootstrap_destination_exclusively_from_source_canonical_changes() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();

        sync(&source, &destination).await.unwrap();
        assert_eq!(
            people_rows(&destination, &["id", "name", "city"]).await,
            vec![Row::new(vec![
                uuid_value(1),
                Value::from("Ada"),
                Value::from("London"),
            ])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn copied_files_reopen_with_distinct_actors_and_converge() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        {
            let source = replica(&source_path);
            source.create_table(people_schema()).await.unwrap();
            source
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "people".into(),
                    row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                    returning: None,
                }))])
                .await
                .unwrap();
        }
        std::fs::copy(&source_path, &destination_path).unwrap();

        let source = replica(&source_path);
        let destination = replica(&destination_path);
        update(&source, "name", Value::from("Grace")).await;
        update(&destination, "name", Value::from("Linus")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();

        assert_eq!(
            people_rows(&destination, &["name"]).await,
            people_rows(&source, &["name"]).await
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_different_column_updates_converge() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        update(&source, "name", Value::from("Grace")).await;
        update(&destination, "city", Value::from("Paris")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();

        let expected = vec![Row::new(vec![Value::from("Grace"), Value::from("Paris")])];
        assert_eq!(people_rows(&source, &["name", "city"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["name", "city"]).await,
            people_rows(&source, &["name", "city"]).await
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_same_column_updates_expose_null_and_explicit_resolution_converges() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        update(&source, "name", Value::from("Grace")).await;
        update(&destination, "name", Value::Null).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        let conflicts = source
            .row_conflict_values("people", &Row::new(vec![uuid_value(1)]))
            .await
            .unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].0, "name");
        assert_eq!(conflicts[0].1.len(), 2);
        assert!(conflicts[0].1.contains(&Value::from("Grace")));
        assert!(conflicts[0].1.contains(&Value::Null));
        assert_eq!(
            people_rows(&destination, &["name"]).await,
            people_rows(&source, &["name"]).await
        );
        assert!(
            source
                .execute(vec![Statement::Query(Query::Update(QueryUpdate {
                    from: QueryFrom {
                        table: "people".into(),
                        joins: vec![],
                    },
                    assignments: vec![QueryUpdateAssignment {
                        column: QueryColumn::new("people".into(), "name".into()),
                        value: QueryExprValue::Value(Value::from("Margaret")),
                    }],
                    predicate: id(1),
                    returning: None,
                }))])
                .await
                .is_err()
        );
        assert_eq!(
            source
                .row_conflicts("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap(),
            vec!["name"]
        );
        source
            .resolve_row(
                "people",
                &Row::new(vec![uuid_value(1)]),
                vec![("name".into(), Value::Null)],
            )
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        assert!(
            source
                .row_conflicts("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            people_rows(&destination, &["name"]).await,
            vec![Row::new(vec![Value::Null])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn reordered_compatible_schemas_preserve_row_conflicts_and_resolution() {
    let left_path = database_path();
    let right_path = database_path();
    block_on(async {
        let left = replica(&left_path);
        let right = replica(&right_path);
        left.create_table(people_schema()).await.unwrap();
        let mut reordered_schema = people_schema();
        reordered_schema.columns.swap(1, 2);
        right.create_table(reordered_schema).await.unwrap();
        left.execute(vec![Statement::Query(Query::Insert(QueryInsert {
            table: "people".into(),
            row: Row::new(vec![
                uuid_value(1),
                Value::from("Ada"),
                Value::from("London"),
            ]),
            returning: None,
        }))])
        .await
        .unwrap();
        sync(&left, &right).await.unwrap();
        sync(&right, &left).await.unwrap();

        update(&left, "name", Value::from("Grace")).await;
        update(&right, "name", Value::from("Margaret")).await;
        sync(&left, &right).await.unwrap();
        sync(&right, &left).await.unwrap();

        for engine in [&left, &right] {
            let conflicts = engine
                .row_conflict_values("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap();
            assert_eq!(conflicts.len(), 1);
            assert_eq!(conflicts[0].0, "name");
            assert!(conflicts[0].1.contains(&Value::from("Grace")));
            assert!(conflicts[0].1.contains(&Value::from("Margaret")));
        }
        left.resolve_row(
            "people",
            &Row::new(vec![uuid_value(1)]),
            vec![("name".into(), Value::from("resolved"))],
        )
        .await
        .unwrap();
        sync(&left, &right).await.unwrap();
        sync(&right, &left).await.unwrap();
        let expected = vec![Row::new(vec![
            Value::from("resolved"),
            Value::from("London"),
        ])];
        assert_eq!(people_rows(&left, &["name", "city"]).await, expected);
        assert_eq!(people_rows(&right, &["name", "city"]).await, expected);
        assert!(
            left.row_conflicts("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            right
                .row_conflicts("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap()
                .is_empty()
        );
    });
    std::fs::remove_file(left_path).expect("remove left database");
    std::fs::remove_file(right_path).expect("remove right database");
}

#[test]
fn codec_routes_rows_by_table_name_across_instances_and_reopen() {
    let path = database_path();
    block_on(async {
        let catalog_identities = {
            let engine = replica(&path);
            engine.create_table(people_schema()).await.unwrap();
            engine
                .execute(vec![Statement::DataDefinition(
                    DataDefinition::CreateIndex {
                        schema: IndexSchema {
                            name: "people_by_name".into(),
                            table_name: "people".into(),
                            column_indices: vec![1],
                            unique: true,
                        },
                        if_not_exists: false,
                    },
                )])
                .await
                .unwrap();
            let mut other_schema = people_schema();
            other_schema.name = "other_people".into();
            engine.create_table(other_schema).await.unwrap();
            for (table, name) in [("people", "Ada"), ("other_people", "Grace")] {
                engine
                    .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                        table: table.into(),
                        row: Row::new(vec![uuid_value(1), Value::from(name), Value::Null]),
                        returning: None,
                    }))])
                    .await
                    .unwrap();
            }
            assert_eq!(
                table_rows(&engine, "people", &["name"]).await,
                vec![Row::new(vec![Value::from("Ada")])]
            );
            assert_eq!(
                table_rows(&engine, "other_people", &["name"]).await,
                vec![Row::new(vec![Value::from("Grace")])]
            );
            assert_eq!(
                engine.table_schema("people").await.unwrap(),
                people_schema()
            );
            assert_eq!(
                engine.index_schema("people_by_name").await.unwrap(),
                IndexSchema {
                    name: "people_by_name".into(),
                    table_name: "people".into(),
                    column_indices: vec![1],
                    unique: true,
                }
            );
            let mut identities = export_sync_state_for(&engine)
                .await
                .unwrap()
                .into_iter()
                .filter_map(|unit| match unit.key {
                    SyncKey::Row { table, row }
                        if table == engine::ENGINE_TABLES_STORAGE
                            || table == engine::ENGINE_INDICES_STORAGE =>
                    {
                        Some((table, row.as_bytes().to_vec()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            identities.sort();
            identities
        };

        let database = Arc::new(redb::Database::open(&path).unwrap());
        let reopened = Engine::new(RedbKernel::new(database), AutomergeRowCodec::new());
        assert_eq!(
            table_rows(&reopened, "people", &["name"]).await,
            vec![Row::new(vec![Value::from("Ada")])]
        );
        assert_eq!(
            table_rows(&reopened, "other_people", &["name"]).await,
            vec![Row::new(vec![Value::from("Grace")])]
        );
        let mut reopened_catalog_identities = export_sync_state_for(&reopened)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|unit| match unit.key {
                SyncKey::Row { table, row }
                    if table == engine::ENGINE_TABLES_STORAGE
                        || table == engine::ENGINE_INDICES_STORAGE =>
                {
                    Some((table, row.as_bytes().to_vec()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        reopened_catalog_identities.sort();
        assert_eq!(reopened_catalog_identities, catalog_identities);
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn dropped_table_name_can_be_recreated_with_new_definition() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        let schema = people_schema();
        engine.create_table(schema.clone()).await.unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_by_name".into(),
                        table_name: "people".into(),
                        column_indices: vec![1],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        for (id, name, city) in [(1, "Ada", "London"), (2, "Grace", "Paris")] {
            engine
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "people".into(),
                    row: Row::new(vec![uuid_value(id), Value::from(name), Value::from(city)]),
                    returning: None,
                }))])
                .await
                .unwrap();
        }
        engine
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                predicate: id(1),
                returning: None,
            }))])
            .await
            .unwrap();

        let old_definition = live_table_definition_ids(&engine).await[0];
        let dependent_schema_rows = schema_row_ids(&engine).await;
        engine.drop_table("people").await.unwrap();
        let dropped_state = export_sync_state_for(&engine).await.unwrap();
        for (table, row) in dependent_schema_rows {
            let state = dropped_state
                .iter()
                .find(|unit| {
                    unit.key
                        == SyncKey::Row {
                            table: table.clone(),
                            row,
                        }
                })
                .expect("dropped dependent definition remains in state");
            assert!(
                postcard::from_bytes::<RowMetadata>(&state.metadata)
                    .expect("decode deletion metadata")
                    .deleted,
                "DROP TABLE did not tombstone a dependent schema row"
            );
        }
        assert!(engine.table_schema("people").await.is_err());
        assert!(engine.index_schema("people_by_name").await.is_err());
        engine
            .create_table(schema)
            .await
            .expect("same-name table recreation creates a new definition row");
        assert_eq!(
            engine.table_schema("people").await.unwrap(),
            people_schema()
        );
        assert!(
            people_rows(&engine, &["id", "name", "city"])
                .await
                .is_empty()
        );
        let new_definition = live_table_definition_ids(&engine).await[0];
        assert!(new_definition > old_definition);
        assert_eq!(new_definition.get_version_num(), 7);
        for row in [row_uuid(1), row_uuid(2)] {
            assert!(
                engine
                    .read_transaction(move |codec, transaction| {
                        Box::pin(
                            async move { codec.row_is_deleted(transaction, "people", &row).await },
                        )
                    })
                    .await
                    .unwrap(),
                "table recreation cleared a prior application-row tombstone"
            );
        }
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn dropping_and_recreating_index_preserves_and_rebuilds_rows() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        engine.create_table(people_schema()).await.unwrap();
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
        for (id, name) in [(1, "Ada"), (2, "Grace")] {
            engine
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "people".into(),
                    row: Row::new(vec![uuid_value(id), Value::from(name), Value::Null]),
                    returning: None,
                }))])
                .await
                .unwrap();
        }
        let old_index_id = schema_row_ids(&engine)
            .await
            .into_iter()
            .filter_map(|(table, id)| (table == engine::ENGINE_INDICES_STORAGE).then_some(id))
            .max()
            .expect("old index definition exists");

        engine
            .execute(vec![Statement::DataDefinition(DataDefinition::DropIndex {
                index_name: "people_by_name".into(),
                if_exists: false,
            })])
            .await
            .unwrap();
        let live_index_fields = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let rows =
                        codec.scan_row_states(transaction, engine::ENGINE_INDEX_FIELDS_STORAGE);
                    futures::pin_mut!(rows);
                    let mut live = 0;
                    while let Some(row) = rows.next().await {
                        if !row?.2 {
                            live += 1;
                        }
                    }
                    Ok(live)
                })
            })
            .await
            .unwrap();
        assert_eq!(live_index_fields, 0);
        let live_rows = people_rows(&engine, &["id", "name"]).await;
        assert_eq!(live_rows.len(), 2);
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
        let new_index_id = schema_row_ids(&engine)
            .await
            .into_iter()
            .filter_map(|(table, id)| (table == engine::ENGINE_INDICES_STORAGE).then_some(id))
            .max()
            .expect("recreated index definition exists");
        assert!(new_index_id > old_index_id);
        for row in live_rows {
            let key = Row::new(vec![row.values[1].clone()]);
            assert_eq!(
                engine.index_lookup("people_by_name", &key).await.unwrap(),
                Some(Row::new(vec![
                    row.values[0].clone(),
                    row.values[1].clone(),
                    Value::Null,
                ]))
            );
        }
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn unique_index_rejects_duplicate_values() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        engine.create_table(people_schema()).await.unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_by_name".into(),
                        table_name: "people".into(),
                        column_indices: vec![1],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        for id in [1, 2] {
            let result = engine
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "people".into(),
                    row: Row::new(vec![uuid_value(id), Value::from("Ada"), Value::Null]),
                    returning: None,
                }))])
                .await;
            if id == 1 {
                result.unwrap();
            } else {
                assert!(result.is_err(), "duplicate unique-index value was accepted");
            }
        }
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn generated_schema_replacement_must_exceed_deleted_owner() {
    let path = database_path();
    block_on(async {
        let engine = replica_with_timestamp(&path, online_timestamp);
        engine.create_table(people_schema()).await.unwrap();
        engine.drop_table("people").await.unwrap();
        drop(engine);

        let engine = replica_with_timestamp(&path, offline_timestamp);
        let before = sync_manifest_for(&engine).await.unwrap();
        let error = engine
            .create_table(people_schema())
            .await
            .expect_err("older generated schema UUID replaced a deleted owner");
        assert!(
            error.to_string().contains("larger") || error.to_string().contains("deleted"),
            "replacement error must explain the retained owner: {error}"
        );
        assert!(engine.table_schema("people").await.is_err());
        assert_eq!(sync_manifest_for(&engine).await.unwrap(), before);
    });
    std::fs::remove_file(path).expect("remove database");
}

#[test]
fn local_schema_name_duplicates_are_rejected() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        engine
            .create_table(people_schema())
            .await
            .expect("create initial table");
        assert!(
            engine.create_table(people_schema()).await.is_err(),
            "duplicate live table name was accepted"
        );
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
            .expect("create initial index");
        assert!(
            engine
                .execute(vec![Statement::DataDefinition(
                    DataDefinition::CreateIndex {
                        schema: IndexSchema {
                            name: "people_by_name".into(),
                            table_name: "people".into(),
                            column_indices: vec![2],
                            unique: false,
                        },
                        if_not_exists: false,
                    },
                )])
                .await
                .is_err(),
            "duplicate live index name was accepted"
        );
    });
    std::fs::remove_file(path).expect("remove test database");
}

#[test]
fn inserting_an_existing_primary_key_is_rejected() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        engine.create_table(people_schema()).await.unwrap();
        for name in ["Ada", "Duplicate"] {
            let result = engine
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "people".into(),
                    row: Row::new(vec![uuid_value(1), Value::from(name), Value::Null]),
                    returning: None,
                }))])
                .await;
            if name == "Ada" {
                result.unwrap();
            } else {
                assert!(result.is_err(), "duplicate primary key was accepted");
            }
        }
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn stale_offline_row_does_not_return_after_table_recreation() {
    let offline_path = database_path();
    let online_path = database_path();
    block_on(async {
        let offline = replica_with_timestamp(&offline_path, offline_timestamp);
        let online = replica_with_timestamp(&online_path, online_timestamp);
        let late_stale_row = Uuid::new_v7(uuid::Timestamp::from_unix_time(1_700_000_020, 0, 0, 0));
        offline.create_table(people_schema()).await.unwrap();
        let offline_definition = live_table_definition_ids(&offline).await[0];
        offline
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("initial"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        online.create_table(people_schema()).await.unwrap();
        sync(&offline, &online).await.unwrap();
        let online_definition = live_table_definition_ids(&online).await[0];
        assert!(
            offline_definition < online_definition,
            "fixture must make the offline definition the losing contender"
        );
        update(&offline, "name", Value::from("stale")).await;
        offline
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    Value::Uuid(late_stale_row),
                    Value::from("stale insert"),
                    Value::Null,
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        let stale_changes = {
            let mut stale_changes = Vec::new();
            for stale_row in [row_uuid(1), late_stale_row] {
                let changes = offline
                    .read_transaction(|codec, transaction| {
                        Box::pin(codec.change_inventory(
                            transaction,
                            "people",
                            stale_row,
                            usize::MAX,
                        ))
                    })
                    .await
                    .unwrap();
                for change_id in changes {
                    let export_id = change_id.clone();
                    let payload = offline
                        .read_transaction(move |codec, transaction| {
                            Box::pin(async move {
                                codec
                                    .export_change(
                                        transaction,
                                        "people",
                                        stale_row,
                                        &export_id,
                                        usize::MAX,
                                    )
                                    .await
                            })
                        })
                        .await
                        .unwrap()
                        .unwrap();
                    stale_changes.push(SyncIncrementalChange {
                        table: "people".into(),
                        row: stale_row,
                        id: change_id,
                        payload,
                    });
                }
            }
            stale_changes
        };
        let stale_insert_state = export_sync_state_for(&offline)
            .await
            .unwrap()
            .into_iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: "people".into(),
                        row: late_stale_row,
                    }
            })
            .expect("stale offline insert snapshot exists");
        let old_definition = live_table_definition_ids(&online).await[0];
        online.drop_table("people").await.unwrap();
        apply_sync_state_batch_for(&online, vec![stale_insert_state])
            .await
            .unwrap();
        assert!(
            online
                .read_transaction(|codec, transaction| {
                    let row = late_stale_row;
                    Box::pin(async move { codec.row_is_deleted(transaction, "people", &row).await })
                })
                .await
                .unwrap(),
            "stale insert snapshot did not receive the completed table drop"
        );
        let dropped_definition = export_sync_state_for(&online)
            .await
            .unwrap()
            .into_iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: engine::ENGINE_TABLES_STORAGE.into(),
                        row: old_definition,
                    }
            })
            .unwrap();
        let metadata: RowMetadata = postcard::from_bytes(&dropped_definition.metadata).unwrap();
        assert!(
            !metadata.drop_events.is_empty(),
            "drop event was not recorded"
        );
        assert!(
            late_stale_row > metadata.drop_events[0],
            "stale row fixture must have a larger UUID than the drop event"
        );
        let losing_definition = export_sync_state_for(&online)
            .await
            .unwrap()
            .into_iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: engine::ENGINE_TABLES_STORAGE.into(),
                        row: offline_definition,
                    }
            })
            .expect("losing definition state remains retained");
        let losing_metadata: RowMetadata =
            postcard::from_bytes(&losing_definition.metadata).unwrap();
        assert_eq!(losing_metadata.drop_events, metadata.drop_events);
        drop(online);
        let online = Engine::new(
            RedbKernel::new(Arc::new(redb::Database::open(&online_path).unwrap())),
            AutomergeRowCodec::new(),
        );
        online.create_table(people_schema()).await.unwrap();
        online
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(3), Value::from("fresh"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();

        apply_incremental_changes_for(&online, &stale_changes)
            .await
            .unwrap();
        apply_incremental_changes_for(&online, &stale_changes)
            .await
            .unwrap();
        sync(&offline, &online).await.unwrap();
        assert_eq!(
            people_rows(&online, &["id", "name"]).await,
            vec![Row::new(vec![uuid_value(3), Value::from("fresh")])]
        );
        for stale_row in [row_uuid(1), late_stale_row] {
            assert!(
                online
                    .read_transaction(move |codec, transaction| {
                        Box::pin(async move {
                            codec
                                .row_is_deleted(transaction, "people", &stale_row)
                                .await
                        })
                    })
                    .await
                    .unwrap(),
                "stale offline row was not permanently tombstoned"
            );
        }
        sync(&online, &offline).await.unwrap();
        sync(&offline, &online).await.unwrap();
        let online_states = export_sync_state_for(&online).await.unwrap();
        let offline_states = export_sync_state_for(&offline).await.unwrap();
        for row in [row_uuid(1), late_stale_row] {
            let key = SyncKey::Row {
                table: "people".into(),
                row,
            };
            let online_state = online_states.iter().find(|unit| unit.key == key).unwrap();
            let offline_state = offline_states.iter().find(|unit| unit.key == key).unwrap();
            assert_eq!(online_state.metadata, offline_state.metadata, "row {row}");
        }
        let online_manifest = sync_manifest_for(&online).await.unwrap();
        assert_eq!(online_manifest, sync_manifest_for(&offline).await.unwrap());
    });
    std::fs::remove_file(offline_path).unwrap();
    std::fs::remove_file(online_path).unwrap();
}

#[test]
fn stale_offline_index_is_tombstoned_after_table_recreation() {
    let offline_path = database_path();
    let online_path = database_path();
    block_on(async {
        let offline = replica(&offline_path);
        let online = replica(&online_path);
        offline.create_table(people_schema()).await.unwrap();
        sync(&offline, &online).await.unwrap();

        offline
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
        online.drop_table("people").await.unwrap();
        online.create_table(people_schema()).await.unwrap();
        sync(&offline, &online).await.unwrap();
        sync(&online, &offline).await.unwrap();

        assert!(
            online
                .index_lookup("people_by_name", &Row::new(vec![Value::from("Ada")]))
                .await
                .is_err()
        );
        assert_eq!(live_table_definition_ids(&online).await.len(), 1);
    });
    std::fs::remove_file(offline_path).unwrap();
    std::fs::remove_file(online_path).unwrap();
}

#[test]
fn concurrent_table_drop_converges_between_replicas() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        let schema = people_schema();
        source.create_table(schema.clone()).await.unwrap();
        sync(&source, &destination).await.unwrap();
        let original_definition = live_table_definition_ids(&source).await[0];

        source.drop_table("people").await.unwrap();
        destination.drop_table("people").await.unwrap();
        source.create_table(schema.clone()).await.unwrap();
        destination.create_table(schema.clone()).await.unwrap();
        for (engine, row_id) in [(&source, 31), (&destination, 32)] {
            engine
                .execute(vec![
                    Statement::DataDefinition(DataDefinition::CreateIndex {
                        schema: IndexSchema {
                            name: "people_by_name".into(),
                            table_name: "people".into(),
                            column_indices: vec![1],
                            unique: false,
                        },
                        if_not_exists: false,
                    }),
                    Statement::Query(Query::Insert(QueryInsert {
                        table: "people".into(),
                        row: Row::new(vec![
                            uuid_value(row_id),
                            Value::from("unobserved recreation"),
                            Value::Null,
                        ]),
                        returning: None,
                    })),
                ])
                .await
                .expect("write against unobserved recreation");
        }

        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();

        assert!(source.table_schema("people").await.is_err());
        assert!(destination.table_schema("people").await.is_err());
        for (engine, row) in [(&source, 31), (&destination, 32)] {
            let row_id = row_uuid(row);
            assert!(
                engine
                    .read_transaction(|codec, transaction| {
                        Box::pin(async move {
                            codec.row_is_deleted(transaction, "people", &row_id).await
                        })
                    })
                    .await
                    .expect("read invalid-recreation row tombstone"),
                "row {row} from unobserved recreation remained live"
            );
        }
        for engine in [&source, &destination] {
            engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move {
                        for storage in [
                            engine::ENGINE_TABLE_FIELDS_STORAGE,
                            engine::ENGINE_INDICES_STORAGE,
                            engine::ENGINE_INDEX_FIELDS_STORAGE,
                        ] {
                            let rows = codec.scan_row_states(transaction, storage);
                            futures::pin_mut!(rows);
                            while let Some(entry) = rows.next().await {
                                let (_, row, deleted) = entry?;
                                let belongs_to_table = match storage {
                                    engine::ENGINE_TABLE_FIELDS_STORAGE => {
                                        row.values.get(1).and_then(Value::as_text) == Some("people")
                                    }
                                    engine::ENGINE_INDICES_STORAGE => {
                                        row.values.get(1).and_then(Value::as_text) == Some("people")
                                    }
                                    engine::ENGINE_INDEX_FIELDS_STORAGE => true,
                                    _ => false,
                                };
                                if belongs_to_table && !deleted {
                                    return Err(engine::EngineError::custom(
                                        "unobserved recreation left live schema dependents",
                                    ));
                                }
                            }
                        }
                        Ok(())
                    })
                })
                .await
                .expect("unobserved recreation dependents are tombstoned");
        }
        let dropped_definition = export_sync_state_for(&source)
            .await
            .expect("export concurrent drop history")
            .into_iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: engine::ENGINE_TABLES_STORAGE.into(),
                        row: original_definition,
                    }
            })
            .expect("original definition remains tombstoned");
        let dropped_metadata: RowMetadata = postcard::from_bytes(&dropped_definition.metadata)
            .expect("decode concurrent drop history");
        assert_eq!(dropped_metadata.drop_events.len(), 2);

        source.create_table(schema).await.unwrap();
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        assert!(source.table_schema("people").await.is_ok());
        assert!(destination.table_schema("people").await.is_ok());
        let recreated_definition = live_table_definition_ids(&source).await[0];
        let recreated_state = export_sync_state_for(&source)
            .await
            .expect("export observed recreation")
            .into_iter()
            .find(|unit| {
                unit.key
                    == SyncKey::Row {
                        table: engine::ENGINE_TABLES_STORAGE.into(),
                        row: recreated_definition,
                    }
            })
            .expect("recreated definition exists");
        let recreated_metadata: RowMetadata =
            postcard::from_bytes(&recreated_state.metadata).expect("decode recreation metadata");
        assert_eq!(recreated_metadata.observed_drops.len(), 2);
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn catalog_row_mutation_rebuilds_active_tables_without_an_application_row_address() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        engine
            .create_table(people_schema())
            .await
            .expect("create table");
        let (table_id, table_row) = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let rows = codec.scan_row_states(transaction, engine::ENGINE_TABLES_STORAGE);
                    futures::pin_mut!(rows);
                    while let Some(entry) = rows.next().await {
                        let (id, row, deleted) = entry?;
                        if !deleted && row.values.first().and_then(Value::as_text) == Some("people")
                        {
                            return Ok((id, row));
                        }
                    }
                    Err(engine::EngineError::custom("people definition is missing"))
                })
            })
            .await
            .expect("read table definition");
        let mutation_row = table_row.clone();
        engine
            .mutate_rows(
                &[(engine::ENGINE_TABLES_STORAGE.into(), table_id)],
                move |codec, transaction| {
                    Box::pin(async move {
                        codec
                            .put_row(
                                transaction,
                                engine::ENGINE_TABLES_STORAGE,
                                table_id,
                                mutation_row.clone(),
                            )
                            .await?;
                        Ok((
                            (),
                            vec![engine::RowMutation {
                                table: engine::ENGINE_TABLES_STORAGE.into(),
                                row: table_id,
                                old: Some(mutation_row.clone()),
                                new: Some(mutation_row),
                            }],
                        ))
                    })
                },
            )
            .await
            .expect("reconcile catalog-only mutation");
        assert!(engine.table_schema("people").await.is_ok());
    });
    std::fs::remove_file(path).expect("remove database");
}

#[test]
fn mutate_rows_reconciles_replicated_unique_claims() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        engine.create_table(people_schema()).await.unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_city".into(),
                        table_name: "people".into(),
                        column_indices: vec![2],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("smaller"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();

        let row_id = row_uuid(2);
        let row = Row::new(vec![
            Value::Uuid(row_id),
            Value::from("larger"),
            Value::from("London"),
        ]);
        let mutation_row = row.clone();
        engine
            .mutate_rows(&[("people".into(), row_id)], move |codec, transaction| {
                Box::pin(async move {
                    codec
                        .put_row(transaction, "people", row_id, mutation_row.clone())
                        .await?;
                    Ok((
                        (),
                        vec![engine::RowMutation {
                            table: "people".into(),
                            row: row_id,
                            old: None,
                            new: Some(mutation_row),
                        }],
                    ))
                })
            })
            .await
            .unwrap();

        assert_eq!(
            people_rows(&engine, &["id", "name", "city"]).await,
            vec![row.clone()]
        );
        assert!(
            engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move {
                        codec
                            .row_is_deleted(transaction, "people", &row_uuid(1))
                            .await
                    })
                })
                .await
                .unwrap()
        );
        assert_eq!(
            engine
                .index_lookup("people_city", &Row::new(vec![Value::from("London")]))
                .await
                .unwrap(),
            Some(row)
        );

        let replacement_id = row_uuid(3);
        let replacement = Row::new(vec![
            Value::Uuid(replacement_id),
            Value::from("replacement"),
            Value::from("London"),
        ]);
        let transaction_row = replacement.clone();
        engine
            .mutate_transaction("people", replacement_id, move |codec, transaction, _| {
                Box::pin(async move {
                    codec
                        .put_row(
                            transaction,
                            "people",
                            replacement_id,
                            transaction_row.clone(),
                        )
                        .await?;
                    Ok(((), Some(transaction_row)))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            people_rows(&engine, &["id", "name", "city"]).await,
            vec![replacement.clone()]
        );
        for deleted_id in [row_uuid(1), row_uuid(2)] {
            assert!(
                engine
                    .read_transaction(move |codec, transaction| {
                        Box::pin(async move {
                            codec
                                .row_is_deleted(transaction, "people", &deleted_id)
                                .await
                        })
                    })
                    .await
                    .unwrap()
            );
        }
        assert_eq!(
            engine
                .index_lookup("people_city", &Row::new(vec![Value::from("London")]))
                .await
                .unwrap(),
            Some(replacement)
        );
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn replicated_unique_conflict_keeps_largest_uuid_and_does_not_promote_deleted_row() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_city".into(),
                        table_name: "people".into(),
                        column_indices: vec![2],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        destination
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(2),
                    Value::from("Grace"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        let london = Row::new(vec![Value::from("London")]);
        let winner = Some(Row::new(vec![
            uuid_value(2),
            Value::from("Grace"),
            Value::from("London"),
        ]));
        assert_eq!(
            source.index_lookup("people_city", &london).await.unwrap(),
            winner
        );
        assert_eq!(
            destination
                .index_lookup("people_city", &london)
                .await
                .unwrap(),
            winner
        );
        assert_eq!(
            people_rows(&source, &["id", "name", "city"]).await,
            vec![Row::new(vec![
                uuid_value(2),
                Value::from("Grace"),
                Value::from("London"),
            ])]
        );
        for engine in [&source, &destination] {
            assert!(
                engine
                    .read_transaction(|codec, transaction| {
                        Box::pin(async move {
                            codec
                                .row_is_deleted(transaction, "people", &row_uuid(1))
                                .await
                        })
                    })
                    .await
                    .unwrap(),
                "smaller UUID contender was not permanently tombstoned"
            );
        }

        source
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                predicate: id(2),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        let expected = None;
        assert_eq!(
            source.index_lookup("people_city", &london).await.unwrap(),
            expected
        );
        assert_eq!(
            destination
                .index_lookup("people_city", &london)
                .await
                .unwrap(),
            source.index_lookup("people_city", &london).await.unwrap()
        );
        assert_eq!(
            people_rows(&source, &["id", "name", "city"]).await,
            people_rows(&destination, &["id", "name", "city"]).await
        );
        for engine in [&source, &destination] {
            for contender in [1, 2] {
                assert!(
                    engine
                        .read_transaction(move |codec, transaction| {
                            Box::pin(async move {
                                codec
                                    .row_is_deleted(transaction, "people", &row_uuid(contender))
                                    .await
                            })
                        })
                        .await
                        .unwrap(),
                    "deleted unique winner promoted or cleared an old contender"
                );
            }
        }
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn schema_add_column_then_row_mutation_replicates() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        add_column(&source, "role", Value::from("member")).await;
        update(&source, "role", Value::from("admin")).await;
        sync(&source, &destination).await.unwrap();
        assert_eq!(
            people_rows(&destination, &["role"]).await,
            vec![Row::new(vec![Value::from("admin")])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_add_column_converges_with_defaults_and_later_updates() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        add_column(&source, "role", Value::from("member")).await;
        add_column(&destination, "team", Value::from("core")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        let expected = vec![Row::new(vec![Value::from("member"), Value::from("core")])];
        assert_eq!(people_rows(&source, &["role", "team"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["role", "team"]).await,
            people_rows(&source, &["role", "team"]).await
        );

        update(&source, "role", Value::from("admin")).await;
        update(&destination, "team", Value::from("storage")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        let expected = vec![Row::new(vec![Value::from("admin"), Value::from("storage")])];
        assert_eq!(people_rows(&source, &["role", "team"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["role", "team"]).await,
            people_rows(&source, &["role", "team"]).await
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}
