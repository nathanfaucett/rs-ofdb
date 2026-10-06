use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use btree_automerge::DocumentChangeKey;
use engine::{Engine, Kernel, KernelTransaction, RowCodec, RowIdentity};
use engine_automerge::{AutomergeRowCodec, RowMetadata};
use engine_redb::RedbKernel;

use futures::{StreamExt, TryStreamExt, executor::block_on};
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

fn uuid_value(value: u128) -> Value {
    Value::Uuid(Uuid::from_u128(value))
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

fn id(value: u128) -> Option<QueryExpr> {
    Some(QueryExpr::Equals(
        Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
            "people".into(),
            "id".into(),
        )))),
        Box::new(QueryExpr::Value(QueryExprValue::Value(uuid_value(value)))),
    ))
}

async fn table_rows(
    engine: &Engine<RedbKernel, AutomergeRowCodec>,
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

async fn people_rows(engine: &Engine<RedbKernel, AutomergeRowCodec>, columns: &[&str]) -> Vec<Row> {
    table_rows(engine, "people", columns).await
}

async fn sync(
    source: &Engine<RedbKernel, AutomergeRowCodec>,
    destination: &Engine<RedbKernel, AutomergeRowCodec>,
) -> engine::EngineResult<()> {
    apply_sync_state_batch_for(destination, export_sync_state_for(source).await?).await
}

async fn latest_table_identity(engine: &Engine<RedbKernel, AutomergeRowCodec>) -> Vec<u8> {
    export_sync_state_for(engine)
        .await
        .expect("catalog state exports")
        .into_iter()
        .filter_map(|unit| match unit.key {
            SyncKey::Row {
                table,
                row: RowIdentity::Catalog(bytes),
            } if table == engine::ENGINE_TABLES_STORAGE && bytes.len() == 16 => Some(bytes),
            _ => None,
        })
        .max()
        .expect("table catalog identity exists")
}

async fn scoped_row(
    engine: &Engine<RedbKernel, AutomergeRowCodec>,
    table: &str,
    id: Uuid,
) -> RowIdentity {
    let table = table.to_string();
    engine.read_transaction(|codec, transaction| Box::pin(async move {
        codec.row_ids(transaction, &table).try_collect::<Vec<_>>().await?.into_iter()
            .find(|row| matches!(row, RowIdentity::ScopedUser { row, .. } if *row == *id.as_bytes()))
            .ok_or(engine::EngineError::custom("Missing scoped row"))
    })).await.unwrap()
}

async fn update(engine: &Engine<RedbKernel, AutomergeRowCodec>, column: &str, value: Value) {
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
        let row = scoped_row(&source, table, Uuid::from_u128(1)).await;
        let keys = source
            .read_transaction(|codec, transaction| {
                Box::pin(codec.change_inventory(transaction, table, row.clone(), usize::MAX))
            })
            .await
            .unwrap();
        assert_eq!(keys.len(), 2);
        let key = keys[0].clone();
        let export_row = row.clone();
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
                    row: row.clone(),
                    id: keys[0].clone(),
                    payload: first_payload,
                },
                SyncIncrementalChange {
                    table: table.into(),
                    row: row.clone(),
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
        let row = scoped_row(&source, table, Uuid::from_u128(1)).await;
        assert!(
            source
                .read_transaction(|codec, transaction| {
                    Box::pin(codec.change_inventory(transaction, table, row.clone(), usize::MAX))
                })
                .await
                .unwrap()
                .is_empty()
        );

        update(&source, "city", Value::from("Paris")).await;
        assert!(
            source
                .read_transaction(|codec, transaction| {
                    Box::pin(codec.change_inventory(transaction, table, row.clone(), 1))
                })
                .await
                .is_err()
        );
        let inventory = source
            .read_transaction(|codec, transaction| {
                Box::pin(codec.change_inventory(transaction, table, row.clone(), usize::MAX))
            })
            .await
            .unwrap();
        assert_eq!(inventory.len(), 1);
        let key = inventory[0].clone();
        let export_key = key.clone();
        let export_row = row.clone();
        let limited_key = key.clone();
        let limited_row = row.clone();
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
fn newer_catalog_tombstone_hides_older_live_name() {
    let path = database_path();
    let engine = replica(&path);
    block_on(async {
        engine.create_table(people_schema()).await.unwrap();
        let mut later = *Uuid::now_v7().as_bytes();
        later[..6].fill(0xff);
        let id = RowIdentity::catalog(later.to_vec());
        let rows = [(engine::ENGINE_TABLES_STORAGE.into(), id.clone())];
        engine
            .mutate_rows(&rows, |codec, transaction| {
                Box::pin(async move {
                    codec
                        .put_row(
                            transaction,
                            engine::ENGINE_TABLES_STORAGE,
                            id.clone(),
                            Row::new(vec![Value::from("people")]),
                        )
                        .await?;
                    codec
                        .delete_row(transaction, engine::ENGINE_TABLES_STORAGE, &id)
                        .await?;
                    Ok(((), Vec::new()))
                })
            })
            .await
            .unwrap();
        assert!(engine.table_names().await.unwrap().is_empty());
        assert!(engine.table_schema("people").await.is_err());
        assert!(engine.create_table(people_schema()).await.is_err());
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn newer_index_tombstone_masks_older_active_index() {
    let path = database_path();
    let engine = replica(&path);
    block_on(async {
        engine.create_table(people_schema()).await.unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "by_name".into(),
                        table_name: "people".into(),
                        column_indices: vec![1],
                        unique: false,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        let RowIdentity::Catalog(mut id) = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    Ok(codec
                        .row_ids(transaction, engine::ENGINE_TABLES_STORAGE)
                        .try_collect::<Vec<_>>()
                        .await?[0]
                        .clone())
                })
            })
            .await
            .unwrap()
        else {
            panic!("table id");
        };
        let mut future = *Uuid::now_v7().as_bytes();
        future[..6].fill(0xff);
        id.extend_from_slice(&future);
        let id = RowIdentity::catalog(id);
        let rows = [(engine::ENGINE_INDICES_STORAGE.into(), id.clone())];
        engine
            .mutate_rows(&rows, |codec, transaction| {
                Box::pin(async move {
                    codec
                        .put_row(
                            transaction,
                            engine::ENGINE_INDICES_STORAGE,
                            id.clone(),
                            Row::new(vec![
                                Value::from("by_name"),
                                Value::from("people"),
                                Value::Bool(false),
                            ]),
                        )
                        .await?;
                    codec
                        .delete_row(transaction, engine::ENGINE_INDICES_STORAGE, &id)
                        .await?;
                    Ok(((), Vec::new()))
                })
            })
            .await
            .unwrap();
        assert!(engine.index_schema("by_name").await.is_err());
        assert!(
            engine
                .execute(vec![Statement::DataDefinition(
                    DataDefinition::CreateIndex {
                        schema: IndexSchema {
                            name: "by_name".into(),
                            table_name: "people".into(),
                            column_indices: vec![1],
                            unique: false
                        },
                        if_not_exists: false,
                    }
                )])
                .await
                .is_err()
        );
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn concurrent_catalog_children_select_one_identity_per_logical_key() {
    let path = database_path();
    let engine = replica(&path);
    block_on(async {
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
        let (table, index) = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    Ok((
                        codec
                            .row_ids(transaction, engine::ENGINE_TABLES_STORAGE)
                            .try_collect::<Vec<_>>()
                            .await?[0]
                            .clone(),
                        codec
                            .row_ids(transaction, engine::ENGINE_INDICES_STORAGE)
                            .try_collect::<Vec<_>>()
                            .await?[0]
                            .clone(),
                    ))
                })
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_by_name".into(),
                        table_name: "people".into(),
                        column_indices: vec![1],
                        unique: false,
                    },
                    if_not_exists: true,
                },
            )])
            .await
            .unwrap();
        let index_count = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    Ok(codec
                        .row_ids(transaction, engine::ENGINE_INDICES_STORAGE)
                        .try_collect::<Vec<_>>()
                        .await?
                        .len())
                })
            })
            .await
            .unwrap();
        assert_eq!(index_count, 1);
        let mut future = *Uuid::now_v7().as_bytes();
        future[..6].fill(0xff);
        let RowIdentity::Catalog(mut column_id) = table else {
            panic!("table id");
        };
        column_id.extend_from_slice(&future);
        let RowIdentity::Catalog(mut field_id) = index else {
            panic!("index id");
        };
        field_id.extend_from_slice(&future);
        let column_id = RowIdentity::catalog(column_id);
        let field_id = RowIdentity::catalog(field_id);
        let rows = [
            (
                engine::ENGINE_TABLE_FIELDS_STORAGE.into(),
                column_id.clone(),
            ),
            (engine::ENGINE_INDEX_FIELDS_STORAGE.into(), field_id.clone()),
        ];
        engine
            .mutate_rows(&rows, |codec, transaction| {
                Box::pin(async move {
                    codec
                        .put_row(
                            transaction,
                            engine::ENGINE_TABLE_FIELDS_STORAGE,
                            column_id,
                            Row::new(vec![
                                Value::from("name"),
                                ValueType::Text.into(),
                                Value::from("later"),
                                Value::Integer(1),
                                Value::Bool(false),
                            ]),
                        )
                        .await?;
                    codec
                        .put_row(
                            transaction,
                            engine::ENGINE_INDEX_FIELDS_STORAGE,
                            field_id,
                            Row::new(vec![
                                Value::Integer(0),
                                Value::from("city"),
                                Value::Uuid(Uuid::now_v7()),
                            ]),
                        )
                        .await?;
                    Ok(((), Vec::new()))
                })
            })
            .await
            .unwrap();
        let schema = engine.table_schema("people").await.unwrap();
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[1].default, Value::from("later"));
        assert!(engine.index_schema("people_by_name").await.is_err());
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn late_old_generation_row_cannot_reappear_or_populate_new_index() {
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
        sync(&source, &dest).await.unwrap();
        let stale = export_sync_state_for(&source).await.unwrap().into_iter()
            .find(|unit| matches!(&unit.key, SyncKey::Row { table, row: RowIdentity::ScopedUser { .. } } if table == "people"))
            .unwrap();
        dest.drop_table("people").await.unwrap();
        dest.create_table(people_schema()).await.unwrap();
        dest.execute(vec![Statement::DataDefinition(
            DataDefinition::CreateIndex {
                schema: IndexSchema {
                    name: "by_name".into(),
                    table_name: "people".into(),
                    column_indices: vec![1],
                    unique: true,
                },
                if_not_exists: false,
            },
        )])
        .await
        .unwrap();
        apply_sync_state_batch_for(&dest, vec![stale])
            .await
            .unwrap();
        assert!(
            table_rows(&dest, "people", &["id", "name"])
                .await
                .is_empty()
        );
        assert!(
            dest.index_lookup("by_name", &Row::new(vec![Value::from("Ada")]))
                .await
                .unwrap()
                .is_none()
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(dest_path).unwrap();
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
        let row = scoped_row(&source, "people", Uuid::from_u128(1)).await;
        assert!(
            source
                .read_transaction(|codec, transaction| {
                    Box::pin(codec.export_state(transaction, "people", row.clone(), 1))
                })
                .await
                .is_err()
        );
        let state = source
            .read_transaction(|codec, transaction| {
                let row = row.clone();
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
        })
        .unwrap();
        let rows = [("people".into(), row.clone())];
        dest.mutate_rows(&rows, |codec, transaction| {
            Box::pin(async move {
                codec
                    .merge_metadata(transaction, "people", row.clone(), &metadata)
                    .await?;
                assert!(
                    codec
                        .merge_state(transaction, "people", row.clone(), &state)
                        .await?
                        .is_none()
                );
                assert_eq!(
                    codec
                        .export_state(transaction, "people", row.clone(), usize::MAX)
                        .await?,
                    Some(state)
                );
                assert!(codec.get_row(transaction, "people", &row).await?.is_none());
                Ok(((), Vec::new()))
            })
        })
        .await
        .unwrap();
        let row = rows[0].1.clone();
        let retained = dest
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    Ok((
                        codec
                            .export_state(transaction, "people", row.clone(), usize::MAX)
                            .await?,
                        codec.get_row(transaction, "people", &row).await?,
                    ))
                })
            })
            .await
            .unwrap();
        assert!(retained.0.is_some());
        assert!(retained.1.is_none());
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(dest_path).unwrap();
}

#[test]
fn catalog_rows_have_parent_scoped_uuidv7_identities() {
    let path = database_path();
    let engine = replica(&path);
    block_on(async {
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
        let ids = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let mut result = Vec::new();
                    for storage in [
                        engine::ENGINE_TABLES_STORAGE,
                        engine::ENGINE_TABLE_FIELDS_STORAGE,
                        engine::ENGINE_INDICES_STORAGE,
                        engine::ENGINE_INDEX_FIELDS_STORAGE,
                    ] {
                        result.push(
                            codec
                                .row_ids(transaction, storage)
                                .try_collect::<Vec<_>>()
                                .await?,
                        );
                    }
                    Ok(result)
                })
            })
            .await
            .unwrap();
        let RowIdentity::Catalog(table) = &ids[0][0] else {
            panic!("table identity must be catalog");
        };
        let RowIdentity::Catalog(index) = &ids[2][0] else {
            panic!("index identity must be catalog");
        };
        assert_eq!(table.len(), 16);
        assert_eq!(
            Uuid::from_slice(table).unwrap().get_version(),
            Some(uuid::Version::SortRand)
        );
        assert_eq!(ids[1].len(), 3);
        assert_eq!(index.len(), 32);
        assert!(index.starts_with(table));
        for identity in &ids[1] {
            let RowIdentity::Catalog(field) = identity else {
                panic!("field identity must be catalog");
            };
            assert_eq!(field.len(), 32);
            assert!(field.starts_with(table));
        }
        let RowIdentity::Catalog(field) = &ids[3][0] else {
            panic!("index field identity must be catalog");
        };
        assert_eq!(field.len(), 48);
        assert!(field.starts_with(index));
        engine.drop_table("people").await.unwrap();
        assert!(engine.table_names().await.unwrap().is_empty());
        assert!(engine.index_schema("people_by_name").await.is_err());
    });
    std::fs::remove_file(path).unwrap();
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
fn catalog_prefix_scan_returns_only_children_of_requested_table() {
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
                    let tables = codec.scan_row_states(transaction, engine::ENGINE_TABLES_STORAGE);
                    futures::pin_mut!(tables);
                    let mut people_id = None;
                    while let Some(row) = tables.next().await {
                        let (identity, value, deleted) = row?;
                        if !deleted && value.values[0].as_text() == Some("people") {
                            let RowIdentity::Catalog(id) = identity else {
                                return Err(engine::EngineError::custom(
                                    "table identity is not catalog",
                                ));
                            };
                            people_id = Some(id);
                        }
                    }
                    let prefix = RowIdentity::catalog(
                        people_id.ok_or(engine::EngineError::custom("people table missing"))?,
                    )
                    .to_bytes();
                    let fields = codec.scan_row_states_prefix(
                        transaction,
                        engine::ENGINE_TABLE_FIELDS_STORAGE,
                        &prefix,
                    );
                    futures::pin_mut!(fields);
                    let mut count = 0;
                    while fields.next().await.transpose()?.is_some() {
                        count += 1;
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
fn built_in_catalog_layout_decodes_without_catalog_field_rows() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = engine::ENGINE_TABLES_STORAGE;

        let identity = RowIdentity::catalog(Uuid::now_v7().as_bytes().to_vec());
        let expected = Row::new(vec![Value::from("people")]);
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        reconciler
            .put_row(&mut transaction, table, identity.clone(), expected.clone())
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
                RowIdentity::user(row_id),
                Row::new(vec![uuid_value(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, table, &RowIdentity::user(row_id))
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
            .put_row(
                &mut transaction,
                table,
                RowIdentity::user(row_id),
                row.clone(),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, table, &RowIdentity::user(row_id))
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
                RowIdentity::user(row_id),
                Row::new(vec![uuid_value(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        assert!(
            reconciler
                .remove_row(&mut transaction, table, &RowIdentity::user(row_id))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            reconciler
                .put_row(
                    &mut transaction,
                    table,
                    RowIdentity::user(row_id),
                    Row::new(vec![uuid_value(2), Value::from("Grace")]),
                )
                .await
                .is_err()
        );
        reconciler
            .merge_metadata(
                &mut transaction,
                table,
                RowIdentity::user(row_id),
                &postcard::to_allocvec(&RowMetadata {
                    version: 1,
                    deleted: false,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert!(
            reconciler
                .get_row(&transaction, table, &RowIdentity::user(row_id))
                .await
                .unwrap()
                .is_none()
        );
        let metadata_key =
            DocumentChangeKey::new_metadata(RowIdentity::user(row_id).to_bytes()).encode_ordered();
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
            assert_eq!(
                row_ids.next().await.unwrap().unwrap(),
                RowIdentity::user(row_id)
            );
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
        let catalog_id = RowIdentity::catalog(Uuid::now_v7().as_bytes().to_vec());
        reconciler
            .put_row(
                &mut transaction,
                engine::ENGINE_TABLES_STORAGE,
                catalog_id.clone(),
                Row::new(vec![Value::from("people")]),
            )
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                table,
                RowIdentity::user(row_id),
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
                        row: RowIdentity::catalog(uuid::Uuid::now_v7().as_bytes().to_vec()),
                    },
                    postcard::to_allocvec(&Row::new(vec![Value::Bool(false)])).unwrap(),
                    vec![],
                ),
                SyncStateUnit::new(
                    SyncKey::Row {
                        table: engine::ENGINE_TABLES_STORAGE.into(),
                        row: RowIdentity::catalog(uuid::Uuid::now_v7().as_bytes().to_vec()),
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
                    SyncKey::Row {
                        table,
                        row: RowIdentity::Catalog(bytes),
                    } if (table == engine::ENGINE_TABLES_STORAGE
                        || table == engine::ENGINE_INDICES_STORAGE)
                        && bytes.len() == 16 =>
                    {
                        Some((table, bytes))
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
                SyncKey::Row {
                    table,
                    row: RowIdentity::Catalog(bytes),
                } if (table == engine::ENGINE_TABLES_STORAGE
                    || table == engine::ENGINE_INDICES_STORAGE)
                    && bytes.len() == 16 =>
                {
                    Some((table, bytes))
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
fn dropping_and_recreating_a_table_name_isolates_rows_and_indexes() {
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

        engine.drop_table("people").await.unwrap();
        assert!(engine.table_schema("people").await.is_err());
        assert!(engine.index_schema("people_by_name").await.is_err());
        engine.create_table(schema.clone()).await.unwrap();

        assert_eq!(engine.table_schema("people").await.unwrap(), schema);
        assert!(
            table_rows(&engine, "people", &["id", "name", "city"])
                .await
                .is_empty()
        );
        assert!(engine.index_schema("people_by_name").await.is_err());
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
        assert_eq!(
            engine
                .index_lookup("people_by_name", &Row::new(vec![Value::from("Ada")]))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            engine
                .index_lookup("people_by_name", &Row::new(vec![Value::from("Grace")]))
                .await
                .unwrap(),
            None
        );
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(2),
                    Value::from("New"),
                    Value::from("Paris"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        assert_eq!(
            engine
                .index_lookup("people_by_name", &Row::new(vec![Value::from("New")]))
                .await
                .unwrap(),
            Some(Row::new(vec![
                uuid_value(2),
                Value::from("New"),
                Value::from("Paris")
            ]))
        );
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
fn dropping_and_recreating_a_table_name_restores_schema_visibility() {
    let path = database_path();
    block_on(async {
        let engine = replica(&path);
        let schema = people_schema();
        engine.create_table(schema.clone()).await.unwrap();
        engine.drop_table("people").await.unwrap();
        assert!(engine.table_schema("people").await.is_err());
        engine.create_table(schema.clone()).await.unwrap();
        assert_eq!(engine.table_schema("people").await.unwrap(), schema);
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn concurrent_table_drop_and_recreate_converge_between_replicas() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        let schema = people_schema();
        source.create_table(schema.clone()).await.unwrap();
        sync(&source, &destination).await.unwrap();

        source.drop_table("people").await.unwrap();
        destination.drop_table("people").await.unwrap();
        destination.create_table(schema.clone()).await.unwrap();

        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();

        assert_eq!(source.table_schema("people").await.unwrap(), schema);
        assert_eq!(destination.table_schema("people").await.unwrap(), schema);
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn simultaneous_table_recreates_converge_on_the_greatest_uuidv7() {
    let left_path = database_path();
    let right_path = database_path();
    block_on(async {
        let left = replica(&left_path);
        let right = replica(&right_path);
        left.create_table(people_schema()).await.unwrap();
        sync(&left, &right).await.unwrap();
        let previous = latest_table_identity(&left).await;
        left.drop_table("people").await.unwrap();
        right.drop_table("people").await.unwrap();

        let left_schema = people_schema();
        let mut right_schema = people_schema();
        right_schema.columns[2].default = Value::from("unknown");
        left.create_table(left_schema.clone()).await.unwrap();
        right.create_table(right_schema.clone()).await.unwrap();
        let left_id = latest_table_identity(&left).await;
        let right_id = latest_table_identity(&right).await;
        assert!(left_id > previous);
        assert!(right_id > previous);
        let expected = if left_id > right_id {
            left_schema
        } else {
            right_schema
        };

        sync(&left, &right).await.unwrap();
        sync(&right, &left).await.unwrap();
        assert_eq!(left.table_schema("people").await.unwrap(), expected);
        assert_eq!(right.table_schema("people").await.unwrap(), expected);
    });
    std::fs::remove_file(left_path).unwrap();
    std::fs::remove_file(right_path).unwrap();
}

#[test]
fn resolving_an_indexed_conflict_promotes_the_next_unique_contender() {
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
        assert_eq!(
            source.index_lookup("people_city", &london).await.unwrap(),
            Some(Row::new(vec![
                uuid_value(1),
                Value::from("Ada"),
                Value::from("London")
            ]))
        );

        update(&source, "city", Value::from("Paris")).await;
        update(&destination, "city", Value::from("Berlin")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        assert_eq!(
            source
                .row_conflicts("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap(),
            vec!["city"]
        );

        source
            .resolve_row(
                "people",
                &Row::new(vec![uuid_value(1)]),
                vec![("city".into(), Value::from("Paris"))],
            )
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();

        let expected = Some(Row::new(vec![
            uuid_value(2),
            Value::from("Grace"),
            Value::from("London"),
        ]));
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
