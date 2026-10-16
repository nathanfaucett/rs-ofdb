use automerge::{ActorId, AutoCommit, ROOT, ReadDoc, ScalarValue, transaction::Transactable};
use btree::BTreeTransaction;
use btree_automerge::{AutomergeBTreeTransaction, AutomergeChangeStore, ThresholdPolicy};
use engine::{InMemoryKernel, Kernel, KernelTransaction, RowCodec};
use engine_automerge::AutomergeRowCodec;
use futures::{StreamExt, executor::block_on, pin_mut};
use sync::SyncRowCodec;
use uuid::Uuid;
use value::{Row, Value, ValueType};

fn id(value: u64) -> Uuid {
    Uuid::parse_str(&format!("018f0f8e-7b6d-7c4a-8f12-{value:012x}"))
        .expect("fixed UUIDv7 identity")
}

fn field(table: Uuid, name: &str, position: i64, kind: ValueType, default: Value) -> Row {
    Row::new(vec![
        Value::Uuid(table),
        Value::from("items"),
        Value::from(name),
        kind.into(),
        default,
        Value::Integer(position),
        Value::Bool(position == 0),
    ])
}

#[test]
fn table_state_observation_preserves_defaults_tombstones_conflicts_and_freshness() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let database = engine::Engine::new(kernel.clone(), AutomergeRowCodec::new());
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, label TEXT); \
                 CREATE TABLE empty_items (id UUID PRIMARY KEY, label TEXT); \
                 INSERT INTO items VALUES \
                 (CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID), 'base'), \
                 (CAST('018f0f8e-7b6d-7c4a-8f12-000000000002' AS UUID), 'base')",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("live and empty table fixture");
        let codec = AutomergeRowCodec::new();
        let competitor = AutomergeRowCodec::new();
        let mut transaction = kernel
            .transaction()
            .await
            .expect("concurrent row transaction");
        for key in [id(1), id(2)] {
            let left = codec
                .encode_row(
                    &transaction,
                    "items",
                    &key,
                    &Row::new(vec![Value::Uuid(key), Value::from("left")]),
                    &[1],
                )
                .await
                .expect("left concurrent value");
            let right = competitor
                .encode_row(
                    &transaction,
                    "items",
                    &key,
                    &Row::new(vec![Value::Uuid(key), Value::from("right")]),
                    &[1],
                )
                .await
                .expect("right concurrent value");
            for change in [left, right] {
                codec
                    .merge_row(&mut transaction, "items", key, &change)
                    .await
                    .expect("merge concurrent values");
            }
        }
        codec
            .delete_row(&mut transaction, "items", &id(2))
            .await
            .expect("tombstone conflicted row");
        transaction
            .commit()
            .await
            .expect("commit conflicts and tombstone");
        database
            .translate_and_execute(
                "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'current-default'",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("new decoder default for existing documents");
        let (observed, empty) = database
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    Ok((
                        codec.observe_table_state(transaction, "items").await?,
                        codec
                            .observe_table_state(transaction, "empty_items")
                            .await?,
                    ))
                })
            })
            .await
            .expect("complete immutable table observations");
        assert_eq!(observed.name, "items");
        assert_eq!(observed.columns, vec!["id", "label", "extra"]);
        assert_eq!(observed.rows.len(), 2);
        for key in [id(1), id(2)] {
            let row = observed
                .rows
                .iter()
                .find(|row| row.id == key)
                .expect("all rows retained");
            assert_eq!(row.deleted, key == id(2));
            assert_eq!(row.row.values.len(), 3);
            assert_eq!(row.row.values[0], Value::Uuid(key));
            assert_eq!(row.row.values[2], Value::from("current-default"));
            assert_eq!(row.conflicts.len(), 1);
            assert_eq!(row.conflicts[0].0, "label");
            let values = &row.conflicts[0].1;
            assert_eq!(values.len(), 2);
            assert!(values.contains(&Value::from("left")));
            assert!(values.contains(&Value::from("right")));
            assert!(values.contains(&row.row.values[1]));
        }
        assert_eq!(empty.name, "empty_items");
        assert_eq!(empty.columns, vec!["id", "label"]);
        assert!(empty.rows.is_empty());

        database
            .translate_and_execute(
                "ALTER TABLE items ADD COLUMN fresh INTEGER DEFAULT 7",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("later committed layout");
        let current = database
            .read_transaction(|codec, transaction| {
                Box::pin(async move { codec.observe_table_state(transaction, "items").await })
            })
            .await
            .expect("fresh observation");
        assert_eq!(current.columns, vec!["id", "label", "extra", "fresh"]);
        assert_eq!(current.rows.len(), 2);
        assert!(
            current
                .rows
                .iter()
                .all(|row| row.row.values[3] == Value::Integer(7))
        );
        assert_eq!(observed.columns.len(), 3);
    });
}

#[test]
fn table_state_observation_rejects_malformed_losing_values_even_on_tombstones() {
    block_on(async {
        for deleted in [false, true] {
            for malformed in [
                ScalarValue::Str("not encoded bytes".into()),
                ScalarValue::Bytes(vec![255]),
            ] {
                let kernel = InMemoryKernel::new();
                let database = engine::Engine::new(kernel.clone(), AutomergeRowCodec::new());
                database
                    .translate_and_execute(
                        "CREATE TABLE items (id UUID PRIMARY KEY, label TEXT); \
                         INSERT INTO items VALUES \
                         (CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID), 'base')",
                        &sql_translator::SqlTranslator,
                    )
                    .await
                    .expect("valid row fixture");
                let codec = AutomergeRowCodec::new();
                let mut transaction = kernel
                    .transaction()
                    .await
                    .expect("malformed conflict transaction");
                let state = codec
                    .export_state(&transaction, "items", id(1), usize::MAX)
                    .await
                    .expect("existing state")
                    .expect("row document");
                let mut document = AutoCommit::load(&state).expect("existing document");
                let mut losing = document.fork();
                losing.set_actor(ActorId::from(vec![0; 32]));
                document.set_actor(ActorId::from(vec![255; 32]));
                losing
                    .put(ROOT, "label", malformed)
                    .expect("malformed losing value");
                let safe = postcard::to_allocvec(&Value::from("safe")).expect("encoded safe value");
                document
                    .put(ROOT, "label", ScalarValue::Bytes(safe.clone()))
                    .expect("safe winner");
                document
                    .merge(&mut losing)
                    .expect("merge hostile local conflict");
                assert_eq!(document.get_all(ROOT, "label").expect("conflicts").len(), 2);
                assert_eq!(
                    document
                        .get(ROOT, "label")
                        .expect("visible value")
                        .expect("label")
                        .0
                        .to_bytes(),
                    Some(safe.as_slice()),
                    "the malformed value must be a losing conflict"
                );
                codec
                    .merge_state(&mut transaction, "items", id(1), &document.save())
                    .await
                    .expect("visible safe value remains decodable");
                if deleted {
                    codec
                        .delete_row(&mut transaction, "items", &id(1))
                        .await
                        .expect("tombstone hostile row");
                }
                transaction
                    .commit()
                    .await
                    .expect("commit hostile local state");
                let error = database
                    .read_transaction(|codec, transaction| {
                        Box::pin(
                            async move { codec.observe_table_state(transaction, "items").await },
                        )
                    })
                    .await
                    .expect_err("losing values must be decoded even on deleted rows");
                assert!(matches!(error, engine::EngineError::Custom(_)));
            }
        }
    });
}

#[test]
fn catalog_snapshot_preserves_largest_identity_defaults_and_stored_fallback() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let codec = AutomergeRowCodec::new();
        let competitor = AutomergeRowCodec::new();
        let mut transaction = kernel.transaction().await.expect("snapshot transaction");
        for table in [
            engine::ENGINE_TABLES_STORAGE,
            engine::ENGINE_TABLE_FIELDS_STORAGE,
            engine::ENGINE_INDICES_STORAGE,
            engine::ENGINE_INDEX_FIELDS_STORAGE,
            "items",
        ] {
            codec
                .ensure_table(&mut transaction, table)
                .await
                .expect("fixture storage");
        }
        let key = id(1);
        codec
            .put_row(
                &mut transaction,
                "items",
                key,
                Row::new(vec![Value::Uuid(key), Value::from("base")]),
            )
            .await
            .expect("stored positional fallback");
        let left = codec
            .encode_row(
                &transaction,
                "items",
                &key,
                &Row::new(vec![Value::Uuid(key), Value::from("left")]),
                &[1],
            )
            .await
            .expect("first concurrent change");
        let right = competitor
            .encode_row(
                &transaction,
                "items",
                &key,
                &Row::new(vec![Value::Uuid(key), Value::from("right")]),
                &[1],
            )
            .await
            .expect("second concurrent change");
        codec
            .merge_row(&mut transaction, "items", key, &left)
            .await
            .expect("first change");
        codec
            .merge_row(&mut transaction, "items", key, &right)
            .await
            .expect("concurrent conflict");
        assert_eq!(
            codec
                .conflicted_columns(&transaction, "items", &key, &[], &[])
                .await
                .expect("stored layout fallback"),
            vec![1]
        );

        let tables = vec![
            (id(99), Row::new(vec![Value::Integer(42)])),
            (id(20), Row::from(["items"])),
            (id(10), Row::from(["items"])),
        ];
        let fields = vec![
            field(id(10), "0", 0, ValueType::Uuid, Value::Null),
            field(id(10), "loser", 1, ValueType::Text, Value::Null),
            field(id(20), "0", 0, ValueType::Uuid, Value::Null),
            field(id(20), "1", 1, ValueType::Text, Value::Null),
            field(
                id(20),
                "2",
                2,
                ValueType::Text,
                Value::from("fresh-default"),
            ),
        ];
        for (table_id, row) in &tables {
            codec
                .put_row(
                    &mut transaction,
                    engine::ENGINE_TABLES_STORAGE,
                    *table_id,
                    row.clone(),
                )
                .await
                .expect("competing live table definitions");
        }
        for (index, row) in fields.iter().enumerate() {
            codec
                .put_row(
                    &mut transaction,
                    engine::ENGINE_TABLE_FIELDS_STORAGE,
                    id(200 + index as u64),
                    row.clone(),
                )
                .await
                .expect("current raw fields");
        }
        assert_eq!(
            codec
                .conflicted_columns(&transaction, "items", &key, &tables, &fields)
                .await
                .expect("codec skips malformed unrelated names and selects largest matching UUID"),
            vec![1]
        );
        let row = codec
            .get_row(&transaction, "items", &key)
            .await
            .expect("current catalog read")
            .expect("retained row");
        assert_eq!(row.values.len(), 3);
        assert_eq!(row.values[0], Value::Uuid(key));
        assert_eq!(row.values[2], Value::from("fresh-default"));
        {
            let rows = codec.scan_rows(&transaction, "items", Some((&tables, &fields)));
            pin_mut!(rows);
            let (row_id, row) = rows
                .next()
                .await
                .expect("one live row")
                .expect("snapshot decoding");
            assert_eq!(row_id, key);
            assert_eq!(row.values.len(), 3);
            assert_eq!(row.values[2], Value::from("fresh-default"));
            assert!(rows.next().await.is_none());
        }
        codec
            .remove_row(&mut transaction, engine::ENGINE_TABLES_STORAGE, &id(99))
            .await
            .expect("tombstone malformed definition before engine validation");
        transaction.commit().await.expect("persist fixture");
        let database = engine::Engine::new(kernel, codec);
        assert_eq!(
            database
                .row_conflicts("items", &Row::new(vec![Value::Uuid(key)]))
                .await
                .expect("engine current raw snapshot"),
            vec!["1"]
        );
        let results = database
            .translate_and_execute(
                "SELECT * FROM items LIMIT 0; SELECT * FROM items; SELECT * FROM items",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("read batch preserves winning schema, defaults and conflicted rows");
        assert!(results[0].rows.is_empty());
        assert_eq!(results[1].rows.len(), 1);
        assert_eq!(results[1].rows[0].values.len(), 3);
        assert_eq!(results[1].rows[0].values[0], Value::Uuid(key));
        assert_eq!(results[1].rows[0].values[2], Value::from("fresh-default"));
        assert_eq!(results[1].rows, results[2].rows);
        assert_eq!(results[1].columns, results[2].columns);
    });
}

#[test]
fn catalog_snapshot_rejects_malformed_unrelated_table_before_invalid_key() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let codec = AutomergeRowCodec::new();
        let mut transaction = kernel
            .transaction()
            .await
            .expect("catalog fixture transaction");
        for table in [
            engine::ENGINE_TABLES_STORAGE,
            engine::ENGINE_TABLE_FIELDS_STORAGE,
            engine::ENGINE_INDICES_STORAGE,
            engine::ENGINE_INDEX_FIELDS_STORAGE,
        ] {
            codec
                .ensure_table(&mut transaction, table)
                .await
                .expect("catalog storage");
        }
        codec
            .put_row(
                &mut transaction,
                engine::ENGINE_TABLES_STORAGE,
                id(10),
                Row::from(["items"]),
            )
            .await
            .expect("valid target definition");
        codec
            .put_row(
                &mut transaction,
                engine::ENGINE_TABLES_STORAGE,
                id(20),
                Row::new(vec![Value::Integer(42)]),
            )
            .await
            .expect("malformed unrelated definition fixture");
        transaction
            .commit()
            .await
            .expect("persist malformed fixture");
        let database = engine::Engine::new(kernel, codec);
        let error = database
            .translate_and_execute(
                "SELECT * FROM items LIMIT 0; SELECT * FROM items LIMIT 0",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect_err("read batch must validate unrelated catalog definitions");
        assert!(
            matches!(error, engine::EngineError::Custom(message) if message == "Invalid table name")
        );
        let error = database
            .row_conflicts("items", &Row::new(vec![Value::Bool(true)]))
            .await
            .expect_err("catalog validation precedes invalid key");
        assert!(
            matches!(error, engine::EngineError::Custom(message) if message == "Invalid table name")
        );
    });
}

#[test]
fn catalog_snapshot_limit_zero_preserves_aggregate_validation_and_metadata() {
    block_on(async {
        let database = engine::Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); \
                 INSERT INTO items VALUES \
                 (CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID), 'good')",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("nonempty aggregate fixture");
        let count = database
            .translate_and_execute("SELECT COUNT(*) FROM items", &sql_translator::SqlTranslator)
            .await
            .expect("ordinary count");
        assert_eq!(count[0].rows, vec![Row::new(vec![Value::Integer(1)])]);
        for sql in [
            "SELECT COUNT(*) FROM items LIMIT 0",
            "SELECT COUNT(value) FROM items LIMIT 0",
        ] {
            let error = database
                .translate_and_execute(sql, &sql_translator::SqlTranslator)
                .await
                .expect_err("ungrouped aggregate LIMIT remains unsupported");
            assert!(matches!(
                error,
                engine::EngineError::Unsupported("combined aggregate SELECT")
            ));
        }
        let grouped = database
            .translate_and_execute(
                "SELECT value, COUNT(*) FROM items GROUP BY value HAVING COUNT(*) > 0 LIMIT 0",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("grouped aggregate LIMIT 0");
        assert!(grouped[0].rows.is_empty());
        assert_eq!(
            grouped[0].columns,
            vec![
                query::QueryResultColumn {
                    name: "value".into(),
                    source_table: Some("items".into()),
                    source_column: Some("value".into()),
                },
                query::QueryResultColumn {
                    name: "COUNT(*)".into(),
                    source_table: Some("items".into()),
                    source_column: None,
                },
            ]
        );
        let error = database
            .translate_and_execute(
                "SELECT value, COUNT(*) FROM items GROUP BY value ORDER BY id LIMIT 0",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect_err("LIMIT 0 must validate aggregate grouping");
        assert!(matches!(
            error,
            engine::EngineError::Unsupported("GROUP BY ORDER BY column")
        ));
    });
}

#[test]
fn catalog_snapshot_read_batches_keep_table_schemas_and_observe_writes() {
    block_on(async {
        let database = engine::Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); \
                 CREATE TABLE other (id UUID PRIMARY KEY, amount INTEGER); \
                 INSERT INTO items VALUES \
                 (CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID), 'good'); \
                 INSERT INTO other VALUES \
                 (CAST('018f0f8e-7b6d-7c4a-8f12-000000000002' AS UUID), 42)",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("two distinct table schemas");
        let results = database
            .translate_and_execute(
                "SELECT value FROM items LIMIT 0; SELECT amount FROM other; SELECT value FROM items",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("read-only batch shares catalog but not table schema");
        assert_eq!(results.len(), 3);
        assert!(results[0].rows.is_empty());
        assert_eq!(results[0].columns[0].name, "value");
        assert_eq!(results[1].rows, vec![Row::new(vec![Value::Integer(42)])]);
        assert_eq!(results[2].rows, vec![Row::from(["good"])]);

        let mut transaction = database.transaction().await.expect("explicit transaction");
        let results = transaction
            .translate_and_execute(
                "SELECT value FROM items; ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'new'; \
                 SELECT extra FROM items; UPDATE items SET extra = 'changed'; SELECT extra FROM items",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("mixed batch observes schema and row writes");
        assert_eq!(results[0].rows, vec![Row::from(["good"])]);
        assert_eq!(results[2].rows, vec![Row::from(["new"])]);
        assert_eq!(results[4].rows, vec![Row::from(["changed"])]);
        let results = transaction
            .translate_and_execute(
                "SELECT extra FROM items LIMIT 0; SELECT extra FROM items",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("read-only batch sees earlier writes in the same transaction");
        assert!(results[0].rows.is_empty());
        assert_eq!(results[1].rows, vec![Row::from(["changed"])]);
        transaction.commit().await.expect("commit current schema");
        let results = database
            .translate_and_execute(
                "SELECT extra FROM items; DROP TABLE items; \
                 CREATE TABLE items (id UUID PRIMARY KEY, replacement INTEGER); \
                 SELECT replacement FROM items LIMIT 0",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("later batch and table recreation use current catalog");
        assert_eq!(results[0].rows, vec![Row::from(["changed"])]);
        assert!(results[3].rows.is_empty());
        assert_eq!(results[3].columns[0].name, "replacement");
    });
}

#[test]
fn catalog_snapshot_read_batches_reject_current_malformed_unrelated_fields() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let database = engine::Engine::new(kernel.clone(), AutomergeRowCodec::new());
        let sql = "SELECT * FROM items LIMIT 0; SELECT * FROM items LIMIT 0";
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT)",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("valid table schema");
        database
            .translate_and_execute(sql, &sql_translator::SqlTranslator)
            .await
            .expect("valid earlier read-only batch");
        let codec = AutomergeRowCodec::new();
        let mut transaction = kernel.transaction().await.expect("corrupt field fixture");
        codec
            .put_row(
                &mut transaction,
                engine::ENGINE_TABLE_FIELDS_STORAGE,
                id(200),
                Row::new(vec![Value::Integer(42)]),
            )
            .await
            .expect("malformed unrelated field");
        transaction.commit().await.expect("commit malformed field");
        let error = database
            .translate_and_execute(sql, &sql_translator::SqlTranslator)
            .await
            .expect_err("later transaction must validate all current catalog fields");
        assert!(
            matches!(error, engine::EngineError::Custom(message) if message == "Invalid column table identity")
        );
    });
}

#[test]
fn catalog_snapshot_select_decodes_malformed_nonmatching_rows() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let database = engine::Engine::new(kernel.clone(), AutomergeRowCodec::new());
        database.translate_and_execute("CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); INSERT INTO items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID), 'good')", &sql_translator::SqlTranslator).await.expect("valid target row and catalog");
        let mut transaction = kernel
            .transaction()
            .await
            .expect("corrupt-row fixture transaction");
        let mut malformed = AutoCommit::new();
        malformed
            .put(ROOT, "value", 42_i64)
            .expect("non-byte cell fixture");
        {
            let changes = AutomergeChangeStore::new(engine::BytesTableTransaction::new(
                &mut transaction,
                "items",
            ));
            let mut documents = AutomergeBTreeTransaction::new(changes, ThresholdPolicy::default());
            documents
                .insert(id(2).as_bytes().to_vec(), malformed)
                .await
                .expect("persist nonmatching malformed document through public raw store");
        }
        transaction
            .commit()
            .await
            .expect("commit malformed fixture");
        for (sql, expected_columns) in [
            (
                "SELECT * FROM items LIMIT 0",
                vec![
                    query::QueryResultColumn {
                        name: "id".into(),
                        source_table: Some("items".into()),
                        source_column: Some("id".into()),
                    },
                    query::QueryResultColumn {
                        name: "value".into(),
                        source_table: Some("items".into()),
                        source_column: Some("value".into()),
                    },
                ],
            ),
            (
                "SELECT DISTINCT value || '!' AS label FROM items ORDER BY id LIMIT 0 OFFSET 1",
                vec![query::QueryResultColumn {
                    name: "label".into(),
                    source_table: None,
                    source_column: None,
                }],
            ),
        ] {
            let results = database
                .translate_and_execute(sql, &sql_translator::SqlTranslator)
                .await
                .expect("LIMIT 0 validates metadata without decoding malformed user rows");
            assert!(results[0].rows.is_empty());
            assert_eq!(results[0].columns, expected_columns);
        }
        for (sql, expected_error) in [
            ("SELECT * FROM missing LIMIT 0", "Table not found"),
            (
                "SELECT missing FROM items LIMIT 0",
                "Unknown projection column",
            ),
            (
                "SELECT other.value FROM items LIMIT 0",
                "Unknown projection table",
            ),
            (
                "SELECT * FROM items ORDER BY missing LIMIT 0",
                "Unknown ORDER BY column",
            ),
            (
                "SELECT id || '!' AS label FROM items LIMIT 0",
                "Text concatenation requires a text column",
            ),
        ] {
            for sql in [
                sql.to_owned(),
                format!("SELECT value FROM items LIMIT 0; {sql}"),
            ] {
                let error = database
                    .translate_and_execute(&sql, &sql_translator::SqlTranslator)
                    .await
                    .expect_err(
                        "LIMIT 0 must retain schema validation, including later batch reads",
                    );
                assert!(
                    matches!(error, engine::EngineError::InvalidQuery(message) if message == expected_error),
                    "{sql}: expected {expected_error}"
                );
            }
        }
        let error = database
            .execute(vec![query::Statement::Query(query::Query::Select(
                query::QuerySelect {
                    from: query::QueryFrom {
                        table: "items".into(),
                        joins: vec![],
                    },
                    text_concats: vec![Some(query::QueryTextConcat {
                        column: query::QueryColumn::new("items".into(), "value".into()),
                        literal: "!".into(),
                        alias: "label".into(),
                    })],
                    limit: Some(0),
                    ..Default::default()
                },
            ))])
            .await
            .expect_err("computed projection position must exist even with LIMIT 0");
        assert!(matches!(
            error,
            engine::EngineError::InvalidQuery("Invalid computed projection")
        ));
        for sql in [
            "SELECT * FROM items",
            "SELECT * FROM items WHERE value = 'good' LIMIT 0",
            "SELECT value FROM items LIMIT 0; SELECT * FROM items WHERE value = 'good' LIMIT 0",
            "SELECT value FROM items LIMIT 0; SELECT value FROM items WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID)",
        ] {
            let error = database
                .translate_and_execute(sql, &sql_translator::SqlTranslator)
                .await
                .expect_err("ordinary and predicate-bearing SELECT must still decode rows");
            assert!(
                matches!(error, engine::EngineError::Custom(message) if message == "Logical row column is not encoded bytes")
            );
        }
        let error = database.translate_and_execute("SELECT value FROM items WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID)", &sql_translator::SqlTranslator).await.expect_err("SELECT must decode every live row before filtering");
        assert!(
            matches!(error, engine::EngineError::Custom(message) if message == "Logical row column is not encoded bytes")
        );
    });
}
