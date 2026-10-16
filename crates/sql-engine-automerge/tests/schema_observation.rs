use engine::{Engine, InMemoryKernel, Kernel, KernelTransaction, RowCodec};
use engine_automerge::AutomergeRowCodec;
use futures::executor::block_on;
use uuid::Uuid;
use value::{Row, Value};

#[test]
fn table_names_preserve_live_identities_order_and_fresh_transactions() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let codec = AutomergeRowCodec::new();
        let database = Engine::new(kernel.clone(), AutomergeRowCodec::new());
        assert!(
            database
                .table_names()
                .await
                .expect("empty catalog")
                .is_empty()
        );

        let ids = [
            "018f0f8e-7b6d-7c4a-8f12-000000000001",
            "018f0f8e-7b6d-7c4a-8f12-000000000002",
            "018f0f8e-7b6d-7c4a-8f12-000000000003",
            "018f0f8e-7b6d-7c4a-8f12-000000000004",
            "018f0f8e-7b6d-7c4a-8f12-000000000005",
        ]
        .map(|id| Uuid::parse_str(id).expect("catalog UUIDv7"));
        let mut transaction = kernel
            .transaction()
            .await
            .expect("catalog fixture transaction");
        codec
            .ensure_table(&mut transaction, engine::ENGINE_TABLES_STORAGE)
            .await
            .expect("table catalog storage");
        for (id, name, deleted) in [
            (ids[0], "zulu", false),
            (ids[1], "alpha", false),
            (ids[2], "alpha", false),
            (ids[3], "zulu", true),
            (ids[4], "removed", true),
        ] {
            codec
                .put_row(
                    &mut transaction,
                    engine::ENGINE_TABLES_STORAGE,
                    id,
                    Row::from([name]),
                )
                .await
                .expect("raw competing table definition");
            if deleted {
                codec
                    .delete_row(&mut transaction, engine::ENGINE_TABLES_STORAGE, &id)
                    .await
                    .expect("tombstone table definition");
            }
        }
        transaction.commit().await.expect("commit raw catalog");
        assert_eq!(
            database.table_names().await.expect("live names"),
            vec!["alpha", "zulu"]
        );

        let mut transaction = kernel
            .transaction()
            .await
            .expect("later catalog transaction");
        for id in [ids[0], ids[2]] {
            codec
                .delete_row(&mut transaction, engine::ENGINE_TABLES_STORAGE, &id)
                .await
                .expect("delete live definition");
        }
        codec
            .put_row(
                &mut transaction,
                engine::ENGINE_TABLES_STORAGE,
                Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-000000000006")
                    .expect("new catalog UUIDv7"),
                Row::from(["beta"]),
            )
            .await
            .expect("new table name");
        transaction.commit().await.expect("commit later catalog");
        assert_eq!(
            database.table_names().await.expect("fresh live names"),
            vec!["alpha", "beta"]
        );
    });
}

#[test]
fn table_names_reject_fresh_malformed_live_and_deleted_raw_rows() {
    block_on(async {
        for deleted in [false, true] {
            let kernel = InMemoryKernel::new();
            let codec = AutomergeRowCodec::new();
            let database = Engine::new(kernel.clone(), AutomergeRowCodec::new());
            database
                .translate_and_execute(
                    "CREATE TABLE items (id UUID PRIMARY KEY)",
                    &sql_translator::SqlTranslator,
                )
                .await
                .expect("valid schema fixture");
            assert_eq!(
                database.table_names().await.expect("earlier valid names"),
                vec!["items"]
            );
            let id = Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-000000000200")
                .expect("malformed catalog row UUIDv7");
            let mut transaction = kernel
                .transaction()
                .await
                .expect("malformed catalog transaction");
            codec
                .put_row(
                    &mut transaction,
                    engine::ENGINE_TABLES_STORAGE,
                    id,
                    Row::new(vec![Value::Integer(42)]),
                )
                .await
                .expect("inject unrelated malformed table name");
            if deleted {
                codec
                    .delete_row(&mut transaction, engine::ENGINE_TABLES_STORAGE, &id)
                    .await
                    .expect("tombstone malformed table row");
            }
            transaction
                .commit()
                .await
                .expect("commit malformed catalog");
            let error = database
                .table_names()
                .await
                .expect_err("fresh names must parse every raw row, including tombstones");
            assert!(matches!(
                error,
                engine::EngineError::Custom(message) if message == "Invalid table name"
            ));
        }
    });
}

#[test]
fn bulk_schema_observation_preserves_order_defaults_indices_and_current_visibility() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT DEFAULT 'initial', amount INTEGER); \
                 CREATE TABLE other (id UUID PRIMARY KEY, label TEXT); \
                 CREATE INDEX items_order ON items (amount, value)",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("schema fixture");
        let (tables, indices, rows) = database
            .observe_schema(&["other", "items", "items"], None)
            .await
            .expect("fresh bulk schema observation");
        assert_eq!(tables[0].name, "other");
        assert_eq!(tables[1], tables[2]);
        assert_eq!(tables[1].columns[1].default, Value::from("initial"));
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].column_indices, vec![2, 1]);
        assert_eq!(indices[0].table_name, "items");
        assert!(!indices[0].unique);
        assert!(
            rows.iter()
                .any(|(table, _, _)| *table == engine::ENGINE_INDICES_STORAGE)
        );
        assert_eq!(
            tables[1],
            database.table_schema("items").await.expect("single table")
        );
        assert_eq!(
            indices[0],
            database
                .index_schema("items_order")
                .await
                .expect("single index")
        );

        database
            .translate_and_execute(
                "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'current'; \
                 DROP INDEX items_order; CREATE UNIQUE INDEX items_order ON items (value, extra)",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("commit new schema");
        let (current, indices, _) = database
            .observe_schema(&["items"], Some(&["items_order"]))
            .await
            .expect("later observation sees current committed metadata");
        assert_eq!(current[0].columns.len(), 4);
        assert_eq!(current[0].columns[3].default, Value::from("current"));
        assert_eq!(indices[0].column_indices, vec![1, 3]);
        assert!(indices[0].unique);
        assert_eq!(tables[1].columns.len(), 3);
        database
            .drop_table("items")
            .await
            .expect("drop observed table");
        assert!(
            database
                .observe_schema(&["items"], Some(&[]))
                .await
                .is_err()
        );
    });
}

#[test]
fn bulk_schema_observation_uses_largest_table_identity_and_checks_index_column_identity() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let codec = AutomergeRowCodec::new();
        let database = Engine::new(kernel.clone(), AutomergeRowCodec::new());
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT, amount INTEGER); \
                 CREATE INDEX items_order ON items (amount, value)",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("initial schema");
        let (_, _, rows) = database
            .observe_schema(&["items"], None)
            .await
            .expect("initial observation");
        let table_id =
            Uuid::parse_str("ffffffff-ffff-7fff-bfff-fffffffffff0").expect("larger UUIDv7");
        let mut transaction = kernel
            .transaction()
            .await
            .expect("new definition transaction");
        codec
            .put_row(
                &mut transaction,
                engine::ENGINE_TABLES_STORAGE,
                table_id,
                Row::from(["items"]),
            )
            .await
            .expect("larger table definition");
        for (offset, (name, kind, default, position, primary_key)) in [
            (
                "value",
                value::ValueType::Text,
                Value::from("winner"),
                2,
                false,
            ),
            ("id", value::ValueType::Uuid, Value::Null, 0, true),
            (
                "amount",
                value::ValueType::Integer,
                Value::Integer(7),
                1,
                false,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let id = Uuid::parse_str(&format!("ffffffff-ffff-7fff-bfff-{:012x}", offset + 1))
                .expect("field UUIDv7");
            codec
                .put_row(
                    &mut transaction,
                    engine::ENGINE_TABLE_FIELDS_STORAGE,
                    id,
                    Row::new(vec![
                        Value::Uuid(table_id),
                        Value::from("items"),
                        Value::from(name),
                        kind.into(),
                        default,
                        Value::Integer(position),
                        Value::Bool(primary_key),
                    ]),
                )
                .await
                .expect("winning definition field");
        }
        transaction
            .commit()
            .await
            .expect("commit competing table definition");
        let (tables, indices, _) = database
            .observe_schema(&["items"], Some(&["items_order"]))
            .await
            .expect("current largest definition");
        assert_eq!(
            tables[0]
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "amount", "value"]
        );
        assert_eq!(tables[0].columns[2].default, Value::from("winner"));
        assert_eq!(indices[0].column_indices, vec![1, 2]);
        assert_eq!(
            tables[0],
            database
                .table_schema("items")
                .await
                .expect("single winning schema")
        );
        assert_eq!(
            indices[0],
            database
                .index_schema("items_order")
                .await
                .expect("single index mapping")
        );

        let (_, id, mut row) = rows
            .into_iter()
            .find(|(storage, _, _)| *storage == engine::ENGINE_INDEX_FIELDS_STORAGE)
            .expect("index field reference");
        row.values[3] = Value::Uuid(Uuid::nil());
        let mut transaction = kernel
            .transaction()
            .await
            .expect("inactive reference transaction");
        codec
            .put_row(
                &mut transaction,
                engine::ENGINE_INDEX_FIELDS_STORAGE,
                id,
                row,
            )
            .await
            .expect("replace reference with absent column identity");
        transaction
            .commit()
            .await
            .expect("commit inactive reference");
        for error in [
            database
                .observe_schema(&["items"], Some(&["items_order"]))
                .await
                .expect_err("batch rejects inactive reference"),
            database
                .index_schema("items_order")
                .await
                .expect_err("single rejects inactive reference"),
        ] {
            assert!(matches!(
                error,
                engine::EngineError::InvalidQuery("Index column identity is inactive")
            ));
        }
    });
}

#[test]
fn bulk_schema_discovery_omits_dropped_indices_but_retains_raw_tombstones() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); \
                 CREATE INDEX items_value ON items (value); DROP INDEX items_value",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("dropped index fixture");
        let (_, indices, rows) = database
            .observe_schema(&["items"], None)
            .await
            .expect("discovery excludes dropped index");
        assert!(indices.is_empty());
        assert!(rows.iter().any(|(storage, _, row)| {
            *storage == engine::ENGINE_INDICES_STORAGE
                && row.values.first().and_then(Value::to_text).as_deref() == Some("items_value")
        }));
        assert!(matches!(
            database
                .observe_schema(&["items"], Some(&["items_value"]))
                .await,
            Err(engine::EngineError::InvalidQuery("Index not found"))
        ));
    });
}

#[test]
fn bulk_schema_discovery_filters_by_winning_index_table_not_losing_definition() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let database = Engine::new(kernel.clone(), AutomergeRowCodec::new());
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); \
                 CREATE TABLE other (id UUID PRIMARY KEY, value TEXT); \
                 CREATE INDEX shared_name ON items (value); \
                 CREATE INDEX other_value ON other (value)",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("competing index fixture");
        let (_, _, rows) = database
            .observe_schema(&["items", "other"], None)
            .await
            .expect("initial index definitions");
        let mut winner = rows
            .iter()
            .find(|(storage, _, row)| {
                *storage == engine::ENGINE_INDICES_STORAGE
                    && row.values.first().and_then(Value::to_text).as_deref() == Some("other_value")
            })
            .expect("other table index definition")
            .2
            .clone();
        winner.values[0] = Value::from("shared_name");
        let mut transaction = kernel
            .transaction()
            .await
            .expect("winning definition transaction");
        AutomergeRowCodec::new()
            .put_row(
                &mut transaction,
                engine::ENGINE_INDICES_STORAGE,
                Uuid::parse_str("ffffffff-ffff-7fff-bfff-fffffffffff0").expect("larger UUIDv7"),
                winner,
            )
            .await
            .expect("winning name moves to other table");
        transaction
            .commit()
            .await
            .expect("commit competing definition");

        let (_, indices, rows) = database
            .observe_schema(&["items"], None)
            .await
            .expect("discovery filters by winner table");
        assert!(indices.is_empty());
        assert_eq!(
            rows.iter()
                .filter(|(storage, _, row)| {
                    *storage == engine::ENGINE_INDICES_STORAGE
                        && row.values.first().and_then(Value::to_text).as_deref()
                            == Some("shared_name")
                })
                .count(),
            2
        );
        let (_, explicit, _) = database
            .observe_schema(&["items"], Some(&["shared_name"]))
            .await
            .expect("explicit lookup still resolves winning name");
        assert_eq!(explicit.len(), 1);
        assert_eq!(explicit[0].table_name, "other");
        let (_, discovered, _) = database
            .observe_schema(&["other"], None)
            .await
            .expect("winner is discoverable for its current table");
        assert!(discovered.iter().any(|index| index.name == "shared_name"));
    });
}

#[test]
fn bulk_schema_observation_rejects_current_malformed_unrelated_catalogs() {
    block_on(async {
        for storage in [
            engine::ENGINE_TABLES_STORAGE,
            engine::ENGINE_TABLE_FIELDS_STORAGE,
            engine::ENGINE_INDICES_STORAGE,
            engine::ENGINE_INDEX_FIELDS_STORAGE,
        ] {
            let kernel = InMemoryKernel::new();
            let database = Engine::new(kernel.clone(), AutomergeRowCodec::new());
            database
                .translate_and_execute(
                    "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); \
                     CREATE INDEX items_value ON items (value)",
                    &sql_translator::SqlTranslator,
                )
                .await
                .expect("valid schema fixture");
            database
                .observe_schema(&["items"], Some(&["items_value"]))
                .await
                .expect("earlier valid observation");
            let mut transaction = kernel
                .transaction()
                .await
                .expect("malformed fixture transaction");
            AutomergeRowCodec::new()
                .put_row(
                    &mut transaction,
                    storage,
                    Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-000000000200").expect("UUIDv7"),
                    Row::new(vec![Value::Integer(42)]),
                )
                .await
                .expect("inject malformed unrelated catalog row");
            transaction
                .commit()
                .await
                .expect("commit malformed metadata");
            let error = database
                .observe_schema(&["items"], Some(&["items_value"]))
                .await
                .expect_err("fresh observation must not reuse earlier valid metadata");
            let expected = match storage {
                engine::ENGINE_TABLES_STORAGE => "Invalid table name",
                engine::ENGINE_TABLE_FIELDS_STORAGE => "Invalid column table identity",
                engine::ENGINE_INDICES_STORAGE => "Invalid index name",
                _ => "Invalid index identity",
            };
            assert!(matches!(error, engine::EngineError::Custom(message) if message == expected));
            let error = database
                .index_schema("items_value")
                .await
                .expect_err("single index resolution must retain unrelated catalog refusals");
            if storage == engine::ENGINE_TABLES_STORAGE {
                assert!(matches!(
                    error,
                    engine::EngineError::InvalidQuery("Index not found")
                ));
            } else {
                assert!(matches!(
                    error,
                    engine::EngineError::Custom(message) if message == expected
                ));
            }
        }
    });
}
