use engine::{Engine, InMemoryKernel, RowCodec};
use engine_automerge::AutomergeRowCodec;
use futures::{TryStreamExt, executor::block_on};
use query::{DataDefinition, Query, QueryInsert, Statement};
use schema::{ColumnSchema, IndexSchema, TableSchema};
use sql_translator::SqlTranslator;
use value::{Row, Value, ValueType};

#[test]
fn composite_primary_key_preserves_additional_column_uniqueness() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database.translate_and_execute(
            "CREATE TABLE items (a INTEGER UNIQUE, b TEXT, PRIMARY KEY (a, b)); INSERT INTO items VALUES (1, 'a')",
            &SqlTranslator,
        ).await.expect("composite key with additional constraint");
        assert!(
            database
                .translate_and_execute("INSERT INTO items VALUES (1, 'b')", &SqlTranslator)
                .await
                .is_err()
        );
    });
}

#[test]
fn primary_key_index_name_collision_rolls_back_table_creation() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database.translate_and_execute(
            "CREATE TABLE existing (id INTEGER PRIMARY KEY); CREATE INDEX pk_items ON existing (id)",
            &SqlTranslator,
        ).await.expect("reserved default index name");
        assert!(
            database
                .translate_and_execute(
                    "CREATE TABLE items (id INTEGER PRIMARY KEY)",
                    &SqlTranslator
                )
                .await
                .is_err()
        );
        assert!(
            database.table_schema("items").await.is_err(),
            "failed DDL rolls back the whole table"
        );
        assert_eq!(
            database
                .index_schema("pk_items")
                .await
                .expect("original index preserved")
                .table_name,
            "existing"
        );
    });
}

#[test]
fn natural_keys_keep_their_generated_row_identity_across_updates_and_rebuilds() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database.translate_and_execute(
            "CREATE TABLE items (code TEXT PRIMARY KEY, value INTEGER); INSERT INTO items VALUES ('a', 1)",
            &SqlTranslator,
        ).await.expect("natural key");
        let before = database
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .scan_rows(transaction, "items", None)
                        .try_collect::<Vec<_>>()
                        .await
                })
            })
            .await
            .expect("row identity observation");
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].0.get_version_num(), 7);
        database.translate_and_execute(
            "UPDATE items SET value = 2 WHERE code = 'a'; CREATE INDEX by_value ON items (value)",
            &SqlTranslator,
        ).await.expect("update and rebuild");
        let after = database
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .scan_rows(transaction, "items", None)
                        .try_collect::<Vec<_>>()
                        .await
                })
            })
            .await
            .expect("stable identity observation");
        assert_eq!(
            after,
            vec![(
                before[0].0,
                Row::new(vec![Value::from("a"), Value::Integer(2)])
            )]
        );
        assert_eq!(
            database
                .index_lookup("by_value", &Row::new(vec![Value::Integer(2)]))
                .await
                .expect("secondary lookup"),
            Some(after[0].1.clone())
        );
        assert!(
            database
                .translate_and_execute("UPDATE items SET code = 'b'", &SqlTranslator)
                .await
                .is_err()
        );
    });
}

#[test]
fn single_uuid_primary_key_keeps_supplied_and_generated_identities() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database.translate_and_execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); \
             INSERT INTO items VALUES (CAST('018f0f8e-7b6d-5c4a-8f12-000000000001' AS UUID), 'stable'); \
             INSERT INTO items (value) VALUES ('generated')",
            &SqlTranslator,
        ).await.expect("UUID identity regression");
        assert!(
            database.index_schema("pk_items").await.is_err(),
            "single UUID keys need no implicit index"
        );
        let rows = database
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .scan_rows(transaction, "items", None)
                        .try_collect::<Vec<_>>()
                        .await
                })
            })
            .await
            .expect("UUID row identities");
        assert_eq!(rows.len(), 2);
        for (id, row) in &rows {
            assert_eq!(row.values[0], Value::Uuid(*id));
        }
        for sql in [
            "INSERT INTO items VALUES (CAST('018f0f8e-7b6d-5c4a-8f12-000000000001' AS UUID), 'duplicate')",
            "INSERT INTO items VALUES (CAST('00000000-0000-0000-0000-000000000001' AS UUID), 'invalid')",
            "INSERT INTO items VALUES (NULL, 'null')",
        ] {
            assert!(
                database
                    .translate_and_execute(sql, &SqlTranslator)
                    .await
                    .is_err(),
                "{sql}"
            );
        }
    });
}

#[test]
fn named_inline_and_reordered_composite_primary_key_indexes_are_configurable() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database.translate_and_execute(
            "CREATE TABLE items (code TEXT CONSTRAINT item_code PRIMARY KEY); INSERT INTO items VALUES ('a'); \
             CREATE TABLE pairs (a INTEGER, b TEXT, CONSTRAINT pair_key PRIMARY KEY (b, a)); INSERT INTO pairs VALUES (1, 'b')",
            &SqlTranslator,
        ).await.expect("SQL index configuration");
        database
            .translate_and_execute(
                "INSERT INTO pairs VALUES (1, 'b') ON CONFLICT (b, a) DO NOTHING",
                &SqlTranslator,
            )
            .await
            .expect("conflict target follows configured composite order");
        assert_eq!(
            database
                .index_schema("item_code")
                .await
                .expect("inline name")
                .column_indices,
            vec![0]
        );
        assert_eq!(
            database
                .index_schema("pair_key")
                .await
                .expect("composite order")
                .column_indices,
            vec![1, 0]
        );
        assert!(
            database.index_schema("pk_pairs").await.is_err(),
            "configured composite index replaces default even in reverse order"
        );
        assert_eq!(
            database
                .index_lookup(
                    "pair_key",
                    &Row::new(vec![Value::from("b"), Value::Integer(1)])
                )
                .await
                .expect("ordered lookup"),
            Some(Row::new(vec![Value::Integer(1), Value::from("b")]))
        );
    });
}

#[test]
fn text_primary_key_supports_typed_index_configuration_and_upsert() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateTableWithIndexes {
                    schema: TableSchema {
                        name: "items".into(),
                        columns: vec![
                            ColumnSchema {
                                name: "code".into(),
                                r#type: ValueType::Text,
                                default: Value::Null,
                                primary_key: true,
                            },
                            ColumnSchema {
                                name: "value".into(),
                                r#type: ValueType::Integer,
                                default: Value::Integer(7),
                                primary_key: false,
                            },
                        ],
                    },
                    indexes: vec![IndexSchema {
                        name: "item_code".into(),
                        table_name: "items".into(),
                        column_indices: vec![0],
                        unique: true,
                    }],
                    if_not_exists: false,
                },
            )])
            .await
            .expect("typed configurable primary index");
        database
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "items".into(),
                row: Row::new(vec![Value::from("a")]),
                returning: None,
            }))])
            .await
            .expect("typed natural key uses defaults, not UUID column insertion");
        assert!(database.index_schema("pk_items").await.is_err());
        assert_eq!(
            database
                .index_schema("item_code")
                .await
                .expect("configured schema")
                .column_indices,
            vec![0]
        );
        database.translate_and_execute(
            "INSERT INTO items VALUES ('a', 8) ON CONFLICT (code) DO NOTHING; \
             INSERT INTO items VALUES ('a', 9) ON CONFLICT (code) DO UPDATE SET value = EXCLUDED.value",
            &SqlTranslator,
        ).await.expect("natural key upsert");
        assert_eq!(
            database
                .index_lookup("item_code", &Row::new(vec![Value::from("a")]))
                .await
                .expect("text lookup"),
            Some(Row::new(vec![Value::from("a"), Value::Integer(9)]))
        );
        assert!(
            database
                .translate_and_execute("DROP INDEX item_code", &SqlTranslator)
                .await
                .is_err(),
            "cannot remove primary key enforcement"
        );
    });
}

#[test]
fn composite_uuid_primary_key_uses_a_unique_index_not_either_uuid_as_identity() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database.translate_and_execute(
            "CREATE TABLE items (a UUID, b UUID, PRIMARY KEY (a, b)); \
             INSERT INTO items VALUES (CAST('00000000-0000-0000-0000-000000000001' AS UUID), CAST('00000000-0000-0000-0000-000000000002' AS UUID)), \
             (CAST('00000000-0000-0000-0000-000000000001' AS UUID), CAST('00000000-0000-0000-0000-000000000003' AS UUID))",
            &SqlTranslator,
        ).await.expect("composite UUIDs are user values, not UUIDv5/v7 row identities");
        let index = database
            .index_schema("pk_items")
            .await
            .expect("composite UUID index");
        assert!(index.unique);
        assert_eq!(index.column_indices, vec![0, 1]);
        assert!(database.translate_and_execute(
            "INSERT INTO items VALUES (CAST('00000000-0000-0000-0000-000000000001' AS UUID), CAST('00000000-0000-0000-0000-000000000002' AS UUID))", &SqlTranslator,
        ).await.is_err());
        let result = database
            .translate_and_execute("SELECT * FROM items", &SqlTranslator)
            .await
            .expect("both composite rows");
        assert_eq!(result[0].rows.len(), 2);
    });
}

#[test]
fn named_composite_primary_key_indexes_the_whole_key() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database.translate_and_execute(
            "CREATE TABLE items (tenant INTEGER, code TEXT, value TEXT, CONSTRAINT item_key PRIMARY KEY (tenant, code)); \
             INSERT INTO items VALUES (1, 'a', 'first'), (1, 'b', 'second'), (2, 'a', 'third')",
            &SqlTranslator,
        ).await.expect("named composite primary key");
        let key = Row::new(vec![Value::Integer(1), Value::from("a")]);
        assert_eq!(
            database
                .index_lookup("item_key", &key)
                .await
                .expect("composite lookup"),
            Some(Row::new(vec![
                Value::Integer(1),
                Value::from("a"),
                Value::from("first")
            ]))
        );
        for sql in [
            "INSERT INTO items VALUES (1, 'a', 'duplicate')",
            "INSERT INTO items VALUES (NULL, 'a', 'null')",
            "INSERT INTO items VALUES (1, NULL, 'null')",
            "UPDATE items SET tenant = 3 WHERE tenant = 1",
        ] {
            assert!(
                database
                    .translate_and_execute(sql, &SqlTranslator)
                    .await
                    .is_err(),
                "{sql}"
            );
        }
        database
            .translate_and_execute(
                "UPDATE items SET value = 'changed' WHERE tenant = 1 AND code = 'a'; \
             DELETE FROM items WHERE tenant = 1 AND code = 'b'",
                &SqlTranslator,
            )
            .await
            .expect("composite update/delete");
        assert_eq!(
            database
                .index_lookup("item_key", &key)
                .await
                .expect("updated composite lookup"),
            Some(Row::new(vec![
                Value::Integer(1),
                Value::from("a"),
                Value::from("changed")
            ]))
        );
        assert_eq!(
            database
                .index_lookup(
                    "item_key",
                    &Row::new(vec![Value::Integer(1), Value::from("b")])
                )
                .await
                .expect("deleted composite lookup"),
            None
        );
    });
}

#[test]
fn integer_primary_key_enforces_constraints_and_maintains_lookup() {
    block_on(async {
        let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        database.translate_and_execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES (42, 'first')",
            &SqlTranslator,
        ).await.expect("integer primary key");
        let selected = database
            .translate_and_execute("SELECT value FROM items WHERE id = 42", &SqlTranslator)
            .await
            .expect("natural-key predicate");
        assert_eq!(selected[0].rows, vec![Row::new(vec![Value::from("first")])]);
        let key = Row::new(vec![Value::Integer(42)]);
        assert_eq!(
            database
                .index_lookup("pk_items", &key)
                .await
                .expect("lookup"),
            Some(Row::new(vec![Value::Integer(42), Value::from("first")]))
        );
        for sql in [
            "INSERT INTO items VALUES (42, 'duplicate')",
            "INSERT INTO items VALUES (NULL, 'null')",
            "INSERT INTO items (value) VALUES ('missing')",
        ] {
            assert!(
                database
                    .translate_and_execute(sql, &SqlTranslator)
                    .await
                    .is_err(),
                "{sql}"
            );
        }
        database
            .translate_and_execute(
                "UPDATE items SET value = 'changed' WHERE id = 42",
                &SqlTranslator,
            )
            .await
            .expect("update");
        assert_eq!(
            database
                .index_lookup("pk_items", &key)
                .await
                .expect("updated lookup"),
            Some(Row::new(vec![Value::Integer(42), Value::from("changed")]))
        );
        database
            .translate_and_execute("DELETE FROM items WHERE id = 42", &SqlTranslator)
            .await
            .expect("delete");
        assert_eq!(
            database
                .index_lookup("pk_items", &key)
                .await
                .expect("deleted lookup"),
            None
        );
    });
}
