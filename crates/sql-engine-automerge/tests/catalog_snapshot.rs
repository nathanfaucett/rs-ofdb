use automerge::{AutoCommit, ROOT, transaction::Transactable};
use btree::BTreeTransaction;
use btree_automerge::{AutomergeBTreeTransaction, AutomergeChangeStore, ThresholdPolicy};
use engine::{InMemoryKernel, Kernel, KernelTransaction, RowCodec};
use engine_automerge::AutomergeRowCodec;
use futures::{StreamExt, executor::block_on, pin_mut};
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
            .row_conflicts("items", &Row::new(vec![Value::Bool(true)]))
            .await
            .expect_err("catalog validation precedes invalid key");
        assert!(
            matches!(error, engine::EngineError::Custom(message) if message == "Invalid table name")
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
        let error = database.translate_and_execute("SELECT value FROM items WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID)", &sql_translator::SqlTranslator).await.expect_err("SELECT must decode every live row before filtering");
        assert!(
            matches!(error, engine::EngineError::Custom(message) if message == "Logical row column is not encoded bytes")
        );
    });
}
