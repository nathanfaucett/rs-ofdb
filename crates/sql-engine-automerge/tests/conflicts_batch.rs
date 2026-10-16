use engine::{Engine, EngineError, InMemoryKernel, Kernel, KernelTransaction, RowCodec};
use engine_automerge::AutomergeRowCodec;
use futures::executor::block_on;
use uuid::Uuid;
use value::{Row, Value};

fn id(value: u64) -> Uuid {
    Uuid::parse_str(&format!("018f0f8e-7b6d-7c4a-8f12-{value:012x}"))
        .expect("fixed UUIDv7 identity")
}

#[test]
fn row_conflicts_batch_preserves_current_malformed_catalog_and_key_error_order() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let database = Engine::new(kernel.clone(), AutomergeRowCodec::new());
        database
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT)",
                &sql_translator::SqlTranslator,
            )
            .await
            .expect("valid catalog");
        let valid = Row::new(vec![Value::Uuid(id(1))]);
        let invalid = Row::new(vec![Value::Bool(true)]);
        let other_invalid = Row::new(vec![]);
        assert_eq!(
            database
                .row_conflicts_batch("items", &[valid.clone(), valid.clone()])
                .await
                .expect("valid earlier snapshot"),
            vec![Vec::<String>::new(), vec![]]
        );
        let invalid_message = database
            .row_conflicts("items", &invalid)
            .await
            .expect_err("invalid single key")
            .to_string();
        let codec = AutomergeRowCodec::new();
        let mut transaction = kernel.transaction().await.expect("malformed field fixture");
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
        let single = database
            .row_conflicts("items", &valid)
            .await
            .expect_err("single inspection validates unrelated fields");
        assert!(
            matches!(&single, EngineError::Custom(message) if message == "Invalid column table identity")
        );
        for keys in [vec![valid.clone()], vec![valid.clone(), invalid.clone()]] {
            let batch = database
                .row_conflicts_batch("items", &keys)
                .await
                .expect_err("first row's field error precedes later invalid key");
            assert_eq!(batch.to_string(), single.to_string());
        }
        let batch = database
            .row_conflicts_batch("items", &[invalid.clone(), valid.clone(), other_invalid])
            .await
            .expect_err("first invalid key precedes field validation and later keys");
        assert_eq!(batch.to_string(), invalid_message);

        let mut transaction = kernel.transaction().await.expect("malformed table fixture");
        codec
            .put_row(
                &mut transaction,
                engine::ENGINE_TABLES_STORAGE,
                id(201),
                Row::new(vec![Value::Integer(42)]),
            )
            .await
            .expect("malformed unrelated table");
        transaction.commit().await.expect("commit malformed table");
        let batch = database
            .row_conflicts_batch("items", &[invalid, valid])
            .await
            .expect_err("table validation precedes key and field validation");
        assert!(matches!(batch, EngineError::Custom(message) if message == "Invalid table name"));
    });
}
