mod common;
#[path = "common/sql.rs"]
mod sql;

use common::{case_concurrent_user_inserts, standard_suite};
use ofdb_test::{SingleNodeRunner, test_case, test_suite};
use sql::{sql_limits_suite, sql_suite};

test_suite!(
    standard_local,
    standard_suite(),
    SingleNodeRunner::default()
);
test_suite!(sql_surface_local, sql_suite(), SingleNodeRunner::default());
test_suite!(
    sql_limits_local,
    sql_limits_suite(),
    SingleNodeRunner::default()
);
test_case!(
    concurrent_inserts_local,
    case_concurrent_user_inserts(),
    SingleNodeRunner::default()
);

#[test]
fn explicit_transactions_span_api_calls() {
    futures::executor::block_on(async {
        use ofdb_sql::{Database, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        client
            .translate_and_execute(
                "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                &SqlTranslator,
            )
            .await
            .expect("create table");

        let mut transaction = client.transaction().await.expect("begin transaction");
        transaction
            .translate_and_execute(
                "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
                &SqlTranslator,
            )
            .await
            .expect("insert inside transaction");
        let rows = transaction
            .translate_and_execute("SELECT name FROM people", &SqlTranslator)
            .await
            .expect("read own write");
        assert_eq!(rows[0].rows, vec![ofdb_sql::Row::from(["Ada"])]);
        transaction.rollback().await.expect("rollback transaction");
        let rows = client
            .translate_and_execute("SELECT name FROM people", &SqlTranslator)
            .await
            .expect("read after rollback");
        assert!(rows[0].rows.is_empty());

        let mut transaction = client.transaction().await.expect("begin transaction");
        transaction
            .translate_and_execute(
                "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
                &SqlTranslator,
            )
            .await
            .expect("insert before commit");
        transaction.commit().await.expect("commit transaction");
        let rows = client
            .translate_and_execute("SELECT name FROM people", &SqlTranslator)
            .await
            .expect("read after commit");
        assert_eq!(rows[0].rows, vec![ofdb_sql::Row::from(["Lin"])]);

        let mut transaction = client.transaction().await.expect("begin transaction");
        transaction
            .translate_and_execute(
                "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Grace')",
                &SqlTranslator,
            )
            .await
            .expect("insert before failed statement");
        assert!(transaction
            .translate_and_execute(
                "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Duplicate')",
                &SqlTranslator,
            )
            .await
            .is_err());
        assert!(transaction.commit().await.is_err());
        let rows = client
            .translate_and_execute("SELECT name FROM people ORDER BY name", &SqlTranslator)
            .await
            .expect("read after aborted transaction");
        assert_eq!(rows[0].rows, vec![ofdb_sql::Row::from(["Lin"])]);
    });
}

#[test]
fn bound_parameters_via_public_api() {
    futures::executor::block_on(async {
        use std::collections::BTreeMap;

        use ofdb_sql::{Database, QueryParams, SqlTranslator, Value};

        let database = Database::in_memory();
        let client = database.client();
        client
            .translate_and_execute(
                "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                &SqlTranslator,
            )
            .await
            .expect("create table");

        let person_id = ofdb_sql::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc")
            .expect("valid test UUID");
        let insert = QueryParams::Positional(vec![Value::Uuid(person_id), Value::from("Ada")]);
        client
            .translate_and_execute_with_params(
                "INSERT INTO people VALUES ($1, $2)",
                Some(&insert),
                &SqlTranslator,
            )
            .await
            .expect("insert bound values");

        let named = QueryParams::Named(BTreeMap::from([
            ("name".to_string(), Value::from("Ada")),
            ("id".to_string(), Value::Uuid(person_id)),
        ]));
        let rows = client
            .translate_and_execute_with_params(
                "SELECT name FROM people WHERE id = :id AND name = :name",
                Some(&named),
                &SqlTranslator,
            )
            .await
            .expect("select with named parameters");
        assert_eq!(rows[0].rows, vec![ofdb_sql::Row::from(["Ada"])]);

        let update = QueryParams::Positional(vec![Value::from("Lin"), Value::Uuid(person_id)]);
        client
            .translate_and_execute_with_params(
                "UPDATE people SET name = ? WHERE id = ?",
                Some(&update),
                &SqlTranslator,
            )
            .await
            .expect("update with positional parameters");

        let delete = QueryParams::Named(BTreeMap::from([("name".to_string(), Value::from("Lin"))]));
        client
            .translate_and_execute_with_params(
                "DELETE FROM people WHERE name = :name",
                Some(&delete),
                &SqlTranslator,
            )
            .await
            .expect("delete with named parameters");

        let rows = client
            .translate_and_execute("SELECT * FROM people", &SqlTranslator)
            .await
            .expect("query remaining rows");
        assert!(rows[0].rows.is_empty());

        assert!(
            client
                .translate_and_execute_with_params(
                    "SELECT id FROM people WHERE id = $1",
                    None,
                    &SqlTranslator,
                )
                .await
                .is_err()
        );
        assert!(
            client
                .translate_and_execute_with_params(
                    "SELECT id FROM people WHERE id = $1",
                    Some(&QueryParams::Positional(vec![
                        Value::Uuid(person_id),
                        Value::Uuid(person_id),
                    ])),
                    &SqlTranslator,
                )
                .await
                .is_err()
        );
        assert!(
            client
                .translate_and_execute_with_params(
                    "SELECT id FROM people WHERE id = $1 AND name = :name",
                    Some(&insert),
                    &SqlTranslator,
                )
                .await
                .is_err()
        );
        assert!(
            client
                .translate_and_execute_with_params(
                    "INSERT INTO people (id, name) VALUES ($1, $2)",
                    Some(&QueryParams::Positional(vec![
                        Value::from("not a UUID"),
                        Value::from("Wrong type"),
                    ])),
                    &SqlTranslator,
                )
                .await
                .is_err()
        );
    });
}

#[test]
fn internal_catalog_tables_are_not_user_ddl_targets() {
    futures::executor::block_on(async {
        use ofdb_sql::{Database, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        assert!(
            client
                .translate_and_execute(
                    "CREATE TABLE __engine_tables (id UUID PRIMARY KEY)",
                    &SqlTranslator,
                )
                .await
                .is_err()
        );
        client
            .translate_and_execute("CREATE TABLE users (id UUID PRIMARY KEY)", &SqlTranslator)
            .await
            .expect("create a user table");
        assert!(client
            .translate_and_execute(
                "CREATE INDEX __engine_tables ON users (id)",
                &SqlTranslator,
            )
            .await
            .is_err());
    });
}
