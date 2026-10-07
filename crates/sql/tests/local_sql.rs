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
fn logical_rows_require_uuidv7_primary_keys() {
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

        let error = client
            .translate_and_execute(
                "INSERT INTO people VALUES (CAST('11111111-1111-4111-8111-111111111111' AS UUID), 'Ada')",
                &SqlTranslator,
            )
            .await
            .expect_err("UUIDv4 primary keys are not valid Logical Row IDs");
        assert!(error.to_string().contains("UUIDv7"));
    });
}

#[test]
fn dropping_and_recreating_table_starts_empty() {
    futures::executor::block_on(async {
        use ofdb_sql::{Database, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        client
            .translate_and_execute(
                "CREATE TABLE people (id UUID PRIMARY KEY, city TEXT)",
                &SqlTranslator,
            )
            .await
            .expect("create table");
        client
            .translate_and_execute("CREATE INDEX people_city ON people (city)", &SqlTranslator)
            .await
            .expect("create index");
        client
            .translate_and_execute(
                "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'London')",
                &SqlTranslator,
            )
            .await
            .expect("insert row");

        client
            .translate_and_execute("DROP TABLE people", &SqlTranslator)
            .await
            .expect("drop table");
        client
            .translate_and_execute(
                "CREATE TABLE people (id UUID PRIMARY KEY, city TEXT)",
                &SqlTranslator,
            )
            .await
            .expect("recreate table");
        let rows = client
            .translate_and_execute("SELECT * FROM people", &SqlTranslator)
            .await
            .expect("read recreated table");
        assert!(rows[0].rows.is_empty());

        client
            .translate_and_execute("CREATE INDEX people_city ON people (city)", &SqlTranslator)
            .await
            .expect("recreate index");
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
fn schema_relationships_and_indexes_are_table_scoped() {
    futures::executor::block_on(async {
        use ofdb_sql::{Database, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        for statement in [
            "CREATE TABLE first_table (id UUID PRIMARY KEY, value TEXT)",
            "CREATE TABLE second_table (id UUID PRIMARY KEY, value TEXT)",
            "ALTER TABLE first_table ADD COLUMN note TEXT DEFAULT 'first'",
            "CREATE INDEX first_value ON first_table (value)",
            "INSERT INTO first_table (id, value) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'shared')",
            "INSERT INTO second_table (id, value) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'shared')",
        ] {
            client
                .translate_and_execute(statement, &SqlTranslator)
                .await
                .expect("execute schema or row statement");
        }

        let first = client
            .translate_and_execute("SELECT value, note FROM first_table", &SqlTranslator)
            .await
            .expect("query first table");
        assert_eq!(
            first[0].rows,
            vec![ofdb_sql::Row::from(["shared", "first"])]
        );

        let second = client
            .translate_and_execute("SELECT value FROM second_table", &SqlTranslator)
            .await
            .expect("query second table");
        assert_eq!(second[0].rows, vec![ofdb_sql::Row::from(["shared"])]);

        let indexed = client
            .translate_and_execute(
                "SELECT id FROM first_table WHERE value = 'shared'",
                &SqlTranslator,
            )
            .await
            .expect("query indexed value");
        assert_eq!(indexed[0].rows.len(), 1);
    });
}

#[cfg(feature = "redb")]
#[test]
fn schema_relationships_and_index_survive_redb_reopen() {
    futures::executor::block_on(async {
        use ofdb_sql::{Database, SqlTranslator};

        let directory = tempfile::tempdir().expect("create temporary directory");
        let path = directory.path().join("schema.redb");
        {
            let database = Database::open(&path).expect("open database");
            let client = database.client();
            for statement in [
                "CREATE TABLE first_table (id UUID PRIMARY KEY, value TEXT)",
                "CREATE TABLE second_table (id UUID PRIMARY KEY, value TEXT)",
                "ALTER TABLE first_table ADD COLUMN note TEXT DEFAULT 'first'",
                "CREATE INDEX first_value ON first_table (value)",
                "INSERT INTO first_table (id, value) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'shared')",
                "INSERT INTO second_table (id, value) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'shared')",
            ] {
                client
                    .translate_and_execute(statement, &SqlTranslator)
                    .await
                    .expect("execute schema or row statement");
            }
        }

        let database = Database::open(&path).expect("reopen database");
        let client = database.client();
        let rows = client
            .translate_and_execute(
                "SELECT id, note FROM first_table WHERE value = 'shared'",
                &SqlTranslator,
            )
            .await
            .expect("query persisted indexed row");
        assert_eq!(rows[0].rows.len(), 1);
        assert_eq!(rows[0].rows[0].values[1].as_text(), Some("first"));
        let other = client
            .translate_and_execute("SELECT value FROM second_table", &SqlTranslator)
            .await
            .expect("query other table");
        assert_eq!(other[0].rows, vec![ofdb_sql::Row::from(["shared"])]);
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
