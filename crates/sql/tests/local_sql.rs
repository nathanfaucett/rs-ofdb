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
fn indexed_native_crud_on_bounded_stack() {
    std::thread::Builder::new()
        .name("indexed-native-crud".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            futures::executor::block_on(async {
                use ofdb_sql::{Database, Row, SqlTranslator};

                let root = tempfile::tempdir().expect("isolated native store");
                let path = root.path().join("indexed.redb");
                let database = Database::open(&path).expect("open native store");
                let client = database.client();
                for statement in [
                    "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT UNIQUE)",
                    "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
                    "CREATE INDEX people_name ON people (name)",
                    "UPDATE people SET name = 'Lin' WHERE name = 'Ada'",
                ] {
                    client.translate_and_execute(statement, &SqlTranslator).await.expect("indexed DDL and write");
                }
                let mut transaction = client.transaction().await.expect("begin indexed transaction");
                transaction.translate_and_execute("DELETE FROM people WHERE name = 'Lin'", &SqlTranslator).await.expect("delete in transaction");
                transaction.rollback().await.expect("rollback indexed deletion");
                drop(client);
                drop(database);

                let reopened = Database::open(&path).expect("reopen native store");
                let client = reopened.client();
                let rows = client.translate_and_execute("SELECT name FROM people WHERE name = 'Lin'", &SqlTranslator).await.expect("read retained index");
                assert_eq!(rows[0].rows, vec![Row::from(["Lin"])]);
                client.translate_and_execute("DELETE FROM people WHERE name = 'Lin'", &SqlTranslator).await.expect("committed indexed deletion");
                let rows = client.translate_and_execute("SELECT name FROM people", &SqlTranslator).await.expect("read after deletion");
                assert!(rows[0].rows.is_empty());
            });
        })
        .expect("spawn bounded-stack worker")
        .join()
        .expect("indexed CRUD must not overflow its stack");
}

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
fn logical_rows_reject_unsupported_uuid_primary_keys() {
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
                .translate_and_execute("CREATE TABLE tables (id UUID PRIMARY KEY)", &SqlTranslator,)
                .await
                .is_err()
        );
        client
            .translate_and_execute("CREATE TABLE users (id UUID PRIMARY KEY)", &SqlTranslator)
            .await
            .expect("create a user table");
        assert!(
            client
                .translate_and_execute("CREATE INDEX tables ON users (id)", &SqlTranslator,)
                .await
                .is_err()
        );
    });
}

#[test]
fn human_readable_catalogs_are_queryable() {
    ofdb_test::run(async {
        use ofdb_sql::{Database, Row, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        client.translate_and_execute(
            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT); CREATE INDEX people_name ON people (name)",
            &SqlTranslator,
        ).await.expect("create named schema");
        for (query, expected) in [
            ("SELECT name FROM tables", vec![Row::from(["people"])]),
            (
                "SELECT name FROM table_fields ORDER BY position",
                vec![Row::from(["id"]), Row::from(["name"])],
            ),
            ("SELECT name FROM indices", vec![Row::from(["people_name"])]),
            ("SELECT column FROM index_fields", vec![Row::from(["name"])]),
        ] {
            let result = client
                .translate_and_execute(query, &SqlTranslator)
                .await
                .expect("query human-readable catalog");
            assert_eq!(result[0].rows, expected, "{query}");
        }
    });
}

#[test]
fn table_and_index_names_share_a_protected_storage_namespace() {
    ofdb_test::run(async {
        use ofdb_sql::{Database, Row, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        client.translate_and_execute(
            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT); CREATE TABLE occupied (id UUID PRIMARY KEY); CREATE UNIQUE INDEX people_name ON people (name)",
            &SqlTranslator,
        ).await.expect("create tables and index");
        for statement in [
            "CREATE INDEX occupied ON people (name)",
            "CREATE INDEX IF NOT EXISTS occupied ON people (name)",
            "CREATE TABLE people_name (id UUID PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS people_name (id UUID PRIMARY KEY)",
        ] {
            assert!(
                client
                    .translate_and_execute(statement, &SqlTranslator)
                    .await
                    .is_err(),
                "{statement} must reject shared storage"
            );
        }
        for name in ["tables", "table_fields", "indices", "index_fields"] {
            for statement in [
                format!("CREATE TABLE {name} (id UUID PRIMARY KEY)"),
                format!("CREATE INDEX {name} ON people (name)"),
            ] {
                assert!(
                    client
                        .translate_and_execute(&statement, &SqlTranslator)
                        .await
                        .is_err(),
                    "{statement} must reserve catalogs"
                );
            }
        }
        client.translate_and_execute(
            "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            &SqlTranslator,
        ).await.expect("index survives rejected DDL");
        let rows = client
            .translate_and_execute("SELECT name FROM people", &SqlTranslator)
            .await
            .expect("rows survive rejected DDL");
        assert_eq!(rows[0].rows, vec![Row::from(["Ada"])]);
    });
}

#[test]
fn dropping_an_old_index_owner_preserves_a_recreated_named_index() {
    ofdb_test::run(async {
        use ofdb_sql::{Database, Row, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        for statement in [
            "CREATE TABLE old_owner (id UUID PRIMARY KEY, name TEXT)",
            "CREATE TABLE new_owner (id UUID PRIMARY KEY, name TEXT)",
            "CREATE INDEX shared_name ON old_owner (name)",
            "DROP INDEX shared_name",
            "CREATE UNIQUE INDEX shared_name ON new_owner (name)",
            "INSERT INTO new_owner VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "DROP TABLE old_owner",
        ] {
            client
                .translate_and_execute(statement, &SqlTranslator)
                .await
                .expect("recreate index on another table and drop old owner");
        }
        assert!(client.translate_and_execute(
            "INSERT INTO new_owner VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Ada')",
            &SqlTranslator,
        ).await.is_err(), "old definition cleanup must not clear the active unique index");
        let rows = client
            .translate_and_execute("SELECT name FROM new_owner", &SqlTranslator)
            .await
            .expect("retained rows");
        assert_eq!(rows[0].rows, vec![Row::from(["Ada"])]);
        client
            .translate_and_execute("DROP INDEX shared_name", &SqlTranslator)
            .await
            .expect("drop current index");
        assert!(
            client
                .translate_and_execute(
                    "CREATE TABLE shared_name (id UUID PRIMARY KEY)",
                    &SqlTranslator
                )
                .await
                .is_err(),
            "historical index claims cannot become user row storage"
        );
        assert!(
            client
                .translate_and_execute("CREATE INDEX old_owner ON new_owner (name)", &SqlTranslator)
                .await
                .is_err(),
            "historical table claims cannot become index storage"
        );
    });
}

#[test]
fn former_engine_catalog_names_are_available_for_user_tables() {
    ofdb_test::run(async {
        use ofdb_sql::{Database, Row, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        for name in [
            "__engine_index_user",
            "__engine_tables",
            "__engine_table_fields",
            "__engine_indices",
            "__engine_index_fields",
        ] {
            for statement in [
                format!("CREATE TABLE {name} (id UUID PRIMARY KEY, name TEXT)"),
                format!(
                    "INSERT INTO {name} VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')"
                ),
            ] {
                client
                    .translate_and_execute(&statement, &SqlTranslator)
                    .await
                    .expect("former engine names are ordinary user tables");
            }
            let rows = client
                .translate_and_execute(&format!("SELECT name FROM {name}"), &SqlTranslator)
                .await
                .expect("read user table");
            assert_eq!(rows[0].rows, vec![Row::from(["Ada"])]);
            client
                .translate_and_execute(&format!("DROP TABLE {name}"), &SqlTranslator)
                .await
                .expect("drop user table");
        }
    });
}

#[test]
fn former_engine_catalog_names_are_available_for_user_indices() {
    ofdb_test::run(async {
        use ofdb_sql::{Database, Row, SqlTranslator};

        for name in [
            "__engine_index_user",
            "__engine_tables",
            "__engine_table_fields",
            "__engine_indices",
            "__engine_index_fields",
        ] {
            let database = Database::in_memory();
            let client = database.client();
            client
                .translate_and_execute(
                    "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT)",
                    &SqlTranslator,
                )
                .await
                .expect("create index owner");
            client
                .translate_and_execute(
                    &format!("CREATE UNIQUE INDEX {name} ON people (name)"),
                    &SqlTranslator,
                )
                .await
                .expect("former engine names are ordinary user indices");
            client.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", &SqlTranslator).await.expect("insert indexed row");
            assert!(client.translate_and_execute("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Ada')", &SqlTranslator).await.is_err(), "named unique index must reject duplicate values");
            let rows = client
                .translate_and_execute(
                    &format!("SELECT name FROM indices WHERE name = '{name}'"),
                    &SqlTranslator,
                )
                .await
                .expect("query named index definition");
            assert_eq!(rows[0].rows, vec![Row::from([name])]);
            client
                .translate_and_execute(&format!("DROP INDEX {name}"), &SqlTranslator)
                .await
                .expect("drop named index");
        }
    });
}

#[test]
fn catalogs_reject_user_mutations() {
    futures::executor::block_on(async {
        use ofdb_sql::{Database, SqlTranslator};

        let database = Database::in_memory();
        let client = database.client();
        client.translate_and_execute(
            "CREATE TABLE protected (id UUID PRIMARY KEY, tenant TEXT, code INTEGER); CREATE UNIQUE INDEX protected_key ON protected (tenant, code); INSERT INTO protected VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', 7)",
            &SqlTranslator,
        ).await.expect("create indexed row through internal catalog writes");
        for (table, column) in [
            ("tables", "name"),
            ("table_fields", "name"),
            ("indices", "name"),
            ("index_fields", "column"),
        ] {
            let select = format!("SELECT * FROM {table}");
            let before = client
                .translate_and_execute(&select, &SqlTranslator)
                .await
                .expect("read catalog")[0]
                .rows
                .clone();
            assert!(!before.is_empty(), "fixture populates {table}");
            for statement in [
                format!("DELETE FROM {table}"),
                format!("UPDATE {table} SET {column} = 'corrupt'"),
                format!("INSERT INTO {table} ({column}) VALUES ('corrupt')"),
            ] {
                let error = client
                    .translate_and_execute(&statement, &SqlTranslator)
                    .await
                    .expect_err("catalog mutation must be rejected");
                assert!(
                    error.to_string().contains("Catalog tables are read-only"),
                    "{statement}: {error}"
                );
            }
            let error = client
                .execute(vec![ofdb_sql::Statement::Query(ofdb_sql::Query::Insert(
                    ofdb_sql::QueryInsert {
                        table: table.into(),
                        row: ofdb_sql::Row::from(["corrupt"]),
                        returning: None,
                    },
                ))])
                .await
                .expect_err("typed Insert must also be rejected");
            assert!(
                error.to_string().contains("Catalog tables are read-only"),
                "{error}"
            );
            let after = client
                .translate_and_execute(&select, &SqlTranslator)
                .await
                .expect("catalog remains readable");
            assert_eq!(after[0].rows, before, "{table} remains intact");
        }
        let rows = client
            .translate_and_execute(
                "SELECT tenant, code FROM protected WHERE tenant = 'Ada' AND code = 7",
                &SqlTranslator,
            )
            .await
            .expect("indexed row remains visible");
        assert_eq!(
            rows[0].rows,
            vec![ofdb_sql::Row::new(vec![
                ofdb_sql::Value::from("Ada"),
                ofdb_sql::Value::Integer(7)
            ])]
        );
        assert!(client.translate_and_execute("INSERT INTO protected VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Ada', 7)", &SqlTranslator).await.is_err(), "composite uniqueness remains enforced");
    });
}

#[test]
fn heterogeneous_catalog_defaults_project_and_concat_safely() {
    futures::executor::block_on(async {
        use ofdb_sql::{Database, ErrorKind, Row, SqlTranslator, Value};

        let database = Database::in_memory();
        let client = database.client();
        client.translate_and_execute(
            "CREATE TABLE defaults (id UUID PRIMARY KEY, amount INTEGER DEFAULT 7, label TEXT DEFAULT 'hello')",
            &SqlTranslator,
        ).await.expect("create heterogeneous defaults");
        let rows = client
            .translate_and_execute(
                "SELECT default FROM table_fields WHERE table = 'defaults' ORDER BY position",
                &SqlTranslator,
            )
            .await
            .expect("project original default values");
        assert_eq!(
            rows[0].rows,
            vec![
                Row::new(vec![Value::Null]),
                Row::new(vec![Value::Integer(7)]),
                Row::from(["hello"])
            ]
        );
        let error = client.translate_and_execute(
            "SELECT default || '!' AS decorated FROM table_fields WHERE table = 'defaults' AND name = 'amount'",
            &SqlTranslator,
        ).await.expect_err("integer concat returns a query error, not a panic");
        assert_eq!(error.kind, ErrorKind::Rejected);
        assert!(
            error
                .to_string()
                .contains("Text concatenation requires text operands"),
            "{error}"
        );
        let rows = client.translate_and_execute(
            "SELECT default || '!' AS decorated FROM table_fields WHERE table = 'defaults' AND name = 'label'",
            &SqlTranslator,
        ).await.expect("text default concatenates");
        assert_eq!(rows[0].rows, vec![Row::from(["hello!"])]);
        let rows = client.translate_and_execute(
            "SELECT default || '!' AS decorated FROM table_fields WHERE table = 'defaults' AND name = 'id'",
            &SqlTranslator,
        ).await.expect("null default propagates");
        assert_eq!(rows[0].rows, vec![Row::new(vec![Value::Null])]);
    });
}
