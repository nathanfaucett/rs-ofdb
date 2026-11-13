#![cfg(feature = "postgres")]

use ofdb_sql::{
    Client, PostgresStatement, QueryErrorKind, QueryParams, Row, SqlTranslator, Translator, Value,
};

#[tokio::test]
async fn translates_owner_schema_and_bound_values() {
    let statements = SqlTranslator.translate("CREATE TABLE devices (id UUID PRIMARY KEY, data BLOB, created_at INTEGER); INSERT INTO devices (id, data, created_at) VALUES ('01890f4e-7c00-7000-8000-000000000001', NULL, 1700000000000)").await.expect("parse owner SQL");
    let ddl = PostgresStatement::translate(&statements[0]).expect("translate schema");
    assert_eq!(
        ddl.sql,
        "CREATE TABLE \"devices\" (\"id\" UUID, \"data\" BYTEA, \"created_at\" BIGINT, PRIMARY KEY (\"id\"))"
    );
    let insert = PostgresStatement::translate(&statements[1]).expect("translate insert");
    assert_eq!(
        insert.sql,
        "INSERT INTO \"devices\" (\"id\", \"data\", \"created_at\") VALUES ($1, $2, $3)"
    );
    assert_eq!(insert.params[2], Value::Integer(1700000000000));
}

#[tokio::test]
#[ignore = "requires OF_TEST_POSTGRES_URL for a disposable PostgreSQL database"]
async fn postgres_values_and_failed_transactions() {
    let url = std::env::var("OF_TEST_POSTGRES_URL").expect("set disposable PostgreSQL URL");
    let client = Client::connect_postgres(&url)
        .await
        .expect("connect PostgreSQL");
    let schema = "CREATE TABLE postgres_adapter_values (id UUID PRIMARY KEY, flag BOOLEAN, number INTEGER, fraction FLOAT, bytes BLOB, data JSON, optional TEXT)";
    let id = ofdb_sql::Uuid::parse_str("01890f4e-7c00-7000-8000-000000000001").expect("test UUID");
    let expected = vec![
        Value::Uuid(id),
        Value::Bool(true),
        Value::Integer(i64::MAX),
        Value::Float(1.25),
        Value::Blob(vec![0, 255]),
        Value::Json(serde_json::json!({"owner": "O'Brien", "count": 2}).into()),
        Value::Null,
    ];
    let params = QueryParams::Positional(expected.clone());
    let mut transaction = client.transaction().await.expect("begin value transaction");
    transaction
        .execute_sql(schema, None)
        .await
        .expect("create typed table");
    let result = transaction
        .execute_sql(
            "INSERT INTO postgres_adapter_values VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *",
            Some(&params),
        )
        .await
        .expect("bind and return all supported value types");
    assert_eq!(result[0].rows, vec![Row::new(expected)]);
    let sum = transaction
        .execute(vec![ofdb_sql::Statement::Query(ofdb_sql::Query::Select(
            ofdb_sql::QuerySelect {
                from: ofdb_sql::QueryFrom {
                    table: "postgres_adapter_values".into(),
                    joins: Vec::new(),
                },
                aggregates: vec![query::QueryAggregate::Sum(ofdb_sql::QueryColumn {
                    table: String::new(),
                    column: "fraction".into(),
                })],
                ..Default::default()
            },
        ))])
        .await
        .expect("sum floating-point values through the query interface");
    assert_eq!(sum[0].rows, vec![Row::new(vec![Value::Float(1.25)])]);
    let error = transaction
        .execute_sql(
            "INSERT INTO postgres_adapter_values VALUES ($1, $2, $3, $4, $5, $6, $7)",
            Some(&params),
        )
        .await
        .expect_err("duplicate key aborts transaction");
    assert_eq!(error.kind, QueryErrorKind::Rejected);
    assert_eq!(
        transaction
            .commit()
            .await
            .expect_err("failed transaction cannot commit")
            .kind,
        QueryErrorKind::Rejected
    );
    assert!(
        client
            .execute_sql("SELECT * FROM postgres_adapter_values", None)
            .await
            .is_err(),
        "DDL and writes must roll back together"
    );

    let mut transaction = client
        .transaction()
        .await
        .expect("begin translation failure");
    transaction
        .execute_sql(schema, None)
        .await
        .expect("create table before translation failure");
    assert!(transaction.execute_sql("not SQL", None).await.is_err());
    assert!(
        transaction.commit().await.is_err(),
        "translation failure cannot commit prior writes"
    );
    assert!(
        client
            .execute_sql("SELECT * FROM postgres_adapter_values", None)
            .await
            .is_err()
    );

    let mut transaction = client
        .transaction()
        .await
        .expect("begin dropped transaction");
    transaction
        .execute_sql(schema, None)
        .await
        .expect("create uncommitted table");
    drop(transaction);
    assert!(
        client
            .execute_sql("SELECT * FROM postgres_adapter_values", None)
            .await
            .is_err(),
        "drop must roll back"
    );
}
