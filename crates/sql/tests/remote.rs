use std::net::{SocketAddr, TcpListener};

use ofdb_sql::{
    Client, Database, Query, QueryColumn, QueryErrorKind, QueryFrom, QuerySelect, Statement,
};

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind temporary listener");
    listener.local_addr().expect("read temporary address")
}

#[tokio::test]
async fn remote_client_executes_typed_queries_over_tcp() {
    let address = free_address();
    let database = Database::in_memory();
    database
        .execute_sql("CREATE TABLE items (id UUID PRIMARY KEY)", None)
        .await
        .expect("create table in embedded database");

    let hosted_database = database.clone();
    let (shutdown, signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        hosted_database
            .serve_tcp(address, async {
                let _ = signal.await;
            })
            .await
            .expect("server should stop cleanly");
    });

    loop {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let client = Client::connect(format!("http://{address}"))
        .await
        .expect("connect eagerly to running server");
    let results = client
        .execute(vec![Statement::Query(Query::Select(QuerySelect {
            from: QueryFrom {
                table: "items".into(),
                joins: Vec::new(),
            },
            projection: vec![QueryColumn {
                table: "items".into(),
                column: "id".into(),
            }],
            ..QuerySelect::default()
        }))])
        .await
        .expect("execute query remotely");
    assert_eq!(results.len(), 1);
    assert!(results[0].rows.is_empty());

    let empty_error = client
        .execute(Vec::new())
        .await
        .expect_err("reject empty remote batches");
    assert_eq!(empty_error.kind, QueryErrorKind::Validation);

    client
        .execute_sql("CREATE TABLE atomic_items (id UUID PRIMARY KEY)", None)
        .await
        .expect("create batch rollback table");
    let duplicate_batch = "INSERT INTO atomic_items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)); INSERT INTO atomic_items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))";
    let batch_error = client
        .execute_sql(duplicate_batch, None)
        .await
        .expect_err("reject duplicate row in batch");
    let local_error = database
        .execute_sql(duplicate_batch, None)
        .await
        .expect_err("reject duplicate local batch");
    assert_eq!(batch_error.kind, QueryErrorKind::Rejected);
    assert_eq!(batch_error.kind, local_error.kind);
    let rows = client
        .execute_sql("SELECT id FROM atomic_items", None)
        .await
        .expect("read after failed batch");
    assert!(rows[0].rows.is_empty());
    assert!(client.execute_sql("BEGIN", None).await.is_err());
    assert!(client.execute_sql("COMMIT", None).await.is_err());
    assert!(client.execute_sql("ROLLBACK", None).await.is_err());

    shutdown.send(()).expect("server is awaiting shutdown");
    server.await.expect("server task should complete");
}
