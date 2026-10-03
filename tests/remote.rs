use std::net::{SocketAddr, TcpListener};

use engine::{Engine, InMemoryKernel};
use engine_automerge::AutomergeRowCodec;
use ofdb::{
    Database, Query, QueryColumn, QueryFrom, QuerySelect, Statement, Uuid, Value, ValueType,
};
use schema::{ColumnSchema, TableSchema};
use server::{EngineExecutor, Server};

#[test]
fn unix_uri_constructs_a_lazy_remote_database() {
    let database = Database::open_uri("ofdb+unix:///tmp/ofdb-not-connected.sock")
        .expect("Unix URI should produce a lazy client");
    assert!(matches!(database, Database::Remote(_)));
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind temporary listener");
    listener.local_addr().expect("read temporary address")
}

#[tokio::test]
async fn remote_database_opens_and_executes_over_tcp() {
    let address = free_address();
    let engine = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
    let (shutdown, signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::tcp(address, EngineExecutor::new(engine))
            .serve(async {
                let _ = signal.await;
            })
            .await
            .expect("server should stop cleanly");
    });

    let database = Database::open_uri(&format!("ofdb+grpc://{address}"))
        .expect("remote URI should produce a lazy client");
    assert!(database.index_schema("missing").await.is_err());
    database
        .create_table(TableSchema {
            name: "items".into(),
            columns: vec![ColumnSchema {
                name: "id".into(),
                r#type: ValueType::Uuid,
                default: Value::Uuid(Uuid::nil()),
                primary_key: true,
            }],
        })
        .await
        .expect("create table remotely");

    let results = database
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

    shutdown.send(()).expect("server is awaiting shutdown");
    server.await.expect("server task should complete");
}
