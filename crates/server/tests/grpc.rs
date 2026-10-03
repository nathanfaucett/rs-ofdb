use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use client::Client;
use engine::{Engine, InMemoryKernel};
use engine_automerge::AutomergeRowCodec;
use proto::{ExecuteRequest, query_service_server::QueryService as QueryServiceTrait};
use protocol::{QueryExecutor, QueryServiceError, statement_to_proto};
use query::{DataDefinition, Query, QueryResult, QuerySelect, Statement};
use schema::{ColumnSchema, TableSchema};
use server::{EngineExecutor, QueryService, Server};
use value::{Value, ValueType};

struct TestExecutor;

struct FailingExecutor(QueryServiceError);

struct SlowExecutor;

impl QueryExecutor for SlowExecutor {
    async fn execute(
        &self,
        _statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryServiceError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(vec![QueryResult::new(Vec::new())])
    }
}

impl QueryExecutor for FailingExecutor {
    async fn execute(
        &self,
        _statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryServiceError> {
        Err(self.0.clone())
    }
}

impl QueryExecutor for TestExecutor {
    async fn execute(
        &self,
        _statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryServiceError> {
        Ok(vec![QueryResult::new(Vec::new())])
    }
}

#[tokio::test]
async fn service_maps_executor_errors_to_grpc_statuses() {
    let statement = statement_to_proto(Statement::Query(Query::Select(QuerySelect::default())));
    for (error, code) in [
        (
            QueryServiceError::Invalid("invalid".into()),
            tonic::Code::InvalidArgument,
        ),
        (
            QueryServiceError::Rejected("rejected".into()),
            tonic::Code::FailedPrecondition,
        ),
        (
            QueryServiceError::Unsupported("unsupported"),
            tonic::Code::Unimplemented,
        ),
        (
            QueryServiceError::Internal("internal".into()),
            tonic::Code::Internal,
        ),
    ] {
        let service = QueryService::new(FailingExecutor(error));
        let request = tonic::Request::new(ExecuteRequest {
            statements: vec![statement.clone()],
        });
        let status = service
            .execute(request)
            .await
            .expect_err("executor error should become a gRPC status");
        assert_eq!(status.code(), code);
    }
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind temporary listener");
    listener.local_addr().expect("read temporary address")
}

#[tokio::test]
async fn tcp_client_executes_query_batch() {
    let address = free_address();
    let (shutdown, signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::tcp(address, TestExecutor)
            .serve(async {
                let _ = signal.await;
            })
            .await
            .expect("server should stop cleanly");
    });

    let client = Client::lazy_tcp(format!("http://{address}")).expect("valid client endpoint");
    let results = client
        .execute(vec![Statement::Query(query::Query::Select(
            query::QuerySelect::default(),
        ))])
        .await
        .expect("execute over gRPC");
    assert_eq!(results.len(), 1);

    shutdown.send(()).expect("server is awaiting shutdown");
    server.await.expect("server task should complete");
}

#[tokio::test]
async fn client_deadline_is_reported() {
    let address = free_address();
    let (shutdown, signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::tcp(address, SlowExecutor)
            .serve(async {
                let _ = signal.await;
            })
            .await
            .expect("server should stop cleanly");
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .expect("valid endpoint")
        .connect_lazy();
    let mut client = proto::query_service_client::QueryServiceClient::new(channel);
    let mut request = tonic::Request::new(ExecuteRequest {
        statements: vec![statement_to_proto(Statement::Query(Query::Select(
            QuerySelect::default(),
        )))],
    });
    request.set_timeout(Duration::from_millis(10));
    let status = client
        .execute(request)
        .await
        .expect_err("deadline should expire before executor completes");
    assert!(matches!(
        status.code(),
        tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
    ));

    shutdown.send(()).expect("server is awaiting shutdown");
    server.await.expect("server task should complete");
}

#[tokio::test]
async fn failed_write_batch_does_not_commit_earlier_statements() {
    let engine = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
    let address = free_address();
    let (shutdown, signal) = tokio::sync::oneshot::channel();
    let server_engine = engine.clone();
    let server = tokio::spawn(async move {
        Server::tcp(address, EngineExecutor::new(server_engine))
            .serve(async {
                let _ = signal.await;
            })
            .await
            .expect("server should stop cleanly");
    });

    let client = Client::lazy_tcp(format!("http://{address}")).expect("valid client endpoint");
    let error = client
        .execute(vec![
            Statement::DataDefinition(DataDefinition::CreateTable {
                schema: TableSchema {
                    name: "atomic_batch".into(),
                    columns: vec![ColumnSchema {
                        name: "id".into(),
                        r#type: ValueType::Uuid,
                        default: Value::Null,
                        primary_key: true,
                    }],
                },
                if_not_exists: false,
            }),
            Statement::DataDefinition(DataDefinition::DropTable {
                table_name: "missing_table".into(),
                if_exists: false,
            }),
        ])
        .await
        .expect_err("second statement should fail the batch");
    assert!(error.to_string().contains("FailedPrecondition"));
    assert!(engine.table_schema("atomic_batch").await.is_err());

    shutdown.send(()).expect("server is awaiting shutdown");
    server.await.expect("server task should complete");
}

#[cfg(unix)]
#[tokio::test]
async fn unix_socket_client_executes_batch() {
    let path = std::env::temp_dir().join(format!("ofdb-server-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let (shutdown, signal) = tokio::sync::oneshot::channel();
    let server_path = path.clone();
    let server = tokio::spawn(async move {
        Server::unix(server_path, TestExecutor)
            .serve(async {
                let _ = signal.await;
            })
            .await
            .expect("Unix server should stop cleanly");
    });
    let client = Client::lazy_unix(path.clone()).expect("Unix client endpoint");
    let results = client
        .execute(vec![Statement::Query(query::Query::Select(
            query::QuerySelect::default(),
        ))])
        .await
        .expect("execute over Unix socket");
    assert_eq!(results.len(), 1);
    shutdown.send(()).expect("server is awaiting shutdown");
    server.await.expect("server task should complete");
    std::fs::remove_file(path).expect("remove test socket");
}

#[tokio::test]
async fn empty_batch_is_rejected() {
    let address = free_address();
    let (_shutdown, signal) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = Server::tcp(address, TestExecutor)
            .serve(async {
                let _ = signal.await;
            })
            .await;
    });

    let client = Client::lazy_tcp(format!("http://{address}")).expect("valid client endpoint");
    let error = client
        .execute(Vec::new())
        .await
        .expect_err("empty batch rejected");
    assert!(error.to_string().contains("InvalidArgument"));
}
