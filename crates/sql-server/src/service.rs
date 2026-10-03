use std::sync::Arc;

use proto::{
    ExecuteRequest, ExecuteResponse, TransactionRequest, TransactionResponse,
    query_service_server::QueryService as QueryServiceTrait, transaction_request::Command,
    transaction_response::Outcome,
};
use protocol::{
    QueryExecutor, QueryServiceError, encode_error_detail, query_result_to_proto,
    statement_from_proto,
};
use query::QueryErrorKind;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct QueryService<E> {
    executor: Arc<E>,
    max_decoding_message_size: Option<usize>,
}

impl<E> Clone for QueryService<E> {
    fn clone(&self) -> Self {
        Self {
            executor: Arc::clone(&self.executor),
            max_decoding_message_size: self.max_decoding_message_size,
        }
    }
}

impl<E> QueryService<E> {
    pub fn new(executor: E) -> Self {
        Self {
            executor: Arc::new(executor),
            max_decoding_message_size: None,
        }
    }

    pub fn max_decoding_message_size(mut self, size: usize) -> Self {
        self.max_decoding_message_size = Some(size);
        self
    }

    pub fn into_tonic_service(self) -> proto::query_service_server::QueryServiceServer<Self> {
        let max_decoding_message_size = self.max_decoding_message_size;
        let service = proto::query_service_server::QueryServiceServer::new(self);
        match max_decoding_message_size {
            Some(size) => service.max_decoding_message_size(size),
            None => service,
        }
    }
}

#[tonic::async_trait]
impl<E: QueryExecutor + 'static> QueryServiceTrait for QueryService<E> {
    type TransactionStream = ReceiverStream<Result<TransactionResponse, Status>>;

    async fn transaction(
        &self,
        request: Request<tonic::Streaming<TransactionRequest>>,
    ) -> Result<Response<Self::TransactionStream>, Status> {
        let mut transaction = self
            .executor
            .transaction()
            .await
            .map_err(status_from_error)?;
        let mut requests = request.into_inner();
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut failed = false;
            if sender
                .send(Ok(TransactionResponse {
                    outcome: Some(Outcome::Ready(())),
                }))
                .await
                .is_err()
            {
                let _ = transaction.rollback().await;
                return;
            }
            loop {
                let request = tokio::select! {
                    _ = sender.closed() => break,
                    request = requests.message() => match request {
                        Ok(Some(request)) => request,
                        _ => break,
                    },
                };
                let outcome = match request.command {
                    Some(Command::Execute(request)) => {
                        let result = if failed {
                            Err(QueryServiceError::Rejected("Transaction is aborted".into()))
                        } else {
                            match decode_statements(request) {
                                Ok(statements) => tokio::select! {
                                    _ = sender.closed() => break,
                                    result = transaction.execute(statements) => result,
                                },
                                Err(error) => Err(error),
                            }
                        };
                        match result {
                            Ok(results) => Outcome::Executed(ExecuteResponse {
                                results: results.into_iter().map(query_result_to_proto).collect(),
                            }),
                            Err(error) => {
                                failed = true;
                                transaction_error(error)
                            }
                        }
                    }
                    Some(Command::Commit(())) | Some(Command::Rollback(())) => {
                        let result =
                            if matches!(request.command, Some(Command::Commit(()))) && !failed {
                                transaction.commit().await
                            } else {
                                let result = transaction.rollback().await;
                                if matches!(request.command, Some(Command::Commit(()))) {
                                    result.and(Err(QueryServiceError::Rejected(
                                        "Cannot commit an aborted transaction".into(),
                                    )))
                                } else {
                                    result
                                }
                            };
                        let outcome = match result {
                            Ok(()) => Outcome::Completed(()),
                            Err(error) => transaction_error(error),
                        };
                        let _ = sender
                            .send(Ok(TransactionResponse {
                                outcome: Some(outcome),
                            }))
                            .await;
                        return;
                    }
                    None => {
                        failed = true;
                        transaction_error(QueryServiceError::Invalid(
                            "transaction command is missing".into(),
                        ))
                    }
                };
                if sender
                    .send(Ok(TransactionResponse {
                        outcome: Some(outcome),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let _ = transaction.rollback().await;
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let statements = decode_statements(request.into_inner()).map_err(status_from_error)?;
        let results = self
            .executor
            .execute(statements)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(ExecuteResponse {
            results: results.into_iter().map(query_result_to_proto).collect(),
        }))
    }
}

fn decode_statements(request: ExecuteRequest) -> Result<Vec<query::Statement>, QueryServiceError> {
    let statements = request
        .statements
        .into_iter()
        .map(statement_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    if statements.is_empty() {
        return Err(QueryServiceError::Invalid(
            "statement batch must not be empty".into(),
        ));
    }
    Ok(statements)
}

fn transaction_error(error: QueryServiceError) -> Outcome {
    Outcome::ErrorDetail(status_from_error(error).details().to_vec())
}

fn status_from_error(error: QueryServiceError) -> Status {
    let (kind, code, message) = match error {
        QueryServiceError::Invalid(message) => (
            QueryErrorKind::Validation,
            tonic::Code::InvalidArgument,
            message,
        ),
        QueryServiceError::Rejected(message) => (
            QueryErrorKind::Rejected,
            tonic::Code::FailedPrecondition,
            message,
        ),
        QueryServiceError::Unsupported(message) => (
            QueryErrorKind::Unsupported,
            tonic::Code::Unimplemented,
            message.into(),
        ),
        QueryServiceError::Storage(message) => {
            (QueryErrorKind::Storage, tonic::Code::Internal, message)
        }
        QueryServiceError::Internal(message) => {
            (QueryErrorKind::Internal, tonic::Code::Internal, message)
        }
    };
    Status::with_details(
        code,
        message.clone(),
        tonic::codegen::Bytes::from(encode_error_detail(kind, &message)),
    )
}

#[cfg(test)]
mod tests {
    use protocol::{QueryServiceError, decode_error_detail};
    use query::QueryErrorKind;

    use super::status_from_error;

    #[test]
    fn status_preserves_query_error_category_in_details() {
        for (error, expected) in [
            (
                QueryServiceError::Invalid("bad".into()),
                QueryErrorKind::Validation,
            ),
            (
                QueryServiceError::Rejected("no".into()),
                QueryErrorKind::Rejected,
            ),
            (
                QueryServiceError::Unsupported("unsupported"),
                QueryErrorKind::Unsupported,
            ),
            (
                QueryServiceError::Storage("disk".into()),
                QueryErrorKind::Storage,
            ),
            (
                QueryServiceError::Internal("bug".into()),
                QueryErrorKind::Internal,
            ),
        ] {
            let status = status_from_error(error);
            assert_eq!(
                decode_error_detail(status.details()).map(|(kind, _)| kind),
                Some(expected)
            );
        }
    }
}
