use proto::{ExecuteRequest, query_service_client::QueryServiceClient};
use protocol::{decode_error_detail, query_result_from_proto, statement_to_proto};
use query::{QueryError, QueryErrorKind, QueryResult, Statement};
use tonic::transport::{Channel, Endpoint};

#[derive(Clone, Debug)]
pub struct Client {
    channel: Channel,
}

impl Client {
    pub async fn tcp(uri: impl AsRef<str>) -> Result<Self, QueryError> {
        let endpoint = Endpoint::from_shared(uri.as_ref().to_string()).map_err(|error| {
            QueryError::new(
                QueryErrorKind::Validation,
                format!("invalid gRPC endpoint: {error}"),
            )
        })?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| QueryError::new(QueryErrorKind::Transport, error.to_string()))?;
        Ok(Self { channel })
    }

    #[cfg(all(unix, feature = "unix"))]
    pub async fn unix(path: impl Into<std::path::PathBuf>) -> Result<Self, QueryError> {
        use tower::service_fn;

        let path = path.into();
        let channel = Endpoint::from_static("http://[::]:50051")
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move {
                    tokio::net::UnixStream::connect(path)
                        .await
                        .map(hyper_util::rt::TokioIo::new)
                }
            }))
            .await
            .map_err(|error| QueryError::new(QueryErrorKind::Transport, error.to_string()))?;
        Ok(Self { channel })
    }

    pub async fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        let mut client = QueryServiceClient::new(self.channel.clone());
        let request = ExecuteRequest {
            statements: statements.into_iter().map(statement_to_proto).collect(),
        };
        let response = client.execute(request).await.map_err(status_error)?;
        response
            .into_inner()
            .results
            .into_iter()
            .map(|result| {
                query_result_from_proto(result)
                    .map_err(|error| QueryError::new(QueryErrorKind::Protocol, error.to_string()))
            })
            .collect()
    }
}

fn status_error(status: tonic::Status) -> QueryError {
    if let Some((kind, message)) = decode_error_detail(status.details()) {
        return QueryError::new(kind, message);
    }

    let kind = match status.code() {
        tonic::Code::DeadlineExceeded => QueryErrorKind::Timeout,
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange => QueryErrorKind::Validation,
        tonic::Code::FailedPrecondition | tonic::Code::Aborted => QueryErrorKind::Rejected,
        tonic::Code::Unimplemented => QueryErrorKind::Unsupported,
        tonic::Code::Internal | tonic::Code::DataLoss => QueryErrorKind::Internal,
        tonic::Code::Unavailable | tonic::Code::Cancelled => QueryErrorKind::Transport,
        _ => QueryErrorKind::Protocol,
    };
    QueryError::new(kind, status.message())
}

#[cfg(test)]
mod tests {
    use super::status_error;
    use query::{QueryError, QueryErrorKind};
    use tonic::{Code, Status};

    #[test]
    fn status_error_uses_structured_details() {
        let status = Status::with_details(
            Code::Internal,
            "different human message",
            tonic::codegen::Bytes::from_static(&[QueryErrorKind::Rejected as u8, b'n', b'o']),
        );
        assert_eq!(
            status_error(status),
            QueryError::new(QueryErrorKind::Rejected, "no")
        );
    }

    #[test]
    fn status_error_falls_back_to_tonic_code() {
        assert_eq!(
            status_error(Status::new(Code::DeadlineExceeded, "deadline")),
            QueryError::new(QueryErrorKind::Timeout, "deadline"),
        );
    }
}
