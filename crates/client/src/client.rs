use engine::EngineError;
use proto::{ExecuteRequest, query_service_client::QueryServiceClient};
use protocol::{query_result_from_proto, statement_to_proto};
use query::{QueryResult, Statement};
use tonic::{
    Code,
    transport::{Channel, Endpoint},
};

#[derive(Clone, Debug)]
enum Connector {
    Tcp(Box<Endpoint>),
    #[cfg(all(unix, feature = "unix"))]
    Unix(std::path::PathBuf),
}

#[derive(Clone, Debug)]
pub struct Client {
    connector: Connector,
}

impl Client {
    pub fn lazy_tcp(uri: impl AsRef<str>) -> Result<Self, EngineError> {
        let endpoint = Endpoint::from_shared(uri.as_ref().to_string())
            .map_err(|error| EngineError::custom(format!("invalid gRPC endpoint: {error}")))?;
        Ok(Self {
            connector: Connector::Tcp(Box::new(endpoint)),
        })
    }

    #[cfg(all(unix, feature = "unix"))]
    pub fn lazy_unix(path: impl Into<std::path::PathBuf>) -> Result<Self, EngineError> {
        Ok(Self {
            connector: Connector::Unix(path.into()),
        })
    }

    pub async fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, EngineError> {
        let mut client = QueryServiceClient::new(self.channel());
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
                    .map_err(|error| EngineError::custom(error.to_string()))
            })
            .collect()
    }

    fn channel(&self) -> Channel {
        match &self.connector {
            Connector::Tcp(endpoint) => endpoint.connect_lazy(),
            #[cfg(all(unix, feature = "unix"))]
            Connector::Unix(path) => {
                use tower::service_fn;
                let path = path.clone();
                Endpoint::from_static("http://[::]:50051").connect_with_connector_lazy(service_fn(
                    move |_| {
                        let path = path.clone();
                        async move {
                            tokio::net::UnixStream::connect(path)
                                .await
                                .map(hyper_util::rt::TokioIo::new)
                        }
                    },
                ))
            }
        }
    }
}

fn status_error(status: tonic::Status) -> EngineError {
    let code: Code = status.code();
    EngineError::custom(format!("gRPC {code:?}: {}", status.message()))
}

#[cfg(test)]
mod tests {
    use super::{Client, status_error};
    use tonic::{Code, Status};

    #[test]
    fn status_error_preserves_code_and_message() {
        let error = status_error(Status::new(Code::Unavailable, "offline")).to_string();
        assert!(error.contains("Unavailable"));
        assert!(error.contains("offline"));
    }

    #[cfg(all(unix, feature = "unix"))]
    #[test]
    fn unix_constructor_does_not_need_runtime() {
        let _client = Client::lazy_unix("/tmp/client-is-lazy.sock")
            .expect("Unix endpoint construction is lazy");
    }
}
