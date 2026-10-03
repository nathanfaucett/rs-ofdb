use std::sync::Arc;

use proto::{
    ExecuteRequest, ExecuteResponse, query_service_server::QueryService as QueryServiceTrait,
};
use protocol::{QueryExecutor, QueryServiceError, query_result_to_proto, statement_from_proto};
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
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let statements = request
            .into_inner()
            .statements
            .into_iter()
            .map(statement_from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(status_from_error)?;
        if statements.is_empty() {
            return Err(Status::invalid_argument(
                "statement batch must not be empty",
            ));
        }
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

fn status_from_error(error: QueryServiceError) -> Status {
    match error {
        QueryServiceError::Invalid(message) => Status::invalid_argument(message),
        QueryServiceError::Rejected(message) => Status::failed_precondition(message),
        QueryServiceError::Unsupported(message) => Status::unimplemented(message),
        QueryServiceError::Internal(message) => Status::internal(message),
    }
}
