use engine::{Engine, EngineError, Kernel, RowCodec};
use protocol::{QueryExecutor, QueryServiceError};
use query::{QueryResult, Statement};

pub struct EngineExecutor<K, R>(pub Engine<K, R>);

impl<K, R> EngineExecutor<K, R> {
    pub fn new(engine: Engine<K, R>) -> Self {
        Self(engine)
    }
}

impl<K, R> QueryExecutor for EngineExecutor<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    async fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryServiceError> {
        Engine::execute(&self.0, statements)
            .await
            .map_err(|error| match error {
                EngineError::TranslateError(error) => QueryServiceError::Invalid(error.to_string()),
                EngineError::Unsupported(message) => QueryServiceError::Unsupported(message),
                EngineError::InvalidQuery(message) => QueryServiceError::Rejected(message.into()),
                error => QueryServiceError::Internal(error.to_string()),
            })
    }
}
