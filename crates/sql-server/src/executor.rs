use std::{future::Future, pin::Pin};

use engine::{Engine, EngineError, EngineTransaction, Kernel, RowCodec};
use protocol::{ConflictValues, QueryExecutor, QueryServiceError, QueryTransaction};
use query::{QueryResult, Statement};
use value::{Row, Value};

pub struct EngineExecutor<K, R>(pub Engine<K, R>);

impl<K, R> EngineExecutor<K, R> {
    pub fn new(engine: Engine<K, R>) -> Self {
        Self(engine)
    }
}

impl<K, R> QueryExecutor for EngineExecutor<K, R>
where
    K: Kernel + 'static,
    R: RowCodec<K::Transaction> + Send + Sync + 'static,
{
    async fn row_conflicts_batch(
        &self,
        table: &str,
        keys: &[Row],
    ) -> Result<Vec<Vec<String>>, QueryServiceError> {
        conflict_stage(|| self.0.row_conflicts_batch(table, keys))
            .await
            .map_err(engine_error)
    }

    async fn row_conflict_values(
        &self,
        table: &str,
        key: &Row,
    ) -> Result<Vec<(String, Vec<Value>)>, QueryServiceError> {
        conflict_stage(|| self.0.row_conflict_values(table, key))
            .await
            .map_err(engine_error)
    }

    async fn resolve_row(
        &self,
        table: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> Result<(), QueryServiceError> {
        conflict_stage(|| self.0.resolve_row(table, key, values))
            .await
            .map_err(engine_error)
    }

    async fn transaction(&self) -> Result<Box<dyn QueryTransaction>, QueryServiceError> {
        Ok(Box::new(EngineQueryTransaction(
            self.0.transaction().await.map_err(engine_error)?,
        )))
    }

    async fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryServiceError> {
        conflict_stage(|| Engine::execute(&self.0, statements))
            .await
            .map_err(engine_error)
    }
}

struct EngineQueryTransaction<K, R>(EngineTransaction<K, R>)
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync;

impl<K, R> QueryTransaction for EngineQueryTransaction<K, R>
where
    K: Kernel + 'static,
    R: RowCodec<K::Transaction> + Send + Sync + 'static,
{
    fn row_conflicts<'a>(
        &'a mut self,
        table: &'a str,
        key: &'a Row,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, QueryServiceError>> + Send + 'a>> {
        Box::pin(async move {
            conflict_stage(|| self.0.row_conflicts(table, key))
                .await
                .map_err(engine_error)
        })
    }

    fn row_conflict_values<'a>(
        &'a mut self,
        table: &'a str,
        key: &'a Row,
    ) -> Pin<Box<dyn Future<Output = Result<ConflictValues, QueryServiceError>> + Send + 'a>> {
        Box::pin(async move {
            conflict_stage(|| self.0.row_conflict_values(table, key))
                .await
                .map_err(engine_error)
        })
    }

    fn execute(
        &mut self,
        statements: Vec<Statement>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<QueryResult>, QueryServiceError>> + Send + '_>>
    {
        Box::pin(async move {
            conflict_stage(|| self.0.execute(statements))
                .await
                .map_err(engine_error)
        })
    }

    fn commit(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueryServiceError>> + Send>> {
        Box::pin(async move { self.0.commit().await.map_err(engine_error) })
    }

    fn rollback(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueryServiceError>> + Send>> {
        Box::pin(async move { self.0.rollback().await.map_err(engine_error) })
    }
}

#[inline(never)]
fn conflict_stage<F: Future>(make: impl FnOnce() -> F) -> Pin<Box<F>> {
    Box::pin(make())
}

fn engine_error(error: EngineError) -> QueryServiceError {
    match error {
        EngineError::TranslateError(error) => QueryServiceError::Invalid(error.to_string()),
        EngineError::Unsupported(message) => QueryServiceError::Unsupported(message),
        EngineError::InvalidQuery(message) => QueryServiceError::Rejected(message.into()),
        error => QueryServiceError::Internal(error.to_string()),
    }
}
