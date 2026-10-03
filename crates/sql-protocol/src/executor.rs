use core::future::Future;

use query::{QueryResult, Statement};

use crate::QueryServiceError;

pub trait QueryExecutor: Send + Sync {
    fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> impl Future<Output = Result<Vec<QueryResult>, QueryServiceError>> + Send;
}
