use alloc::{boxed::Box, vec::Vec};
use core::{future::Future, pin::Pin};

use query::{QueryResult, Statement};

use crate::QueryServiceError;

pub trait QueryTransaction: Send {
    fn execute(
        &mut self,
        statements: Vec<Statement>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<QueryResult>, QueryServiceError>> + Send + '_>>;
    fn commit(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueryServiceError>> + Send>>;
    fn rollback(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueryServiceError>> + Send>>;
}

pub trait QueryExecutor: Send + Sync {
    fn transaction(
        &self,
    ) -> impl Future<Output = Result<Box<dyn QueryTransaction>, QueryServiceError>> + Send {
        async { Err(QueryServiceError::Unsupported("transactions")) }
    }
    fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> impl Future<Output = Result<Vec<QueryResult>, QueryServiceError>> + Send;
}
