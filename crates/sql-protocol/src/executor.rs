use alloc::{boxed::Box, string::String, vec::Vec};
use core::{future::Future, pin::Pin};

use query::{QueryResult, Statement};
use value::{Row, Value};

use crate::QueryServiceError;

pub type ConflictValues = Vec<(String, Vec<Value>)>;

pub trait QueryTransaction: Send {
    fn row_conflicts<'a>(
        &'a mut self,
        table: &'a str,
        key: &'a Row,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, QueryServiceError>> + Send + 'a>> {
        let _ = (table, key);
        Box::pin(async {
            Err(QueryServiceError::Unsupported(
                "transaction conflict inspection",
            ))
        })
    }
    fn row_conflict_values<'a>(
        &'a mut self,
        table: &'a str,
        key: &'a Row,
    ) -> Pin<Box<dyn Future<Output = Result<ConflictValues, QueryServiceError>> + Send + 'a>> {
        let _ = (table, key);
        Box::pin(async {
            Err(QueryServiceError::Unsupported(
                "transaction conflict values",
            ))
        })
    }
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
    fn row_conflicts_batch(
        &self,
        table: &str,
        keys: &[Row],
    ) -> impl Future<Output = Result<Vec<Vec<String>>, QueryServiceError>> + Send {
        let _ = (table, keys);
        async { Err(QueryServiceError::Unsupported("conflict inspection")) }
    }
    fn row_conflict_values(
        &self,
        table: &str,
        key: &Row,
    ) -> impl Future<Output = Result<Vec<(String, Vec<Value>)>, QueryServiceError>> + Send {
        let _ = (table, key);
        async { Err(QueryServiceError::Unsupported("conflict values")) }
    }
    fn resolve_row(
        &self,
        table: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> impl Future<Output = Result<(), QueryServiceError>> + Send {
        let _ = (table, key, values);
        async { Err(QueryServiceError::Unsupported("conflict resolution")) }
    }
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
