use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec,
    vec::Vec,
};
use schema::{IndexSchema, TableSchema};
use value::{FromRow, Row, Value};

#[cfg(feature = "std")]
use std::sync::Arc;

use thiserror::Error;

use query::{QueryParams, QueryResult, Statement, TranslateError, Translator};

use crate::{
    codec::RowCodec,
    executor::{
        execute_in_transaction, execute_statement, resolve_row as resolve_conflicted_row,
        row_conflict_values as conflicted_row_values, row_conflicts as conflicted_row_columns,
    },
    index::lookup as index_lookup,
    kernel::{Kernel, KernelTransaction},
    schema::index_schema,
};

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Translate error: {0}")]
    TranslateError(#[from] TranslateError),

    #[error("Unsupported query shape for MVP executor: {0}")]
    Unsupported(&'static str),

    #[error("Invalid query: {0}")]
    InvalidQuery(&'static str),

    #[error("Sync dependency is unavailable")]
    SyncDependencyUnavailable,

    #[error("Error: {0}")]
    Custom(String),
}

impl EngineError {
    pub fn custom<T>(error: T) -> Self
    where
        T: ToString,
    {
        Self::Custom(error.to_string())
    }
}

pub type EngineResult<T> = Result<T, EngineError>;

pub type TimestampProvider = fn() -> uuid::Timestamp;

#[cfg(feature = "std")]
fn default_timestamp_provider() -> uuid::Timestamp {
    uuid::Timestamp::now(uuid::NoContext)
}

pub struct Engine<K, R> {
    pub(crate) kernel: Arc<K>,
    pub(crate) reconciler: Arc<R>,
    pub(crate) timestamp_provider: TimestampProvider,
}

impl<K, R> Clone for Engine<K, R> {
    fn clone(&self) -> Self {
        Self {
            kernel: self.kernel.clone(),
            reconciler: self.reconciler.clone(),
            timestamp_provider: self.timestamp_provider,
        }
    }
}

#[cfg(feature = "std")]
impl<K, R> From<(K, R)> for Engine<K, R> {
    fn from((kernel, reconciler): (K, R)) -> Self {
        Self {
            kernel: Arc::new(kernel),
            reconciler: Arc::new(reconciler),
            timestamp_provider: default_timestamp_provider,
        }
    }
}

impl<K, R> Engine<K, R> {
    #[cfg(feature = "std")]
    pub fn new(kernel: K, reconciler: R) -> Self {
        Self::from((kernel, reconciler))
    }

    pub fn with_timestamp_provider(
        kernel: K,
        reconciler: R,
        timestamp_provider: TimestampProvider,
    ) -> Self {
        Self {
            kernel: Arc::new(kernel),
            reconciler: Arc::new(reconciler),
            timestamp_provider,
        }
    }
}

pub struct EngineTransaction<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    engine: Engine<K, R>,
    transaction: Option<K::Transaction>,
    changes: Vec<crate::Change>,
    failed: bool,
}

impl<K, R> EngineTransaction<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    pub async fn execute(&mut self, statements: Vec<Statement>) -> EngineResult<Vec<QueryResult>> {
        if self.failed {
            return Err(EngineError::InvalidQuery("Transaction is aborted"));
        }
        let transaction = self
            .transaction
            .as_mut()
            .ok_or(EngineError::InvalidQuery("Transaction is closed"))?;
        match Box::pin(execute_in_transaction(
            &self.engine,
            transaction,
            statements,
            &mut self.changes,
        ))
        .await
        {
            Ok(results) => Ok(results),
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }

    pub async fn translate_and_execute<T>(
        &mut self,
        query: &str,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        let statements = translator.translate(query).await?;
        self.execute(statements).await
    }

    pub async fn translate_and_execute_with_params<T>(
        &mut self,
        query: &str,
        params: Option<&QueryParams>,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        let statements = translator.translate_with_params(query, params).await?;
        self.execute(statements).await
    }

    pub async fn commit(mut self) -> EngineResult<()> {
        let transaction = self
            .transaction
            .take()
            .ok_or(EngineError::InvalidQuery("Transaction is closed"))?;
        if self.failed {
            transaction.rollback().await?;
            return Err(EngineError::InvalidQuery(
                "Cannot commit an aborted transaction",
            ));
        }
        transaction.commit().await
    }

    pub async fn rollback(mut self) -> EngineResult<()> {
        let transaction = self
            .transaction
            .take()
            .ok_or(EngineError::InvalidQuery("Transaction is closed"))?;
        transaction.rollback().await
    }
}

impl<K, R> Engine<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    pub async fn transaction(&self) -> EngineResult<EngineTransaction<K, R>> {
        Ok(EngineTransaction {
            engine: self.clone(),
            transaction: Some(self.kernel.transaction().await?),
            changes: Vec::new(),
            failed: false,
        })
    }

    pub async fn index_schema(&self, name: &str) -> EngineResult<IndexSchema> {
        let transaction = self.kernel.transaction().await?;
        let schema = index_schema(&transaction, self.reconciler.as_ref(), name).await?;
        transaction.rollback().await?;
        schema.ok_or(EngineError::InvalidQuery("Index not found"))
    }

    pub async fn index_lookup(&self, name: &str, values: &Row) -> EngineResult<Option<Row>> {
        let transaction = self.kernel.transaction().await?;
        let row = index_lookup(&transaction, self.reconciler.as_ref(), name, values).await?;
        transaction.rollback().await?;
        Ok(row)
    }

    pub async fn table_schema(&self, name: &str) -> EngineResult<TableSchema> {
        let transaction = self.kernel.transaction().await?;
        let schema =
            crate::executor::table_schema(&transaction, self.reconciler.as_ref(), name).await?;
        transaction.rollback().await?;
        Ok(schema)
    }

    pub async fn create_table(&self, table_schema: TableSchema) -> EngineResult<()> {
        self.execute(vec![Statement::DataDefinition(
            query::DataDefinition::CreateTable {
                schema: table_schema,
                if_not_exists: false,
            },
        )])
        .await?;
        Ok(())
    }

    pub async fn drop_table(&self, table_name: &str) -> EngineResult<()> {
        self.execute(vec![Statement::DataDefinition(
            query::DataDefinition::DropTable {
                table_name: String::from(table_name),
                if_exists: false,
            },
        )])
        .await?;
        Ok(())
    }

    pub async fn translate_and_execute_with_params<T>(
        &self,
        query: &str,
        params: Option<&QueryParams>,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        let statements = translator.translate_with_params(query, params).await?;
        self.execute(statements).await
    }

    pub async fn translate_and_execute<T>(
        &self,
        query: &str,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        let statements = translator.translate(query).await?;
        self.execute(statements).await
    }

    pub async fn translate_and_select<T, U>(
        &self,
        query: &str,
        translator: &T,
    ) -> EngineResult<Vec<U>>
    where
        T: Translator,
        U: FromRow,
    {
        let mut results = self.translate_and_execute(query, translator).await?;
        if results.len() != 1 {
            return Err(EngineError::InvalidQuery("Expected one query result"));
        }
        results
            .pop()
            .expect("result length was checked")
            .rows_as()
            .map_err(EngineError::custom)
    }

    pub async fn execute(&self, statements: Vec<Statement>) -> EngineResult<Vec<QueryResult>> {
        execute_statement(self, statements).await
    }

    pub async fn row_conflicts(&self, table_name: &str, key: &Row) -> EngineResult<Vec<String>> {
        let transaction = self.kernel.transaction().await?;
        let result =
            conflicted_row_columns(&transaction, self.reconciler.as_ref(), table_name, key).await?;
        transaction.rollback().await?;
        Ok(result)
    }

    pub async fn row_conflict_values(
        &self,
        table_name: &str,
        key: &Row,
    ) -> EngineResult<Vec<(String, Vec<Value>)>> {
        let transaction = self.kernel.transaction().await?;
        let result =
            conflicted_row_values(&transaction, self.reconciler.as_ref(), table_name, key).await?;
        transaction.rollback().await?;
        Ok(result)
    }

    pub async fn resolve_row(
        &self,
        table_name: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> EngineResult<()> {
        let mut transaction = self.kernel.transaction().await?;
        let result = resolve_conflicted_row(
            &mut transaction,
            self.reconciler.as_ref(),
            self.timestamp_provider,
            table_name,
            key,
            values,
        )
        .await;
        match result {
            Ok(()) => transaction.commit().await,
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }
}
