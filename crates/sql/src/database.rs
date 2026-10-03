#[cfg(feature = "redb")]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use engine::Engine;
#[cfg(any(feature = "redb", feature = "in-memory",))]
use engine::EngineTransaction;
#[cfg(feature = "in-memory")]
use engine::InMemoryKernel;
use query::QueryError;
use query::{QueryParams, QueryResult, Statement, Translator};
use schema::{IndexSchema, TableSchema};
#[cfg(feature = "sync")]
use sync::{
    SessionConfig, SyncError, SyncManifest, SyncResult, SyncRole, SyncStateUnit, SyncTransport,
    apply_sync_state_for, export_sync_state_for, sync_manifest_for,
    synchronize as synchronize_engine,
};
use value::{FromRow, Row, Value};

#[cfg(any(feature = "redb", feature = "in-memory", feature = "sync"))]
use engine_automerge::AutomergeRowCodec;
#[cfg(feature = "redb")]
use engine_redb::{RedbKernel, redb};

/// An application-facing database using the Automerge row codec.
#[derive(Clone)]
pub enum Database {
    #[cfg(feature = "redb")]
    File(Engine<RedbKernel, AutomergeRowCodec>),
    #[cfg(feature = "in-memory")]
    InMemory(Engine<InMemoryKernel, AutomergeRowCodec>),
}

pub enum DatabaseTransaction {
    #[cfg(feature = "redb")]
    File(Box<EngineTransaction<RedbKernel, AutomergeRowCodec>>),
    #[cfg(feature = "in-memory")]
    InMemory(EngineTransaction<InMemoryKernel, AutomergeRowCodec>),
}

impl DatabaseTransaction {
    pub async fn translate_and_execute<T>(
        &mut self,
        query: &str,
        translator: &T,
    ) -> SqlResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .translate_and_execute(query, translator)
                .await
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .translate_and_execute(query, translator)
                .await
                .map_err(crate::error::from_engine),
        }
    }

    pub async fn translate_and_execute_with_params<T>(
        &mut self,
        query: &str,
        params: Option<&QueryParams>,
        translator: &T,
    ) -> SqlResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .translate_and_execute_with_params(query, params, translator)
                .await
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .translate_and_execute_with_params(query, params, translator)
                .await
                .map_err(crate::error::from_engine),
        }
    }

    pub async fn commit(self) -> SqlResult<()> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .commit()
                .await
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .commit()
                .await
                .map_err(crate::error::from_engine),
        }
    }

    pub async fn rollback(self) -> SqlResult<()> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .rollback()
                .await
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .rollback()
                .await
                .map_err(crate::error::from_engine),
        }
    }
}

type SqlResult<T> = Result<T, QueryError>;

macro_rules! database_call {
    ($database:expr, |$engine:ident| $body:expr) => {{
        match $database {
            #[cfg(feature = "redb")]
            Database::File($engine) => $body.map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Database::InMemory($engine) => $body.map_err(crate::error::from_engine),
        }
    }};
}

impl Database {
    #[cfg(feature = "redb")]
    pub fn open(path: impl AsRef<std::path::Path>) -> SqlResult<Self> {
        let database = redb::Database::create(path)
            .map_err(|error| QueryError::new(crate::ErrorKind::Storage, error.to_string()))?;
        Ok(Self::File(Engine::new(
            RedbKernel::new(std::sync::Arc::new(database)),
            AutomergeRowCodec::new(),
        )))
    }

    #[cfg(feature = "in-memory")]
    pub fn in_memory() -> Self {
        Self::InMemory(Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new()))
    }

    pub async fn transaction(&self) -> SqlResult<DatabaseTransaction> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(engine) => engine
                .transaction()
                .await
                .map(Box::new)
                .map(DatabaseTransaction::File)
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(engine) => engine
                .transaction()
                .await
                .map(DatabaseTransaction::InMemory)
                .map_err(crate::error::from_engine),
        }
    }

    pub async fn index_schema(&self, name: &str) -> SqlResult<IndexSchema> {
        database_call!(self, |engine| engine.index_schema(name).await)
    }

    pub async fn index_lookup(&self, name: &str, values: &Row) -> SqlResult<Option<Row>> {
        database_call!(self, |engine| engine.index_lookup(name, values).await)
    }

    pub async fn table_schema(&self, name: &str) -> SqlResult<TableSchema> {
        database_call!(self, |engine| engine.table_schema(name).await)
    }

    pub async fn create_table(&self, table_schema: TableSchema) -> SqlResult<()> {
        database_call!(self, |engine| engine.create_table(table_schema).await)
    }

    pub async fn drop_table(&self, table_name: &str) -> SqlResult<()> {
        database_call!(self, |engine| engine.drop_table(table_name).await)
    }

    pub async fn translate_and_execute_with_params<T>(
        &self,
        query: &str,
        params: Option<&QueryParams>,
        translator: &T,
    ) -> SqlResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        database_call!(self, |engine| {
            engine
                .translate_and_execute_with_params(query, params, translator)
                .await
        })
    }

    pub async fn translate_and_execute<T>(
        &self,
        query: &str,
        translator: &T,
    ) -> SqlResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        database_call!(self, |engine| engine
            .translate_and_execute(query, translator)
            .await)
    }

    pub async fn translate_and_select<T, U>(&self, query: &str, translator: &T) -> SqlResult<Vec<U>>
    where
        T: Translator,
        U: FromRow,
    {
        database_call!(self, |engine| engine
            .translate_and_select(query, translator)
            .await)
    }

    pub async fn execute(&self, statements: Vec<Statement>) -> SqlResult<Vec<QueryResult>> {
        if statements.is_empty() {
            return Err(QueryError::new(
                crate::ErrorKind::Validation,
                "statement batch must not be empty",
            ));
        }
        database_call!(self, |engine| engine.execute(statements).await)
    }

    #[cfg(feature = "sql")]
    pub async fn execute_sql(
        &self,
        sql: &str,
        params: Option<&QueryParams>,
    ) -> SqlResult<Vec<QueryResult>> {
        use query::Translator;

        let statements = sql_translator::SqlTranslator
            .translate_with_params(sql, params)
            .await
            .map_err(|error| QueryError::new(crate::ErrorKind::Validation, error.to_string()))?;
        self.execute(statements).await
    }

    #[cfg(feature = "server")]
    pub async fn serve_tcp(
        &self,
        address: std::net::SocketAddr,
        shutdown: impl core::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(engine) => {
                crate::Server::tcp(address, crate::EngineExecutor::new(engine.clone()))
                    .serve(shutdown)
                    .await
            }
            #[cfg(feature = "in-memory")]
            Self::InMemory(engine) => {
                crate::Server::tcp(address, crate::EngineExecutor::new(engine.clone()))
                    .serve(shutdown)
                    .await
            }
        }
    }

    #[cfg(all(feature = "server", unix))]
    pub async fn serve_unix(
        &self,
        path: impl Into<std::path::PathBuf>,
        shutdown: impl core::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(engine) => {
                crate::Server::unix(path, crate::EngineExecutor::new(engine.clone()))
                    .serve(shutdown)
                    .await
            }
            #[cfg(feature = "in-memory")]
            Self::InMemory(engine) => {
                crate::Server::unix(path, crate::EngineExecutor::new(engine.clone()))
                    .serve(shutdown)
                    .await
            }
        }
    }

    #[cfg(feature = "sync")]
    pub async fn synchronize<T>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
    {
        #[cfg(any(feature = "redb", feature = "in-memory",))]
        {
            match self {
                #[cfg(feature = "redb")]
                Self::File(engine) => synchronize_engine(engine, transport, config, role).await,
                #[cfg(feature = "in-memory")]
                Self::InMemory(engine) => synchronize_engine(engine, transport, config, role).await,
            }
        }
    }

    #[cfg(feature = "sync")]
    pub async fn sync_manifest(&self) -> SqlResult<SyncManifest> {
        database_call!(self, |engine| sync_manifest_for(engine).await)
    }

    #[cfg(feature = "sync")]
    pub async fn export_sync_state(&self) -> SqlResult<Vec<SyncStateUnit>> {
        database_call!(self, |engine| export_sync_state_for(engine).await)
    }

    #[cfg(feature = "sync")]
    pub async fn apply_sync_state(&self, unit: SyncStateUnit) -> SqlResult<()> {
        database_call!(self, |engine| apply_sync_state_for(engine, unit).await)
    }

    pub async fn row_conflicts(&self, table_name: &str, key: &Row) -> SqlResult<Vec<String>> {
        database_call!(self, |engine| engine.row_conflicts(table_name, key).await)
    }

    pub async fn resolve_row(
        &self,
        table_name: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> SqlResult<()> {
        database_call!(self, |engine| engine
            .resolve_row(table_name, key, values)
            .await)
    }
}

#[cfg(all(test, feature = "redb", feature = "sql"))]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(feature = "in-memory")]
    #[test]
    fn embedded_handles_reject_empty_statement_batches() {
        block_on(async {
            let database = Database::in_memory();
            let error = database
                .execute(Vec::new())
                .await
                .expect_err("empty statement batches are invalid");
            assert_eq!(error.kind, crate::ErrorKind::Validation);
        });
    }

    #[cfg(feature = "in-memory")]
    #[test]
    fn in_memory_database_uses_the_engine_api() {
        block_on(async {
            let database = Database::in_memory();
            database
                .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", None)
                .await
                .expect("execute SQL text");
            assert_eq!(database.table_schema("users").await.unwrap().name, "users");
        });
    }

    #[test]
    fn file_database_reopens_persisted_schema() {
        block_on(async {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let database_path = std::env::temp_dir()
                .join(format!("ofdb-database-{}-{nanos}.redb", std::process::id()));

            {
                let database = Database::open(&database_path).unwrap();
                database
                    .translate_and_execute(
                        "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                        &crate::SqlTranslator,
                    )
                    .await
                    .unwrap();
            }

            let database = Database::open(&database_path).unwrap();
            assert_eq!(database.table_schema("users").await.unwrap().name, "users");
            let _ = std::fs::remove_file(&database_path);
        });
    }
}
