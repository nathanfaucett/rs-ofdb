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

#[cfg(feature = "sync")]
use sync::{
    SessionConfig, SyncError, SyncManifest, SyncResult, SyncRole, SyncStateUnit, SyncTransport,
    apply_sync_state_for, export_sync_state_for, sync_manifest_for,
    synchronize as synchronize_engine,
};
use value::FromRow;

#[cfg(any(feature = "redb", feature = "in-memory", feature = "sync"))]
use engine_automerge::AutomergeRowCodec;
#[cfg(feature = "redb")]
use engine_redb::{RedbKernel, redb};

/// An application-facing database using the Automerge row codec.
#[derive(Clone)]
pub struct Database {
    backend: DatabaseBackend,
}

#[derive(Clone)]
enum DatabaseBackend {
    #[cfg(feature = "redb")]
    File(Engine<RedbKernel, AutomergeRowCodec>),
    #[cfg(feature = "in-memory")]
    InMemory(Engine<InMemoryKernel, AutomergeRowCodec>),
}

pub(crate) enum EmbeddedTransaction {
    #[cfg(feature = "redb")]
    File(Box<EngineTransaction<RedbKernel, AutomergeRowCodec>>),
    #[cfg(feature = "in-memory")]
    InMemory(EngineTransaction<InMemoryKernel, AutomergeRowCodec>),
}

impl EmbeddedTransaction {
    pub(crate) async fn execute(
        &mut self,
        statements: Vec<Statement>,
    ) -> SqlResult<Vec<QueryResult>> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .execute(statements)
                .await
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .execute(statements)
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
        match &$database.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File($engine) => $body.map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory($engine) => $body.map_err(crate::error::from_engine),
        }
    }};
}

impl Database {
    pub fn client(&self) -> crate::Client {
        crate::Client::embedded(self.clone())
    }

    #[cfg(feature = "redb")]
    pub fn open(path: impl AsRef<std::path::Path>) -> SqlResult<Self> {
        let database = redb::Database::create(path)
            .map_err(|error| QueryError::new(crate::ErrorKind::Storage, error.to_string()))?;
        Ok(Self {
            backend: DatabaseBackend::File(Engine::new(
                RedbKernel::new(std::sync::Arc::new(database)),
                AutomergeRowCodec::new(),
            )),
        })
    }

    #[cfg(feature = "in-memory")]
    pub fn in_memory() -> Self {
        Self {
            backend: DatabaseBackend::InMemory(Engine::new(
                InMemoryKernel::new(),
                AutomergeRowCodec::new(),
            )),
        }
    }

    pub(crate) async fn begin_transaction(&self) -> SqlResult<EmbeddedTransaction> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(engine) => engine
                .transaction()
                .await
                .map(Box::new)
                .map(EmbeddedTransaction::File)
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => engine
                .transaction()
                .await
                .map(EmbeddedTransaction::InMemory)
                .map_err(crate::error::from_engine),
        }
    }

    pub(crate) async fn query_execute_untimed(
        &self,
        statements: Vec<Statement>,
    ) -> SqlResult<Vec<QueryResult>> {
        if statements.is_empty() {
            return Err(QueryError::new(
                crate::ErrorKind::Validation,
                "statement batch must not be empty",
            ));
        }
        database_call!(self, |engine| engine.execute(statements).await)
    }

    pub(crate) async fn query_translate_and_execute_with_params<T>(
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

    pub(crate) async fn query_translate_and_execute<T>(
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

    pub(crate) async fn query_translate_and_select<T, U>(
        &self,
        query: &str,
        translator: &T,
    ) -> SqlResult<Vec<U>>
    where
        T: Translator,
        U: FromRow,
    {
        database_call!(self, |engine| engine
            .translate_and_select(query, translator)
            .await)
    }

    #[cfg(feature = "server")]
    pub async fn serve_tcp(
        &self,
        address: std::net::SocketAddr,
        shutdown: impl core::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(engine) => {
                crate::Server::tcp(address, crate::EngineExecutor::new(engine.clone()))
                    .serve(shutdown)
                    .await
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => {
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
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(engine) => {
                crate::Server::unix(path, crate::EngineExecutor::new(engine.clone()))
                    .serve(shutdown)
                    .await
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => {
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
            match &self.backend {
                #[cfg(feature = "redb")]
                DatabaseBackend::File(engine) => {
                    synchronize_engine(engine, transport, config, role).await
                }
                #[cfg(feature = "in-memory")]
                DatabaseBackend::InMemory(engine) => {
                    synchronize_engine(engine, transport, config, role).await
                }
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
                .client()
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
            let client = database.client();
            client
                .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", None)
                .await
                .expect("execute SQL text");
            client
                .execute_sql("SELECT id FROM users", None)
                .await
                .expect("query created table");
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
                    .client()
                    .translate_and_execute(
                        "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                        &crate::SqlTranslator,
                    )
                    .await
                    .unwrap();
            }

            let database = Database::open(&database_path).unwrap();
            database
                .client()
                .translate_and_execute("SELECT id FROM users", &crate::SqlTranslator)
                .await
                .expect("query persisted schema");
            let _ = std::fs::remove_file(&database_path);
        });
    }
}
