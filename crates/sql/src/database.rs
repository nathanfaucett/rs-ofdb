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
    SessionConfig, SyncError, SyncManifest, SyncResult, SyncRole, SyncStageFactory, SyncStateUnit,
    SyncTransport, apply_sync_state_for, export_sync_state_for, sync_manifest_for,
    synchronize as synchronize_engine, synchronize_with_stage_factory as synchronize_staged_engine,
};
use value::{FromRow, Row, Value};

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
    pub(crate) async fn row_conflicts(&self, table: &str, key: &Row) -> SqlResult<Vec<String>> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .row_conflicts(table, key)
                .await
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .row_conflicts(table, key)
                .await
                .map_err(crate::error::from_engine),
        }
    }

    pub(crate) async fn row_conflict_values(
        &self,
        table: &str,
        key: &Row,
    ) -> SqlResult<Vec<(String, Vec<Value>)>> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .row_conflict_values(table, key)
                .await
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .row_conflict_values(table, key)
                .await
                .map_err(crate::error::from_engine),
        }
    }

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

    pub(crate) async fn query_row_conflicts(
        &self,
        table: &str,
        key: &Row,
    ) -> SqlResult<Vec<String>> {
        database_call!(self, |engine| engine.row_conflicts(table, key).await)
    }

    pub(crate) async fn query_row_conflict_values(
        &self,
        table: &str,
        key: &Row,
    ) -> SqlResult<Vec<(String, Vec<Value>)>> {
        database_call!(self, |engine| engine.row_conflict_values(table, key).await)
    }

    pub(crate) async fn query_resolve_row(
        &self,
        table: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> SqlResult<()> {
        database_call!(self, |engine| engine.resolve_row(table, key, values).await)
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
        self.serve_tcp_with_tls(address, None, shutdown).await
    }

    #[cfg(feature = "server")]
    pub async fn serve_tcp_with_tls(
        &self,
        address: std::net::SocketAddr,
        identity: Option<(Vec<u8>, Vec<u8>)>,
        shutdown: impl core::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.serve_tcp_with_mtls(address, identity, None, shutdown)
            .await
    }

    #[cfg(feature = "server")]
    pub async fn serve_tcp_with_mtls(
        &self,
        address: std::net::SocketAddr,
        mut identity: Option<(Vec<u8>, Vec<u8>)>,
        client_ca: Option<Vec<u8>>,
        shutdown: impl core::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(engine) => {
                let server =
                    crate::Server::tcp(address, crate::EngineExecutor::new(engine.clone()));
                let server = match identity.take() {
                    Some((certificate, private_key)) => {
                        server.tls_identity(certificate, private_key)
                    }
                    None => server,
                };
                let server = match client_ca {
                    Some(ca) => server.tls_client_ca(ca),
                    None => server,
                };
                server.serve(shutdown).await
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => {
                let server =
                    crate::Server::tcp(address, crate::EngineExecutor::new(engine.clone()));
                let server = match identity.take() {
                    Some((certificate, private_key)) => {
                        server.tls_identity(certificate, private_key)
                    }
                    None => server,
                };
                let server = match client_ca {
                    Some(ca) => server.tls_client_ca(ca),
                    None => server,
                };
                server.serve(shutdown).await
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

    /// Unchecked sync. Caller owns admission; use `synchronize_checked` for per-apply
    /// authorization/cancellation that can change during the session.
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

    /// Unchecked custom staging; use the checked variant for changing caller decisions.
    #[cfg(feature = "sync")]
    pub async fn synchronize_with_stage_factory<T, F>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        stage_factory: &F,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        F: SyncStageFactory,
        F::Error: core::fmt::Display,
        F::Stage: 'static,
    {
        #[cfg(any(feature = "redb", feature = "in-memory",))]
        {
            match &self.backend {
                #[cfg(feature = "redb")]
                DatabaseBackend::File(engine) => {
                    synchronize_staged_engine(engine, transport, config, role, stage_factory).await
                }
                #[cfg(feature = "in-memory")]
                DatabaseBackend::InMemory(engine) => {
                    synchronize_staged_engine(engine, transport, config, role, stage_factory).await
                }
            }
        }
    }

    /// Uses ofdb-owned native temporary records in the caller-selected directory.
    #[cfg(feature = "native-stage")]
    pub async fn synchronize_with_stage_directory<T>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        directory: impl AsRef<std::path::Path>,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
    {
        let factory = crate::FileSyncStageFactory::new(directory);
        self.synchronize_with_stage_factory(transport, config, role, &factory)
            .await
    }

    /// Checks caller authorization/cancellation immediately before every atomic incoming apply.
    /// Returning Err stops subsequent applies; earlier committed operations remain.
    /// Abort delivery is attempted once without waiting for the peer. Discard the
    /// transport after failure: a canceled send may have partially written a frame.
    #[cfg(feature = "sync")]
    pub async fn synchronize_checked<T, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        let factory = sync::MemorySyncStageFactory::new(config.max_session_bytes);
        self.synchronize_with_stage_factory_checked(transport, config, role, &factory, check)
            .await
    }

    /// Checked sync with caller-provided staging; see `Self::synchronize_checked`.
    #[cfg(feature = "sync")]
    pub async fn synchronize_with_stage_factory_checked<T, F, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        factory: &F,
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        F: SyncStageFactory,
        F::Stage: 'static,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(engine) => {
                sync::synchronize_with_stage_factory_checked(
                    engine, transport, config, role, factory, check,
                )
                .await
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => {
                sync::synchronize_with_stage_factory_checked(
                    engine, transport, config, role, factory, check,
                )
                .await
            }
        }
    }

    /// Native staging plus the same per-operation caller check as memory sync.
    /// Local failure releases stages without waiting for Abort delivery; discard
    /// the transport after failure.
    #[cfg(feature = "native-stage")]
    pub async fn synchronize_with_stage_directory_checked<T, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        directory: impl AsRef<std::path::Path>,
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        let factory = crate::FileSyncStageFactory::new(directory);
        self.synchronize_with_stage_factory_checked(transport, config, role, &factory, check)
            .await
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
    /// Direct unchecked apply: caller must authorize each atomic operation. Prefer checked sync.
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
