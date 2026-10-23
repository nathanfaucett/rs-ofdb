#[cfg(any(feature = "redb", feature = "in-memory", feature = "sync"))]
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

#[cfg(feature = "sync")]
#[inline(never)]
fn sync_stage<F: core::future::Future>(make: impl FnOnce() -> F) -> core::pin::Pin<Box<F>> {
    Box::pin(make())
}

// Keep concrete engine conflict futures out of the caller's poll frame during construction.
#[cfg(any(feature = "redb", feature = "in-memory"))]
#[inline(never)]
fn conflict_stage<F: core::future::Future>(make: impl FnOnce() -> F) -> core::pin::Pin<Box<F>> {
    Box::pin(make())
}

#[cfg(feature = "sync")]
type CandidateValidator<'a> =
    dyn Fn(&sync::SyncCandidateView) -> Result<(), alloc::string::String> + Send + Sync + 'a;

#[cfg(any(feature = "redb", feature = "in-memory", feature = "sync"))]
use engine_automerge::AutomergeRowCodec;
#[cfg(feature = "redb")]
use engine_redb::RedbKernel;

/// An application-facing database using the Automerge row codec.
/// Explicit user-row keys accept RFC UUIDv5 or UUIDv7; generated and catalog
/// identities remain UUIDv7. Keys are immutable and tombstoned keys cannot revive.
#[derive(Clone)]
pub struct Database {
    backend: DatabaseBackend,
    #[cfg(feature = "sync")]
    changes: Option<sync::ChangeSubscription>,
}

#[derive(Clone)]
enum DatabaseBackend {
    #[cfg(feature = "redb")]
    File(engine_redb::NativeResource<Engine<RedbKernel, AutomergeRowCodec>>),
    #[cfg(feature = "in-memory")]
    InMemory(Engine<InMemoryKernel, AutomergeRowCodec>),
}

pub(crate) enum EmbeddedTransaction {
    #[cfg(feature = "redb")]
    File(
        engine_redb::NativeTransaction<
            Engine<RedbKernel, AutomergeRowCodec>,
            Box<EngineTransaction<RedbKernel, AutomergeRowCodec>>,
        >,
    ),
    #[cfg(feature = "in-memory")]
    InMemory(EngineTransaction<InMemoryKernel, AutomergeRowCodec>),
}

impl EmbeddedTransaction {
    pub(crate) async fn row_conflicts(&self, table: &str, key: &Row) -> SqlResult<Vec<String>> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => {
                let transaction = transaction.read().await.map_err(native_error)?;
                conflict_stage(|| transaction.row_conflicts(table, key))
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => conflict_stage(|| transaction.row_conflicts(table, key))
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
            Self::File(transaction) => {
                let transaction = transaction.read().await.map_err(native_error)?;
                conflict_stage(|| transaction.row_conflict_values(table, key))
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => {
                conflict_stage(|| transaction.row_conflict_values(table, key))
                    .await
                    .map_err(crate::error::from_engine)
            }
        }
    }

    pub(crate) async fn execute(
        &mut self,
        statements: Vec<Statement>,
    ) -> SqlResult<Vec<QueryResult>> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .read()
                .await
                .map_err(native_error)?
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

    pub(crate) async fn execute_translated<F>(
        &mut self,
        translation: F,
    ) -> SqlResult<Vec<QueryResult>>
    where
        F: core::future::Future<Output = SqlResult<Vec<Statement>>>,
    {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => {
                let mut transaction = transaction.read().await.map_err(native_error)?;
                transaction
                    .execute(translation.await?)
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .execute(translation.await?)
                .await
                .map_err(crate::error::from_engine),
        }
    }

    pub async fn commit(self) -> SqlResult<()> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => {
                let (_admitted, transaction) = transaction.take().await.map_err(native_error)?;
                transaction
                    .commit()
                    .await
                    .map_err(crate::error::from_engine)
            }
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
            Self::File(transaction) => {
                let (_admitted, transaction) = transaction.take().await.map_err(native_error)?;
                transaction
                    .rollback()
                    .await
                    .map_err(crate::error::from_engine)
            }
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
            DatabaseBackend::File(resource) => {
                let admitted = resource.read().await.map_err(native_error)?;
                let $engine = &*admitted;
                $body.map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory($engine) => $body.map_err(crate::error::from_engine),
        }
    }};
}

impl Database {
    /// Permanently fences issued handles and opens, drains admitted operations,
    /// cancels sessions, rolls back idle transactions, then removes this dedicated file.
    /// Native hosting is not managed: an owner that attempted native hosting returns Unsupported.
    /// Unix-only; callers must exclusively own the pathname and marker namespace.
    /// The permanent `.ofdb-retired` marker is retained, including after success.
    #[cfg(feature = "redb")]
    pub async fn retire_and_remove(&self) -> std::io::Result<()> {
        match &self.backend {
            DatabaseBackend::File(resource) => resource.retire_and_remove().await,
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "not a native resource",
            )),
        }
    }

    /// Resumes cleanup of a permanently retired file, refusing live native owners.
    #[cfg(feature = "redb")]
    pub fn remove_retired(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        engine_redb::remove_retired(path)
    }

    #[cfg(feature = "native-stage")]
    fn native_stage_factory(
        &self,
        directory: impl AsRef<std::path::Path>,
    ) -> crate::FileSyncStageFactory {
        let factory = crate::FileSyncStageFactory::new(directory);
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(resource) => {
                #[cfg(unix)]
                {
                    let tracker = resource.stage_tracker();
                    let closed = tracker.clone();
                    factory.with_file_tracking(
                        move |path| tracker.track(path),
                        move |path| closed.forget(path),
                    )
                }
                #[cfg(not(unix))]
                {
                    let _ = resource;
                    factory
                }
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(_) => factory,
        }
    }

    /// Creates the sync-owned lifecycle for committed changes, including remote apply.
    /// `ChangeSubscription::run` owns initial sync, coalescing, races and retry backoff.
    /// Merge with a KV subscription to reconcile both stores in one callback.
    /// Dropping the runner cancels its in-flight work.
    #[cfg(feature = "sync")]
    pub fn sync_changes(&self) -> std::io::Result<sync::ChangeSubscription> {
        self.changes.clone().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "backend does not support committed subscriptions",
            )
        })
    }

    /// Interpret selected work before exporting state. Acknowledge only after successful sync.
    #[cfg(feature = "sync")]
    pub async fn sync_invalidations(
        &self,
        work: &crate::ChangeWork,
    ) -> SqlResult<Vec<crate::SqlInvalidation>> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(resource) => {
                let engine = resource.read().await.map_err(native_error)?;
                engine_automerge::committed_invalidations(&engine, work)
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => {
                engine_automerge::committed_invalidations(engine, work)
                    .await
                    .map_err(crate::error::from_engine)
            }
        }
    }

    pub fn client(&self) -> crate::Client {
        crate::Client::embedded(self.clone())
    }

    #[cfg(feature = "redb")]
    pub fn open(path: impl AsRef<std::path::Path>) -> SqlResult<Self> {
        let path = path.as_ref();
        let database = engine_redb::open_unretired(path, true).map_err(native_error)?;
        let database = engine_redb::RedbDatabase::new(std::sync::Arc::new(database));
        #[cfg(feature = "sync")]
        let changes = database.subscribe_changes();
        Ok(Self {
            #[cfg(feature = "sync")]
            changes: Some(changes.into()),
            backend: DatabaseBackend::File(
                engine_redb::NativeResource::new(
                    path,
                    Engine::new(
                        RedbKernel::from_database(database),
                        AutomergeRowCodec::new(),
                    ),
                )
                .map_err(native_error)?,
            ),
        })
    }

    #[cfg(feature = "in-memory")]
    pub fn in_memory() -> Self {
        let kernel = InMemoryKernel::new();
        #[cfg(feature = "sync")]
        let changes = kernel.subscribe_changes();
        Self {
            #[cfg(feature = "sync")]
            changes: Some(changes.into()),
            backend: DatabaseBackend::InMemory(Engine::new(kernel, AutomergeRowCodec::new())),
        }
    }

    pub(crate) async fn begin_transaction(&self) -> SqlResult<EmbeddedTransaction> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(resource) => {
                let engine = resource.read().await.map_err(native_error)?;
                let transaction = engine
                    .transaction()
                    .await
                    .map_err(crate::error::from_engine)?;
                Ok(EmbeddedTransaction::File(
                    resource.transaction(Box::new(transaction)),
                ))
            }
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
        database_call!(self, |engine| conflict_stage(
            || engine.row_conflicts(table, key)
        )
        .await)
    }

    pub(crate) async fn query_row_conflicts_batch(
        &self,
        table: &str,
        keys: &[Row],
    ) -> SqlResult<Vec<Vec<String>>> {
        database_call!(self, |engine| conflict_stage(
            || engine.row_conflicts_batch(table, keys)
        )
        .await)
    }

    pub(crate) async fn query_row_conflict_values(
        &self,
        table: &str,
        key: &Row,
    ) -> SqlResult<Vec<(String, Vec<Value>)>> {
        database_call!(self, |engine| conflict_stage(
            || engine.row_conflict_values(table, key)
        )
        .await)
    }

    pub(crate) async fn query_resolve_row(
        &self,
        table: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> SqlResult<()> {
        database_call!(self, |engine| conflict_stage(
            || engine.resolve_row(table, key, values)
        )
        .await)
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
                engine
                    .hosting(async {
                        let engine = engine.hosting_backend().await?;
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
                    })
                    .await?
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
                engine
                    .hosting(async {
                        let engine = engine.hosting_backend().await?;
                        crate::Server::unix(path, crate::EngineExecutor::new(engine.clone()))
                            .serve(shutdown)
                            .await
                    })
                    .await?
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
                DatabaseBackend::File(engine) => engine
                    .session(async {
                        let engine = engine
                            .read()
                            .await
                            .map_err(|error| SyncError::PreApply(error.to_string()))?;
                        synchronize_engine(&*engine, transport, config, role).await
                    })
                    .await
                    .map_err(|error| SyncError::PreApply(error.to_string()))?,
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
                DatabaseBackend::File(engine) => engine
                    .session(async {
                        let engine = engine
                            .read()
                            .await
                            .map_err(|error| SyncError::PreApply(error.to_string()))?;
                        synchronize_staged_engine(&*engine, transport, config, role, stage_factory)
                            .await
                    })
                    .await
                    .map_err(|error| SyncError::PreApply(error.to_string()))?,
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
        let factory = self.native_stage_factory(directory);
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
            DatabaseBackend::File(engine) => engine
                .session(async {
                    let engine = engine
                        .read()
                        .await
                        .map_err(|error| SyncError::PreApply(error.to_string()))?;
                    sync::synchronize_with_stage_factory_checked(
                        &*engine, transport, config, role, factory, check,
                    )
                    .await
                })
                .await
                .map_err(|error| SyncError::PreApply(error.to_string()))?,
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
        let factory = self.native_stage_factory(directory);
        self.synchronize_with_stage_factory_checked(transport, config, role, &factory, check)
            .await
    }

    /// Checked full-history sync restricted to predeclared, fixed-schema tables.
    /// Rejects scope/schema/history widening; never projects or sanitizes history.
    /// `config.direction` constrains publication/apply independently of turn role:
    /// SendOnly/ReceiveOnly peers must be complementary. ReceiveOnly publishes no
    /// history (including catalog/index/recovery), but exchanges real progress metadata.
    /// Both scoped peers must support the direction-bearing scope handshake.
    /// Does not validate domain value content; use `Self::synchronize_tables_validated_checked`
    /// when allowed SQL values can contain forbidden historical content.
    /// Earlier committed applies remain on failure. Discard the transport after
    /// any failure; local stage cleanup does not wait for peer Abort delivery.
    #[cfg(feature = "sync")]
    pub async fn synchronize_tables_checked<T, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        tables: &[&str],
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        self.synchronize_tables_validated_checked(
            transport,
            config,
            role,
            tables,
            |_, _, _| Ok(()),
            check,
        )
        .await
    }

    /// Fixed-schema scoped sync with owner-native temporary stages.
    /// Uses the same structural-only checks and failure/cleanup contract as
    /// `Self::synchronize_tables_checked`; domain values are not validated.
    #[cfg(feature = "native-stage")]
    #[allow(clippy::too_many_arguments)]
    pub async fn synchronize_tables_with_stage_directory_checked<T, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        directory: impl AsRef<std::path::Path>,
        tables: &[&str],
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        self.synchronize_tables_with_stage_directory_validated_checked(
            transport,
            config,
            role,
            directory,
            tables,
            |_, _, _| Ok(()),
            check,
        )
        .await
    }

    /// Scoped sync validating every historical value before permitted export or incoming apply.
    /// Direction constraints and progress metadata follow `Self::synchronize_tables_checked`.
    /// Calls `validate(table, column, value)` for defaults and all stored values,
    /// including overwritten/deleted and losing-conflict values. It must accept
    /// every observation; rejection never prunes history. Domain policy is caller-owned.
    /// Observations may repeat and include implicit Null defaults. Validation errors
    /// are engine errors and may be sent in Abort: return non-sensitive reasons.
    /// `check` still authorizes each incoming apply separately; validation is not
    /// a freshness/revocation guarantee. Retains the failure/cleanup contract of
    /// `Self::synchronize_tables_checked`.
    #[cfg(feature = "sync")]
    #[allow(clippy::too_many_arguments)]
    pub async fn synchronize_tables_validated_checked<T, V, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        tables: &[&str],
        validate: V,
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        V: Fn(&str, &str, &Value) -> Result<(), alloc::string::String> + Send + Sync,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        let factory = sync::MemorySyncStageFactory::new(config.max_session_bytes);
        self.synchronize_tables_staged_checked(
            transport, config, role, &factory, tables, validate, None, check,
        )
        .await
    }

    /// Historical-value validated scoped sync with owner-native temporary stages.
    /// Uses the same validation and failure contract as `Self::synchronize_tables_validated_checked`.
    #[cfg(feature = "native-stage")]
    #[allow(clippy::too_many_arguments)]
    pub async fn synchronize_tables_with_stage_directory_validated_checked<T, V, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        directory: impl AsRef<std::path::Path>,
        tables: &[&str],
        validate: V,
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        V: Fn(&str, &str, &Value) -> Result<(), alloc::string::String> + Send + Sync,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        let factory = self.native_stage_factory(directory);
        self.synchronize_tables_staged_checked(
            transport, config, role, &factory, tables, validate, None, check,
        )
        .await
    }

    /// Scoped sync with synchronous, transaction-consistent complete candidate validation.
    /// Direction constraints follow `Self::synchronize_tables_checked`; ReceiveOnly
    /// does not run outgoing candidate/history validation or publish reverse history.
    /// The callback observes selected related rows and conflicts, after each incoming
    /// batch merges but before commit, and actual source export reads before disclosure.
    /// Do not block, reenter this database, or perform network IO in the callback.
    /// Return non-sensitive errors; denial rolls back the batch, not earlier progress.
    /// Incomplete rows/conflicts do not confer trust; that decision remains caller-owned.
    /// Views cover selected tables in one transaction, not a session or mutation ledger.
    /// Validation may observe a batch that later fails: do not publish external trust.
    /// Materializing related evidence scans all selected rows per observed transaction.
    #[cfg(feature = "sync")]
    #[allow(clippy::too_many_arguments)]
    pub async fn synchronize_tables_candidate_validated_checked<T, V, C, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        tables: &[&str],
        validate: V,
        validate_candidate: C,
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        V: Fn(&str, &str, &Value) -> Result<(), alloc::string::String> + Send + Sync,
        C: Fn(&sync::SyncCandidateView) -> Result<(), alloc::string::String> + Send + Sync,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        let factory = sync::MemorySyncStageFactory::new(config.max_session_bytes);
        self.synchronize_tables_staged_checked(
            transport,
            config,
            role,
            &factory,
            tables,
            validate,
            Some(&validate_candidate),
            check,
        )
        .await
    }

    /// Candidate/historical-value validated scoped sync with owner-native stages.
    /// Same contract as `Self::synchronize_tables_candidate_validated_checked`.
    /// Discard the transport on error; stages are removed without waiting for Abort delivery.
    #[cfg(feature = "native-stage")]
    #[allow(clippy::too_many_arguments)]
    pub async fn synchronize_tables_with_stage_directory_candidate_validated_checked<
        T,
        V,
        C,
        Q,
        U,
    >(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        directory: impl AsRef<std::path::Path>,
        tables: &[&str],
        validate: V,
        validate_candidate: C,
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        V: Fn(&str, &str, &Value) -> Result<(), alloc::string::String> + Send + Sync,
        C: Fn(&sync::SyncCandidateView) -> Result<(), alloc::string::String> + Send + Sync,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        let factory = self.native_stage_factory(directory);
        self.synchronize_tables_staged_checked(
            transport,
            config,
            role,
            &factory,
            tables,
            validate,
            Some(&validate_candidate),
            check,
        )
        .await
    }

    #[cfg(feature = "sync")]
    #[allow(clippy::too_many_arguments)]
    async fn synchronize_tables_staged_checked<T, F, V, Q, U>(
        &self,
        transport: &mut T,
        config: &SessionConfig,
        role: SyncRole,
        factory: &F,
        tables: &[&str],
        validate: V,
        validate_candidate: Option<&CandidateValidator<'_>>,
        check: Q,
    ) -> Result<SyncResult, SyncError<T::Error>>
    where
        T: SyncTransport,
        T::Error: core::fmt::Display,
        F: SyncStageFactory,
        F::Stage: 'static,
        V: Fn(&str, &str, &Value) -> Result<(), alloc::string::String> + Send + Sync,
        Q: FnMut() -> U + Send,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + Send,
    {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(engine) => engine
                .session(sync_stage(|| async {
                    let engine = engine
                        .read()
                        .await
                        .map_err(|error| SyncError::PreApply(error.to_string()))?;
                    if let Some(candidate) = validate_candidate {
                        sync_stage(|| {
                            sync::synchronize_tables_with_stage_factory_candidate_validated_checked(
                                &*engine, transport, config, role, factory, tables, validate,
                                candidate, check,
                            )
                        })
                        .await
                    } else {
                        sync_stage(|| {
                            sync::synchronize_tables_with_stage_factory_validated_checked(
                                &*engine, transport, config, role, factory, tables, validate, check,
                            )
                        })
                        .await
                    }
                }))
                .await
                .map_err(|error| SyncError::PreApply(error.to_string()))?,
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => {
                if let Some(candidate) = validate_candidate {
                    sync_stage(|| {
                        sync::synchronize_tables_with_stage_factory_candidate_validated_checked(
                            engine, transport, config, role, factory, tables, validate, candidate,
                            check,
                        )
                    })
                    .await
                } else {
                    sync_stage(|| {
                        sync::synchronize_tables_with_stage_factory_validated_checked(
                            engine, transport, config, role, factory, tables, validate, check,
                        )
                    })
                    .await
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

    #[test]
    fn conflict_query_futures_keep_engine_state_indirect() {
        let directory = tempfile::tempdir().expect("database directory");
        let database =
            Database::open(directory.path().join("conflicts.redb")).expect("conflict database");
        let key = Row::new(Vec::new());
        let keys = [key.clone()];
        let sizes = [
            core::mem::size_of_val(&database.query_row_conflicts("security", &key)),
            core::mem::size_of_val(&database.query_row_conflicts_batch("security", &keys)),
            core::mem::size_of_val(&database.query_row_conflict_values("security", &key)),
            core::mem::size_of_val(&database.query_resolve_row("security", &key, Vec::new())),
        ];
        assert!(
            sizes.iter().all(|size| *size < 4096),
            "conflict dispatch must not embed engine futures: {sizes:?}"
        );
    }

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

#[cfg(feature = "redb")]
fn native_error(error: std::io::Error) -> QueryError {
    QueryError::new(crate::ErrorKind::Storage, error.to_string())
}
