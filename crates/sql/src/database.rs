#[cfg(any(
    feature = "redb",
    feature = "in-memory",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"),
    feature = "sync"
))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use engine::Engine;
#[cfg(any(
    feature = "redb",
    feature = "in-memory",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"),
))]
use engine::EngineTransaction;
#[cfg(feature = "in-memory")]
use engine::InMemoryKernel;
use query::QueryError;
use query::{QueryParams, QueryResult, Statement, Translator};

#[cfg(feature = "sync")]
use sync::{SyncResult, SyncTransport, SynchronizeConfig, SynchronizeError};

use value::{FromRow, Row, Value};

// Keep concrete engine conflict futures out of the caller's poll frame during construction.
#[cfg(any(
    feature = "redb",
    feature = "in-memory",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown")
))]
#[inline(never)]
fn conflict_stage<F: core::future::Future>(make: impl FnOnce() -> F) -> core::pin::Pin<Box<F>> {
    Box::pin(make())
}

#[cfg(any(
    feature = "redb",
    feature = "in-memory",
    all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"),
    feature = "sync"
))]
use engine_automerge::AutomergeRowCodec;
#[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
use engine_indexeddb::IndexedDbKernel;
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
    #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
    IndexedDb(Engine<IndexedDbKernel, AutomergeRowCodec>),
    #[cfg(feature = "redb")]
    File(engine_redb::NativeResource<Engine<RedbKernel, AutomergeRowCodec>>),
    #[cfg(feature = "in-memory")]
    InMemory(Engine<InMemoryKernel, AutomergeRowCodec>),
}

pub(crate) enum EmbeddedTransaction {
    #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
    IndexedDb(EngineTransaction<IndexedDbKernel, AutomergeRowCodec>),
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
    pub(crate) async fn set_read_limits(&mut self, entries: usize, bytes: usize) -> SqlResult<()> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => transaction
                .read()
                .await
                .map_err(native_error)?
                .set_read_limits(entries, bytes)
                .map_err(crate::error::from_engine),
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Self::IndexedDb(transaction) => transaction
                .set_read_limits(entries, bytes)
                .map_err(crate::error::from_engine),
            #[cfg(feature = "in-memory")]
            Self::InMemory(transaction) => transaction
                .set_read_limits(entries, bytes)
                .map_err(crate::error::from_engine),
        }
    }

    pub(crate) async fn row_conflicts(&mut self, table: &str, key: &Row) -> SqlResult<Vec<String>> {
        match self {
            #[cfg(feature = "redb")]
            Self::File(transaction) => {
                let mut transaction = transaction.read().await.map_err(native_error)?;
                conflict_stage(|| transaction.row_conflicts(table, key))
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Self::IndexedDb(transaction) => {
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
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Self::IndexedDb(transaction) => {
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
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Self::IndexedDb(transaction) => transaction
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
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Self::IndexedDb(transaction) => transaction
                .execute(translation.await?)
                .await
                .map_err(crate::error::from_engine),
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
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Self::IndexedDb(transaction) => transaction
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
            Self::File(transaction) => {
                let (_admitted, transaction) = transaction.take().await.map_err(native_error)?;
                transaction
                    .rollback()
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Self::IndexedDb(transaction) => transaction
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
            DatabaseBackend::File(resource) => {
                let admitted = resource.read().await.map_err(native_error)?;
                let $engine = &*admitted;
                $body.map_err(crate::error::from_engine)
            }
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            DatabaseBackend::IndexedDb($engine) => $body.map_err(crate::error::from_engine),
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
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            DatabaseBackend::IndexedDb(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "not a native resource",
            )),
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

    /// Creates the sync-owned lifecycle for committed changes, including remote apply.
    /// `ChangeSubscription::events` returns a stream of committed changes.
    /// Merge with a KV subscription to reconcile both stores in one callback.
    /// Dropping the stream cancels its in-flight work.
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
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            DatabaseBackend::IndexedDb(engine) => {
                engine_automerge::committed_invalidations(engine, work)
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

    /// Opens origin-scoped browser persistence. Never falls back to memory.
    /// Transactions materialize the whole database; stale writes fail without retry.
    #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
    pub async fn open_indexeddb(name: &str) -> SqlResult<Self> {
        let kernel = IndexedDbKernel::open(name)
            .await
            .map_err(crate::error::from_engine)?;
        let engine = Engine::new(kernel, AutomergeRowCodec::new());
        #[cfg(feature = "sync")]
        let changes = engine.subscribe_changes();
        Ok(Self {
            #[cfg(feature = "sync")]
            changes: changes.map(Into::into),
            backend: DatabaseBackend::IndexedDb(engine),
        })
    }

    pub fn client(&self) -> crate::Client {
        crate::Client::embedded(self.clone())
    }

    /// Opens or initializes native persistence. Use `open_existing` for acknowledged resources.
    #[cfg(feature = "redb")]
    pub fn open(path: impl AsRef<std::path::Path>) -> SqlResult<Self> {
        Self::open_native(path.as_ref(), true)
    }

    /// Validates and opens existing native persistence without creating or initializing it.
    #[cfg(feature = "redb")]
    pub fn open_existing(path: impl AsRef<std::path::Path>) -> SqlResult<Self> {
        Self::open_native(path.as_ref(), false)
    }

    #[cfg(feature = "redb")]
    fn open_native(path: &std::path::Path, create: bool) -> SqlResult<Self> {
        let database = engine_redb::open_unretired(path, create).map_err(native_error)?;
        let database = engine_redb::RedbDatabase::new(std::sync::Arc::new(database));
        #[cfg(feature = "sync")]
        let changes = database.subscribe_changes();
        Ok(Self {
            #[cfg(feature = "sync")]
            changes: Some(changes),
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
            changes: Some(changes),
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
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            DatabaseBackend::IndexedDb(engine) => engine
                .transaction()
                .await
                .map(EmbeddedTransaction::IndexedDb)
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
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            DatabaseBackend::IndexedDb(engine) => {
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
                engine
                    .hosting(async {
                        let engine = engine.hosting_backend().await?;
                        crate::Server::unix(path, crate::EngineExecutor::new(engine.clone()))
                            .serve(shutdown)
                            .await
                    })
                    .await?
            }
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            DatabaseBackend::IndexedDb(engine) => {
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
    /// Synchronization. Streams local changes to peer and applies remote changes.
    /// Uses the engine's change subscription for outbound events and applies inbound events via the reconciler.
    #[cfg(feature = "sync")]
    pub async fn synchronize<T: SyncTransport>(
        &self,
        transport: T,
        config: SynchronizeConfig,
    ) -> Result<SyncResult, SynchronizeError<T::Error>> {
        let mut subscription = self
            .sync_changes()
            .map_err(|e| SynchronizeError::Apply(e.to_string()))?;
        let outbound = subscription.events();

        let backend = self.backend.clone();
        let apply_inbound = move |event: sync::ChangeEvent| {
            let table = event.key.table.clone();
            let key_bytes = event.key.key.clone();
            let blob = event.blob.clone();

            let backend = backend.clone();
            async move {
                let row_uuid = uuid::Uuid::from_slice(&key_bytes)
                    .map_err(|e| format!("Invalid row UUID: {}", e))?;
                let change = automerge::Change::from_bytes(blob.clone())
                    .map_err(|e| format!("Invalid change: {}", e))?;
                let change_hash = change.hash();
                let change_id = engine::DocumentChangeId(change_hash.0.to_vec());

                let backend = backend; // move ownership into async block
                let result: Result<(), String> = (async move {
                    match &backend {
                        #[cfg(feature = "redb")]
                        DatabaseBackend::File(resource) => {
                            let engine = resource.read().await.map_err(|e| e.to_string())?;
                            engine
                                .apply_inbound_change(&table, row_uuid, &change_id, &blob)
                                .await
                                .map_err(|e| e.to_string())?;
                            Ok(())
                        }
                        #[cfg(all(
                            feature = "indexeddb",
                            target_arch = "wasm32",
                            target_os = "unknown"
                        ))]
                        DatabaseBackend::IndexedDb(engine) => {
                            engine
                                .apply_inbound_change(&table, row_uuid, &change_id, &blob)
                                .await
                                .map_err(|e| e.to_string())?;
                            Ok(())
                        }
                        #[cfg(feature = "in-memory")]
                        DatabaseBackend::InMemory(engine) => {
                            engine
                                .apply_inbound_change(&table, row_uuid, &change_id, &blob)
                                .await
                                .map_err(|e| e.to_string())?;
                            Ok(())
                        }
                    }
                })
                .await;
                result
            }
        };

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            sync::synchronize(transport, outbound, apply_inbound, config).await
        }
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            sync::synchronize(transport, outbound, apply_inbound, config).await
        }
    }
}

#[cfg(feature = "sync")]
/// Placeholder for deprecated SyncKey API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncKey {
    pub table: String,
    pub row: uuid::Uuid,
}

#[cfg(feature = "sync")]
/// Placeholder for deprecated SyncSelection API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncSelection {
    All,
    Selected {
        tables: Vec<String>,
        rows: Vec<SyncKey>,
    },
}

#[cfg(feature = "sync")]
/// Placeholder for deprecated SyncStageFactory API.
pub struct SyncStageFactory;

#[cfg(feature = "sync")]
/// Placeholder for deprecated SyncCandidateView API.
pub type SyncCandidateView = ();

#[cfg(feature = "sync")]
/// Placeholder for deprecated SyncManifest API.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncManifest;

#[cfg(feature = "sync")]
/// Placeholder for deprecated SyncStateUnit API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncStateUnit;

#[cfg(feature = "sync")]
/// Placeholder for deprecated SessionConfig API.
#[derive(Clone, Debug, Default)]
pub struct SessionConfig {
    pub max_session_bytes: usize,
}

#[cfg(feature = "sync")]
/// Placeholder for deprecated SyncRole API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncRole {
    Leader,
    Follower,
}

#[cfg(feature = "sync")]
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
