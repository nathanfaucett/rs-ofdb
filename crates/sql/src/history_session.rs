use super::{Database, DatabaseBackend};
use alloc::{boxed::Box, string::ToString};
use history::{SessionConfig, SyncError, SyncResult, SyncRole, SyncTransport};
use value::Value;

#[inline(never)]
fn sync_stage<F: core::future::Future>(make: impl FnOnce() -> F) -> core::pin::Pin<Box<F>> {
    Box::pin(make())
}

impl Database {
    pub async fn history_manifest(&self) -> Result<history::SyncManifest, crate::Error> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(resource) => {
                let guard = resource
                    .read()
                    .await
                    .map_err(crate::database::native_error)?;
                let engine = &*guard;
                history::sync_manifest_for(engine)
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => history::sync_manifest_for(engine)
                .await
                .map_err(crate::error::from_engine),
        }
    }
    pub async fn export_history(
        &self,
    ) -> Result<alloc::vec::Vec<history::SyncStateUnit>, crate::Error> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(resource) => {
                let guard = resource
                    .read()
                    .await
                    .map_err(crate::database::native_error)?;
                let engine = &*guard;
                history::export_sync_state_for(engine)
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => history::export_sync_state_for(engine)
                .await
                .map_err(crate::error::from_engine),
        }
    }
    /// Trusted native restore. The caller must authorize the payload; network owners must use checked exchange.
    pub async fn apply_history(&self, unit: history::SyncStateUnit) -> Result<(), crate::Error> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(resource) => {
                let guard = resource
                    .read()
                    .await
                    .map_err(crate::database::native_error)?;
                let engine = &*guard;
                history::apply_sync_state_for(engine, unit)
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => history::apply_sync_state_for(engine, unit)
                .await
                .map_err(crate::error::from_engine),
        }
    }
    /// Atomically restores a trusted catalog and its rows, without an untrusted peer admission policy.
    pub async fn import_history(
        &self,
        units: alloc::vec::Vec<history::SyncStateUnit>,
    ) -> Result<(), crate::Error> {
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(resource) => {
                let guard = resource
                    .read()
                    .await
                    .map_err(crate::database::native_error)?;
                let engine = &*guard;
                history::apply_sync_state_batch_for(engine, units)
                    .await
                    .map_err(crate::error::from_engine)
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => history::apply_sync_state_batch_for(engine, units)
                .await
                .map_err(crate::error::from_engine),
        }
    }
    fn native_stage_factory(
        &self,
        directory: impl AsRef<std::path::Path>,
    ) -> history::FileSyncStageFactory {
        let factory = history::FileSyncStageFactory::new(directory);
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
    pub async fn exchange_history<T, V, C, Q, U>(
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
        V: Fn(&str, &str, &Value) -> Result<(), alloc::string::String>
            + platform::MaybeSend
            + platform::MaybeSync,
        C: Fn(&history::SyncCandidateView) -> Result<(), alloc::string::String> + Send + Sync,
        Q: FnMut() -> U + platform::MaybeSend,
        U: core::future::Future<Output = Result<(), alloc::string::String>> + platform::MaybeSend,
    {
        let factory = self.native_stage_factory(directory);
        let factory = &factory;
        match &self.backend {
            #[cfg(feature = "redb")]
            DatabaseBackend::File(engine) => engine
                .session(sync_stage(|| async {
                    let engine = engine
                        .read()
                        .await
                        .map_err(|error| SyncError::PreApply(error.to_string()))?;

                    sync_stage(|| {
                        history::synchronize_tables_with_stage_factory_candidate_validated_checked(
                            &*engine,
                            transport,
                            config,
                            role,
                            factory,
                            tables,
                            validate,
                            &validate_candidate,
                            check,
                        )
                    })
                    .await
                }))
                .await
                .map_err(|error| SyncError::PreApply(error.to_string()))?,
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            DatabaseBackend::IndexedDb(engine) => {
                sync_stage(|| {
                    history::synchronize_tables_with_stage_factory_candidate_validated_checked(
                        engine,
                        transport,
                        config,
                        role,
                        factory,
                        tables,
                        validate,
                        &validate_candidate,
                        check,
                    )
                })
                .await
            }
            #[cfg(feature = "in-memory")]
            DatabaseBackend::InMemory(engine) => {
                sync_stage(|| {
                    history::synchronize_tables_with_stage_factory_candidate_validated_checked(
                        engine,
                        transport,
                        config,
                        role,
                        factory,
                        tables,
                        validate,
                        &validate_candidate,
                        check,
                    )
                })
                .await
            }
        }
    }
}
