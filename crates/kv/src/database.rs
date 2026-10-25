#[cfg(feature = "redb")]
use std::{path::Path, sync::Arc};

use btree::BTree;
#[cfg(feature = "in-memory")]
use btree::InMemoryBTree;
use kv_store::KvStore;

use value::Value;

use crate::Error;

#[derive(Clone)]
pub(super) enum Storage {
    #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
    IndexedDb(KvStore<btree_indexeddb::ByteBTree>),
    #[cfg(feature = "in-memory")]
    Memory(KvStore<InMemoryBTree<Vec<u8>, Vec<u8>>>),
    #[cfg(feature = "redb")]
    Redb(btree_redb::NativeResource<KvStore<btree_redb::RedbByteBTree>>),
}

pub struct Database {
    pub(super) storage: Storage,
    #[cfg(feature = "sync")]
    changes: Option<kv_sync::ChangeSubscription>,
    #[cfg(feature = "sync")]
    storage_table: Option<String>,
}

impl Database {
    /// Permanently fences issued handles and opens, drains admitted operations,
    /// cancels sessions, rolls back idle transactions, then removes this dedicated file.
    /// Native hosting is not managed: an owner that attempted native hosting returns Unsupported.
    /// Unix-only; callers must exclusively own the pathname and marker namespace.
    /// The permanent `.ofdb-retired` marker is retained, including after success.
    #[cfg(feature = "redb")]
    pub async fn retire_and_remove(&self) -> std::io::Result<()> {
        match &self.storage {
            Storage::Redb(resource) => resource.retire_and_remove().await,
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "not a native resource",
            )),
            #[cfg(feature = "in-memory")]
            Storage::Memory(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "not a native resource",
            )),
        }
    }

    /// Resumes cleanup of a permanently retired file, refusing live native owners.
    #[cfg(feature = "redb")]
    pub fn remove_retired(path: impl AsRef<Path>) -> std::io::Result<()> {
        btree_redb::remove_retired(path)
    }

    /// Creates an owned query-only handle that shares this database's storage.
    /// The client remains usable after this handle is dropped.
    pub fn client(&self) -> crate::Client {
        crate::Client::embedded(self.clone_shared())
    }

    /// Creates the sync-owned lifecycle for committed changes, including remote apply.
    /// `ChangeSubscription::run` owns initial sync, coalescing, races and retry backoff.
    /// Merge with a SQL subscription to reconcile both stores in one callback.
    /// Dropping the runner cancels its in-flight work. Returns Unsupported for memory.
    #[cfg(feature = "sync")]
    pub fn sync_changes(&self) -> std::io::Result<kv_sync::ChangeSubscription> {
        self.changes.clone().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "continuous sync requires a redb database",
            )
        })
    }

    pub(crate) fn clone_shared(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            #[cfg(feature = "sync")]
            changes: self.changes.clone(),
            #[cfg(feature = "sync")]
            storage_table: self.storage_table.clone(),
        }
    }

    /// Opens durable browser KV storage; never falls back to memory.
    /// Each operation materializes a whole-database snapshot. Mutating stale
    /// transactions fail with Conflict and are not replayed. Issued commits
    /// remain observed after cancellation; timeout does not prove rollback.
    #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
    pub async fn open_indexeddb(name: &str) -> Result<Self, Error> {
        let database = btree_indexeddb::Database::open(name, "kv").await?;
        Ok(Self {
            storage: Storage::IndexedDb(KvStore::new(
                btree_indexeddb::ByteBTree::new(database, "ofdb-kv"),
                uuid_timestamp,
            )),
            #[cfg(feature = "sync")]
            changes: None,
            #[cfg(feature = "sync")]
            storage_table: None,
        })
    }

    #[cfg(feature = "in-memory")]
    pub fn in_memory() -> Self {
        Self {
            storage: Storage::Memory(KvStore::new(InMemoryBTree::new(), uuid_timestamp)),
            #[cfg(feature = "sync")]
            changes: None,
            #[cfg(feature = "sync")]
            storage_table: None,
        }
    }

    #[cfg(feature = "redb")]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with_table(path, "ofdb-kv")
    }

    #[cfg(feature = "redb")]
    pub fn open_with_table(
        path: impl AsRef<Path>,
        table: impl Into<String>,
    ) -> Result<Self, Error> {
        let table = table.into();
        let path = path.as_ref();
        let existed = path.exists();
        let db = btree_redb::open_unretired(path, !existed)
            .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
        let definition = btree_redb::table_definition::<btree_redb::Bytes, Vec<u8>>(&table);
        if existed {
            use redb::ReadableDatabase;

            let transaction = db
                .begin_read()
                .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
            transaction
                .open_table(definition)
                .map_err(|_| Error::Storage {
                    kind: crate::ErrorKind::Storage,
                    message: "unsupported metadata store schema".into(),
                })?;
        }
        let transaction = db
            .begin_write()
            .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
        transaction
            .open_table(definition)
            .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
        transaction
            .commit()
            .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
        let tree = btree_redb::RedbByteBTree::new(Arc::new(db), table.clone());
        #[cfg(feature = "sync")]
        let changes = tree.subscribe_changes();
        Ok(Self {
            #[cfg(feature = "sync")]
            changes: Some(changes.into()),
            #[cfg(feature = "sync")]
            storage_table: Some(table),
            storage: Storage::Redb(
                btree_redb::NativeResource::new(path, KvStore::new(tree, uuid_timestamp))
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?,
            ),
        })
    }

    pub(crate) async fn query_get(&self, key: &str) -> Result<Option<Value>, Error> {
        let now = current_time_millis();
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => read_get(store, key, now).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => read_get(store, key, now).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                read_get(&*store, key, now).await
            }
        }
    }

    pub(crate) async fn query_set(
        &self,
        key: &str,
        value: Value,
        expires_at: Option<i64>,
    ) -> Result<(), Error> {
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => write_set(store, key, value, expires_at).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => write_set(store, key, value, expires_at).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                write_set(&*store, key, value, expires_at).await
            }
        }
    }

    pub(crate) async fn query_delete(&self, key: &str) -> Result<(), Error> {
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => write_delete(store, key).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => write_delete(store, key).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                write_delete(&*store, key).await
            }
        }
    }

    pub(crate) async fn query_scan(
        &self,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Value)>, Error> {
        let now = current_time_millis();
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => read_scan(store, start, end, now).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => read_scan(store, start, end, now).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                read_scan(&*store, start, end, now).await
            }
        }
    }

    pub(crate) async fn query_scan_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Value)>, Error> {
        let now = current_time_millis();
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => read_prefix(store, prefix, now).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => read_prefix(store, prefix, now).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                read_prefix(&*store, prefix, now).await
            }
        }
    }

    pub(crate) async fn query_scan_all(&self) -> Result<Vec<(String, Value)>, Error> {
        let now = current_time_millis();
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => read_all(store, now).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => read_all(store, now).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                read_all(&*store, now).await
            }
        }
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
        self.serve_tcp_with_client_ca(address, identity, None, shutdown)
            .await
    }

    #[cfg(feature = "server")]
    pub async fn serve_tcp_with_client_ca(
        &self,
        address: std::net::SocketAddr,
        identity: Option<(Vec<u8>, Vec<u8>)>,
        client_ca: Option<Vec<u8>>,
        shutdown: impl core::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native hosting is unavailable for IndexedDB",
            )
            .into()),
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => {
                let server = kv_server::Server::tcp(address, store.clone());
                let server = match identity {
                    Some((certificate, private_key)) => {
                        server.tls_identity(certificate, private_key)
                    }
                    None => server,
                };
                let server = match client_ca.clone() {
                    Some(certificate) => server.tls_client_ca(certificate),
                    None => server,
                };
                server.serve(shutdown).await
            }
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                store
                    .hosting(async {
                        let store = store.hosting_backend().await?;
                        let server = kv_server::Server::tcp(address, store.clone());
                        let server = match identity {
                            Some((certificate, private_key)) => {
                                server.tls_identity(certificate, private_key)
                            }
                            None => server,
                        };
                        let server = match client_ca {
                            Some(certificate) => server.tls_client_ca(certificate),
                            None => server,
                        };
                        server.serve(shutdown).await
                    })
                    .await?
            }
        }
    }

    #[cfg(all(feature = "server", unix))]
    pub async fn serve_unix(
        &self,
        path: impl Into<std::path::PathBuf>,
        shutdown: impl core::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => {
                kv_server::Server::unix(path, store.clone())
                    .serve(shutdown)
                    .await
            }
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                store
                    .hosting(async {
                        let store = store.hosting_backend().await?;
                        kv_server::Server::unix(path, store.clone())
                            .serve(shutdown)
                            .await
                    })
                    .await?
            }
        }
    }

    /// Exports winning generations, including tombstones, from one store snapshot.
    #[cfg(feature = "sync")]
    pub async fn export_snapshots(&self) -> Result<Vec<crate::KvSnapshot>, Error> {
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => export_snapshots(store).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => export_snapshots(store).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                export_snapshots(&*store).await
            }
        }
    }

    /// Imports all snapshots atomically. A failed import commits no changes.
    /// Caller must authorize this direct atomic apply; production received operations
    /// should use `synchronize_checked` for per-batch authorization/cancellation.
    #[cfg(feature = "sync")]
    pub async fn import_snapshots(&self, snapshots: Vec<crate::KvSnapshot>) -> Result<(), Error> {
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => import_snapshots(store, snapshots).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => import_snapshots(store, snapshots).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                import_snapshots(&*store, snapshots).await
            }
        }
    }

    /// Checks caller authorization/cancellation before each complete incoming data batch.
    /// Earlier commits remain on denial. Abort delivery does not wait for the peer;
    /// discard the transport after failure.
    #[cfg(feature = "sync")]
    pub async fn synchronize_checked<T, Q, U>(
        &self,
        transport: &mut T,
        role: crate::SyncRole,
        config: crate::SyncConfig,
        check: Q,
    ) -> Result<(), crate::SyncError<T::Error>>
    where
        T: crate::SyncTransport,
        Q: FnMut() -> U,
        U: core::future::Future<Output = Result<(), String>>,
    {
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => {
                kv_sync::synchronize_checked(store, transport, role, config, check).await
            }
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => {
                kv_sync::synchronize_checked(store, transport, role, config, check).await
            }
            #[cfg(feature = "redb")]
            Storage::Redb(store) => store
                .session(async {
                    let store = store
                        .read()
                        .await
                        .map_err(|error| crate::SyncError::PreApply(error.to_string()))?;
                    kv_sync::synchronize_checked(&*store, transport, role, config, check).await
                })
                .await
                .map_err(|error| crate::SyncError::PreApply(error.to_string()))?,
        }
    }

    /// Map a token from this database's own (unmerged) subscription.
    #[cfg(feature = "sync")]
    pub async fn sync_invalidations(
        &self,
        work: &crate::ChangeWork,
    ) -> Result<Vec<crate::KvInvalidation>, Error> {
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(_) => {
                let _ = work;
                Ok(vec![crate::KvInvalidation::All])
            }
            #[cfg(feature = "in-memory")]
            Storage::Memory(_) => {
                let _ = work;
                Ok(vec![crate::KvInvalidation::All])
            }
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                let store = store
                    .read()
                    .await
                    .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
                Ok(store.committed_invalidations(
                    self.storage_table
                        .as_deref()
                        .expect("native database retains its physical table"),
                    work,
                ))
            }
        }
    }

    /// Reconcile one peer's selected work, acknowledging only on success.
    /// Keep one unmerged subscription per peer, created anew on reconnect. Call even
    /// with no local work when a remote wakeup requests reconciliation: local changes
    /// cannot detect remote-only writes. Memory uses explicit broad synchronization.
    #[cfg(feature = "sync")]
    pub async fn synchronize_selected_checked<T, Q, U>(
        &self,
        changes: &mut crate::ChangeSubscription,
        transport: &mut T,
        role: crate::SyncRole,
        config: crate::SyncConfig,
        check: Q,
    ) -> Result<(), crate::SyncError<T::Error>>
    where
        T: crate::SyncTransport,
        Q: FnMut() -> U,
        U: core::future::Future<Output = Result<(), String>>,
    {
        let work = changes.select();
        let invalidations = match &work {
            Some(work) => self
                .sync_invalidations(work)
                .await
                .map_err(|error| crate::SyncError::Storage(error.to_string()))?,
            None => Vec::new(),
        };
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => {
                kv_sync::synchronize_selected_checked(
                    store,
                    &invalidations,
                    transport,
                    role,
                    config,
                    check,
                )
                .await?
            }
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => {
                kv_sync::synchronize_selected_checked(
                    store,
                    &invalidations,
                    transport,
                    role,
                    config,
                    check,
                )
                .await?
            }
            #[cfg(feature = "redb")]
            Storage::Redb(store) => store
                .session(async {
                    let store = store
                        .read()
                        .await
                        .map_err(|error| crate::SyncError::PreApply(error.to_string()))?;
                    kv_sync::synchronize_selected_checked(
                        &*store,
                        &invalidations,
                        transport,
                        role,
                        config,
                        check,
                    )
                    .await
                })
                .await
                .map_err(|error| crate::SyncError::PreApply(error.to_string()))??,
        }
        if let Some(work) = work
            && !changes.acknowledge(work)
        {
            return Err(crate::SyncError::Storage(
                "selected work belongs to a different subscription".into(),
            ));
        }
        Ok(())
    }

    /// Unchecked sync: caller owns admission; prefer `synchronize_checked`.
    #[cfg(feature = "sync")]
    pub async fn synchronize<T: kv_sync::SyncTransport>(
        &self,
        transport: &mut T,
        role: kv_sync::SyncRole,
        config: kv_sync::Config,
    ) -> Result<(), kv_sync::Error<T::Error>> {
        match &self.storage {
            #[cfg(all(feature = "indexeddb", target_arch = "wasm32", target_os = "unknown"))]
            Storage::IndexedDb(store) => kv_sync::synchronize(store, transport, role, config).await,
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => kv_sync::synchronize(store, transport, role, config).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => store
                .session(async {
                    let store = store
                        .read()
                        .await
                        .map_err(|error| crate::SyncError::PreApply(error.to_string()))?;
                    kv_sync::synchronize(&*store, transport, role, config).await
                })
                .await
                .map_err(|error| crate::SyncError::PreApply(error.to_string()))?,
        }
    }
}

async fn read_get<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    key: &str,
    now: i64,
) -> Result<Option<Value>, Error> {
    let transaction = store.transaction().await?;
    let result = transaction.get(key, now).await?;
    transaction.rollback().await?;
    Ok(result)
}

async fn write_set<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    key: &str,
    value: Value,
    expires_at: Option<i64>,
) -> Result<(), Error> {
    let mut transaction = store.transaction().await?;
    transaction.set(key, value, expires_at).await?;
    transaction.commit().await?;
    Ok(())
}

#[cfg(feature = "sync")]
async fn export_snapshots<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
) -> Result<Vec<crate::KvSnapshot>, Error> {
    let transaction = store.transaction().await?;
    let snapshots = transaction.export_snapshots().await?;
    transaction.rollback().await?;
    Ok(snapshots
        .into_iter()
        .map(|(key, payload)| crate::KvSnapshot { key, payload })
        .collect())
}

#[cfg(feature = "sync")]
async fn import_snapshots<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    snapshots: Vec<crate::KvSnapshot>,
) -> Result<(), Error> {
    let mut transaction = store.transaction().await?;
    for snapshot in snapshots {
        if let Err(error) = transaction
            .import_snapshot((snapshot.key, snapshot.payload))
            .await
        {
            transaction.rollback().await?;
            return Err(error.into());
        }
    }
    transaction.commit().await?;
    Ok(())
}

async fn write_delete<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    key: &str,
) -> Result<(), Error> {
    let mut transaction = store.transaction().await?;
    transaction.delete(key).await?;
    transaction.commit().await?;
    Ok(())
}

async fn read_scan<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    start: &str,
    end: &str,
    now: i64,
) -> Result<Vec<(String, Value)>, Error> {
    let transaction = store.transaction().await?;
    let result = transaction
        .scan(start.to_owned()..end.to_owned(), now)
        .await?;
    transaction.rollback().await?;
    Ok(result)
}

async fn read_prefix<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    prefix: &str,
    now: i64,
) -> Result<Vec<(String, Value)>, Error> {
    let transaction = store.transaction().await?;
    let result = transaction.scan_prefix(prefix, now).await?;
    transaction.rollback().await?;
    Ok(result)
}

async fn read_all<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    now: i64,
) -> Result<Vec<(String, Value)>, Error> {
    let transaction = store.transaction().await?;
    let result = transaction.scan_all(now).await?;
    transaction.rollback().await?;
    Ok(result)
}

fn uuid_timestamp() -> uuid::Timestamp {
    uuid::Uuid::now_v7()
        .get_timestamp()
        .expect("UUIDv7 has a timestamp")
}

fn current_time_millis() -> i64 {
    platform::unix_millis()
}

#[cfg(all(test, feature = "in-memory"))]
mod tests {
    use futures::executor::block_on;
    use value::Value;

    use super::Database;

    #[test]
    fn facade_preserves_expiry_and_scan_contract() {
        block_on(async {
            let database = Database::in_memory();
            let client = database.client();
            client
                .set("a", Value::Blob(vec![1]), Some(0))
                .await
                .expect("set expired value");
            assert_eq!(client.get("a").await.expect("read expired value"), None);
            client
                .set("a", Value::Blob(vec![1]), None)
                .await
                .expect("clear expiry");
            client
                .set("ab", Value::Blob(vec![2]), None)
                .await
                .expect("set second value");
            assert_eq!(
                client.scan("a", "ab").await.expect("exclusive range"),
                vec![("a".into(), Value::Blob(vec![1]))]
            );
            assert_eq!(
                client
                    .scan_prefix("")
                    .await
                    .expect("empty prefix matches all")
                    .len(),
                2
            );
            client.delete("missing").await.expect("delete missing key");
        });
    }
}
