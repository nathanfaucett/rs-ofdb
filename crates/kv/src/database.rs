use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "redb")]
use std::{path::Path, sync::Arc};

use btree::BTree;
#[cfg(feature = "in-memory")]
use btree::InMemoryBTree;
use kv_store::KvStore;

use crate::Error;

enum Storage {
    #[cfg(feature = "in-memory")]
    Memory(KvStore<InMemoryBTree<Vec<u8>, Vec<u8>>>),
    #[cfg(feature = "redb")]
    Redb(KvStore<btree_redb::RedbByteBTree>),
}

pub struct Database {
    storage: Storage,
}

impl Database {
    #[cfg(feature = "in-memory")]
    pub fn in_memory() -> Self {
        Self {
            storage: Storage::Memory(KvStore::new(InMemoryBTree::new(), uuid_timestamp)),
        }
    }

    #[cfg(feature = "redb")]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let db = if path.exists() {
            redb::Database::open(path)
                .map_err(|error| Error::from(btree::BTreeError::custom(error)))?
        } else {
            redb::Database::create(path)
                .map_err(|error| Error::from(btree::BTreeError::custom(error)))?
        };
        let definition = btree_redb::table_definition::<btree_redb::Bytes, Vec<u8>>("ofdb-kv");
        let transaction = db
            .begin_write()
            .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
        transaction
            .open_table(definition)
            .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
        transaction
            .commit()
            .map_err(|error| Error::from(btree::BTreeError::custom(error)))?;
        let tree = btree_redb::RedbByteBTree::new(Arc::new(db), "ofdb-kv");
        Ok(Self {
            storage: Storage::Redb(KvStore::new(tree, uuid_timestamp)),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let now = current_time_millis();
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => read_get(store, key, now).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => read_get(store, key, now).await,
        }
    }

    pub async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        expires_at: Option<i64>,
    ) -> Result<(), Error> {
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => write_set(store, key, value, expires_at).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => write_set(store, key, value, expires_at).await,
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => write_delete(store, key).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => write_delete(store, key).await,
        }
    }

    pub async fn scan(&self, start: &str, end: &str) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let now = current_time_millis();
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => read_scan(store, start, end, now).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => read_scan(store, start, end, now).await,
        }
    }

    pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let now = current_time_millis();
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => read_prefix(store, prefix, now).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => read_prefix(store, prefix, now).await,
        }
    }

    pub async fn scan_all(&self) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let now = current_time_millis();
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => read_all(store, now).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => read_all(store, now).await,
        }
    }

    #[cfg(feature = "server")]
    pub async fn serve_tcp(
        &self,
        address: std::net::SocketAddr,
        shutdown: impl core::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => {
                kv_server::Server::tcp(address, store.clone())
                    .serve(shutdown)
                    .await
            }
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                kv_server::Server::tcp(address, store.clone())
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
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => {
                kv_server::Server::unix(path, store.clone())
                    .serve(shutdown)
                    .await
            }
            #[cfg(feature = "redb")]
            Storage::Redb(store) => {
                kv_server::Server::unix(path, store.clone())
                    .serve(shutdown)
                    .await
            }
        }
    }

    #[cfg(feature = "sync")]
    pub async fn synchronize<T: kv_sync::SyncTransport>(
        &self,
        transport: &mut T,
        role: kv_sync::SyncRole,
        config: kv_sync::Config,
    ) -> Result<(), kv_sync::Error<T::Error>> {
        match &self.storage {
            #[cfg(feature = "in-memory")]
            Storage::Memory(store) => kv_sync::synchronize(store, transport, role, config).await,
            #[cfg(feature = "redb")]
            Storage::Redb(store) => kv_sync::synchronize(store, transport, role, config).await,
        }
    }
}

async fn read_get<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    key: &str,
    now: i64,
) -> Result<Option<Vec<u8>>, Error> {
    let transaction = store.transaction().await?;
    let result = transaction.get(key, now).await?;
    transaction.rollback().await?;
    Ok(result)
}

async fn write_set<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    key: &str,
    value: Vec<u8>,
    expires_at: Option<i64>,
) -> Result<(), Error> {
    let mut transaction = store.transaction().await?;
    transaction.set(key, value, expires_at).await?;
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
) -> Result<Vec<(String, Vec<u8>)>, Error> {
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
) -> Result<Vec<(String, Vec<u8>)>, Error> {
    let transaction = store.transaction().await?;
    let result = transaction.scan_prefix(prefix, now).await?;
    transaction.rollback().await?;
    Ok(result)
}

async fn read_all<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    now: i64,
) -> Result<Vec<(String, Vec<u8>)>, Error> {
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
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[cfg(all(test, feature = "in-memory"))]
mod tests {
    use futures::executor::block_on;

    use super::Database;

    #[test]
    fn facade_preserves_expiry_and_scan_contract() {
        block_on(async {
            let database = Database::in_memory();
            database
                .set("a", vec![1], Some(0))
                .await
                .expect("set expired value");
            assert_eq!(database.get("a").await.expect("read expired value"), None);
            database
                .set("a", vec![1], None)
                .await
                .expect("clear expiry");
            database
                .set("ab", vec![2], None)
                .await
                .expect("set second value");
            assert_eq!(
                database.scan("a", "ab").await.expect("exclusive range"),
                vec![("a".into(), vec![1])]
            );
            assert_eq!(
                database
                    .scan_prefix("")
                    .await
                    .expect("empty prefix matches all")
                    .len(),
                2
            );
            database
                .delete("missing")
                .await
                .expect("delete missing key");
        });
    }
}
