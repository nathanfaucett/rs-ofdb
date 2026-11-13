use std::{
    any::Any,
    collections::BTreeMap,
    io,
    path::Path,
    sync::{Arc, Mutex, Weak},
};

use async_lock::Mutex as AsyncMutex;
use redb::{ReadableDatabase, TableDefinition, TableHandle};

use crate::{RedbByteBTree, RedbDatabase};

const RETIRED: TableDefinition<&str, bool> = TableDefinition::new("ofdb.shared.retired");
type Owners = BTreeMap<String, Weak<dyn Any + Send + Sync>>;

#[derive(Clone)]
pub struct SharedDatabase {
    database: Arc<redb::Database>,
    admission: Arc<AsyncMutex<()>>,
    owners: Arc<Mutex<Owners>>,
}

impl SharedDatabase {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::from_database(redb::Database::create(path).map_err(io::Error::other)?)
    }

    pub fn in_memory() -> io::Result<Self> {
        Self::from_database(
            redb::Database::builder()
                .create_with_backend(redb::backends::InMemoryBackend::new())
                .map_err(io::Error::other)?,
        )
    }

    pub fn from_database(database: redb::Database) -> io::Result<Self> {
        let transaction = database.begin_write().map_err(io::Error::other)?;
        transaction.open_table(RETIRED).map_err(io::Error::other)?;
        transaction.commit().map_err(io::Error::other)?;
        Ok(Self {
            database: Arc::new(database),
            admission: Arc::new(AsyncMutex::new(())),
            owners: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Process-local identity for caches; never a persistent resource identity.
    pub fn instance_id(&self) -> usize {
        Arc::as_ptr(&self.database) as usize
    }

    pub fn namespace(&self, name: &str) -> io::Result<SharedNamespace> {
        if name.is_empty() || name.len() > 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid storage namespace",
            ));
        }
        let namespace = SharedNamespace {
            host: self.clone(),
            prefix: format!("ofdb.ns.{}:{name}:", name.len()),
        };
        namespace.check()?;
        Ok(namespace)
    }
}

#[derive(Clone)]
pub struct SharedNamespace {
    host: SharedDatabase,
    prefix: String,
}

impl SharedNamespace {
    pub fn table(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    pub fn database(&self) -> RedbDatabase {
        RedbDatabase::with_admission(self.host.database.clone(), self.host.admission.clone())
    }

    pub fn tree(&self, name: &str) -> RedbByteBTree {
        RedbByteBTree::with_admission(
            self.host.database.clone(),
            self.table(name),
            self.host.admission.clone(),
        )
    }

    pub fn check(&self) -> io::Result<()> {
        let transaction = self.host.database.begin_read().map_err(io::Error::other)?;
        let table = transaction.open_table(RETIRED).map_err(io::Error::other)?;
        if table
            .get(self.prefix.as_str())
            .map_err(io::Error::other)?
            .is_some()
        {
            return Err(crate::resource::retired_error());
        }
        Ok(())
    }

    pub fn owner<T: Any + Send + Sync>(&self, build: impl FnOnce() -> T) -> io::Result<Arc<T>> {
        let mut owners = self
            .host
            .owners
            .lock()
            .expect("shared namespace owners lock");
        self.check()?;
        if let Some(owner) = owners.get(&self.prefix).and_then(Weak::upgrade) {
            return owner.downcast().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "namespace backend type mismatch",
                )
            });
        }
        owners.retain(|_, owner| owner.strong_count() != 0);
        let owner = Arc::new(build());
        let erased: Arc<dyn Any + Send + Sync> = owner.clone();
        owners.insert(self.prefix.clone(), Arc::downgrade(&erased));
        Ok(owner)
    }

    pub async fn remove_table(&self, name: &str) -> io::Result<()> {
        let _admission = self.host.admission.lock().await;
        let transaction = self.host.database.begin_write().map_err(io::Error::other)?;
        transaction
            .delete_table(crate::table_definition::<crate::Bytes, Vec<u8>>(
                &self.table(name),
            ))
            .map_err(io::Error::other)?;
        transaction.commit().map_err(io::Error::other)
    }

    /// Low-level cleanup. Fence and drain resource owners before calling this method.
    pub async fn retire(&self) -> io::Result<()> {
        let _admission = self.host.admission.lock().await;
        let transaction = self.host.database.begin_write().map_err(io::Error::other)?;
        transaction
            .open_table(RETIRED)
            .map_err(io::Error::other)?
            .insert(self.prefix.as_str(), true)
            .map_err(io::Error::other)?;
        let tables = transaction
            .list_tables()
            .map_err(io::Error::other)?
            .filter(|table| table.name().starts_with(&self.prefix))
            .collect::<Vec<_>>();
        for table in tables {
            transaction.delete_table(table).map_err(io::Error::other)?;
        }
        transaction.commit().map_err(io::Error::other)
    }
}
