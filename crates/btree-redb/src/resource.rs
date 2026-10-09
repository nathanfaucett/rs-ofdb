use std::{
    fs,
    io::{self, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, Weak},
};

use async_lock::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
use futures::future::{AbortHandle, Abortable};

use crate::stage_cleanup::{NativeStageTracker, OwnedFile};

#[derive(Clone)]
pub struct NativeResource<R> {
    inner: Arc<Inner<R>>,
}

struct Inner<R> {
    path: PathBuf,
    identity: Option<String>,
    stages: NativeStageTracker,
    backend: RwLock<Option<R>>,
    state: StdMutex<State>,
}

#[derive(Default)]
struct State {
    retired: bool,
    hosted: bool,
    transactions: Vec<Weak<dyn Erase + Send + Sync>>,
    sessions: Vec<Weak<AbortHandle>>,
}

trait Erase {
    fn erase(&self) -> bool;
}

impl<T: Send> Erase for Mutex<Option<T>> {
    fn erase(&self) -> bool {
        if let Some(mut transaction) = self.try_lock() {
            transaction.take();
            true
        } else {
            false
        }
    }
}

pub fn retired_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "native resource is retired",
    )
}

fn marker(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".ofdb-retired");
    PathBuf::from(name)
}

pub fn check_not_retired(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resource must be a regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    match fs::symlink_metadata(marker(path)) {
        Ok(_) => Err(retired_error()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::File::open(parent)?.sync_all()
}

pub(crate) fn file_identity(path: &Path) -> io::Result<String> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "resource must be a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(format!(
            "ofdb-retired-v1 {} {}\n",
            metadata.dev(),
            metadata.ino()
        ))
    }
    #[cfg(not(unix))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native retirement requires Unix file identity",
        ))
    }
}

pub(crate) fn remove_owned_file(file: &OwnedFile) -> io::Result<()> {
    match file_identity(&file.path) {
        Ok(actual) if actual == file.identity => fs::remove_file(&file.path)?,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "owned file identity changed",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return match sync_parent(&file.path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                result => result,
            };
        }
        Err(error) => return Err(error),
    }
    sync_parent(&file.path)
}

fn read_record(path: &Path) -> io::Result<(String, Vec<OwnedFile>)> {
    let marker_path = marker(path);
    if !fs::symlink_metadata(&marker_path)?.is_file()
        || fs::symlink_metadata(&marker_path)?.file_type().is_symlink()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "retirement marker must be a regular file",
        ));
    }
    let bytes = fs::read(marker_path)?;
    let mut fields = bytes.split(|byte| *byte == 0);
    let header = std::str::from_utf8(fields.next().expect("record contains header"))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if !header.starts_with("ofdb-retired-v1 ") || header.split_whitespace().count() != 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid retirement marker",
        ));
    }
    let mut stages = Vec::new();
    while let Some(identity) = fields.next() {
        let identity = std::str::from_utf8(identity)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            .to_owned();
        let path = fields.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated retirement marker")
        })?;
        #[cfg(unix)]
        let path = {
            use std::os::unix::ffi::OsStringExt;
            PathBuf::from(std::ffi::OsString::from_vec(path.to_vec()))
        };
        #[cfg(not(unix))]
        let path = PathBuf::from(
            std::str::from_utf8(path)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
        );
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stage pathname must be absolute",
            ));
        }
        stages.push(OwnedFile { identity, path });
    }
    Ok((header.to_owned(), stages))
}

/// Resumes exact-file cleanup. A native lock probe refuses cleanup while another
/// owner still holds persistence. The permanent marker fences ordinary opens.
pub fn remove_retired(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let (identity, stages) = read_record(path)?;
    let persistence = OwnedFile {
        path: path.to_path_buf(),
        identity,
    };
    match file_identity(path) {
        Ok(actual) if actual == persistence.identity => {
            let probe = redb::Database::open(path).map_err(io::Error::other)?;
            drop(probe);
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "resource file identity changed",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    for stage in stages {
        remove_owned_file(&stage)?;
    }
    remove_owned_file(&persistence)
}

impl<R: Clone> NativeResource<R> {
    pub fn new(path: impl AsRef<Path>, backend: R) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let path = std::path::absolute(path)?;
        let identity = match file_identity(&path) {
            Ok(identity) => Some(identity),
            Err(error) if error.kind() == io::ErrorKind::Unsupported => None,
            Err(error) => return Err(error),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                path,
                identity,
                stages: NativeStageTracker::default(),
                backend: RwLock::new(Some(backend)),
                state: StdMutex::new(State::default()),
            }),
        })
    }

    pub fn stage_tracker(&self) -> NativeStageTracker {
        self.inner.stages.clone()
    }

    pub async fn read(&self) -> io::Result<ResourceGuard<'_, R>> {
        self.admit().await
    }

    async fn admit(&self) -> io::Result<ResourceGuard<'_, R>> {
        if self
            .inner
            .state
            .lock()
            .expect("resource state lock")
            .retired
        {
            return Err(retired_error());
        }
        let guard = self.inner.backend.read().await;
        if self
            .inner
            .state
            .lock()
            .expect("resource state lock")
            .retired
            || guard.is_none()
        {
            return Err(retired_error());
        }
        Ok(ResourceGuard { backend: guard })
    }

    pub fn transaction<T: Send + 'static>(&self, transaction: T) -> NativeTransaction<R, T> {
        let slot = Arc::new(Mutex::new(Some(transaction)));
        let erased: Arc<dyn Erase + Send + Sync> = slot.clone();
        let mut state = self.inner.state.lock().expect("resource state lock");
        state
            .transactions
            .retain(|transaction| transaction.strong_count() != 0);
        state.transactions.push(Arc::downgrade(&erased));
        NativeTransaction {
            owner: self.clone(),
            slot,
        }
    }

    pub async fn hosting<F: core::future::Future>(&self, operation: F) -> io::Result<F::Output> {
        {
            let mut state = self.inner.state.lock().expect("resource state lock");
            if state.retired {
                return Err(retired_error());
            }
            state.hosted = true;
        }
        Ok(operation.await)
    }

    pub async fn hosting_backend(&self) -> io::Result<ResourceGuard<'_, R>> {
        self.admit().await
    }

    pub async fn session<F: core::future::Future>(&self, operation: F) -> io::Result<F::Output> {
        let (handle, registration) = AbortHandle::new_pair();
        let handle = Arc::new(handle);
        {
            let mut state = self.inner.state.lock().expect("resource state lock");
            if state.retired {
                return Err(retired_error());
            }
            state.sessions.retain(|session| session.strong_count() != 0);
            state.sessions.push(Arc::downgrade(&handle));
        }
        let result = Abortable::new(operation, registration)
            .await
            .map_err(|_| retired_error());
        drop(handle);
        result
    }

    pub async fn retire_and_remove(&self) -> io::Result<()> {
        if self.inner.identity.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native retirement requires Unix file identity",
            ));
        }
        {
            let mut state = self.inner.state.lock().expect("resource state lock");
            if state.hosted {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "native hosting issued unmanaged backend handles; close hosting and reopen the resource before retirement",
                ));
            }
            state.retired = true;
            for session in &state.sessions {
                if let Some(session) = session.upgrade() {
                    session.abort();
                }
            }
            self.persist_retirement()?;
            for transaction in &state.transactions {
                if let Some(transaction) = transaction.upgrade() {
                    transaction.erase();
                }
            }
        }
        let mut backend = self.inner.backend.write().await;
        let mut state = self.inner.state.lock().expect("resource state lock");
        for transaction in state.transactions.drain(..) {
            if let Some(transaction) = transaction.upgrade() {
                assert!(
                    transaction.erase(),
                    "retirement drained transaction operations"
                );
            }
        }
        backend.take();
        self.inner.stages.cleanup()?;
        remove_retired(&self.inner.path)
    }

    fn persist_retirement(&self) -> io::Result<()> {
        let marker_path = marker(&self.inner.path);
        let identity = self
            .inner
            .identity
            .as_ref()
            .expect("retirement requires native file identity");
        let stages = self.inner.stages.retire();
        match read_record(&self.inner.path) {
            Ok((expected, _)) if &expected == identity => {
                fs::File::open(&marker_path)?.sync_all()?;
            }
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "retirement marker identity mismatch",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let parent = marker_path.parent().expect("resource has parent");
                let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
                temporary.write_all(identity.as_bytes())?;
                for stage in stages {
                    temporary.write_all(&[0])?;
                    temporary.write_all(stage.identity.as_bytes())?;
                    temporary.write_all(&[0])?;
                    temporary.write_all(stage.path.as_os_str().as_encoded_bytes())?;
                }
                temporary.as_file().sync_all()?;
                temporary
                    .persist_noclobber(&marker_path)
                    .map_err(|error| error.error)?;
            }
            Err(error) => return Err(error),
        }
        sync_parent(&self.inner.path)
    }
}

pub struct ResourceGuard<'a, R> {
    backend: RwLockReadGuard<'a, Option<R>>,
}
impl<R> Deref for ResourceGuard<'_, R> {
    type Target = R;
    fn deref(&self) -> &R {
        self.backend
            .as_ref()
            .expect("admitted resource owns backend")
    }
}

pub struct NativeTransaction<R, T> {
    owner: NativeResource<R>,
    slot: Arc<Mutex<Option<T>>>,
}

impl<R: Clone, T> NativeTransaction<R, T> {
    pub async fn read(&self) -> io::Result<TransactionGuard<'_, R, T>> {
        let owner = self.owner.admit().await?;
        let slot = self.slot.lock().await;
        if slot.is_none() {
            return Err(retired_error());
        }
        Ok(TransactionGuard {
            _owner: owner,
            slot,
        })
    }

    pub async fn take(&self) -> io::Result<(ResourceGuard<'_, R>, T)> {
        let owner = self.owner.admit().await?;
        let transaction = self.slot.lock().await.take().ok_or_else(retired_error)?;
        Ok((owner, transaction))
    }
}

pub struct TransactionGuard<'a, R, T> {
    _owner: ResourceGuard<'a, R>,
    slot: MutexGuard<'a, Option<T>>,
}
impl<R, T> Deref for TransactionGuard<'_, R, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.slot.as_ref().expect("admitted transaction")
    }
}
impl<R, T> DerefMut for TransactionGuard<'_, R, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.slot.as_mut().expect("admitted transaction")
    }
}
