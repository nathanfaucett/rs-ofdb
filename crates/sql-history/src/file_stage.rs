use alloc::vec::Vec;
use std::{
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use tempfile::NamedTempFile;

use crate::{SyncStage, SyncStageFactory};

/// Native temporary records. Operations do not suspend while a file is being mutated.
#[derive(Clone)]
pub struct FileSyncStageFactory {
    directory: PathBuf,
    tracking: Option<Tracking>,
}
type StageCreated = dyn Fn(&Path) -> io::Result<()> + Send + Sync;
type StageClosed = dyn Fn(&Path) + Send + Sync;

#[derive(Clone)]
struct Tracking {
    created: Arc<StageCreated>,
    closed: Arc<StageClosed>,
}
impl core::fmt::Debug for FileSyncStageFactory {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("FileSyncStageFactory")
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}
impl FileSyncStageFactory {
    /// Records exact owner-created filenames for resource retirement. Successful
    /// close forgets them; failures remain recorded for durable cleanup retry.
    pub fn with_file_tracking(
        mut self,
        created: impl Fn(&Path) -> io::Result<()> + Send + Sync + 'static,
        closed: impl Fn(&Path) + Send + Sync + 'static,
    ) -> Self {
        self.tracking = Some(Tracking {
            created: Arc::new(created),
            closed: Arc::new(closed),
        });
        self
    }

    pub fn new(directory: impl AsRef<Path>) -> Self {
        Self {
            directory: directory.as_ref().to_path_buf(),
            tracking: None,
        }
    }
}
impl SyncStageFactory for FileSyncStageFactory {
    type Stage = FileSyncStage;
    type Error = io::Error;
    async fn create(&self) -> io::Result<FileSyncStage> {
        std::fs::create_dir_all(&self.directory)?;
        let file = tempfile::Builder::new()
            .prefix("ofdb-sync-")
            .suffix(".stage")
            .tempfile_in(&self.directory)?;
        if let Some(tracking) = &self.tracking {
            (tracking.created)(file.path())?;
        }
        Ok(FileSyncStage {
            file: Some(file),
            finished: false,
            tracking: self.tracking.clone(),
        })
    }
}

pub struct FileSyncStage {
    file: Option<NamedTempFile>,
    finished: bool,
    tracking: Option<Tracking>,
}
impl FileSyncStage {
    /// Closes the handle and removes the file, reporting cleanup failure explicitly.
    /// Drop also attempts cleanup and reports errors to stderr, without masking a session error.
    pub fn close(mut self) -> io::Result<()> {
        let file = self.file.take().expect("stage owns file");
        let path = file.path().to_path_buf();
        file.close()?;
        if let Some(tracking) = &self.tracking {
            (tracking.closed)(&path);
        }
        Ok(())
    }
}
impl SyncStage for FileSyncStage {
    type Error = io::Error;
    async fn append(&mut self, record: Vec<u8>) -> io::Result<()> {
        if self.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sync stage is finished",
            ));
        }
        let length = u64::try_from(record.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "record length overflow"))?;
        let file = self.file.as_mut().expect("stage owns file");
        file.write_all(&length.to_be_bytes())?;
        file.write_all(&record)
    }
    async fn finish(&mut self) -> io::Result<()> {
        if !self.finished {
            let file = self.file.as_mut().expect("stage owns file");
            file.flush()?;
            file.rewind()?;
            self.finished = true;
        }
        Ok(())
    }
    async fn next_record(&mut self) -> io::Result<Option<Vec<u8>>> {
        if !self.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sync stage is not finished",
            ));
        }
        let file = self.file.as_mut().expect("stage owns file");
        let mut prefix = [0; 8];
        if file.read(&mut prefix[..1])? == 0 {
            return Ok(None);
        }
        file.read_exact(&mut prefix[1..])?;
        let length = u64::from_be_bytes(prefix);
        let remaining = file
            .as_file()
            .metadata()?
            .len()
            .saturating_sub(file.stream_position()?);
        if length > remaining {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated sync stage record",
            ));
        }
        let length = usize::try_from(length).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid sync stage record length",
            )
        })?;
        let mut record = Vec::new();
        record.try_reserve_exact(length).map_err(io::Error::other)?;
        record.resize(length, 0);
        file.read_exact(&mut record)?;
        Ok(Some(record))
    }
}
impl Drop for FileSyncStage {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let path = file.path().to_path_buf();
            match file.close() {
                Ok(()) => {
                    if let Some(tracking) = &self.tracking {
                        (tracking.closed)(&path);
                    }
                }
                Err(error) => std::eprintln!("ofdb sync stage cleanup failed: {error}"),
            }
        }
    }
}
