use alloc::{format, string::String, vec::Vec};
use std::io;

use btree::{BTree, BTreeRead, BTreeTransaction};
use btree_redb::SharedNamespace;

use crate::{FileSyncStage, FileSyncStageFactory, SyncStage, SyncStageFactory};

pub enum NativeSyncStageFactory {
    File(FileSyncStageFactory),
    Shared(SharedNamespace),
}

impl From<FileSyncStageFactory> for NativeSyncStageFactory {
    fn from(factory: FileSyncStageFactory) -> Self {
        Self::File(factory)
    }
}

impl NativeSyncStageFactory {
    pub fn shared(namespace: SharedNamespace) -> Self {
        Self::Shared(namespace)
    }
}

pub enum NativeSyncStage {
    File(FileSyncStage),
    Shared(SharedSyncStage),
}

impl SyncStageFactory for NativeSyncStageFactory {
    type Stage = NativeSyncStage;
    type Error = io::Error;

    async fn create(&self) -> io::Result<Self::Stage> {
        match self {
            Self::File(factory) => factory.create().await.map(NativeSyncStage::File),
            Self::Shared(namespace) => {
                namespace.check()?;
                Ok(NativeSyncStage::Shared(SharedSyncStage {
                    namespace: namespace.clone(),
                    table: format!("stage:{}", uuid::Uuid::now_v7()),
                    count: 0,
                    next: 0,
                    finished: false,
                }))
            }
        }
    }
}

pub struct SharedSyncStage {
    namespace: SharedNamespace,
    table: String,
    count: u64,
    next: u64,
    finished: bool,
}

impl SyncStage for NativeSyncStage {
    type Error = io::Error;

    async fn append(&mut self, record: Vec<u8>) -> io::Result<()> {
        match self {
            Self::File(stage) => stage.append(record).await,
            Self::Shared(stage) => {
                if stage.finished {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "sync stage is finished",
                    ));
                }
                stage.namespace.check()?;
                let tree = stage.namespace.tree(&stage.table);
                let mut transaction = tree.transaction().await.map_err(io::Error::other)?;
                transaction
                    .insert(stage.count.to_be_bytes().to_vec(), record)
                    .await
                    .map_err(io::Error::other)?;
                transaction.commit().await.map_err(io::Error::other)?;
                stage.count = stage
                    .count
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("stage record count overflow"))?;
                Ok(())
            }
        }
    }

    async fn finish(&mut self) -> io::Result<()> {
        match self {
            Self::File(stage) => stage.finish().await,
            Self::Shared(stage) => {
                stage.finished = true;
                Ok(())
            }
        }
    }

    async fn next_record(&mut self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::File(stage) => stage.next_record().await,
            Self::Shared(stage) => {
                if !stage.finished {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "sync stage is not finished",
                    ));
                }
                if stage.next == stage.count {
                    return Ok(None);
                }
                stage.namespace.check()?;
                let record = stage
                    .namespace
                    .tree(&stage.table)
                    .get(&stage.next.to_be_bytes().to_vec())
                    .await
                    .map_err(io::Error::other)?
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing sync stage record")
                    })?;
                stage.next += 1;
                Ok(Some(record))
            }
        }
    }
}

impl Drop for SharedSyncStage {
    fn drop(&mut self) {
        let _ = std::thread::scope(|scope| {
            scope
                .spawn(|| futures::executor::block_on(self.namespace.remove_table(&self.table)))
                .join()
        });
    }
}
