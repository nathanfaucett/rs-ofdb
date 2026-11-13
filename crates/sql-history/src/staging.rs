use alloc::{format, string::String, vec::Vec};
use core::{fmt::Display, future::Future};

use platform::{MaybeSend, MaybeSync};

/// A sequential record sink and single-pass source for one sync phase.
///
/// Implementations must reject writes after `finish` and return records in append order.
pub trait SyncStage: MaybeSend {
    type Error: Display + MaybeSend;

    fn append(
        &mut self,
        record: Vec<u8>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    fn finish(&mut self) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    fn next_record(
        &mut self,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + MaybeSend;
}

/// Creates independent stages for catalog, data, and recovery records.
pub trait SyncStageFactory: MaybeSend + MaybeSync {
    type Stage: SyncStage;
    type Error: Display + MaybeSend;

    fn create(&self) -> impl Future<Output = Result<Self::Stage, Self::Error>> + MaybeSend;
}

#[derive(Clone, Copy)]
pub struct MemorySyncStageFactory {
    max_bytes: usize,
}

impl MemorySyncStageFactory {
    pub const fn new(max_bytes: usize) -> Self {
        Self { max_bytes }
    }
}

impl SyncStageFactory for MemorySyncStageFactory {
    type Stage = MemorySyncStage;
    type Error = String;

    async fn create(&self) -> Result<Self::Stage, Self::Error> {
        Ok(MemorySyncStage {
            records: Vec::new(),
            next: 0,
            used_bytes: 0,
            max_bytes: self.max_bytes,
            finished: false,
        })
    }
}

pub struct MemorySyncStage {
    records: Vec<Vec<u8>>,
    next: usize,
    used_bytes: usize,
    max_bytes: usize,
    finished: bool,
}

impl SyncStage for MemorySyncStage {
    type Error = String;

    async fn append(&mut self, record: Vec<u8>) -> Result<(), Self::Error> {
        if self.finished {
            return Err(String::from("sync stage is finished"));
        }
        let next_bytes = self.used_bytes.saturating_add(record.len());
        if next_bytes > self.max_bytes {
            return Err(format!("sync stage exceeds {} bytes", self.max_bytes));
        }
        self.used_bytes = next_bytes;
        self.records.push(record);
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), Self::Error> {
        self.finished = true;
        Ok(())
    }

    async fn next_record(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        if !self.finished {
            return Err(String::from("sync stage is not finished"));
        }
        let Some(record) = self.records.get_mut(self.next) else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(core::mem::take(record)))
    }
}
