use alloc::vec::Vec;

use core::ops::Bound;

use platform::{MaybeSend, MaybeSync};

use futures::Stream;

use crate::EngineResult;

pub trait KernelTransaction: MaybeSend + MaybeSync {
    /// Sets a cumulative budget before owned native read materialization. Unsupported backends fail closed.
    fn set_read_limits(&mut self, _entries: usize, _bytes: usize) -> EngineResult<()> {
        Err(crate::EngineError::Unsupported("native read limits"))
    }

    /// Checks a decoded row before scan accumulation. Backends without limits accept it.
    fn check_read_row(&self, _row: &value::Row) -> EngineResult<()> {
        Ok(())
    }

    fn read_limit_exceeded(&self) -> bool {
        false
    }

    fn ensure_table(&mut self, table: &str) -> impl Future<Output = EngineResult<()>> + MaybeSend;
    fn drop_table(&mut self, table: &str) -> impl Future<Output = EngineResult<()>> + MaybeSend;

    /// Reads never create storage; a missing logical table reads as empty.
    fn get_bytes(
        &self,
        table: &str,
        key: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + MaybeSend;
    /// Missing logical tables yield no entries without creating storage.
    fn scan_bytes<'a>(
        &'a self,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + MaybeSend + 'a;
    /// Missing logical tables yield no entries without creating storage.
    fn scan_bytes_range<'a>(
        &'a self,
        table: &'a str,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + MaybeSend + 'a;
    fn put_bytes(
        &mut self,
        table: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> impl Future<Output = EngineResult<()>> + MaybeSend;
    fn remove_bytes(
        &mut self,
        table: &str,
        key: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + MaybeSend;

    fn commit(self) -> impl Future<Output = EngineResult<()>> + MaybeSend;
    fn rollback(self) -> impl Future<Output = EngineResult<()>> + MaybeSend;
}

pub trait Kernel: MaybeSend + MaybeSync {
    type Transaction: KernelTransaction;

    /// Unsupported backends require caller-driven broad reconciliation.
    #[cfg(feature = "std")]
    fn subscribe_changes(&self) -> Option<btree::ChangeSubscription> {
        None
    }

    fn transaction(&self) -> impl Future<Output = EngineResult<Self::Transaction>> + MaybeSend;
}
