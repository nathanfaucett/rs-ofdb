use alloc::vec::Vec;

use core::ops::Bound;

use platform::{MaybeSend, MaybeSync};

use futures::Stream;

use crate::EngineResult;

pub trait KernelTransaction: MaybeSend + MaybeSync {
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
