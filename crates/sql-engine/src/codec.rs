use alloc::vec::Vec;
use core::future::Future;

use platform::{MaybeSend, MaybeSync};

use async_stream::stream;
use futures::{Stream, StreamExt, pin_mut};
pub use uuid::Uuid;
use value::Row;

use crate::{EngineResult, KernelTransaction};

/// Complete live raw table/field catalogs from the same transaction operation as the scan.
pub type RowCatalog<'a> = (&'a [(Uuid, Row)], &'a [Row]);

pub trait RowCodec<T>: MaybeSend + MaybeSync
where
    T: KernelTransaction,
{
    fn ensure_table(
        &self,
        transaction: &mut T,
        table: &str,
    ) -> impl Future<Output = EngineResult<()>> + MaybeSend;
    fn drop_table(
        &self,
        transaction: &mut T,
        table: &str,
    ) -> impl Future<Output = EngineResult<()>> + MaybeSend;
    fn get_row(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + MaybeSend;
    fn scan_rows<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
        catalog: Option<RowCatalog<'a>>,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row)>> + MaybeSend + 'a;
    fn scan_row_states<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row, bool)>> + MaybeSend + 'a {
        stream! {
            let rows = self.scan_rows(transaction, table, None);
            pin_mut!(rows);
            while let Some(row) = rows.next().await {
                let (id, value) = row?;
                yield Ok((id, value, false));
            }
        }
    }

    fn put_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        value: Row,
    ) -> impl Future<Output = EngineResult<()>> + MaybeSend;
    fn remove_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + MaybeSend;
    fn encode_row(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
        value: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + MaybeSend;
    /// User-table catalogs contain every live table and field row from this operation's snapshot.
    fn conflicted_columns(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
        table_rows: &[(Uuid, Row)],
        table_fields: &[Row],
    ) -> impl Future<Output = EngineResult<Vec<usize>>> + MaybeSend;
    fn conflict_values(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Vec<(usize, Vec<value::Value>)>>> + MaybeSend;
    fn encode_resolution(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
        value: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + MaybeSend;
    fn merge_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        value: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + MaybeSend;
    fn delete_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + MaybeSend;
    fn row_is_deleted(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<bool>> + MaybeSend;
    fn row_has_dropped_definition(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<bool>> + MaybeSend {
        let _ = (transaction, table, row);
        async { Ok(false) }
    }
    fn table_definition_was_dropped(
        &self,
        transaction: &T,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<bool>> + MaybeSend {
        let _ = (transaction, row);
        async { Ok(false) }
    }
    fn drop_table_definition(
        &self,
        transaction: &mut T,
        row: &Uuid,
        event: Uuid,
    ) -> impl Future<Output = EngineResult<()>> + MaybeSend {
        async move {
            let _ = (transaction, row, event);
            Err(crate::EngineError::Unsupported(
                "Row codec does not support causal table drops",
            ))
        }
    }
    fn table_drop_events(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Future<Output = EngineResult<Vec<Uuid>>> + MaybeSend {
        let _ = (transaction, table);
        async { Ok(Vec::new()) }
    }
    fn set_observed_table_drops(
        &self,
        transaction: &mut T,
        row: &Uuid,
        events: Vec<Uuid>,
    ) -> impl Future<Output = EngineResult<()>> + MaybeSend {
        let _ = (transaction, row, events);
        async { Ok(()) }
    }
    fn observed_table_drops(
        &self,
        transaction: &T,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Vec<Uuid>>> + MaybeSend {
        let _ = (transaction, row);
        async { Ok(Vec::new()) }
    }
}
