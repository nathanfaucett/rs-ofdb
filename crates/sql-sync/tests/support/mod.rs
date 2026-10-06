use engine::{EngineError, EngineResult, KernelTransaction, RowCodec, RowIdentity};
use futures::{StreamExt, stream::Stream};
use sync::{SyncChangeId, SyncRowCodec};
use value::{Row, Value};

pub struct TestCodec;

fn decode(bytes: &[u8]) -> EngineResult<Row> {
    postcard::from_bytes(bytes).map_err(EngineError::custom)
}

impl<T: KernelTransaction> RowCodec<T> for TestCodec {
    async fn ensure_table(&self, transaction: &mut T, table: &str) -> EngineResult<()> {
        transaction.ensure_table(table).await
    }

    async fn drop_table(&self, transaction: &mut T, table: &str) -> EngineResult<()> {
        transaction.drop_table(table).await
    }

    async fn get_row(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<Option<Row>> {
        transaction
            .get_bytes(table, &row.to_bytes())
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    fn scan_rows<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(RowIdentity, Row)>> + Send + 'a {
        transaction.scan_bytes(table).map(|entry| {
            let (id, value) = entry?;
            Ok((
                RowIdentity::from_bytes(&id).ok_or(EngineError::custom("Invalid row identity"))?,
                decode(&value)?,
            ))
        })
    }

    async fn put_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        value: Row,
    ) -> EngineResult<()> {
        transaction
            .put_bytes(
                table,
                row.to_bytes(),
                postcard::to_allocvec(&value).map_err(EngineError::custom)?,
            )
            .await
    }

    async fn remove_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<Option<Row>> {
        transaction
            .remove_bytes(table, &row.to_bytes())
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    async fn encode_row(
        &self,
        _transaction: &T,
        _table: &str,
        _row: &RowIdentity,
        value: &Row,
        _changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        postcard::to_allocvec(value).map_err(EngineError::custom)
    }

    async fn conflicted_columns(
        &self,
        _transaction: &T,
        _table: &str,
        _row: &RowIdentity,
    ) -> EngineResult<Vec<usize>> {
        Ok(Vec::new())
    }

    async fn conflict_values(
        &self,
        _transaction: &T,
        _table: &str,
        _row: &RowIdentity,
    ) -> EngineResult<Vec<(usize, Vec<Value>)>> {
        Ok(Vec::new())
    }

    async fn encode_resolution(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
        value: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        self.encode_row(transaction, table, row, value, changed_columns)
            .await
    }

    async fn merge_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        value: &[u8],
    ) -> EngineResult<Option<Row>> {
        let decoded = decode(value)?;
        self.put_row(transaction, table, row, decoded.clone())
            .await?;
        Ok(Some(decoded))
    }

    async fn delete_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<Option<Row>> {
        self.remove_row(transaction, table, row).await
    }

    async fn row_is_deleted(
        &self,
        _transaction: &T,
        _table: &str,
        _row: &RowIdentity,
    ) -> EngineResult<bool> {
        Ok(false)
    }
}

impl<T: KernelTransaction> SyncRowCodec<T> for TestCodec {
    fn row_ids<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl futures::Stream<Item = EngineResult<RowIdentity>> + Send + 'a {
        let entries = transaction.scan_bytes(table);
        futures::stream::unfold(Box::pin(entries), |mut entries| async move {
            match entries.as_mut().next().await {
                None => None,
                Some(Err(EngineError::Custom(message))) if message == "Table not found" => None,
                Some(Err(error)) => Some((Err(error), entries)),
                Some(Ok((id, _))) => Some((
                    RowIdentity::from_bytes(&id).ok_or(EngineError::custom("Invalid row identity")),
                    entries,
                )),
            }
        })
    }

    async fn export_state(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
        max_bytes: usize,
    ) -> EngineResult<Option<Vec<u8>>> {
        let state = transaction.get_bytes(table, &row.to_bytes()).await?;
        if state.as_ref().is_some_and(|state| state.len() > max_bytes) {
            return Err(EngineError::custom("sync state payload exceeds budget"));
        }
        Ok(state)
    }

    async fn merge_state(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        state: &[u8],
    ) -> EngineResult<Option<Row>> {
        self.merge_row(transaction, table, row, state).await
    }

    async fn export_metadata(
        &self,
        _transaction: &T,
        _table: &str,
        _row: RowIdentity,
    ) -> EngineResult<Vec<u8>> {
        Ok(Vec::new())
    }

    async fn merge_metadata(
        &self,
        _transaction: &mut T,
        _table: &str,
        _row: RowIdentity,
        _metadata: &[u8],
    ) -> EngineResult<()> {
        Ok(())
    }

    async fn change_inventory(
        &self,
        _transaction: &T,
        _table: &str,
        _row: RowIdentity,
        _max_bytes: usize,
    ) -> EngineResult<Vec<SyncChangeId>> {
        Ok(Vec::new())
    }

    async fn export_change(
        &self,
        _transaction: &T,
        _table: &str,
        _row: RowIdentity,
        _id: &SyncChangeId,
        _max_bytes: usize,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn apply_change(
        &self,
        _transaction: &mut T,
        _table: &str,
        _row: RowIdentity,
        _id: &SyncChangeId,
        _payload: &[u8],
    ) -> EngineResult<()> {
        Err(EngineError::SyncDependencyUnavailable)
    }
}
