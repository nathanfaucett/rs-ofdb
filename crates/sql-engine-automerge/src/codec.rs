use std::{collections::BTreeMap, string::String, vec::Vec};

use async_stream::stream;
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, ROOT, ReadDoc, ScalarValue, Value as AutomergeValue};
use btree::{BTreeRead, BTreeTransaction};
use btree_automerge::{
    AutomergeBTreeTransaction, AutomergeChangeStore, DocumentChangeKey, DocumentId,
    ThresholdPolicy, hash_heads, reconstruct_document_values,
};
use serde::{Deserialize, Serialize};

use engine::{
    BytesTable, BytesTableTransaction, ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID,
    ENGINE_TABLE_FIELDS_STORAGE, EngineError, EngineResult, KernelTransaction, RowCodec,
    RowIdentity, catalog_table_for_storage,
};
use futures::{Stream, StreamExt, pin_mut};
use sync::{SyncChangeId, SyncRowCodec};
use value::{Row, Value};

const COLUMN_COUNT_BYTES: usize = size_of::<u32>();
const DOCUMENT_COLUMNS: &str = "\0engine_columns";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowMetadata {
    pub version: u8,
    pub deleted: bool,
}

#[derive(Debug)]
pub struct AutomergeRowCodec {
    actor: ActorId,
}

impl AutomergeRowCodec {
    pub fn new() -> Self {
        Self {
            actor: ActorId::from(uuid::Uuid::now_v7().as_bytes().to_vec()),
        }
    }
}

impl Default for AutomergeRowCodec {
    fn default() -> Self {
        Self::new()
    }
}

struct Column {
    id: String,
    default: Value,
}

impl AutomergeRowCodec {
    fn document_id(row: &RowIdentity) -> DocumentId {
        row.to_bytes()
    }

    fn row_id(id: &[u8]) -> EngineResult<RowIdentity> {
        RowIdentity::from_bytes(id).ok_or(EngineError::custom("Invalid row identity"))
    }

    fn catalog_states<'a, T>(
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(RowIdentity, Row, bool)>> + Send + 'a
    where
        T: KernelTransaction,
    {
        stream! {
            let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
            let documents = reconstruct_document_values(changes.range(..).filter_map(|entry| async {
                match &entry {
                    Ok((key, _)) if key.r#type().is_metadata() => None,
                    _ => Some(entry),
                }
            }));
            pin_mut!(documents);
            while let Some(document) = documents.next().await {
                let (id, document) = document.map_err(EngineError::custom)?;
                let row = Self::row_id(&id)?;
                let deleted = Self::metadata_deleted(transaction, table, &row).await?;
                let columns = catalog_table_for_storage(table)
                    .ok_or(EngineError::custom("Invalid catalog storage"))?
                    .columns().iter().map(|column| Column { id: String::from(*column), default: Value::Null })
                    .collect::<Vec<_>>();
                yield Self::decode_row(&document, &columns).map(|value| (row, value, deleted));
            }
        }
    }

    fn active_table_id<'a, T: KernelTransaction>(
        transaction: &'a T,
        table: &'a str,
    ) -> impl core::future::Future<Output = EngineResult<Option<[u8; 16]>>> + Send + 'a {
        Box::pin(async move {
            let states = Self::catalog_states(transaction, engine::ENGINE_TABLES_STORAGE);
            pin_mut!(states);
            let mut latest = None;
            while let Some(state) = states.next().await {
                let (RowIdentity::Catalog(id), value, deleted) = state? else {
                    continue;
                };
                if value.values.first().and_then(Value::as_text) == Some(table)
                    && latest
                        .as_ref()
                        .is_none_or(|(previous, _): &([u8; 16], bool)| {
                            id.as_slice() > &previous[..]
                        })
                {
                    let id: [u8; 16] = id
                        .try_into()
                        .map_err(|_| EngineError::custom("Invalid table identity"))?;
                    latest = Some((id, deleted));
                }
            }
            Ok(latest.and_then(|(id, deleted)| (!deleted).then_some(id)))
        })
    }

    fn columns<'a, T>(
        transaction: &'a T,
        table: &'a str,
        fallback_count: usize,
    ) -> impl core::future::Future<Output = EngineResult<Vec<Column>>> + Send + 'a
    where
        T: KernelTransaction,
    {
        Box::pin(async move {
            if let Some(catalog_table) = catalog_table_for_storage(table) {
                return Ok(catalog_table
                    .columns()
                    .iter()
                    .map(|column| Column {
                        id: String::from(*column),
                        default: Value::Null,
                    })
                    .collect());
            }

            let tables = Self::catalog_states(transaction, engine::ENGINE_TABLES_STORAGE);
            pin_mut!(tables);
            let mut parent = None;
            while let Some(entry) = tables.next().await {
                let (identity, row, deleted) = entry?;
                if row.values.first().and_then(Value::as_text) == Some(table)
                    && parent
                        .as_ref()
                        .is_none_or(|(previous, _): &(RowIdentity, bool)| &identity > previous)
                {
                    parent = Some((identity, deleted));
                }
            }
            let mut columns = Vec::new();
            if let Some((RowIdentity::Catalog(parent), false)) = parent {
                let fields = Self::catalog_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
                pin_mut!(fields);
                let mut latest = BTreeMap::new();
                while let Some(field) = fields.next().await {
                    let (identity, field, deleted) = field?;
                    let RowIdentity::Catalog(key) = identity else {
                        continue;
                    };
                    if key.len() != 32 || !key.starts_with(&parent) {
                        continue;
                    }
                    let id = field
                        .values
                        .first()
                        .and_then(Value::to_text)
                        .ok_or(EngineError::custom(ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID))?;
                    let entry = latest
                        .entry(id)
                        .or_insert_with(|| (key.clone(), field.clone(), deleted));
                    if key > entry.0 {
                        *entry = (key, field, deleted);
                    }
                }
                for (id, (_, field, deleted)) in latest {
                    if deleted {
                        continue;
                    }
                    let index = field
                        .values
                        .get(3)
                        .and_then(Value::to_integer)
                        .ok_or(EngineError::custom("Invalid table field column index"))?;
                    let default = field
                        .values
                        .get(2)
                        .cloned()
                        .ok_or(EngineError::custom("Invalid table field default"))?;
                    columns.push((index, Column { id, default }));
                }
            }
            columns.sort_by(|(left_index, left), (right_index, right)| {
                left_index
                    .cmp(right_index)
                    .then_with(|| left.id.cmp(&right.id))
            });
            if columns.is_empty() {
                return Ok((0..fallback_count)
                    .map(|index| Column {
                        id: index.to_string(),
                        default: Value::Null,
                    })
                    .collect());
            }
            Ok(columns.into_iter().map(|(_, column)| column).collect())
        })
    }

    fn document<'a, T>(
        transaction: &'a T,
        table: &'a str,
        id: &'a DocumentId,
    ) -> impl core::future::Future<Output = EngineResult<Option<AutoCommit>>> + Send + 'a
    where
        T: KernelTransaction,
    {
        Box::pin(async move {
            let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
            let documents = reconstruct_document_values(
                changes
                    .range(DocumentChangeKey::range_for(id))
                    .filter_map(|entry| async {
                        match &entry {
                            Ok((key, _)) if key.r#type().is_metadata() => None,
                            _ => Some(entry),
                        }
                    }),
            );
            pin_mut!(documents);
            documents
                .next()
                .await
                .transpose()
                .map_err(EngineError::custom)
                .map(|document| document.map(|(_, value)| value))
        })
    }

    fn changes<'a, T>(
        transaction: &'a mut T,
        table: &str,
    ) -> AutomergeBTreeTransaction<AutomergeChangeStore<BytesTableTransaction<'a, T>>>
    where
        T: KernelTransaction + Send,
    {
        AutomergeBTreeTransaction::new(
            AutomergeChangeStore::new(BytesTableTransaction::new(transaction, table)),
            ThresholdPolicy::default(),
        )
    }

    async fn metadata_deleted<T>(
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<bool>
    where
        T: KernelTransaction,
    {
        let storage = table;
        let changes = AutomergeChangeStore::new(BytesTable::new(transaction, storage));
        let Some(value) = changes
            .get(&DocumentChangeKey::new_metadata(Self::document_id(row)))
            .await
            .map_err(EngineError::custom)?
        else {
            return Ok(false);
        };
        let metadata: RowMetadata = postcard::from_bytes(&value).map_err(EngineError::custom)?;
        Ok(metadata.deleted)
    }

    fn decode_row(document: &AutoCommit, columns: &[Column]) -> EngineResult<Row> {
        let mut values = Vec::with_capacity(columns.len());
        for column in columns {
            let Some((AutomergeValue::Scalar(value), _)) = document
                .get(ROOT, &column.id)
                .map_err(EngineError::custom)?
            else {
                values.push(column.default.clone());
                continue;
            };
            let ScalarValue::Bytes(bytes) = value.as_ref() else {
                return Err(EngineError::custom(
                    "Logical row column is not encoded bytes",
                ));
            };
            values.push(postcard::from_bytes(bytes).map_err(EngineError::custom)?);
        }
        Ok(Row::new(values))
    }

    fn conflicted_columns(document: &AutoCommit, columns: &[Column]) -> EngineResult<Vec<usize>> {
        let mut result = Vec::new();
        for (index, column) in columns.iter().enumerate() {
            if document
                .get_all(ROOT, &column.id)
                .map_err(EngineError::custom)?
                .len()
                > 1
            {
                result.push(index);
            }
        }
        Ok(result)
    }

    fn has_conflicts(
        document: &AutoCommit,
        columns: &[Column],
        changed_columns: &[usize],
    ) -> EngineResult<bool> {
        for &index in changed_columns {
            let column = columns.get(index).ok_or(EngineError::InvalidQuery(
                "Logical row has an invalid column index",
            ))?;
            if document
                .get_all(ROOT, &column.id)
                .map_err(EngineError::custom)?
                .len()
                > 1
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn encode_row(&self, row: &Row, columns: &[Column]) -> EngineResult<AutoCommit> {
        let mut document = AutoCommit::new().with_actor(self.actor.clone());
        Self::update_document(&mut document, row, columns)?;
        Ok(document)
    }

    fn write_columns(document: &mut AutoCommit, columns: &[Column]) -> EngineResult<()> {
        let layout: Vec<_> = columns
            .iter()
            .map(|column| (column.id.clone(), column.default.clone()))
            .collect();
        document
            .put(
                ROOT,
                DOCUMENT_COLUMNS,
                ScalarValue::Bytes(postcard::to_allocvec(&layout).map_err(EngineError::custom)?),
            )
            .map_err(EngineError::custom)?;
        Ok(())
    }

    fn stored_columns(document: &AutoCommit) -> EngineResult<Option<Vec<Column>>> {
        let Some((AutomergeValue::Scalar(value), _)) = document
            .get(ROOT, DOCUMENT_COLUMNS)
            .map_err(EngineError::custom)?
        else {
            return Ok(None);
        };
        let ScalarValue::Bytes(bytes) = value.as_ref() else {
            return Err(EngineError::custom("Invalid row column layout"));
        };
        let layout: Vec<(String, Value)> =
            postcard::from_bytes(bytes).map_err(EngineError::custom)?;
        Ok(Some(
            layout
                .into_iter()
                .map(|(id, default)| Column { id, default })
                .collect(),
        ))
    }

    fn update_document(
        document: &mut AutoCommit,
        row: &Row,
        columns: &[Column],
    ) -> EngineResult<()> {
        Self::write_columns(document, columns)?;
        for (value, column) in row.values.iter().zip(columns) {
            let bytes = postcard::to_allocvec(value).map_err(EngineError::custom)?;
            document
                .put(ROOT, &column.id, ScalarValue::Bytes(bytes))
                .map_err(EngineError::custom)?;
        }
        Ok(())
    }

    fn update_document_columns(
        document: &mut AutoCommit,
        row: &Row,
        columns: &[Column],
        changed_columns: &[usize],
    ) -> EngineResult<()> {
        Self::write_columns(document, columns)?;
        for &index in changed_columns {
            let (value, column) =
                row.values
                    .get(index)
                    .zip(columns.get(index))
                    .ok_or(EngineError::InvalidQuery(
                        "Logical row has an invalid column index",
                    ))?;
            let bytes = postcard::to_allocvec(value).map_err(EngineError::custom)?;
            document
                .put(ROOT, &column.id, ScalarValue::Bytes(bytes))
                .map_err(EngineError::custom)?;
        }
        Ok(())
    }

    fn encode_incremental(column_count: usize, bytes: Vec<u8>) -> EngineResult<Vec<u8>> {
        let column_count = u32::try_from(column_count)
            .map_err(|_| EngineError::InvalidQuery("Logical row has too many columns"))?;
        let mut value = Vec::with_capacity(COLUMN_COUNT_BYTES + bytes.len());
        value.extend_from_slice(&column_count.to_be_bytes());
        value.extend_from_slice(&bytes);
        Ok(value)
    }

    fn decode_incremental(value: &[u8]) -> EngineResult<(usize, &[u8])> {
        let Some((column_count, bytes)) = value.split_at_checked(COLUMN_COUNT_BYTES) else {
            return Err(EngineError::custom("Invalid Automerge row change"));
        };
        let column_count = u32::from_be_bytes(
            column_count
                .try_into()
                .map_err(|_| EngineError::custom("Invalid Automerge row change"))?,
        );
        Ok((column_count as usize, bytes))
    }

    async fn document_columns<T>(
        transaction: &T,
        table: &str,
        row: &RowIdentity,
        document: &AutoCommit,
    ) -> EngineResult<Vec<Column>>
    where
        T: KernelTransaction,
    {
        if let RowIdentity::ScopedUser { table: owner, .. } = row
            && Self::active_table_id(transaction, table).await? != Some(*owner)
        {
            return Self::stored_columns(document)?
                .ok_or(EngineError::custom("Missing stale row layout"));
        }
        let columns = Self::columns(transaction, table, 0).await?;
        if !columns.is_empty() {
            return Ok(columns);
        }
        if let Some(columns) = Self::stored_columns(document)? {
            return Ok(columns);
        }
        Ok(document
            .keys(ROOT)
            .filter(|key| key != DOCUMENT_COLUMNS)
            .map(|id| Column {
                id: id.to_string(),
                default: Value::Null,
            })
            .collect())
    }

    async fn store_row<T>(
        &self,
        transaction: &mut T,
        table: &str,
        id: DocumentId,
        columns: &[Column],
        value: &Row,
    ) -> EngineResult<()>
    where
        T: KernelTransaction + Send,
    {
        let mut changes = Self::changes(transaction, table);
        if let Some(document) = changes.get(&id).await.map_err(EngineError::custom)? {
            let changed_columns: Vec<_> = (0..columns.len()).collect();
            if Self::has_conflicts(&document, columns, &changed_columns)? {
                return Err(EngineError::InvalidQuery(
                    "Logical row column is conflicted",
                ));
            }
            changes
                .update(id, |document| {
                    Self::update_document(document, value, columns)
                        .map_err(btree::BTreeError::custom)
                })
                .await
                .map_err(EngineError::custom)?;
        } else {
            changes
                .insert(id, self.encode_row(value, columns)?)
                .await
                .map_err(EngineError::custom)?;
        }
        Ok(())
    }
}

impl<T> RowCodec<T> for AutomergeRowCodec
where
    T: KernelTransaction + Send,
{
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
        if Self::metadata_deleted(transaction, table, row).await? {
            return Ok(None);
        }
        if let RowIdentity::ScopedUser { table: owner, .. } = row
            && Self::active_table_id(transaction, table).await? != Some(*owner)
        {
            return Ok(None);
        }
        let id = Self::document_id(row);
        let Some(document) = Self::document(transaction, table, &id).await? else {
            return Ok(None);
        };
        let table_name = table;
        let columns = Self::document_columns(transaction, table_name, row, &document).await?;
        Self::decode_row(&document, &columns).map(Some)
    }

    fn scan_rows(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(RowIdentity, Row)>> {
        stream! {
            let storage = table;
            let changes = AutomergeChangeStore::new(BytesTable::new(transaction, storage));
            let documents = reconstruct_document_values(changes.range(..).filter_map(|entry| async {
                match &entry {
                    Ok((key, _)) if key.r#type().is_metadata() => None,
                    _ => Some(entry),
                }
            }));
            pin_mut!(documents);

            while let Some(document) = documents.next().await {
                let (id, document) = document.map_err(EngineError::custom)?;
                let row = Self::row_id(&id)?;
                if Self::metadata_deleted(transaction, table, &row).await? {
                    continue;
                }
                if let RowIdentity::ScopedUser { table: owner, .. } = &row
                    && Self::active_table_id(transaction, table).await? != Some(*owner)
                { continue; }
                let columns = Self::document_columns(transaction, table, &row, &document).await?;
                yield Self::decode_row(&document, &columns).map(|value| (row, value));
            }
        }
    }

    fn scan_row_states(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(RowIdentity, Row, bool)>> + Send {
        stream! {
            if catalog_table_for_storage(table).is_some() {
                let states = Self::catalog_states(transaction, table);
                pin_mut!(states);
                while let Some(state) = states.next().await { yield state; }
            } else {
                let rows = self.scan_rows(transaction, table);
                pin_mut!(rows);
                while let Some(row) = rows.next().await {
                    let (id, value) = row?;
                    yield Ok((id, value, false));
                }
            }
        }
    }

    async fn encode_row(
        &self,
        transaction: &T,
        table: &str,
        row_id: &RowIdentity,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let table_name = table;
        let id = Self::document_id(row_id);
        let mut document = Self::document(transaction, table, &id)
            .await?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(self.actor.clone());
        let columns = Self::columns(transaction, table_name, row.values.len()).await?;
        if Self::has_conflicts(&document, &columns, changed_columns)? {
            return Err(EngineError::InvalidQuery(
                "Logical row column is conflicted",
            ));
        }
        Self::update_document_columns(&mut document, row, &columns, changed_columns)?;
        Self::encode_incremental(columns.len(), document.save_incremental())
    }

    async fn conflicted_columns(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<Vec<usize>> {
        let table_name = table;
        let id = Self::document_id(row);
        let Some(document) = Self::document(transaction, table, &id).await? else {
            return Ok(Vec::new());
        };
        Self::conflicted_columns(
            &document,
            &Self::document_columns(transaction, table_name, row, &document).await?,
        )
    }

    async fn conflict_values(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<Vec<(usize, Vec<Value>)>> {
        let id = Self::document_id(row);
        let Some(document) = Self::document(transaction, table, &id).await? else {
            return Ok(Vec::new());
        };
        let columns = Self::document_columns(transaction, table, row, &document).await?;
        let mut conflicts = Vec::new();
        for (index, column) in columns.iter().enumerate() {
            let values = document
                .get_all(ROOT, &column.id)
                .map_err(EngineError::custom)?;
            if values.len() < 2 {
                continue;
            }
            let values = values
                .into_iter()
                .map(|(value, _)| {
                    let AutomergeValue::Scalar(value) = value else {
                        return Err(EngineError::custom(
                            "Logical row column is not encoded bytes",
                        ));
                    };
                    let ScalarValue::Bytes(bytes) = value.as_ref() else {
                        return Err(EngineError::custom(
                            "Logical row column is not encoded bytes",
                        ));
                    };
                    postcard::from_bytes(bytes.as_ref()).map_err(EngineError::custom)
                })
                .collect::<EngineResult<Vec<_>>>()?;
            conflicts.push((index, values));
        }
        Ok(conflicts)
    }

    async fn encode_resolution(
        &self,
        transaction: &T,
        table: &str,
        row_id: &RowIdentity,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let table_name = table;
        let id = Self::document_id(row_id);
        let mut document = Self::document(transaction, table, &id)
            .await?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(self.actor.clone());
        let columns = Self::columns(transaction, table_name, row.values.len()).await?;
        Self::update_document_columns(&mut document, row, &columns, changed_columns)?;
        Self::encode_incremental(columns.len(), document.save_incremental())
    }

    async fn merge_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        value: &[u8],
    ) -> EngineResult<Option<Row>> {
        if Self::metadata_deleted(transaction, table, &row).await? {
            return Ok(None);
        }
        let table_name = table;
        let (column_count, incremental) = Self::decode_incremental(value)?;
        let id = Self::document_id(&row);

        let existing = Self::document(transaction, table, &id).await?;
        let was_existing = existing.is_some();
        let previous_heads = existing
            .as_ref()
            .map_or_else(Vec::new, |document| document.clone().get_heads());
        let mut document = existing.unwrap_or_else(AutoCommit::new);
        document
            .load_incremental(incremental)
            .map_err(EngineError::custom)?;
        let columns = Self::columns(transaction, table_name, column_count).await?;
        let row = Self::decode_row(&document, &columns)?;

        let mut changes = AutomergeChangeStore::new(BytesTableTransaction::new(transaction, table));
        let (key, bytes) = if !was_existing {
            (
                DocumentChangeKey::new_snapshot(id, hash_heads(document.get_heads())),
                document.save(),
            )
        } else {
            let changes = document.get_changes(&previous_heads);
            let change = changes
                .last()
                .ok_or(EngineError::custom("Incremental payload has no change"))?;
            (
                DocumentChangeKey::new_incremental(id, change.hash().0),
                incremental.to_vec(),
            )
        };
        if changes
            .get(&key)
            .await
            .map_err(EngineError::custom)?
            .is_none()
        {
            changes
                .insert(key, bytes)
                .await
                .map_err(EngineError::custom)?;
        }
        Ok(Some(row))
    }

    async fn delete_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<Option<Row>> {
        let value = self.get_row(transaction, table, row).await?;
        let key = DocumentChangeKey::new_metadata(Self::document_id(row)).encode_ordered();
        let metadata = RowMetadata {
            version: 1,
            deleted: true,
        };
        transaction
            .put_bytes(
                table,
                key,
                postcard::to_allocvec(&metadata).map_err(EngineError::custom)?,
            )
            .await?;
        Ok(value)
    }

    async fn row_is_deleted(
        &self,
        transaction: &T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<bool> {
        Self::metadata_deleted(transaction, table, row).await
    }

    async fn put_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        value: Row,
    ) -> EngineResult<()> {
        if Self::metadata_deleted(transaction, table, &row).await? {
            return Err(EngineError::InvalidQuery("Logical row is metadatad"));
        }
        let table_name = table;
        let columns = Self::columns(transaction, table_name, value.values.len()).await?;
        let id = Self::document_id(&row);
        self.store_row(transaction, table, id, &columns, &value)
            .await
    }

    async fn remove_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &RowIdentity,
    ) -> EngineResult<Option<Row>> {
        self.delete_row(transaction, table, row).await
    }
}

impl<T> SyncRowCodec<T> for AutomergeRowCodec
where
    T: KernelTransaction + Send,
{
    async fn row_ids(&self, transaction: &T, table: &str) -> EngineResult<Vec<RowIdentity>> {
        let mut ids = BTreeMap::new();
        let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
        let documents = reconstruct_document_values(changes.range(..).filter_map(|entry| async {
            match &entry {
                Ok((key, _)) if key.r#type().is_metadata() => None,
                _ => Some(entry),
            }
        }));
        pin_mut!(documents);
        while let Some(document) = documents.next().await {
            let (id, _) = document.map_err(EngineError::custom)?;
            ids.insert(Self::row_id(&id)?, ());
        }
        let metadata = self.export_metadata_rows(transaction, table);
        pin_mut!(metadata);
        while let Some(row) = metadata.next().await {
            ids.insert(row?.0, ());
        }
        Ok(ids.into_keys().collect())
    }

    async fn export_state(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
    ) -> EngineResult<Option<Vec<u8>>> {
        let id = Self::document_id(&row);
        Self::document(transaction, table, &id)
            .await?
            .map(|mut document| Ok(document.save()))
            .transpose()
    }

    async fn merge_state(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        state: &[u8],
    ) -> EngineResult<Option<Row>> {
        let deleted = Self::metadata_deleted(transaction, table, &row).await?;
        let mut incoming = AutoCommit::load(state).map_err(EngineError::custom)?;
        let id = Self::document_id(&row);
        let existing = Self::document(transaction, table, &id).await?;
        let mut document = existing.clone().unwrap_or_else(AutoCommit::new);
        if existing.is_some() {
            document.merge(&mut incoming).map_err(EngineError::custom)?;
        } else {
            document = incoming;
        }
        let columns = Self::document_columns(transaction, table, &row, &document).await?;
        let value = Self::decode_row(&document, &columns)?;
        let stale = {
            let store = AutomergeChangeStore::new(BytesTable::new(&*transaction, table));
            let entries = store.range(DocumentChangeKey::range_for(&id));
            pin_mut!(entries);
            let mut keys = Vec::new();
            while let Some(entry) = entries.next().await {
                let (key, _) = entry.map_err(EngineError::custom)?;
                if !key.r#type().is_metadata() {
                    keys.push(key.encode_ordered());
                }
            }
            keys
        };
        for key in stale {
            transaction.remove_bytes(table, &key).await?;
        }
        let key =
            DocumentChangeKey::new_snapshot(id, hash_heads(document.get_heads())).encode_ordered();
        transaction.put_bytes(table, key, document.save()).await?;
        Ok((!deleted).then_some(value))
    }

    async fn export_metadata(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
    ) -> EngineResult<Vec<u8>> {
        let key = DocumentChangeKey::new_metadata(Self::document_id(&row)).encode_ordered();
        Ok(transaction
            .get_bytes(table, &key)
            .await?
            .unwrap_or_default())
    }

    async fn merge_metadata(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        metadata: &[u8],
    ) -> EngineResult<()> {
        let incoming: RowMetadata = postcard::from_bytes(metadata).map_err(EngineError::custom)?;
        let key = DocumentChangeKey::new_metadata(Self::document_id(&row)).encode_ordered();
        let existing = transaction
            .get_bytes(table, &key)
            .await?
            .map(|bytes| postcard::from_bytes::<RowMetadata>(&bytes))
            .transpose()
            .map_err(EngineError::custom)?;
        let metadata = RowMetadata {
            version: existing.as_ref().map_or(incoming.version, |value| {
                value.version.max(incoming.version)
            }),
            deleted: incoming.deleted || existing.is_some_and(|value| value.deleted),
        };
        transaction
            .put_bytes(
                table,
                key,
                postcard::to_allocvec(&metadata).map_err(EngineError::custom)?,
            )
            .await
    }

    async fn change_inventory(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
    ) -> EngineResult<Vec<SyncChangeId>> {
        let id = Self::document_id(&row);
        let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
        let entries = changes.range(DocumentChangeKey::range_for(&id));
        pin_mut!(entries);
        let mut inventory = BTreeMap::new();
        while let Some(entry) = entries.next().await {
            let (key, _) = entry.map_err(EngineError::custom)?;
            if key.r#type().is_incremental() {
                inventory.insert(*key.change_hash(), SyncChangeId(key.change_hash().to_vec()));
            }
        }
        let Some(mut document) = Self::document(transaction, table, &id).await? else {
            return Ok(inventory.into_values().collect());
        };
        Ok(document
            .get_changes(&[])
            .into_iter()
            .filter_map(|change| inventory.remove(&change.hash().0))
            .collect())
    }

    async fn export_change(
        &self,
        transaction: &T,
        table: &str,
        row: RowIdentity,
        id: &SyncChangeId,
    ) -> EngineResult<Option<Vec<u8>>> {
        let hash: [u8; 32] =
            id.0.as_slice()
                .try_into()
                .map_err(|_| EngineError::custom("Invalid sync change ID"))?;
        let key = DocumentChangeKey::new_incremental(Self::document_id(&row), hash);
        AutomergeChangeStore::new(BytesTable::new(transaction, table))
            .get(&key)
            .await
            .map_err(EngineError::custom)
    }

    async fn apply_change(
        &self,
        transaction: &mut T,
        table: &str,
        row: RowIdentity,
        id: &SyncChangeId,
        payload: &[u8],
    ) -> EngineResult<Option<Row>> {
        let hash: [u8; 32] =
            id.0.as_slice()
                .try_into()
                .map_err(|_| EngineError::custom("Invalid sync change ID"))?;
        let document_id = Self::document_id(&row);
        let automerge_key = DocumentChangeKey::new_incremental(document_id.clone(), hash);
        let already_applied = AutomergeChangeStore::new(BytesTable::new(&*transaction, table))
            .get(&automerge_key)
            .await
            .map_err(EngineError::custom)?
            .is_some();
        if already_applied {
            return self.get_row(transaction, table, &row).await;
        }
        let Some(mut document) = Self::document(transaction, table, &document_id).await? else {
            return Err(EngineError::SyncDependencyUnavailable);
        };
        let previous_heads = document.get_heads();
        document
            .load_incremental(payload)
            .map_err(EngineError::custom)?;
        if !document
            .get_changes(&previous_heads)
            .iter()
            .any(|change| change.hash().0 == hash)
        {
            return Err(EngineError::custom(
                "Incremental change hash does not match key",
            ));
        }
        let columns = Self::document_columns(transaction, table, &row, &document).await?;
        let value = Self::decode_row(&document, &columns)?;
        AutomergeChangeStore::new(BytesTableTransaction::new(transaction, table))
            .insert(automerge_key, payload.to_vec())
            .await
            .map_err(EngineError::custom)?;
        Ok((!Self::metadata_deleted(transaction, table, &row).await?).then_some(value))
    }
}

impl AutomergeRowCodec {
    fn export_metadata_rows<T>(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(RowIdentity, Vec<u8>)>> + Send
    where
        T: KernelTransaction,
    {
        stream! {
            let entries = transaction.scan_bytes(table);
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry?;
                let key = DocumentChangeKey::decode_ordered(&key).map_err(EngineError::custom)?;
                if key.r#type().is_metadata() { yield Ok((Self::row_id(key.id())?, value)); }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use engine::RowIdentity;
    use uuid::Uuid;

    use super::AutomergeRowCodec;

    #[test]
    fn row_identity_bytes_round_trip_through_document_ids() {
        let identities = [
            RowIdentity::user(Uuid::from_bytes([7; 16])),
            RowIdentity::catalog(vec![7; 16]),
            RowIdentity::catalog(vec![1, 2, 3]),
        ];

        for identity in identities {
            let id = AutomergeRowCodec::document_id(&identity);
            assert_eq!(id, identity.to_bytes());
            assert_eq!(
                AutomergeRowCodec::row_id(&id).expect("identity bytes were encoded"),
                identity
            );
        }
    }
}
