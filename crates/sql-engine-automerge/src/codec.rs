use std::{
    borrow::Cow,
    collections::BTreeMap,
    string::String,
    time::{Duration, Instant},
    vec::Vec,
};

use core::ops::Bound;

use async_stream::stream;
use automerge::legacy::{Key, ObjectId, OpType};
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, Change, ROOT, ReadDoc, ScalarValue, Value as AutomergeValue};
use btree::{BTreeRead, BTreeTransaction};
use btree_automerge::{
    AutomergeBTreeTransaction, AutomergeChangeStore, DocumentChangeKey, DocumentId,
    ThresholdPolicy, hash_heads, reconstruct_document_values,
};
use serde::{Deserialize, Serialize};

use engine::{
    BytesTable, BytesTableTransaction, ENGINE_TABLE_FIELDS_STORAGE, EngineError, EngineResult,
    KernelTransaction, RowCodec, Uuid, catalog_table_for_storage,
};
use futures::{Stream, StreamExt, pin_mut};
use sync::{InspectedHistory, StateDigest, SyncChangeId, SyncRowCodec, SyncStateUnit};
use value::{Row, Value, ValueType};

const COLUMN_COUNT_BYTES: usize = size_of::<u32>();
const DOCUMENT_COLUMNS: &str = "\0engine_columns";
const DOCUMENT_SCHEMA: &str = "\0engine_schema";

type CatalogRows<'a> = (&'a [(Uuid, Row)], &'a [Row]);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowMetadata {
    pub version: u8,
    pub deleted: bool,
    pub drop_events: Vec<Uuid>,
    pub observed_drops: Vec<Uuid>,
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

#[derive(Clone)]
struct Column {
    id: String,
    default: Value,
}

struct CatalogTiming {
    enabled: bool,
    reconstruction: Duration,
    count: usize,
}

impl Drop for CatalogTiming {
    fn drop(&mut self) {
        if self.enabled {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.automerge.catalog_reconstruction elapsed_us={} count={}",
                self.reconstruction.as_micros(), self.count);
        }
    }
}

impl AutomergeRowCodec {
    fn scoped_decode<'a, D: Deserialize<'a>>(bytes: &'a [u8]) -> EngineResult<D> {
        let (value, remaining) = postcard::take_from_bytes(bytes).map_err(EngineError::custom)?;
        if !remaining.is_empty() {
            return Err(EngineError::InvalidQuery(
                "Trailing bytes in scoped history value",
            ));
        }
        Ok(value)
    }

    fn document_id(row: &Uuid) -> DocumentId {
        row.as_bytes().to_vec()
    }

    fn row_id(id: &[u8]) -> EngineResult<Uuid> {
        Uuid::from_slice(id).map_err(EngineError::custom)
    }

    fn catalog_states<'a, T>(
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row, bool)>> + Send + 'a
    where
        T: KernelTransaction,
    {
        Self::catalog_states_range(transaction, table, (Bound::Unbounded, Bound::Unbounded))
    }

    fn catalog_states_range<'a, T>(
        transaction: &'a T,
        table: &'a str,
        range: (Bound<DocumentChangeKey>, Bound<DocumentChangeKey>),
    ) -> impl Stream<Item = EngineResult<(Uuid, Row, bool)>> + Send + 'a
    where
        T: KernelTransaction,
    {
        stream! {
            let mut timing = CatalogTiming {
                enabled: log::log_enabled!(target: "of::idp_stage_timing", log::Level::Info),
                reconstruction: Duration::ZERO,
                count: 0,
            };
            let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
            let documents = reconstruct_document_values(changes.range(range).filter_map(|entry| async {
                match &entry {
                    Ok((key, _)) if key.r#type().is_metadata() => None,
                    _ => Some(entry),
                }
            }));
            pin_mut!(documents);
            loop {
                let started = timing.enabled.then(Instant::now);
                let document = documents.next().await;
                if let Some(started) = started {
                    timing.reconstruction += started.elapsed();
                }
                let Some(document) = document else { break };
                timing.count += 1;
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

    fn columns<'a, T>(
        transaction: &'a T,
        table: &'a str,
        fallback_count: usize,
        catalog: Option<CatalogRows<'a>>,
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

            let tables = match catalog {
                Some((tables, _)) => futures::stream::iter(
                    tables.iter().map(|(id, row)| Ok((*id, row.clone(), false))),
                )
                .boxed(),
                None => Self::catalog_states(transaction, engine::ENGINE_TABLES_STORAGE).boxed(),
            };
            pin_mut!(tables);
            let mut table_id = None;
            while let Some(entry) = tables.next().await {
                let (identity, row, deleted) = entry?;
                if !deleted
                    && row.values.first().and_then(Value::as_text) == Some(table)
                    && table_id.is_none_or(|selected| identity > selected)
                {
                    table_id = Some(identity);
                }
            }
            let mut columns = Vec::new();
            if let Some(table_id) = table_id {
                let fields = match catalog {
                    Some((_, fields)) => futures::stream::iter(
                        fields
                            .iter()
                            .map(|field| Ok((Uuid::nil(), field.clone(), false))),
                    )
                    .boxed(),
                    None => Self::catalog_states(transaction, ENGINE_TABLE_FIELDS_STORAGE).boxed(),
                };
                pin_mut!(fields);
                while let Some(field) = fields.next().await {
                    let (_, field, deleted) = field?;
                    if deleted
                        || field.values.first().and_then(Value::as_uuid).copied() != Some(table_id)
                    {
                        continue;
                    }
                    let id = field
                        .values
                        .get(2)
                        .and_then(Value::to_text)
                        .ok_or(EngineError::custom("Invalid table field name"))?;
                    let index = field
                        .values
                        .get(5)
                        .and_then(Value::to_integer)
                        .ok_or(EngineError::custom("Invalid table field column index"))?;
                    let default = field
                        .values
                        .get(4)
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

    async fn row_metadata<T>(transaction: &T, table: &str, row: &Uuid) -> EngineResult<RowMetadata>
    where
        T: KernelTransaction,
    {
        let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
        let Some(bytes) = changes
            .get(&DocumentChangeKey::new_metadata(Self::document_id(row)))
            .await
            .map_err(EngineError::custom)?
        else {
            return Ok(RowMetadata {
                version: 1,
                deleted: false,
                drop_events: Vec::new(),
                observed_drops: Vec::new(),
            });
        };
        postcard::from_bytes(&bytes).map_err(EngineError::custom)
    }

    async fn metadata_deleted<T>(transaction: &T, table: &str, row: &Uuid) -> EngineResult<bool>
    where
        T: KernelTransaction,
    {
        Ok(Self::row_metadata(transaction, table, row).await?.deleted)
    }

    async fn document_has_dropped_definition<T>(
        transaction: &T,
        document: &AutoCommit,
    ) -> EngineResult<bool>
    where
        T: KernelTransaction,
    {
        for schema_id in Self::document_schema_ids(document)? {
            if !Self::row_metadata(transaction, engine::ENGINE_TABLES_STORAGE, &schema_id)
                .await?
                .drop_events
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn document_is_stale<T>(
        transaction: &T,
        table: &str,
        document: &AutoCommit,
    ) -> EngineResult<bool>
    where
        T: KernelTransaction,
    {
        if catalog_table_for_storage(table).is_some() {
            return Ok(false);
        }
        if Self::document_has_dropped_definition(transaction, document).await? {
            return Ok(true);
        }
        if !Self::document_schema_ids(document)?.is_empty() {
            return Ok(false);
        }
        let tables = Self::catalog_states(transaction, engine::ENGINE_TABLES_STORAGE);
        pin_mut!(tables);
        while let Some(entry) = tables.next().await {
            let (id, value, _) = entry?;
            if value.values.first().and_then(Value::as_text) == Some(table)
                && !Self::row_metadata(transaction, engine::ENGINE_TABLES_STORAGE, &id)
                    .await?
                    .drop_events
                    .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
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
        _row: &Uuid,
        document: &AutoCommit,
    ) -> EngineResult<Vec<Column>>
    where
        T: KernelTransaction,
    {
        let columns = Self::columns(transaction, table, 0, None).await?;
        if columns.is_empty() {
            Ok(Self::document_columns_for(document, &columns)?.into_owned())
        } else {
            Ok(columns)
        }
    }

    fn document_columns_for<'a>(
        document: &AutoCommit,
        columns: &'a [Column],
    ) -> EngineResult<Cow<'a, [Column]>> {
        if !columns.is_empty() {
            return Ok(Cow::Borrowed(columns));
        }
        if let Some(columns) = Self::stored_columns(document)? {
            return Ok(Cow::Owned(columns));
        }
        Ok(Cow::Owned(
            document
                .keys(ROOT)
                .filter(|key| key != DOCUMENT_COLUMNS)
                .map(|id| Column {
                    id: id.to_string(),
                    default: Value::Null,
                })
                .collect(),
        ))
    }

    async fn table_definition_id<T>(transaction: &T, table: &str) -> EngineResult<Option<Uuid>>
    where
        T: KernelTransaction,
    {
        let tables = Self::catalog_states(transaction, engine::ENGINE_TABLES_STORAGE);
        pin_mut!(tables);
        let mut selected = None;
        while let Some(entry) = tables.next().await {
            let (id, row, deleted) = entry?;
            if !deleted
                && row.values.first().and_then(Value::as_text) == Some(table)
                && selected.is_none_or(|selected_id| id > selected_id)
            {
                selected = Some(id);
            }
        }
        Ok(selected)
    }

    fn document_schema_ids(document: &AutoCommit) -> EngineResult<Vec<Uuid>> {
        let mut ids = Vec::new();
        for (value, _) in document
            .get_all(ROOT, DOCUMENT_SCHEMA)
            .map_err(EngineError::custom)?
        {
            let AutomergeValue::Scalar(value) = value else {
                return Err(EngineError::custom("Invalid row schema identity"));
            };
            let ScalarValue::Bytes(bytes) = value.as_ref() else {
                return Err(EngineError::custom("Invalid row schema identity"));
            };
            ids.push(Uuid::from_slice(bytes).map_err(EngineError::custom)?);
        }
        Ok(ids)
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
        let schema_id = if catalog_table_for_storage(table).is_none() {
            Self::table_definition_id(transaction, table).await?
        } else {
            None
        };
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
                        .and_then(|()| {
                            if Self::document_schema_ids(document)?.is_empty()
                                && let Some(schema_id) = schema_id
                            {
                                document
                                    .put(
                                        ROOT,
                                        DOCUMENT_SCHEMA,
                                        ScalarValue::Bytes(schema_id.as_bytes().to_vec()),
                                    )
                                    .map_err(EngineError::custom)?;
                            }
                            Ok(())
                        })
                        .map_err(btree::BTreeError::custom)
                })
                .await
                .map_err(EngineError::custom)?;
        } else {
            let mut document = self.encode_row(value, columns)?;
            if let Some(schema_id) = schema_id {
                document
                    .put(
                        ROOT,
                        DOCUMENT_SCHEMA,
                        ScalarValue::Bytes(schema_id.as_bytes().to_vec()),
                    )
                    .map_err(EngineError::custom)?;
            }
            changes
                .insert(id, document)
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

    async fn get_row(&self, transaction: &T, table: &str, row: &Uuid) -> EngineResult<Option<Row>> {
        if Self::metadata_deleted(transaction, table, row).await? {
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

    fn scan_rows<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row)>> + Send + 'a {
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
            let mut columns = None;

            while let Some(document) = documents.next().await {
                let (id, document) = document.map_err(EngineError::custom)?;
                let row = Self::row_id(&id)?;
                if Self::metadata_deleted(transaction, table, &row).await? {
                    continue;
                }

                if columns.is_none() {
                    columns = Some(Self::columns(transaction, table, 0, None).await?);
                }
                let columns = Self::document_columns_for(
                    &document,
                    columns.as_deref().expect("scan initialized current column layout"),
                )?;
                yield Self::decode_row(&document, &columns).map(|value| (row, value));
            }
        }
    }

    fn scan_row_states<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row, bool)>> + Send + 'a {
        stream! {
            if catalog_table_for_storage(table).is_some() {
                let states = Self::catalog_states(transaction, table);
                pin_mut!(states);
                while let Some(state) = states.next().await { yield state; }
            } else {
                let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
                let documents = reconstruct_document_values(changes.range((
                                    Bound::<DocumentChangeKey>::Unbounded,
                                    Bound::<DocumentChangeKey>::Unbounded,
                                )).filter_map(|entry| async {
                    match &entry {
                        Ok((key, _)) if key.r#type().is_metadata() => None,
                        _ => Some(entry),
                    }
                }));
                pin_mut!(documents);
                let mut columns = None;
                while let Some(document) = documents.next().await {
                    let (id, document) = document.map_err(EngineError::custom)?;
                    let row = Self::row_id(&id)?;
                    let deleted = Self::metadata_deleted(transaction, table, &row).await?;
                    if columns.is_none() {
                        columns = Some(Self::columns(transaction, table, 0, None).await?);
                    }
                    let columns = Self::document_columns_for(
                        &document,
                        columns.as_deref().expect("scan initialized current column layout"),
                    )?;
                    yield Self::decode_row(&document, &columns).map(|value| (row, value, deleted));
                }
            }
        }
    }

    async fn encode_row(
        &self,
        transaction: &T,
        table: &str,
        row_id: &Uuid,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let table_name = table;
        let id = Self::document_id(row_id);
        let mut document = Self::document(transaction, table, &id)
            .await?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(self.actor.clone());
        let columns = Self::columns(transaction, table_name, row.values.len(), None).await?;
        if Self::has_conflicts(&document, &columns, changed_columns)? {
            return Err(EngineError::InvalidQuery(
                "Logical row column is conflicted",
            ));
        }
        let previous_heads = document.get_heads();
        Self::update_document_columns(&mut document, row, &columns, changed_columns)?;
        Self::encode_incremental(columns.len(), document.save_after(&previous_heads))
    }

    async fn conflicted_columns(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
        table_rows: &[(Uuid, Row)],
        table_fields: &[Row],
    ) -> EngineResult<Vec<usize>> {
        let table_name = table;
        let id = Self::document_id(row);
        let started =
            log::log_enabled!(target: "of::idp_stage_timing", log::Level::Info).then(Instant::now);
        let document = Self::document(transaction, table, &id).await;
        if let Some(started) = started {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.automerge.conflict_document_reconstruction elapsed_us={}",
                started.elapsed().as_micros());
        }
        let Some(document) = document? else {
            return Ok(Vec::new());
        };
        let started = started.map(|_| Instant::now());
        let columns = Self::columns(transaction, table_name, 0, Some((table_rows, table_fields)))
            .await
            .and_then(|columns| {
                if columns.is_empty() {
                    Ok(Self::document_columns_for(&document, &columns)?.into_owned())
                } else {
                    Ok(columns)
                }
            });
        if let Some(started) = started {
            log::info!(target: "of::idp_stage_timing",
                "ofdb.automerge.conflict_document_columns elapsed_us={}",
                started.elapsed().as_micros());
        }
        Self::conflicted_columns(&document, &columns?)
    }

    async fn conflict_values(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
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
        row_id: &Uuid,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let table_name = table;
        let id = Self::document_id(row_id);
        let mut document = Self::document(transaction, table, &id)
            .await?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(self.actor.clone());
        let columns = Self::columns(transaction, table_name, row.values.len(), None).await?;
        let previous_heads = document.get_heads();
        Self::update_document_columns(&mut document, row, &columns, changed_columns)?;
        Self::encode_incremental(columns.len(), document.save_after(&previous_heads))
    }

    async fn merge_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
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
        if catalog_table_for_storage(table).is_none()
            && Self::document_schema_ids(&document)?.is_empty()
            && let Some(schema_id) = Self::table_definition_id(transaction, table).await?
        {
            document
                .put(
                    ROOT,
                    DOCUMENT_SCHEMA,
                    ScalarValue::Bytes(schema_id.as_bytes().to_vec()),
                )
                .map_err(EngineError::custom)?;
        }
        if Self::document_is_stale(transaction, table, &document).await? {
            self.delete_row(transaction, table, &row).await?;
            return Ok(None);
        }
        let columns = Self::columns(transaction, table_name, column_count, None).await?;
        let row = Self::decode_row(&document, &columns)?;

        let mut changes = AutomergeChangeStore::new(BytesTableTransaction::new(transaction, table));
        let (key, bytes) = if !was_existing {
            (
                DocumentChangeKey::new_snapshot(id, hash_heads(document.get_heads())),
                document.save(),
            )
        } else {
            let changes = document.get_changes(&previous_heads);
            let Some(change) = changes.last() else {
                if incremental.is_empty() {
                    return Ok(Some(row));
                }
                return Err(EngineError::custom("Incremental payload has no change"));
            };
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
        row: &Uuid,
    ) -> EngineResult<Option<Row>> {
        let value = self.get_row(transaction, table, row).await?;
        let key = DocumentChangeKey::new_metadata(Self::document_id(row)).encode_ordered();
        let mut metadata = Self::row_metadata(transaction, table, row).await?;
        metadata.deleted = true;
        transaction
            .put_bytes(
                table,
                key,
                postcard::to_allocvec(&metadata).map_err(EngineError::custom)?,
            )
            .await?;
        Ok(value)
    }

    async fn row_is_deleted(&self, transaction: &T, table: &str, row: &Uuid) -> EngineResult<bool> {
        Self::metadata_deleted(transaction, table, row).await
    }

    async fn row_has_dropped_definition(
        &self,
        transaction: &T,
        table: &str,
        row: &Uuid,
    ) -> EngineResult<bool> {
        let Some(document) = Self::document(transaction, table, &Self::document_id(row)).await?
        else {
            return Ok(false);
        };
        Self::document_has_dropped_definition(transaction, &document).await
    }

    async fn table_definition_was_dropped(
        &self,
        transaction: &T,
        row: &Uuid,
    ) -> EngineResult<bool> {
        Ok(
            !Self::row_metadata(transaction, engine::ENGINE_TABLES_STORAGE, row)
                .await?
                .drop_events
                .is_empty(),
        )
    }

    async fn drop_table_definition(
        &self,
        transaction: &mut T,
        row: &Uuid,
        event: Uuid,
    ) -> EngineResult<()> {
        let mut metadata =
            Self::row_metadata(transaction, engine::ENGINE_TABLES_STORAGE, row).await?;
        metadata.deleted = true;
        metadata.drop_events.push(event);
        metadata.drop_events.sort_unstable();
        metadata.drop_events.dedup();
        transaction
            .put_bytes(
                engine::ENGINE_TABLES_STORAGE,
                DocumentChangeKey::new_metadata(Self::document_id(row)).encode_ordered(),
                postcard::to_allocvec(&metadata).map_err(EngineError::custom)?,
            )
            .await
    }

    async fn table_drop_events(&self, transaction: &T, table: &str) -> EngineResult<Vec<Uuid>> {
        let rows = Self::catalog_states(transaction, engine::ENGINE_TABLES_STORAGE);
        pin_mut!(rows);
        let mut events = Vec::new();
        while let Some(entry) = rows.next().await {
            let (id, value, _) = entry?;
            if value.values.first().and_then(Value::as_text) == Some(table) {
                events.extend(
                    Self::row_metadata(transaction, engine::ENGINE_TABLES_STORAGE, &id)
                        .await?
                        .drop_events,
                );
            }
        }
        events.sort_unstable();
        events.dedup();
        Ok(events)
    }

    async fn set_observed_table_drops(
        &self,
        transaction: &mut T,
        row: &Uuid,
        events: Vec<Uuid>,
    ) -> EngineResult<()> {
        let mut metadata =
            Self::row_metadata(transaction, engine::ENGINE_TABLES_STORAGE, row).await?;
        metadata.observed_drops = events;
        metadata.observed_drops.sort_unstable();
        metadata.observed_drops.dedup();
        transaction
            .put_bytes(
                engine::ENGINE_TABLES_STORAGE,
                DocumentChangeKey::new_metadata(Self::document_id(row)).encode_ordered(),
                postcard::to_allocvec(&metadata).map_err(EngineError::custom)?,
            )
            .await
    }

    async fn observed_table_drops(&self, transaction: &T, row: &Uuid) -> EngineResult<Vec<Uuid>> {
        Ok(
            Self::row_metadata(transaction, engine::ENGINE_TABLES_STORAGE, row)
                .await?
                .observed_drops,
        )
    }

    async fn put_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        value: Row,
    ) -> EngineResult<()> {
        if Self::metadata_deleted(transaction, table, &row).await? {
            return Err(EngineError::InvalidQuery("Logical row is metadatad"));
        }
        let table_name = table;
        let columns = Self::columns(transaction, table_name, value.values.len(), None).await?;
        let id = Self::document_id(&row);
        self.store_row(transaction, table, id, &columns, &value)
            .await
    }

    async fn remove_row(
        &self,
        transaction: &mut T,
        table: &str,
        row: &Uuid,
    ) -> EngineResult<Option<Row>> {
        self.delete_row(transaction, table, row).await
    }
}

impl<T> SyncRowCodec<T> for AutomergeRowCodec
where
    T: KernelTransaction + Send,
{
    async fn candidate_columns(&self, transaction: &T, table: &str) -> EngineResult<Vec<String>> {
        let columns = Self::columns(transaction, table, 0, None).await?;
        if columns.is_empty() {
            return Err(EngineError::Unsupported(
                "Candidate rows require a transaction catalog layout",
            ));
        }
        Ok(columns.into_iter().map(|column| column.id).collect())
    }

    async fn inspect_history(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        payload: &[u8],
        metadata: &[u8],
        columns: &[(String, ValueType, Value, bool)],
    ) -> EngineResult<InspectedHistory> {
        if !metadata.is_empty() {
            let metadata: RowMetadata = Self::scoped_decode(metadata)?;
            if metadata.version != 1
                || !metadata.drop_events.is_empty()
                || !metadata.observed_drops.is_empty()
            {
                return Err(EngineError::Unsupported(
                    "Scoped sessions require immutable catalog history",
                ));
            }
        }
        let incoming_change = Change::from_bytes(payload.to_vec()).ok();
        if incoming_change.is_none() {
            let incoming = AutoCommit::load(payload).map_err(EngineError::custom)?;
            if !incoming.get_missing_deps(&[]).is_empty() {
                return Err(EngineError::Unsupported(
                    "Scoped history contains uninspectable queued changes",
                ));
            }
        }
        let mut document = Self::document(transaction, table, &Self::document_id(&row))
            .await?
            .unwrap_or_else(AutoCommit::new);
        if !document.get_missing_deps(&[]).is_empty() {
            return Err(EngineError::Unsupported(
                "Existing scoped history contains uninspectable queued changes",
            ));
        }
        document
            .load_incremental(payload)
            .map_err(EngineError::custom)?;
        let mut changes = document.get_changes(&[]);
        if let Some(change) = incoming_change {
            changes.push(change);
        }
        if changes.is_empty() {
            return Err(EngineError::custom(
                "Scoped payload has no inspectable history",
            ));
        }
        let layout = columns
            .iter()
            .map(|(id, _, default, _)| Column {
                id: id.clone(),
                default: default.clone(),
            })
            .collect::<Vec<_>>();
        let value = Self::decode_row(&document, &layout)?;
        let catalog = catalog_table_for_storage(table).is_some();
        let mut schema_ids = Vec::new();
        let mut observations = Vec::new();
        for change in changes {
            if !change.extra_bytes().is_empty() || change.message().is_some() {
                return Err(EngineError::custom("Unsupported scoped change metadata"));
            }
            for op in change.decode().operations {
                let Key::Map(key) = op.key else {
                    return Err(EngineError::custom(
                        "Scoped history contains a sequence property",
                    ));
                };
                if op.obj != ObjectId::Root || op.insert {
                    return Err(EngineError::custom(
                        "Scoped history contains a nested property",
                    ));
                }
                let name = key.as_str();
                let column = columns.iter().position(|(id, _, _, _)| id == name);
                if column.is_none()
                    && name != DOCUMENT_COLUMNS
                    && (name != DOCUMENT_SCHEMA || catalog)
                {
                    return Err(EngineError::custom("Unknown historical scoped column"));
                }
                match op.action {
                    OpType::Put(ScalarValue::Bytes(bytes)) => {
                        if name == DOCUMENT_COLUMNS {
                            let stored: Vec<(String, Value)> = Self::scoped_decode(&bytes)?;
                            if stored.len() != columns.len()
                                || stored.iter().enumerate().any(|(i, (id, default))| {
                                    stored[..i].iter().any(|(previous, _)| previous == id)
                                        || !columns.iter().any(|(allowed, _, expected, _)| {
                                            allowed == id && expected == default
                                        })
                                })
                            {
                                return Err(EngineError::custom(
                                    "Historical scoped layout differs from local schema",
                                ));
                            }
                            observations.extend(stored);
                        } else if name == DOCUMENT_SCHEMA {
                            schema_ids.push(Uuid::from_slice(&bytes).map_err(EngineError::custom)?);
                        } else {
                            let index = column.expect("known column checked before decoding");
                            let decoded: Value = Self::scoped_decode(&bytes)?;
                            if columns[index].3 && decoded != Value::Uuid(row) {
                                return Err(EngineError::custom(
                                    "Historical scoped primary key differs from row identity",
                                ));
                            }
                            let expected = columns[index].1;
                            if decoded != Value::Null
                                && expected != ValueType::Null
                                && decoded.r#type() != expected
                            {
                                return Err(EngineError::custom(
                                    "Invalid historical scoped value type",
                                ));
                            }
                            observations.push((name.to_string(), decoded.clone()));
                            if catalog && value.values[index] != decoded {
                                return Err(EngineError::custom(
                                    "Scoped catalog history is not immutable",
                                ));
                            }
                        }
                    }
                    OpType::Delete
                        if !catalog && (column.is_some() || name == DOCUMENT_COLUMNS) => {}
                    _ => {
                        return Err(EngineError::custom(format!(
                            "Malformed historical scoped property {table}.{name}"
                        )));
                    }
                }
            }
        }
        if catalog && !Self::conflicted_columns(&document, &layout)?.is_empty() {
            return Err(EngineError::custom("Conflicted scoped catalog"));
        }
        schema_ids.sort_unstable();
        schema_ids.dedup();
        Ok((value, schema_ids, observations))
    }

    fn row_ids<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<Uuid>> + Send + 'a {
        stream! {
            let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
            let entries = changes.range(..);
            pin_mut!(entries);
            let mut previous = None;
            while let Some(entry) = entries.next().await {
                let (key, _) = entry.map_err(EngineError::custom)?;
                if previous.as_ref() == Some(key.id()) {
                    continue;
                }
                let row = Self::row_id(key.id())?;
                previous = Some(key.id().clone());
                yield Ok(row);
            }
        }
    }

    async fn export_state(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> EngineResult<Option<Vec<u8>>> {
        let id = Self::document_id(&row);
        let Some(mut document) = Self::document(transaction, table, &id).await? else {
            return Ok(None);
        };
        let state = document.save();
        if state.len() > max_bytes {
            return Err(EngineError::custom(format!(
                "sync state payload of {} bytes exceeds the {max_bytes}-byte budget",
                state.len()
            )));
        }
        Ok(Some(state))
    }

    fn manifest_digest(&self, state: &[u8], metadata: &[u8]) -> EngineResult<StateDigest> {
        let heads = if state.is_empty() {
            [0; 32]
        } else {
            let mut document = AutoCommit::load(state).map_err(EngineError::custom)?;
            hash_heads(document.get_heads())
        };
        Ok(SyncStateUnit::digest_parts(&heads, metadata))
    }

    async fn merge_state(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
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
        let stale_definition = Self::document_is_stale(transaction, table, &document).await?;
        if stale_definition {
            self.delete_row(transaction, table, &row).await?;
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
        Ok((!deleted && !stale_definition).then_some(value))
    }

    async fn export_metadata(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
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
        row: Uuid,
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
            deleted: incoming.deleted || existing.as_ref().is_some_and(|value| value.deleted),
            drop_events: existing
                .as_ref()
                .map(|value| value.drop_events.clone())
                .unwrap_or_default()
                .into_iter()
                .chain(incoming.drop_events)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            observed_drops: existing
                .as_ref()
                .map(|value| value.observed_drops.clone())
                .unwrap_or_default()
                .into_iter()
                .chain(incoming.observed_drops)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
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
        row: Uuid,
        max_bytes: usize,
    ) -> EngineResult<Vec<SyncChangeId>> {
        let id = Self::document_id(&row);
        let changes = AutomergeChangeStore::new(BytesTable::new(transaction, table));
        let entries = changes.range(DocumentChangeKey::range_for(&id));
        pin_mut!(entries);
        let mut inventory = BTreeMap::new();
        let mut used_bytes = 0usize;
        while let Some(entry) = entries.next().await {
            let (key, payload) = entry.map_err(EngineError::custom)?;
            if key.r#type().is_incremental() {
                used_bytes = used_bytes
                    .saturating_add(payload.len())
                    .saturating_add(key.change_hash().len())
                    .saturating_add(128);
                if used_bytes > max_bytes {
                    return Err(EngineError::custom(format!(
                        "sync change inventory exceeds the {max_bytes}-byte budget"
                    )));
                }
                inventory.insert(*key.change_hash(), SyncChangeId(key.change_hash().to_vec()));
            }
        }
        let Some(mut document) = Self::document(transaction, table, &id).await? else {
            return Ok(inventory.into_values().collect());
        };
        let mut known = Vec::new();
        for change in document.get_changes(&[]) {
            let hash = change.hash().0;
            let id = if let Some(id) = inventory.remove(&hash) {
                id
            } else {
                used_bytes = used_bytes.saturating_add(hash.len()).saturating_add(128);
                if used_bytes > max_bytes {
                    return Err(EngineError::custom(format!(
                        "sync change inventory exceeds the {max_bytes}-byte budget"
                    )));
                }
                SyncChangeId(hash.to_vec())
            };
            known.push(id);
        }
        Ok(known)
    }

    async fn export_change(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        id: &SyncChangeId,
        max_bytes: usize,
    ) -> EngineResult<Option<Vec<u8>>> {
        let hash: [u8; 32] =
            id.0.as_slice()
                .try_into()
                .map_err(|_| EngineError::custom("Invalid sync change ID"))?;
        let key = DocumentChangeKey::new_incremental(Self::document_id(&row), hash);
        let payload = AutomergeChangeStore::new(BytesTable::new(transaction, table))
            .get(&key)
            .await
            .map_err(EngineError::custom)?;
        if let Some(payload) = &payload
            && payload.len() > max_bytes
        {
            return Err(EngineError::custom(format!(
                "sync change payload of {} bytes exceeds the {max_bytes}-byte budget",
                payload.len()
            )));
        }
        Ok(payload)
    }

    async fn apply_change(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        id: &SyncChangeId,
        payload: &[u8],
    ) -> EngineResult<()> {
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
        if already_applied || Self::metadata_deleted(transaction, table, &row).await? {
            return Ok(());
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
        Self::decode_row(&document, &columns)?;
        let stale_definition = Self::document_is_stale(transaction, table, &document).await?;
        AutomergeChangeStore::new(BytesTableTransaction::new(transaction, table))
            .insert(automerge_key, payload.to_vec())
            .await
            .map_err(EngineError::custom)?;
        if stale_definition {
            self.delete_row(transaction, table, &row).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::AutomergeRowCodec;

    #[test]
    fn uuid_round_trips_through_document_ids() {
        let row = Uuid::from_bytes([7; 16]);
        let id = AutomergeRowCodec::document_id(&row);
        assert_eq!(id, row.as_bytes());
        assert_eq!(
            AutomergeRowCodec::row_id(&id).expect("UUID bytes were encoded"),
            row
        );
    }
}
