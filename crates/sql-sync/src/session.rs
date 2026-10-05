use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet},
    format,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};

use futures::{StreamExt, pin_mut};

use engine::{
    ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
    ENGINE_TABLES_STORAGE, Engine, EngineError, Kernel, KernelTransaction, MutationSummary,
    RowIdentity,
};
use thiserror::Error;

use crate::{
    PROTOCOL_VERSION, SyncHello, SyncIncrementalChange, SyncKey, SyncManifest, SyncMessage,
    SyncRowCodec, SyncRowInventory, SyncSnapshotRequest, SyncStateUnit, SyncTransport,
};

/// Maximum serialized sync message size (1 MiB). Transports must also bound reads
/// before buffering frames; the session checks received frames only afterward.
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_SESSION_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRole {
    Initiator,
    Responder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionConfig {
    pub max_units_per_frame: usize,
    pub max_session_bytes: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_units_per_frame: 64,
            max_session_bytes: DEFAULT_MAX_SESSION_BYTES,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncResult {
    pub received_units: usize,
    pub sent_units: usize,
    pub received_changes: usize,
    pub sent_changes: usize,
    pub received_snapshots: usize,
    pub sent_snapshots: usize,
}

#[derive(Debug, Error)]
pub enum SyncError<E> {
    #[error("engine error: {0}")]
    Engine(#[from] EngineError),

    #[error("transport error: {0}")]
    Transport(E),

    #[error("invalid session configuration")]
    InvalidConfiguration,

    #[error("protocol codec error: {0}")]
    Protocol(String),

    #[error("sync message size {size} exceeds {max} bytes")]
    MessageTooLarge { size: usize, max: usize },

    #[error("sync session payload size {size} exceeds {max} bytes")]
    SessionTooLarge { size: usize, max: usize },

    #[error("incompatible protocol version: {0}")]
    IncompatibleProtocol(u16),

    #[error("unexpected sync message")]
    UnexpectedMessage,

    #[error("remote sync aborted: {0}")]
    RemoteAbort(String),
}

pub async fn synchronize<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
) -> Result<SyncResult, SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    if config.max_units_per_frame == 0 || config.max_session_bytes == 0 {
        return Err(SyncError::InvalidConfiguration);
    }

    exchange_hello(engine, transport, role).await?;

    let remote_manifest = exchange_manifest(engine, transport, role).await?;
    let local_units = with_engine_abort(
        transport,
        export_sync_state_limited(engine, config.max_session_bytes).await,
    )
    .await?;

    let local_inventories = with_engine_abort(
        transport,
        inventories_for(engine, &local_units, config.max_session_bytes).await,
    )
    .await?;

    let remote_inventories = exchange_inventories(transport, role, local_inventories).await?;

    let remote_inventories = remote_inventories
        .into_iter()
        .map(|inventory| (inventory.key(), inventory))
        .collect::<BTreeMap<_, _>>();
    let outbound = with_engine_abort(
        transport,
        build_outbound(
            engine,
            local_units,
            &remote_manifest,
            remote_inventories,
            config.max_session_bytes,
        )
        .await,
    )
    .await?;

    let (sent, mut received) = match role {
        SyncRole::Initiator => {
            let sent = send_outbound(transport, &outbound, config.max_units_per_frame).await?;
            let received = receive_outbound(
                engine,
                transport,
                &remote_manifest,
                config.max_session_bytes,
            )
            .await?;
            (sent, received)
        }
        SyncRole::Responder => {
            let received = receive_outbound(
                engine,
                transport,
                &remote_manifest,
                config.max_session_bytes,
            )
            .await?;
            let sent = send_outbound(transport, &outbound, config.max_units_per_frame).await?;
            (sent, received)
        }
    };

    let local_requests = core::mem::take(&mut received.requests);
    let remote_requests = exchange_snapshot_requests(transport, role, &local_requests).await?;
    let recovery = with_engine_abort(
        transport,
        recovery_snapshots(engine, remote_requests, config.max_session_bytes).await,
    )
    .await?;
    let (sent_recovery, (received_recovery, retried_changes)) = match role {
        SyncRole::Initiator => {
            let sent = send_snapshots(transport, &recovery, config.max_units_per_frame).await?;
            let received = receive_recovery(
                engine,
                transport,
                &mut received,
                &local_requests,
                &remote_manifest,
                config.max_session_bytes,
            )
            .await?;
            (sent, received)
        }
        SyncRole::Responder => {
            let received = receive_recovery(
                engine,
                transport,
                &mut received,
                &local_requests,
                &remote_manifest,
                config.max_session_bytes,
            )
            .await?;
            let sent = send_snapshots(transport, &recovery, config.max_units_per_frame).await?;
            (sent, received)
        }
    };

    Ok(SyncResult {
        sent_units: sent.snapshots + sent_recovery,
        received_units: received.snapshots + received_recovery,
        sent_changes: sent.changes,
        received_changes: received.changes + retried_changes,
        sent_snapshots: sent.snapshots + sent_recovery,
        received_snapshots: received.snapshots + received_recovery,
    })
}

#[derive(Default)]
struct Outbound {
    snapshots: Vec<SyncStateUnit>,
    changes: Vec<SyncIncrementalChange>,
}

#[derive(Default)]
struct TransferCount {
    snapshots: usize,
    changes: usize,
}

struct Received {
    snapshots: usize,
    changes: usize,
    requests: Vec<SyncSnapshotRequest>,
    pending_snapshots: Vec<SyncStateUnit>,
    pending: Vec<SyncIncrementalChange>,
    received_bytes: usize,
}

pub async fn sync_manifest_for<K, R>(engine: &Engine<K, R>) -> Result<SyncManifest, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    sync_manifest_limited(engine, usize::MAX).await
}

async fn sync_manifest_limited<K, R>(
    engine: &Engine<K, R>,
    max_bytes: usize,
) -> Result<SyncManifest, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut used_bytes = 0;
    let mut entries = Vec::new();
    let mut tables = [
        ENGINE_TABLES_STORAGE,
        ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_INDICES_STORAGE,
        ENGINE_INDEX_FIELDS_STORAGE,
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    tables.extend(engine.table_names().await?);
    tables.sort_unstable();
    tables.dedup();
    for table in tables {
        let table_for_rows = table.clone();
        let row_budget = max_bytes.saturating_sub(used_bytes);
        let rows = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .row_ids(transaction, &table_for_rows, row_budget)
                        .await
                })
            })
            .await?;
        for row in rows {
            let table_for_row = table.clone();
            let row_for_state = row.clone();
            let state_budget = max_bytes.saturating_sub(used_bytes);
            let (state, metadata) = engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move {
                        Ok((
                            codec
                                .export_state(
                                    transaction,
                                    &table_for_row,
                                    row_for_state.clone(),
                                    state_budget,
                                )
                                .await?
                                .unwrap_or_default(),
                            codec
                                .export_metadata(transaction, &table_for_row, row_for_state)
                                .await?,
                        ))
                    })
                })
                .await?;
            if !state.is_empty() || !metadata.is_empty() {
                let key = SyncKey::Row {
                    table: table.clone(),
                    row,
                };
                let digest = SyncStateUnit::new(key.clone(), state, metadata).digest;
                let SyncKey::Row { table, row } = &key;
                let entry_bytes = table
                    .len()
                    .saturating_add(row.to_bytes().len())
                    .saturating_add(48);
                used_bytes = account_engine_payload(used_bytes, entry_bytes, max_bytes)?;
                entries.push((key, digest));
            }
        }
    }
    Ok(SyncManifest::new(entries))
}

pub async fn export_sync_state_for<K, R>(
    engine: &Engine<K, R>,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    export_sync_state_limited(engine, usize::MAX).await
}

async fn export_sync_state_limited<K, R>(
    engine: &Engine<K, R>,
    max_bytes: usize,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut used_bytes = 0;
    let mut units = Vec::new();
    let mut tables = [
        ENGINE_TABLES_STORAGE,
        ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_INDICES_STORAGE,
        ENGINE_INDEX_FIELDS_STORAGE,
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    tables.extend(engine.table_names().await?);
    tables.sort_unstable();
    tables.dedup();
    for table in tables {
        let table_for_rows = table.clone();
        let row_budget = max_bytes.saturating_sub(used_bytes);
        let rows = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .row_ids(transaction, &table_for_rows, row_budget)
                        .await
                })
            })
            .await?;
        for row in rows {
            let table_for_row = table.clone();
            let row_for_state = row.clone();
            let state_budget = max_bytes.saturating_sub(used_bytes);
            let (state, metadata) = engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move {
                        Ok((
                            codec
                                .export_state(
                                    transaction,
                                    &table_for_row,
                                    row_for_state.clone(),
                                    state_budget,
                                )
                                .await?
                                .unwrap_or_default(),
                            codec
                                .export_metadata(transaction, &table_for_row, row_for_state)
                                .await?,
                        ))
                    })
                })
                .await?;
            if !state.is_empty() || !metadata.is_empty() {
                let state_bytes = state
                    .len()
                    .saturating_add(metadata.len())
                    .saturating_add(table.len())
                    .saturating_add(row.to_bytes().len())
                    .saturating_add(64);
                used_bytes = account_engine_payload(used_bytes, state_bytes, max_bytes)?;
                units.push(SyncStateUnit::new(
                    SyncKey::Row {
                        table: table.clone(),
                        row,
                    },
                    state,
                    metadata,
                ));
            }
        }
    }
    units.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    Ok(units)
}

pub async fn apply_sync_state_for<K, R>(
    engine: &Engine<K, R>,
    unit: SyncStateUnit,
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    apply_sync_state_batch_for(engine, alloc::vec![unit]).await
}

pub async fn apply_sync_state_batch_for<K, R>(
    engine: &Engine<K, R>,
    batch: Vec<SyncStateUnit>,
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    apply_received_batch(engine, Arc::new(batch), Arc::new(Vec::new()), None).await
}

async fn apply_received_batch<K, R>(
    engine: &Engine<K, R>,
    snapshots: Arc<Vec<SyncStateUnit>>,
    changes: Arc<Vec<SyncIncrementalChange>>,
    manifest: Option<&SyncManifest>,
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    validate_state_batch(&snapshots)?;
    validate_change_batch(&changes)?;
    let mut tables = BTreeSet::new();
    for unit in snapshots.iter() {
        let SyncKey::Row { table, .. } = &unit.key;
        tables.insert(table.clone());
    }
    for change in changes.iter() {
        tables.insert(change.table.clone());
    }
    let tables = tables.into_iter().collect::<Vec<_>>();
    let expected = manifest.cloned();
    engine
        .mutate_tables(tables.as_slice(), move |codec, transaction| {
            let snapshots = Arc::clone(&snapshots);
            let changes = Arc::clone(&changes);
            Box::pin(async move {
                let mut mutations = MutationSummary::default();
                for order in 0..4 {
                    for unit in snapshots.iter() {
                        let SyncKey::Row { table, .. } = &unit.key;
                        if catalog_order(table) == order {
                            merge_snapshot(codec, transaction, unit).await?;
                            mutations.record_table(table);
                        }
                    }
                }
                for order in 0..4 {
                    for change in changes.iter() {
                        if catalog_order(&change.table) == order {
                            merge_change(codec, transaction, change).await?;
                            mutations.record_table(&change.table);
                        }
                    }
                }
                for unit in snapshots.iter() {
                    let SyncKey::Row { table, .. } = &unit.key;
                    if catalog_order(table) == 4 {
                        merge_snapshot(codec, transaction, unit).await?;
                        mutations.record_table(table);
                    }
                }
                for change in changes.iter() {
                    if catalog_order(&change.table) == 4 {
                        merge_change(codec, transaction, change).await?;
                        mutations.record_table(&change.table);
                    }
                }
                validate_catalog(
                    codec,
                    transaction,
                    snapshots.as_slice(),
                    changes.as_slice(),
                    expected.as_ref(),
                )
                .await?;
                Ok(((), mutations))
            })
        })
        .await
}

async fn merge_snapshot<T, R>(
    codec: &R,
    transaction: &mut T,
    unit: &SyncStateUnit,
) -> Result<(), EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    let SyncKey::Row { table, row } = &unit.key;
    if !unit.metadata.is_empty() {
        codec
            .merge_metadata(transaction, table, row.clone(), &unit.metadata)
            .await?;
    }
    let new = if unit.state.is_empty() {
        None
    } else {
        codec
            .merge_state(transaction, table, row.clone(), &unit.state)
            .await?
    };
    if new.is_none() && !codec.row_is_deleted(transaction, table, row).await? {
        return Err(EngineError::custom(format!(
            "Sync state for {} row {} produced no row",
            table, row
        )));
    }
    Ok(())
}

async fn merge_change<T, R>(
    codec: &R,
    transaction: &mut T,
    change: &SyncIncrementalChange,
) -> Result<(), EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    codec
        .apply_change(
            transaction,
            &change.table,
            change.row.clone(),
            &change.id,
            &change.payload,
        )
        .await?;
    Ok(())
}

async fn validate_catalog<T, R>(
    codec: &R,
    transaction: &T,
    snapshots: &[SyncStateUnit],
    changes: &[SyncIncrementalChange],
    manifest: Option<&SyncManifest>,
) -> Result<(), EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    if snapshots.is_empty() && changes.is_empty() {
        return Ok(());
    }
    let catalog_tables = [
        ENGINE_TABLES_STORAGE,
        ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_INDICES_STORAGE,
        ENGINE_INDEX_FIELDS_STORAGE,
    ];
    if let Some(manifest) = manifest {
        for (key, _) in &manifest.entries {
            let SyncKey::Row { table, row } = key;
            if catalog_order(table) < 4
                && !catalog_has_active_row(codec, transaction, table, row).await?
                && !codec.row_is_deleted(transaction, table, row).await?
            {
                return Err(EngineError::custom(format!(
                    "Incomplete catalog: missing {} row {} advertised in manifest",
                    table, row
                )));
            }
        }
    }
    for table in catalog_tables {
        let rows = codec.scan_row_states(transaction, table);
        pin_mut!(rows);
        while let Some(state) = rows.next().await {
            let (id, value, deleted) = state?;
            if deleted {
                continue;
            }
            let RowIdentity::Catalog(bytes) = &id else {
                return Err(EngineError::custom(format!(
                    "Invalid catalog identity in {}",
                    table
                )));
            };
            let parent = match table {
                ENGINE_TABLES_STORAGE if bytes.len() == 16 => None,
                ENGINE_TABLE_FIELDS_STORAGE | ENGINE_INDICES_STORAGE if bytes.len() == 32 => {
                    Some((ENGINE_TABLES_STORAGE, 16))
                }
                ENGINE_INDEX_FIELDS_STORAGE if bytes.len() == 48 => {
                    Some((ENGINE_INDICES_STORAGE, 32))
                }
                _ => {
                    return Err(EngineError::custom(format!(
                        "Invalid catalog identity length in {}",
                        table
                    )));
                }
            };
            if let Some((storage, size)) = parent {
                let parent_id = RowIdentity::catalog(bytes[..size].to_vec());
                if !catalog_has_active_row(codec, transaction, storage, &parent_id).await?
                    && !codec
                        .row_is_deleted(transaction, storage, &parent_id)
                        .await?
                {
                    return Err(EngineError::custom(format!(
                        "Incomplete catalog: {} row {} has no parent in {}",
                        table, id, storage
                    )));
                }
            }
            let values = &value.values;
            let valid = match table {
                ENGINE_TABLES_STORAGE => values.len() == 1 && values[0].as_text().is_some(),
                ENGINE_TABLE_FIELDS_STORAGE => {
                    values.len() == 5
                        && values[0].as_text().is_some()
                        && values[1].to_type().is_some()
                        && values[3].to_integer().is_some_and(|position| position >= 0)
                        && values[4].to_bool().is_some()
                }
                ENGINE_INDICES_STORAGE => {
                    values.len() == 3
                        && values[0].as_text().is_some()
                        && values[1].as_text().is_some()
                        && values[2].to_bool().is_some()
                }
                ENGINE_INDEX_FIELDS_STORAGE => {
                    values.len() == 3
                        && values[0].to_integer().is_some_and(|position| position >= 0)
                        && values[1].as_text().is_some()
                        && values[2].to_uuid().is_some()
                }
                _ => false,
            };
            if !valid {
                return Err(EngineError::custom(format!(
                    "Invalid catalog row {} in {}",
                    id, table
                )));
            }
        }
    }
    for table in touched_tables(snapshots, changes) {
        if catalog_order(table) == 4 && !table_name_exists(codec, transaction, table).await? {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: data table {} has no definition",
                table
            )));
        }
    }
    let tables = codec.scan_row_states(transaction, ENGINE_TABLES_STORAGE);
    pin_mut!(tables);
    while let Some(state) = tables.next().await {
        let (id, value, deleted) = state?;
        if deleted {
            continue;
        }
        let RowIdentity::Catalog(table_id) = &id else {
            return Err(EngineError::custom("Invalid catalog table identity"));
        };
        let name = value.values[0]
            .as_text()
            .expect("catalog table name was validated");
        if touched_row(snapshots, changes, ENGINE_TABLES_STORAGE, &id)
            || touched_catalog_parent(snapshots, changes, ENGINE_TABLE_FIELDS_STORAGE, table_id)
            || touched_table(snapshots, changes, name)
        {
            let (has_fields, has_uuid_primary_key) =
                table_field_status(codec, transaction, table_id).await?;
            if !has_fields {
                return Err(EngineError::custom(format!(
                    "Incomplete catalog: table {} has no fields",
                    name
                )));
            }
            if !has_uuid_primary_key {
                return Err(EngineError::custom(format!(
                    "Incomplete catalog: table {} has no UUID primary key",
                    name
                )));
            }
        }
    }
    let indices = codec.scan_row_states(transaction, ENGINE_INDICES_STORAGE);
    pin_mut!(indices);
    while let Some(state) = indices.next().await {
        let (id, value, deleted) = state?;
        if deleted {
            continue;
        }
        let name = value.values[1]
            .as_text()
            .expect("catalog index table name was validated");
        let Some(table_id) = table_id_for_name(codec, transaction, name).await? else {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: index {} references missing table {}",
                id, name
            )));
        };
        let RowIdentity::Catalog(index_id) = &id else {
            return Err(EngineError::custom("Invalid catalog index identity"));
        };
        if !index_id.starts_with(&table_id) {
            return Err(EngineError::custom(format!(
                "Invalid catalog: index {} is not scoped to table {}",
                id, name
            )));
        }
        let fields = codec.scan_row_states(transaction, ENGINE_INDEX_FIELDS_STORAGE);
        pin_mut!(fields);
        let mut has_fields = false;
        while let Some(field) = fields.next().await {
            let (field_id, field, deleted) = field?;
            let RowIdentity::Catalog(field_key) = field_id else {
                return Err(EngineError::custom("Invalid catalog index-field identity"));
            };
            if deleted || !field_key.starts_with(index_id) {
                continue;
            }
            has_fields = true;
            let column = field.values[1]
                .as_text()
                .expect("catalog index field column was validated");
            if !table_has_column(codec, transaction, &table_id, column).await? {
                return Err(EngineError::custom(format!(
                    "Incomplete catalog: index {} references missing column {}",
                    id, column
                )));
            }
        }
        if !has_fields {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: index {} has no fields",
                id
            )));
        }
    }
    Ok(())
}

async fn catalog_has_active_row<T, R>(
    codec: &R,
    transaction: &T,
    table: &str,
    target: &RowIdentity,
) -> Result<bool, EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    Ok(codec.get_row(transaction, table, target).await?.is_some())
}

async fn table_name_exists<T, R>(
    codec: &R,
    transaction: &T,
    name: &str,
) -> Result<bool, EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    Ok(table_id_for_name(codec, transaction, name).await?.is_some())
}

async fn table_id_for_name<T, R>(
    codec: &R,
    transaction: &T,
    name: &str,
) -> Result<Option<Vec<u8>>, EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    let rows = codec.scan_row_states(transaction, ENGINE_TABLES_STORAGE);
    pin_mut!(rows);
    while let Some(row) = rows.next().await {
        let (id, value, deleted) = row?;
        if !deleted && value.values[0].as_text() == Some(name) {
            let RowIdentity::Catalog(id) = id else {
                return Err(EngineError::custom("Invalid catalog table identity"));
            };
            return Ok(Some(id));
        }
    }
    Ok(None)
}

async fn table_field_status<T, R>(
    codec: &R,
    transaction: &T,
    table_id: &[u8],
) -> Result<(bool, bool), EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    let fields = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(fields);
    let mut has_fields = false;
    let mut has_uuid_primary_key = false;
    while let Some(field) = fields.next().await {
        let (id, value, deleted) = field?;
        if deleted || !matches!(&id, RowIdentity::Catalog(key) if key.starts_with(table_id)) {
            continue;
        }
        has_fields = true;
        has_uuid_primary_key |= value.values[1].to_type() == Some(value::ValueType::Uuid)
            && value.values[4].to_bool() == Some(true);
    }
    Ok((has_fields, has_uuid_primary_key))
}

async fn table_has_column<T, R>(
    codec: &R,
    transaction: &T,
    table_id: &[u8],
    column: &str,
) -> Result<bool, EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    let fields = codec.scan_row_states(transaction, ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(fields);
    while let Some(field) = fields.next().await {
        let (id, value, deleted) = field?;
        if !deleted
            && matches!(&id, RowIdentity::Catalog(key) if key.starts_with(table_id))
            && value.values[0].as_text() == Some(column)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn touched_tables<'a>(
    snapshots: &'a [SyncStateUnit],
    changes: &'a [SyncIncrementalChange],
) -> impl Iterator<Item = &'a str> + 'a {
    snapshots
        .iter()
        .filter_map(|unit| match &unit.key {
            SyncKey::Row { table, .. } => Some(table.as_str()),
        })
        .chain(changes.iter().map(|change| change.table.as_str()))
}

fn touched_table(
    snapshots: &[SyncStateUnit],
    changes: &[SyncIncrementalChange],
    table: &str,
) -> bool {
    touched_tables(snapshots, changes).any(|touched| touched == table)
}

fn touched_row(
    snapshots: &[SyncStateUnit],
    changes: &[SyncIncrementalChange],
    table: &str,
    row: &RowIdentity,
) -> bool {
    snapshots.iter().any(|unit| {
        matches!(&unit.key, SyncKey::Row { table: touched_table, row: touched_row }
            if touched_table == table && touched_row == row)
    }) || changes
        .iter()
        .any(|change| change.table == table && &change.row == row)
}

fn touched_catalog_parent(
    snapshots: &[SyncStateUnit],
    changes: &[SyncIncrementalChange],
    table: &str,
    parent: &[u8],
) -> bool {
    snapshots.iter().any(|unit| {
        matches!(&unit.key, SyncKey::Row { table: touched_table, row: RowIdentity::Catalog(bytes) }
            if touched_table == table && bytes.starts_with(parent))
    }) || changes.iter().any(|change| {
        change.table == table
            && matches!(&change.row, RowIdentity::Catalog(bytes) if bytes.starts_with(parent))
    })
}

fn catalog_order(table: &str) -> u8 {
    match table {
        ENGINE_TABLES_STORAGE => 0,
        ENGINE_TABLE_FIELDS_STORAGE => 1,
        ENGINE_INDICES_STORAGE => 2,
        ENGINE_INDEX_FIELDS_STORAGE => 3,
        _ => 4,
    }
}

fn validate_recovery_snapshots(
    requests: &[SyncSnapshotRequest],
    snapshots: &[SyncStateUnit],
) -> Result<(), EngineError> {
    let mut remaining = requests.iter().cloned().collect::<BTreeSet<_>>();
    for snapshot in snapshots {
        let SyncKey::Row { table, row } = &snapshot.key;
        let request = SyncSnapshotRequest {
            table: table.clone(),
            row: row.clone(),
        };
        if !remaining.remove(&request) {
            return Err(EngineError::custom(format!(
                "Unexpected or duplicate recovery snapshot for {} row {}",
                table, row
            )));
        }
    }
    if let Some(request) = remaining.first() {
        return Err(EngineError::custom(format!(
            "Incomplete sync: recovery snapshot missing for {} row {}",
            request.table, request.row
        )));
    }
    Ok(())
}

fn validate_state_batch(batch: &[SyncStateUnit]) -> Result<(), EngineError> {
    if batch
        .iter()
        .any(|unit| unit.state.is_empty() && unit.metadata.is_empty())
    {
        return Err(EngineError::custom(
            "Invalid sync state: empty row state and metadata",
        ));
    }
    if batch.iter().all(SyncStateUnit::verify_digest) {
        Ok(())
    } else {
        Err(EngineError::custom("Invalid sync state digest"))
    }
}

pub async fn apply_incremental_changes_for<K, R>(
    engine: &Engine<K, R>,
    batch: &[SyncIncrementalChange],
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    apply_received_batch(engine, Arc::new(Vec::new()), Arc::new(batch.to_vec()), None).await
}

fn validate_change_batch(batch: &[SyncIncrementalChange]) -> Result<(), EngineError> {
    for change in batch {
        if change.id.0.is_empty() || change.payload.is_empty() {
            return Err(EngineError::custom("Invalid incremental change frame"));
        }
    }
    Ok(())
}

async fn inventories_for<K, R>(
    engine: &Engine<K, R>,
    units: &[SyncStateUnit],
    max_bytes: usize,
) -> Result<Vec<SyncRowInventory>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut used_bytes = 0usize;
    let mut result = Vec::new();
    for unit in units {
        let SyncKey::Row { table, row } = unit.key.clone();
        let key_bytes = table.len() + row.to_bytes().len() + 64;
        let table_for_inventory = table.clone();
        let row_for_inventory = row.clone();
        let inventory_budget = max_bytes.saturating_sub(used_bytes.saturating_add(key_bytes));
        let changes = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .change_inventory(
                            transaction,
                            &table_for_inventory,
                            row_for_inventory,
                            inventory_budget,
                        )
                        .await
                })
            })
            .await?;
        let inventory_bytes = changes
            .iter()
            .fold(key_bytes, |sum, id| sum.saturating_add(id.0.len() + 8));
        used_bytes = account_engine_payload(used_bytes, inventory_bytes, max_bytes)?;
        result.push(SyncRowInventory {
            table,
            row,
            changes,
        });
    }
    result.sort_by_key(SyncRowInventory::key);
    result.dedup_by(|left, right| left.key() == right.key());
    Ok(result)
}

async fn exchange_inventories<T>(
    transport: &mut T,
    role: SyncRole,
    local: Vec<SyncRowInventory>,
) -> Result<Vec<SyncRowInventory>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let message = SyncMessage::Inventory(local);
    match role {
        SyncRole::Initiator => {
            send_message(transport, &message).await?;
            receive_inventory(transport).await
        }
        SyncRole::Responder => {
            let remote = receive_inventory(transport).await?;
            send_message(transport, &message).await?;
            Ok(remote)
        }
    }
}

async fn receive_inventory<T>(
    transport: &mut T,
) -> Result<Vec<SyncRowInventory>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    match receive_message(transport).await?.0 {
        SyncMessage::Inventory(inventory) => Ok(inventory),
        SyncMessage::Abort(reason) => Err(SyncError::RemoteAbort(reason)),
        _ => Err(SyncError::UnexpectedMessage),
    }
}

async fn build_outbound<K, R>(
    engine: &Engine<K, R>,
    units: Vec<SyncStateUnit>,
    remote_manifest: &SyncManifest,
    remote_inventories: BTreeMap<SyncKey, SyncRowInventory>,
    max_bytes: usize,
) -> Result<Outbound, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut used_bytes = 0;
    let mut outbound = Outbound::default();
    for unit in units {
        let SyncKey::Row { table, row } = unit.key.clone();
        if remote_manifest.contains(&unit.key, unit.digest) {
            continue;
        }
        let Some(remote) = remote_inventories.get(&unit.key) else {
            used_bytes =
                account_engine_payload(used_bytes, state_unit_payload_size(&unit), max_bytes)?;
            outbound.snapshots.push(unit);
            continue;
        };
        let table_for_inventory = table.clone();
        let row_for_inventory = row.clone();
        let inventory_budget = max_bytes.saturating_sub(used_bytes);
        let local = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .change_inventory(
                            transaction,
                            &table_for_inventory,
                            row_for_inventory,
                            inventory_budget,
                        )
                        .await
                })
            })
            .await?;
        let mut exported = 0;
        for key in local {
            if remote.changes.iter().any(|candidate| candidate == &key) {
                continue;
            }
            let export_id = key.clone();
            let table_for_export = table.clone();
            let row_for_export = row.clone();
            let export_budget = max_bytes.saturating_sub(
                used_bytes
                    .saturating_add(key.0.len())
                    .saturating_add(table.len())
                    .saturating_add(row.to_bytes().len())
                    .saturating_add(64),
            );
            if let Some(payload) = engine
                .read_transaction(move |codec, transaction| {
                    Box::pin(async move {
                        codec
                            .export_change(
                                transaction,
                                &table_for_export,
                                row_for_export,
                                &export_id,
                                export_budget,
                            )
                            .await
                    })
                })
                .await?
            {
                used_bytes = account_engine_payload(
                    used_bytes,
                    payload.len() + key.0.len() + table.len() + row.to_bytes().len() + 64,
                    max_bytes,
                )?;
                outbound.changes.push(SyncIncrementalChange {
                    table: table.clone(),
                    row: row.clone(),
                    id: key,
                    payload,
                });
                exported += 1;
            }
        }
        if exported == 0 {
            used_bytes =
                account_engine_payload(used_bytes, state_unit_payload_size(&unit), max_bytes)?;
            outbound.snapshots.push(unit);
        }
    }
    outbound.snapshots.sort_unstable_by(|left, right| {
        let (
            SyncKey::Row {
                table: left_table, ..
            },
            SyncKey::Row {
                table: right_table, ..
            },
        ) = (&left.key, &right.key);
        catalog_order(left_table)
            .cmp(&catalog_order(right_table))
            .then_with(|| left.key.cmp(&right.key))
    });
    outbound.changes.sort_unstable_by(|left, right| {
        catalog_order(&left.table)
            .cmp(&catalog_order(&right.table))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(outbound)
}

async fn send_outbound<T>(
    transport: &mut T,
    outbound: &Outbound,
    batch_size: usize,
) -> Result<TransferCount, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    for batch in outbound.snapshots.chunks(batch_size) {
        send_message(transport, &SyncMessage::State(batch.to_vec())).await?;
    }
    for batch in outbound.changes.chunks(batch_size) {
        send_message(transport, &SyncMessage::Changes(batch.to_vec())).await?;
    }
    send_message(transport, &SyncMessage::Done).await?;
    Ok(TransferCount {
        snapshots: outbound.snapshots.len(),
        changes: outbound.changes.len(),
    })
}

async fn exchange_snapshot_requests<T>(
    transport: &mut T,
    role: SyncRole,
    local: &[SyncSnapshotRequest],
) -> Result<Vec<SyncSnapshotRequest>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let message = SyncMessage::RequestSnapshots(local.to_vec());
    match role {
        SyncRole::Initiator => {
            send_message(transport, &message).await?;
            receive_snapshot_requests(transport).await
        }
        SyncRole::Responder => {
            let remote = receive_snapshot_requests(transport).await?;
            send_message(transport, &message).await?;
            Ok(remote)
        }
    }
}

async fn receive_snapshot_requests<T>(
    transport: &mut T,
) -> Result<Vec<SyncSnapshotRequest>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    match receive_message(transport).await?.0 {
        SyncMessage::RequestSnapshots(requests) => Ok(requests),
        _ => Err(SyncError::UnexpectedMessage),
    }
}

async fn recovery_snapshots<K, R>(
    engine: &Engine<K, R>,
    requests: Vec<SyncSnapshotRequest>,
    max_bytes: usize,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut used_bytes = 0;
    let mut snapshots = Vec::new();
    for request in requests {
        let state_budget = max_bytes.saturating_sub(used_bytes);
        let snapshot = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let state = codec
                        .export_state(
                            transaction,
                            &request.table,
                            request.row.clone(),
                            state_budget,
                        )
                        .await?
                        .unwrap_or_default();
                    let metadata = codec
                        .export_metadata(transaction, &request.table, request.row.clone())
                        .await?;
                    Ok(if state.is_empty() && metadata.is_empty() {
                        None
                    } else {
                        Some(SyncStateUnit::new(
                            SyncKey::Row {
                                table: request.table,
                                row: request.row,
                            },
                            state,
                            metadata,
                        ))
                    })
                })
            })
            .await?;
        if let Some(snapshot) = snapshot {
            used_bytes =
                account_engine_payload(used_bytes, state_unit_payload_size(&snapshot), max_bytes)?;
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

async fn send_snapshots<T>(
    transport: &mut T,
    snapshots: &[SyncStateUnit],
    batch_size: usize,
) -> Result<usize, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    for batch in snapshots.chunks(batch_size) {
        send_message(transport, &SyncMessage::State(batch.to_vec())).await?;
    }
    send_message(transport, &SyncMessage::Done).await?;
    Ok(snapshots.len())
}

async fn receive_recovery<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    received: &mut Received,
    requested: &[SyncSnapshotRequest],
    manifest: &SyncManifest,
    max_session_bytes: usize,
) -> Result<(usize, usize), SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let mut snapshots = Vec::new();
    let mut received_bytes = received.received_bytes;
    loop {
        let (message, message_bytes) = receive_message(transport).await?;
        received_bytes =
            account_message(transport, received_bytes, message_bytes, max_session_bytes).await?;
        match message {
            SyncMessage::State(batch) => {
                snapshots.extend(batch);
            }
            SyncMessage::Done => break,
            SyncMessage::Abort(reason) => return Err(SyncError::RemoteAbort(reason)),
            _ => return Err(SyncError::UnexpectedMessage),
        }
    }
    let count = snapshots.len();
    if let Err(error) = validate_recovery_snapshots(requested, &snapshots) {
        let reason = format!("{} (recovery snapshot batch)", error);
        let _ = send_message(transport, &SyncMessage::Abort(reason)).await;
        return Err(error.into());
    }
    received.received_bytes = received_bytes;
    received.pending_snapshots.append(&mut snapshots);
    if received.pending.is_empty() && count == 0 {
        return Ok((0, 0));
    }
    if count == 0 && !received.pending.is_empty() {
        let error =
            EngineError::custom("Incomplete sync: requested recovery snapshots were not provided");
        let _ = send_message(transport, &SyncMessage::Abort(error.to_string())).await;
        return Err(error.into());
    }
    if let Err(error) = apply_received_batch(
        engine,
        Arc::new(core::mem::take(&mut received.pending_snapshots)),
        Arc::new(Vec::new()),
        Some(manifest),
    )
    .await
    {
        let reason = format!("{} (recovery snapshot batch)", error);
        let _ = send_message(transport, &SyncMessage::Abort(reason)).await;
        return Err(error.into());
    }

    received.pending_snapshots.clear();

    // A recovery snapshot is the sender's complete current row state. It includes
    // the history needed by the pending changes and supersedes their payloads.
    received.pending.clear();
    Ok((count, 0))
}

async fn receive_outbound<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    manifest: &SyncManifest,
    max_session_bytes: usize,
) -> Result<Received, SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let mut count = Received {
        snapshots: 0,
        changes: 0,
        requests: Vec::new(),
        pending_snapshots: Vec::new(),
        pending: Vec::new(),
        received_bytes: 0,
    };
    let mut snapshots = Vec::new();
    let mut changes = Vec::new();
    loop {
        let (message, message_bytes) = receive_message(transport).await?;
        count.received_bytes = account_message(
            transport,
            count.received_bytes,
            message_bytes,
            max_session_bytes,
        )
        .await?;
        match message {
            SyncMessage::State(batch) => snapshots.extend(batch),
            SyncMessage::Changes(batch) => changes.extend(batch),
            SyncMessage::Done => {
                count.snapshots = snapshots.len();
                let change_count = changes.len();
                let snapshot_batch = Arc::new(snapshots);
                let change_batch = Arc::new(changes);
                if let Err(error) = apply_received_batch(
                    engine,
                    Arc::clone(&snapshot_batch),
                    Arc::clone(&change_batch),
                    Some(manifest),
                )
                .await
                {
                    if matches!(error, EngineError::SyncDependencyUnavailable) {
                        count.pending_snapshots = Arc::try_unwrap(snapshot_batch)
                            .expect("sync apply releases its snapshot batch before returning");
                        for change in Arc::try_unwrap(change_batch)
                            .expect("sync apply releases its change batch before returning")
                        {
                            count.requests.push(SyncSnapshotRequest {
                                table: change.table.clone(),
                                row: change.row.clone(),
                            });
                            count.pending.push(change);
                        }
                    } else {
                        return abort(transport, error, String::from("incremental batch")).await;
                    }
                } else {
                    count.changes = change_count;
                }
                count.requests.sort_unstable();
                count.requests.dedup();
                return Ok(count);
            }
            SyncMessage::Abort(reason) => return Err(SyncError::RemoteAbort(reason)),
            SyncMessage::Hello(_)
            | SyncMessage::Manifest(_)
            | SyncMessage::Inventory(_)
            | SyncMessage::RequestSnapshots(_) => {
                return Err(SyncError::UnexpectedMessage);
            }
        }
    }
}

async fn abort<T>(
    transport: &mut T,
    error: EngineError,
    context: String,
) -> Result<Received, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let reason = format!("{} ({})", error, context);
    let _ = send_message(transport, &SyncMessage::Abort(reason)).await;
    Err(error.into())
}

async fn exchange_hello<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    role: SyncRole,
) -> Result<(), SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let manifest = with_engine_abort(
        transport,
        sync_manifest_limited(engine, MAX_MESSAGE_BYTES - 64).await,
    )
    .await?;
    let hello = SyncMessage::Hello(SyncHello {
        protocol_version: PROTOCOL_VERSION,
        manifest,
    });
    match role {
        SyncRole::Initiator => {
            send_message(transport, &hello).await?;
            validate_hello(receive_message(transport).await?.0)
        }
        SyncRole::Responder => {
            validate_hello(receive_message(transport).await?.0)?;
            send_message(transport, &hello).await
        }
    }
}

fn validate_hello<E>(message: SyncMessage) -> Result<(), SyncError<E>> {
    let SyncMessage::Hello(hello) = message else {
        if let SyncMessage::Abort(reason) = message {
            return Err(SyncError::RemoteAbort(reason));
        }
        return Err(SyncError::UnexpectedMessage);
    };
    if hello.protocol_version != PROTOCOL_VERSION {
        return Err(SyncError::IncompatibleProtocol(hello.protocol_version));
    }
    Ok(())
}

async fn exchange_manifest<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    role: SyncRole,
) -> Result<SyncManifest, SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let manifest = with_engine_abort(
        transport,
        sync_manifest_limited(engine, MAX_MESSAGE_BYTES - 64).await,
    )
    .await?;
    let message = SyncMessage::Manifest(manifest);
    match role {
        SyncRole::Initiator => {
            send_message(transport, &message).await?;
            receive_manifest(transport).await
        }
        SyncRole::Responder => {
            let remote = receive_manifest(transport).await?;
            send_message(transport, &message).await?;
            Ok(remote)
        }
    }
}

async fn receive_manifest<T>(transport: &mut T) -> Result<SyncManifest, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    match receive_message(transport).await?.0 {
        SyncMessage::Manifest(manifest) => Ok(manifest),
        SyncMessage::Abort(reason) => Err(SyncError::RemoteAbort(reason)),
        _ => Err(SyncError::UnexpectedMessage),
    }
}

async fn with_engine_abort<T, O>(
    transport: &mut T,
    result: Result<O, EngineError>,
) -> Result<O, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    match result {
        Ok(output) => Ok(output),
        Err(error) => {
            let _ = send_message(transport, &SyncMessage::Abort(error.to_string())).await;
            Err(error.into())
        }
    }
}

fn state_unit_payload_size(unit: &SyncStateUnit) -> usize {
    let SyncKey::Row { table, row } = &unit.key;
    unit.state
        .len()
        .saturating_add(unit.metadata.len())
        .saturating_add(table.len())
        .saturating_add(row.to_bytes().len())
        .saturating_add(64)
}

fn account_engine_payload(used: usize, incoming: usize, max: usize) -> Result<usize, EngineError> {
    let size = used.saturating_add(incoming);
    if size > max {
        Err(EngineError::custom(format!(
            "sync session payload size {size} exceeds {max} bytes"
        )))
    } else {
        Ok(size)
    }
}

async fn account_message<T>(
    transport: &mut T,
    used: usize,
    message_bytes: usize,
    max: usize,
) -> Result<usize, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let size = used.saturating_add(message_bytes);
    if size > max {
        let error = SyncError::SessionTooLarge { size, max };
        let _ = send_message(transport, &SyncMessage::Abort(error.to_string())).await;
        Err(error)
    } else {
        Ok(size)
    }
}

async fn send_message<T>(
    transport: &mut T,
    message: &SyncMessage,
) -> Result<(), SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let frame =
        postcard::to_allocvec(message).map_err(|error| SyncError::Protocol(error.to_string()))?;
    if frame.len() > MAX_MESSAGE_BYTES {
        return Err(SyncError::MessageTooLarge {
            size: frame.len(),
            max: MAX_MESSAGE_BYTES,
        });
    }
    transport.send(frame).await.map_err(SyncError::Transport)
}

async fn receive_message<T>(transport: &mut T) -> Result<(SyncMessage, usize), SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let frame = transport.receive().await.map_err(SyncError::Transport)?;
    if frame.len() > MAX_MESSAGE_BYTES {
        return Err(SyncError::MessageTooLarge {
            size: frame.len(),
            max: MAX_MESSAGE_BYTES,
        });
    }
    let size = frame.len();
    let message =
        postcard::from_bytes(&frame).map_err(|error| SyncError::Protocol(error.to_string()))?;
    Ok((message, size))
}

#[cfg(test)]
mod tests {
    use alloc::{string::String, vec, vec::Vec};

    use engine::{
        ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_TABLES_STORAGE, RowIdentity,
    };
    use futures::executor::block_on;

    use super::{
        MAX_MESSAGE_BYTES, SyncError, catalog_order, receive_message, send_message, validate_hello,
        validate_recovery_snapshots, validate_state_batch,
    };
    use crate::{
        PROTOCOL_VERSION, SyncHello, SyncKey, SyncManifest, SyncMessage, SyncSnapshotRequest,
        SyncStateUnit, SyncTransport,
    };

    #[derive(Default)]
    struct Transport {
        inbound: Option<Vec<u8>>,
        outbound: Option<Vec<u8>>,
    }

    impl SyncTransport for Transport {
        type Error = &'static str;

        async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
            self.inbound.take().ok_or("no inbound frame")
        }

        async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
            self.outbound = Some(frame);
            Ok(())
        }
    }

    fn abort_with_encoded_size(size: usize) -> SyncMessage {
        let empty = postcard::to_allocvec(&SyncMessage::Abort(String::new()))
            .expect("empty abort serializes");
        let mut payload_len = size - empty.len();
        loop {
            let message = SyncMessage::Abort("x".repeat(payload_len));
            let encoded = postcard::to_allocvec(&message).expect("abort serializes");
            if encoded.len() == size {
                return message;
            }
            payload_len -= encoded.len() - size;
        }
    }

    #[test]
    fn catalog_order_places_system_tables_before_user_tables() {
        assert_eq!(catalog_order(ENGINE_TABLES_STORAGE), 0);
        assert_eq!(catalog_order(ENGINE_TABLE_FIELDS_STORAGE), 1);
        assert_eq!(catalog_order(ENGINE_INDICES_STORAGE), 2);
        assert_eq!(catalog_order(ENGINE_INDEX_FIELDS_STORAGE), 3);
        assert_eq!(catalog_order("user_table"), 4);
    }

    #[test]
    fn recovery_requires_exactly_one_snapshot_for_each_requested_row() {
        let requests = vec![
            SyncSnapshotRequest {
                table: String::from("people"),
                row: RowIdentity::User([1; 16]),
            },
            SyncSnapshotRequest {
                table: String::from("people"),
                row: RowIdentity::User([2; 16]),
            },
        ];
        let snapshot = |row| {
            SyncStateUnit::new(
                SyncKey::Row {
                    table: String::from("people"),
                    row: RowIdentity::User([row; 16]),
                },
                vec![1],
                Vec::new(),
            )
        };

        assert!(validate_recovery_snapshots(&requests, &[snapshot(1)]).is_err());
        assert!(validate_recovery_snapshots(&requests, &[snapshot(1), snapshot(2)]).is_ok());
        assert!(validate_recovery_snapshots(&requests, &[snapshot(1), snapshot(1)]).is_err());
        assert!(
            validate_recovery_snapshots(&requests, &[snapshot(1), snapshot(2), snapshot(3)])
                .is_err()
        );
        assert!(validate_recovery_snapshots(&[], &[snapshot(1)]).is_err());
    }

    #[test]
    fn catalog_row_state_uses_the_same_digest_validation_as_user_rows() {
        let key = SyncKey::Row {
            table: ENGINE_TABLES_STORAGE.into(),
            row: RowIdentity::catalog(vec![1, 2]),
        };
        let unit = SyncStateUnit::new(key, vec![3], vec![4]);
        assert!(validate_state_batch(core::slice::from_ref(&unit)).is_ok());
        let mut corrupted = unit;
        corrupted.state.push(5);
        assert!(validate_state_batch(&[corrupted]).is_err());
    }

    #[test]
    fn rejects_previous_protocol_version() {
        let message = SyncMessage::Hello(SyncHello {
            protocol_version: PROTOCOL_VERSION - 1,
            manifest: SyncManifest::default(),
        });
        assert!(matches!(
            validate_hello::<&'static str>(message),
            Err(SyncError::IncompatibleProtocol(version)) if version == PROTOCOL_VERSION - 1
        ));
    }

    #[test]
    fn outbound_message_over_limit_is_not_sent() {
        let mut transport = Transport::default();
        let error = block_on(send_message(
            &mut transport,
            &abort_with_encoded_size(MAX_MESSAGE_BYTES + 1),
        ))
        .expect_err("oversized outbound message must fail");
        assert!(
            matches!(error, SyncError::MessageTooLarge { size, max } if size == MAX_MESSAGE_BYTES + 1 && max == MAX_MESSAGE_BYTES)
        );
        assert!(transport.outbound.is_none());
    }

    #[test]
    fn inbound_message_over_limit_is_rejected_before_decode() {
        let mut transport = Transport {
            inbound: Some(vec![0xff; MAX_MESSAGE_BYTES + 1]),
            ..Transport::default()
        };
        let error = block_on(receive_message(&mut transport))
            .expect_err("oversized inbound frame must fail before decoding");
        assert!(
            matches!(error, SyncError::MessageTooLarge { size, max } if size == MAX_MESSAGE_BYTES + 1 && max == MAX_MESSAGE_BYTES)
        );
    }

    #[test]
    fn message_at_limit_round_trips() {
        let message = abort_with_encoded_size(MAX_MESSAGE_BYTES);
        let mut transport = Transport::default();
        block_on(send_message(&mut transport, &message)).expect("boundary message must send");
        let frame = transport.outbound.take().expect("message was sent");
        assert_eq!(frame.len(), MAX_MESSAGE_BYTES);
        transport.inbound = Some(frame);
        let (received, encoded_size) =
            block_on(receive_message(&mut transport)).expect("boundary message must decode");
        assert_eq!(received, message);
        assert_eq!(encoded_size, MAX_MESSAGE_BYTES);
    }
}
