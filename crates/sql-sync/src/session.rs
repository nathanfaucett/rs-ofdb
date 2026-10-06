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
    SyncRowCodec, SyncRowInventory, SyncSnapshotRequest, SyncStage, SyncStageFactory,
    SyncStateUnit, SyncTransport,
};

/// Maximum serialized sync message size (1 MiB). Transports must also bound reads
/// before buffering frames; the session checks received frames only afterward.
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
pub const MAX_APPLY_BATCH_BYTES: usize = MAX_MESSAGE_BYTES;
pub const DEFAULT_MAX_SESSION_BYTES: usize = 64 * 1024 * 1024;
const MAX_CATALOG_LOOKUP_BYTES: usize = DEFAULT_MAX_SESSION_BYTES;

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

    #[error("sync staging error: {0}")]
    Staging(String),

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
    let factory = crate::MemorySyncStageFactory::new(config.max_session_bytes);
    synchronize_with_stage_factory(engine, transport, config, role, &factory).await
}

pub async fn synchronize_with_stage_factory<K, R, T, F>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    stage_factory: &F,
) -> Result<SyncResult, SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Stage: 'static,
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
                stage_factory,
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
                stage_factory,
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
                stage_factory,
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
                stage_factory,
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
        let (table_entries, next_used_bytes) = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let mut used_bytes = used_bytes;
                    let mut entries = Vec::new();
                    let rows = codec.row_ids(transaction, &table_for_rows);
                    pin_mut!(rows);
                    while let Some(row) = rows.next().await {
                        let row = row?;
                        let entry_bytes = table_for_rows
                            .len()
                            .saturating_add(row.to_bytes().len())
                            .saturating_add(48);
                        used_bytes = account_engine_payload(used_bytes, entry_bytes, max_bytes)?;
                        let state_budget = max_bytes.saturating_sub(used_bytes);
                        let state = codec
                            .export_state(transaction, &table_for_rows, row.clone(), state_budget)
                            .await?
                            .unwrap_or_default();
                        let metadata = codec
                            .export_metadata(transaction, &table_for_rows, row.clone())
                            .await?;
                        if !state.is_empty() || !metadata.is_empty() {
                            let key = SyncKey::Row {
                                table: table_for_rows.clone(),
                                row,
                            };
                            let digest = SyncStateUnit::new(key.clone(), state, metadata).digest;
                            entries.push((key, digest));
                        } else {
                            used_bytes = used_bytes.saturating_sub(entry_bytes);
                        }
                    }
                    Ok((entries, used_bytes))
                })
            })
            .await?;
        entries.extend(table_entries);
        used_bytes = next_used_bytes;
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
        let (table_units, next_used_bytes) = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let mut used_bytes = used_bytes;
                    let mut units = Vec::new();
                    let rows = codec.row_ids(transaction, &table_for_rows);
                    pin_mut!(rows);
                    while let Some(row) = rows.next().await {
                        let row = row?;
                        let state_budget = max_bytes
                            .saturating_sub(used_bytes)
                            .saturating_sub(table_for_rows.len())
                            .saturating_sub(row.to_bytes().len())
                            .saturating_sub(64);
                        let state = codec
                            .export_state(transaction, &table_for_rows, row.clone(), state_budget)
                            .await?
                            .unwrap_or_default();
                        let metadata = codec
                            .export_metadata(transaction, &table_for_rows, row.clone())
                            .await?;
                        if !state.is_empty() || !metadata.is_empty() {
                            let state_bytes = state
                                .len()
                                .saturating_add(metadata.len())
                                .saturating_add(table_for_rows.len())
                                .saturating_add(row.to_bytes().len())
                                .saturating_add(64);
                            used_bytes =
                                account_engine_payload(used_bytes, state_bytes, max_bytes)?;
                            units.push(SyncStateUnit::new(
                                SyncKey::Row {
                                    table: table_for_rows.clone(),
                                    row,
                                },
                                state,
                                metadata,
                            ));
                        }
                    }
                    Ok((units, used_bytes))
                })
            })
            .await?;
        units.extend(table_units);
        used_bytes = next_used_bytes;
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
    apply_received_batch(engine, Arc::new(batch), Arc::new(Vec::new()), None, false).await
}

async fn apply_received_batch<K, R>(
    engine: &Engine<K, R>,
    snapshots: Arc<Vec<SyncStateUnit>>,
    changes: Arc<Vec<SyncIncrementalChange>>,
    manifest: Option<&SyncManifest>,
    catalog_already_validated: bool,
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    validate_state_batch(&snapshots)?;
    validate_change_batch(&changes)?;
    let has_catalog = snapshots.iter().any(|unit| {
        let SyncKey::Row { table, .. } = &unit.key;
        catalog_order(table) < 4
    }) || changes
        .iter()
        .any(|change| catalog_order(&change.table) < 4);
    let has_data = snapshots.iter().any(|unit| {
        let SyncKey::Row { table, .. } = &unit.key;
        catalog_order(table) == 4
    }) || changes
        .iter()
        .any(|change| catalog_order(&change.table) == 4);
    let expected = manifest.cloned();

    if has_catalog || (!catalog_already_validated && (manifest.is_some() || has_data)) {
        let mut tables = BTreeSet::new();
        for unit in snapshots.iter() {
            let SyncKey::Row { table, .. } = &unit.key;
            if catalog_order(table) < 4 {
                tables.insert(table.clone());
            }
        }
        for change in changes.iter() {
            if catalog_order(&change.table) < 4 {
                tables.insert(change.table.clone());
            }
        }
        let tables = tables.into_iter().collect::<Vec<_>>();
        let expected = expected.clone();
        let batch_snapshots = Arc::clone(&snapshots);
        let batch_changes = Arc::clone(&changes);
        engine
            .mutate_tables(tables.as_slice(), move |codec, transaction| {
                let snapshots = batch_snapshots;
                let changes = batch_changes;
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
            .await?;
    }

    if has_data {
        let mut rows = BTreeMap::<(String, RowIdentity), (Vec<usize>, Vec<usize>, usize)>::new();
        for (index, unit) in snapshots.iter().enumerate() {
            let SyncKey::Row { table, row } = &unit.key;
            if catalog_order(table) == 4 {
                let entry = rows.entry((table.clone(), row.clone())).or_default();
                entry.0.push(index);
                let identity_bytes = match row {
                    RowIdentity::User(_) => 16,
                    RowIdentity::ScopedUser { .. } => 32,
                    RowIdentity::Catalog(bytes) => bytes.len(),
                };
                entry.2 = entry
                    .2
                    .saturating_add(core::mem::size_of::<SyncStateUnit>())
                    .saturating_add(table.len())
                    .saturating_add(identity_bytes)
                    .saturating_add(unit.state.len())
                    .saturating_add(unit.metadata.len());
            }
        }
        for (index, change) in changes.iter().enumerate() {
            if catalog_order(&change.table) == 4 {
                let entry = rows
                    .entry((change.table.clone(), change.row.clone()))
                    .or_default();
                entry.1.push(index);
                let identity_bytes = match &change.row {
                    RowIdentity::User(_) => 16,
                    RowIdentity::ScopedUser { .. } => 32,
                    RowIdentity::Catalog(bytes) => bytes.len(),
                };
                entry.2 = entry
                    .2
                    .saturating_add(core::mem::size_of::<SyncIncrementalChange>())
                    .saturating_add(change.table.len())
                    .saturating_add(identity_bytes)
                    .saturating_add(change.id.0.len())
                    .saturating_add(change.payload.len());
            }
        }
        for ((table, _), (snapshot_indices, change_indices, bytes)) in rows {
            if bytes > MAX_APPLY_BATCH_BYTES {
                return Err(EngineError::custom(format!(
                    "Sync row batch is {bytes} bytes, exceeds {MAX_APPLY_BATCH_BYTES} bytes"
                )));
            }
            let table_names = alloc::vec![table.clone()];
            let batch_snapshots = Arc::clone(&snapshots);
            let batch_changes = Arc::clone(&changes);
            engine
                .mutate_tables(table_names.as_slice(), move |codec, transaction| {
                    let snapshots = batch_snapshots;
                    let changes = batch_changes;
                    Box::pin(async move {
                        let mut mutations = MutationSummary::default();
                        for index in snapshot_indices {
                            merge_snapshot(codec, transaction, &snapshots[index]).await?;
                            mutations.record_table(&table);
                        }
                        for index in change_indices {
                            merge_change(codec, transaction, &changes[index]).await?;
                            mutations.record_table(&table);
                        }
                        Ok(((), mutations))
                    })
                })
                .await?;
        }
    }

    Ok(())
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
    if snapshots.is_empty() && changes.is_empty() && manifest.is_none() {
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
    let mut table_ids_by_name = BTreeMap::new();
    let mut table_lookup_bytes = 0usize;
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
        insert_table_identity(
            &mut table_ids_by_name,
            &mut table_lookup_bytes,
            name,
            table_id,
            MAX_CATALOG_LOOKUP_BYTES,
        )?;
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
    for table in touched_tables(snapshots, changes) {
        if catalog_order(table) == 4 && !table_ids_by_name.contains_key(table) {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: data table {} has no definition",
                table
            )));
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
        let Some(table_id) = table_ids_by_name.get(name) else {
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
        let field_prefix = RowIdentity::catalog(index_id.clone()).to_bytes();
        let fields =
            codec.scan_row_states_prefix(transaction, ENGINE_INDEX_FIELDS_STORAGE, &field_prefix);
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

fn insert_table_identity(
    index: &mut BTreeMap<String, Vec<u8>>,
    used_bytes: &mut usize,
    name: &str,
    id: &[u8],
    max_bytes: usize,
) -> Result<(), EngineError> {
    match index.get(name) {
        Some(current_id) if current_id.as_slice() >= id => Ok(()),
        Some(_) => {
            index.insert(name.to_string(), id.to_vec());
            Ok(())
        }
        None => {
            let next_bytes = used_bytes
                .saturating_add(name.len())
                .saturating_add(id.len())
                .saturating_add(256);
            if next_bytes > max_bytes {
                return Err(EngineError::custom(format!(
                    "Catalog table-name index exceeds the {max_bytes}-byte validation limit"
                )));
            }
            *used_bytes = next_bytes;
            index.insert(name.to_string(), id.to_vec());
            Ok(())
        }
    }
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

async fn table_field_status<T, R>(
    codec: &R,
    transaction: &T,
    table_id: &[u8],
) -> Result<(bool, bool), EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    let field_prefix = RowIdentity::catalog(table_id.to_vec()).to_bytes();
    let fields =
        codec.scan_row_states_prefix(transaction, ENGINE_TABLE_FIELDS_STORAGE, &field_prefix);
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
    let field_prefix = RowIdentity::catalog(table_id.to_vec()).to_bytes();
    let fields =
        codec.scan_row_states_prefix(transaction, ENGINE_TABLE_FIELDS_STORAGE, &field_prefix);
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

fn catalog_order(table: &str) -> u8 {
    match table {
        ENGINE_TABLES_STORAGE => 0,
        ENGINE_TABLE_FIELDS_STORAGE => 1,
        ENGINE_INDICES_STORAGE => 2,
        ENGINE_INDEX_FIELDS_STORAGE => 3,
        _ => 4,
    }
}

fn consume_recovery_snapshot(
    remaining: &mut BTreeSet<SyncSnapshotRequest>,
    snapshot: &SyncStateUnit,
) -> Result<(), EngineError> {
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
    Ok(())
}

fn validate_recovery_complete(
    remaining: &BTreeSet<SyncSnapshotRequest>,
) -> Result<(), EngineError> {
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
    apply_received_batch(
        engine,
        Arc::new(Vec::new()),
        Arc::new(batch.to_vec()),
        None,
        false,
    )
    .await
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
            .then_with(|| left.table.cmp(&right.table))
            .then_with(|| left.row.cmp(&right.row))
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

async fn receive_recovery<K, R, T, F>(
    engine: &Engine<K, R>,
    transport: &mut T,
    received: &mut Received,
    requested: &[SyncSnapshotRequest],
    manifest: &SyncManifest,
    max_session_bytes: usize,
    stage_factory: &F,
) -> Result<(usize, usize), SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Error: core::fmt::Display,
    F::Stage: 'static,
{
    let mut stage = stage_factory
        .create()
        .await
        .map_err(|error| SyncError::Staging(error.to_string()))?;
    let mut remaining = requested.iter().cloned().collect::<BTreeSet<_>>();
    let mut count = 0;
    let mut received_bytes = received.received_bytes;
    loop {
        let (message, message_bytes) = receive_message(transport).await?;
        received_bytes = account_message(
            transport,
            received_bytes,
            message_bytes,
            retained_message_bytes(&message),
            max_session_bytes,
        )
        .await?;
        match message {
            SyncMessage::State(batch) => {
                for snapshot in batch {
                    if let Err(error) = consume_recovery_snapshot(&mut remaining, &snapshot) {
                        let _ = send_message(
                            transport,
                            &SyncMessage::Abort(format!("{} (recovery snapshot batch)", error)),
                        )
                        .await;
                        return Err(error.into());
                    }
                    let record = postcard::to_allocvec(&SyncMessage::State(alloc::vec![snapshot]))
                        .map_err(|error| SyncError::Protocol(error.to_string()))?;
                    stage
                        .append(record)
                        .await
                        .map_err(|error| SyncError::Staging(error.to_string()))?;
                    count += 1;
                }
            }
            SyncMessage::Done => break,
            SyncMessage::Abort(reason) => return Err(SyncError::RemoteAbort(reason)),
            _ => return Err(SyncError::UnexpectedMessage),
        }
    }
    received.received_bytes = received_bytes;
    if let Err(error) = validate_recovery_complete(&remaining) {
        let _ = send_message(
            transport,
            &SyncMessage::Abort(format!("{} (recovery snapshot batch)", error)),
        )
        .await;
        return Err(error.into());
    }
    if requested.is_empty() && count == 0 {
        return Ok((0, 0));
    }

    stage
        .finish()
        .await
        .map_err(|error| SyncError::Staging(error.to_string()))?;
    while let Some(record) = stage
        .next_record()
        .await
        .map_err(|error| SyncError::Staging(error.to_string()))?
    {
        let message: SyncMessage = postcard::from_bytes(&record)
            .map_err(|error| SyncError::Protocol(error.to_string()))?;
        let SyncMessage::State(snapshot) = message else {
            return Err(SyncError::UnexpectedMessage);
        };
        if let Err(error) = apply_received_batch(
            engine,
            Arc::new(snapshot),
            Arc::new(Vec::new()),
            Some(manifest),
            false,
        )
        .await
        {
            let reason = format!("{} (recovery snapshot batch)", error);
            let _ = send_message(transport, &SyncMessage::Abort(reason)).await;
            return Err(error.into());
        }
    }

    // A recovery snapshot is the sender's complete current row state. It includes
    // the history needed by pending changes and supersedes their payloads.
    Ok((count, 0))
}

async fn apply_catalog_stages<K, R, S>(
    engine: &Engine<K, R>,
    stages: Vec<S>,
    manifest: &SyncManifest,
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    S: SyncStage + 'static,
{
    let tables = [
        String::from(ENGINE_TABLES_STORAGE),
        String::from(ENGINE_TABLE_FIELDS_STORAGE),
        String::from(ENGINE_INDICES_STORAGE),
        String::from(ENGINE_INDEX_FIELDS_STORAGE),
    ];
    let manifest = manifest.clone();
    engine
        .mutate_tables(&tables, move |codec, transaction| {
            Box::pin(async move {
                let mut mutations = MutationSummary::default();
                for (stage_index, mut stage) in stages.into_iter().enumerate() {
                    while let Some(record) = stage
                        .next_record()
                        .await
                        .map_err(|error| EngineError::custom(error.to_string()))?
                    {
                        let message: SyncMessage = postcard::from_bytes(&record)
                            .map_err(|error| EngineError::custom(error.to_string()))?;
                        match message {
                            SyncMessage::State(units) => {
                                for unit in units {
                                    validate_state_batch(core::slice::from_ref(&unit))?;
                                    let SyncKey::Row { table, .. } = &unit.key;
                                    if catalog_order(table) != (stage_index % 4) as u8 {
                                        return Err(EngineError::custom(
                                            "Staged catalog state has an invalid apply order",
                                        ));
                                    }
                                    merge_snapshot(codec, transaction, &unit).await?;
                                    mutations.record_table(table);
                                }
                            }
                            SyncMessage::Changes(changes) => {
                                for change in changes {
                                    if catalog_order(&change.table) != (stage_index % 4) as u8 {
                                        return Err(EngineError::custom(
                                            "Staged catalog change has an invalid apply order",
                                        ));
                                    }
                                    merge_change(codec, transaction, &change).await?;
                                    mutations.record_table(&change.table);
                                }
                            }
                            _ => {
                                return Err(EngineError::custom(
                                    "Unexpected message in catalog stage",
                                ));
                            }
                        }
                    }
                }
                validate_catalog(codec, transaction, &[], &[], Some(&manifest)).await?;
                Ok(((), mutations))
            })
        })
        .await
}

struct StagedDataApply {
    snapshots: usize,
    changes: usize,
    requests: Vec<SyncSnapshotRequest>,
}

async fn apply_staged_data<K, R, S>(
    engine: &Engine<K, R>,
    stage: &mut S,
    manifest: &SyncManifest,
    snapshots: bool,
) -> Result<StagedDataApply, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    S: SyncStage,
{
    let mut applied = StagedDataApply {
        snapshots: 0,
        changes: 0,
        requests: Vec::new(),
    };
    while let Some(record) = stage
        .next_record()
        .await
        .map_err(|error| EngineError::custom(error.to_string()))?
    {
        let message: SyncMessage = postcard::from_bytes(&record)
            .map_err(|error| EngineError::custom(error.to_string()))?;
        match (snapshots, message) {
            (true, SyncMessage::State(units)) => {
                let Some(unit) = units.first() else {
                    return Err(EngineError::custom("Empty state record in data stage"));
                };
                let request = match &unit.key {
                    SyncKey::Row { table, row } => SyncSnapshotRequest {
                        table: table.clone(),
                        row: row.clone(),
                    },
                };
                let batch = Arc::new(units);
                match apply_received_batch(
                    engine,
                    Arc::clone(&batch),
                    Arc::new(Vec::new()),
                    Some(manifest),
                    true,
                )
                .await
                {
                    Ok(()) => applied.snapshots += batch.len(),
                    Err(error) if matches!(error, EngineError::SyncDependencyUnavailable) => {
                        applied.requests.push(request);
                    }
                    Err(error) => return Err(error),
                }
            }
            (false, SyncMessage::Changes(changes)) => {
                let Some(first) = changes.first() else {
                    return Err(EngineError::custom("Empty change record in data stage"));
                };
                let request = SyncSnapshotRequest {
                    table: first.table.clone(),
                    row: first.row.clone(),
                };
                let batch = Arc::new(changes);
                match apply_received_batch(
                    engine,
                    Arc::new(Vec::new()),
                    Arc::clone(&batch),
                    Some(manifest),
                    true,
                )
                .await
                {
                    Ok(()) => applied.changes += batch.len(),
                    Err(error) if matches!(error, EngineError::SyncDependencyUnavailable) => {
                        applied.requests.push(request);
                    }
                    Err(error) => return Err(error),
                }
            }
            _ => return Err(EngineError::custom("Unexpected message in data stage")),
        }
    }
    Ok(applied)
}

async fn receive_outbound<K, R, T, F>(
    engine: &Engine<K, R>,
    transport: &mut T,
    manifest: &SyncManifest,
    max_session_bytes: usize,
    stage_factory: &F,
) -> Result<Received, SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Error: core::fmt::Display,
    F::Stage: 'static,
{
    let mut count = Received {
        snapshots: 0,
        changes: 0,
        requests: Vec::new(),
        received_bytes: 0,
    };
    let mut catalog_stages = Vec::with_capacity(8);
    for _ in 0..8 {
        catalog_stages.push(
            stage_factory
                .create()
                .await
                .map_err(|error| SyncError::Staging(error.to_string()))?,
        );
    }
    let mut catalog_snapshots = 0;
    let mut catalog_changes = 0;
    let mut data_snapshot_stage = stage_factory
        .create()
        .await
        .map_err(|error| SyncError::Staging(error.to_string()))?;
    let mut data_change_stage = stage_factory
        .create()
        .await
        .map_err(|error| SyncError::Staging(error.to_string()))?;

    let mut current_changes: Vec<SyncIncrementalChange> = Vec::new();
    let mut current_change_bytes = 0usize;
    let mut current_change_key: Option<(String, RowIdentity)> = None;
    let mut previous_change_key: Option<(String, RowIdentity)> = None;
    let mut previous_change_id = None;
    loop {
        let (message, message_bytes) = receive_message(transport).await?;
        count.received_bytes = account_message(
            transport,
            count.received_bytes,
            message_bytes,
            retained_message_bytes(&message),
            max_session_bytes,
        )
        .await?;
        match message {
            SyncMessage::State(batch) => {
                for unit in batch {
                    let SyncKey::Row { table, .. } = &unit.key;
                    let order = catalog_order(table);
                    if order < 4 {
                        let record = postcard::to_allocvec(&SyncMessage::State(alloc::vec![unit]))
                            .map_err(|error| SyncError::Protocol(error.to_string()))?;
                        catalog_stages[order as usize]
                            .append(record)
                            .await
                            .map_err(|error| SyncError::Staging(error.to_string()))?;
                        catalog_snapshots += 1;
                    } else {
                        let record = postcard::to_allocvec(&SyncMessage::State(alloc::vec![unit]))
                            .map_err(|error| SyncError::Protocol(error.to_string()))?;
                        data_snapshot_stage
                            .append(record)
                            .await
                            .map_err(|error| SyncError::Staging(error.to_string()))?;
                    }
                }
            }
            SyncMessage::Changes(batch) => {
                for change in batch {
                    let order = catalog_order(&change.table);
                    if order < 4 {
                        let record =
                            postcard::to_allocvec(&SyncMessage::Changes(alloc::vec![change]))
                                .map_err(|error| SyncError::Protocol(error.to_string()))?;
                        catalog_stages[4 + order as usize]
                            .append(record)
                            .await
                            .map_err(|error| SyncError::Staging(error.to_string()))?;
                        catalog_changes += 1;
                    } else {
                        let key = (change.table.clone(), change.row.clone());
                        if current_change_key
                            .as_ref()
                            .is_some_and(|current| current != &key)
                        {
                            let record = postcard::to_allocvec(&SyncMessage::Changes(
                                core::mem::take(&mut current_changes),
                            ))
                            .map_err(|error| SyncError::Protocol(error.to_string()))?;
                            data_change_stage
                                .append(record)
                                .await
                                .map_err(|error| SyncError::Staging(error.to_string()))?;
                            current_change_bytes = 0;
                            previous_change_key = current_change_key.take();
                            previous_change_id = None;
                        }
                        if current_change_key.is_none() {
                            if previous_change_key
                                .as_ref()
                                .is_some_and(|previous| previous >= &key)
                            {
                                return Err(SyncError::Protocol(String::from(
                                    "incremental changes are not ordered by row",
                                )));
                            }
                            current_change_key = Some(key);
                        }
                        if previous_change_id
                            .as_ref()
                            .is_some_and(|previous| previous >= &change.id)
                        {
                            return Err(SyncError::Protocol(String::from(
                                "row changes are not ordered by change ID",
                            )));
                        }
                        let change_bytes = incremental_change_estimate(&change);
                        current_change_bytes = current_change_bytes.saturating_add(change_bytes);
                        if current_change_bytes > MAX_APPLY_BATCH_BYTES {
                            return Err(SyncError::SessionTooLarge {
                                size: current_change_bytes,
                                max: MAX_APPLY_BATCH_BYTES,
                            });
                        }
                        previous_change_id = Some(change.id.clone());
                        current_changes.push(change);
                    }
                }
            }
            SyncMessage::Done => {
                if !current_changes.is_empty() {
                    let record = postcard::to_allocvec(&SyncMessage::Changes(current_changes))
                        .map_err(|error| SyncError::Protocol(error.to_string()))?;
                    data_change_stage
                        .append(record)
                        .await
                        .map_err(|error| SyncError::Staging(error.to_string()))?;
                }
                for stage in &mut catalog_stages {
                    stage
                        .finish()
                        .await
                        .map_err(|error| SyncError::Staging(error.to_string()))?;
                }
                data_snapshot_stage
                    .finish()
                    .await
                    .map_err(|error| SyncError::Staging(error.to_string()))?;
                data_change_stage
                    .finish()
                    .await
                    .map_err(|error| SyncError::Staging(error.to_string()))?;
                if catalog_snapshots > 0 || catalog_changes > 0 {
                    if let Err(error) = apply_catalog_stages(engine, catalog_stages, manifest).await
                    {
                        return abort(transport, error, String::from("catalog batch")).await;
                    }
                }
                let staged_snapshots =
                    match apply_staged_data(engine, &mut data_snapshot_stage, manifest, true).await
                    {
                        Ok(applied) => applied,
                        Err(error) => {
                            return abort(transport, error, String::from("data state batch")).await;
                        }
                    };
                count.snapshots = catalog_snapshots + staged_snapshots.snapshots;
                count.requests.extend(staged_snapshots.requests);

                let staged_changes = match apply_staged_data(
                    engine,
                    &mut data_change_stage,
                    manifest,
                    false,
                )
                .await
                {
                    Ok(applied) => applied,
                    Err(error) => {
                        return abort(transport, error, String::from("data change batch")).await;
                    }
                };
                count.changes = catalog_changes + staged_changes.changes;
                count.requests.extend(staged_changes.requests);

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

fn incremental_change_estimate(change: &SyncIncrementalChange) -> usize {
    let identity_bytes = match &change.row {
        RowIdentity::User(_) => 16,
        RowIdentity::ScopedUser { .. } => 32,
        RowIdentity::Catalog(bytes) => bytes.len(),
    };
    core::mem::size_of::<SyncIncrementalChange>()
        .saturating_add(change.table.len())
        .saturating_add(identity_bytes)
        .saturating_add(change.id.0.len())
        .saturating_add(change.payload.len())
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

fn retained_message_bytes(message: &SyncMessage) -> usize {
    match message {
        SyncMessage::State(units) => units.iter().fold(0usize, |used, unit| {
            let SyncKey::Row { table, row } = &unit.key;
            used.saturating_add(core::mem::size_of::<SyncStateUnit>())
                .saturating_add(
                    unit.state
                        .len()
                        .saturating_add(unit.metadata.len())
                        .saturating_add(table.len())
                        .saturating_add(row.to_bytes().len())
                        .saturating_mul(2),
                )
                .saturating_add(128)
        }),
        SyncMessage::Changes(changes) => changes.iter().fold(0usize, |used, change| {
            used.saturating_add(core::mem::size_of::<SyncIncrementalChange>())
                .saturating_add(
                    change
                        .table
                        .len()
                        .saturating_add(change.row.to_bytes().len())
                        .saturating_add(change.id.0.len())
                        .saturating_add(change.payload.len())
                        .saturating_mul(2),
                )
                .saturating_add(128)
        }),
        SyncMessage::RequestSnapshots(requests) => requests.iter().fold(0usize, |used, request| {
            used.saturating_add(core::mem::size_of::<SyncSnapshotRequest>())
                .saturating_add(
                    request
                        .table
                        .len()
                        .saturating_add(request.row.to_bytes().len()),
                )
                .saturating_add(128)
        }),
        _ => 0,
    }
}

async fn account_message<T>(
    transport: &mut T,
    used: usize,
    message_bytes: usize,
    retained_bytes: usize,
    max: usize,
) -> Result<usize, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let size = used
        .saturating_add(message_bytes)
        .saturating_add(retained_bytes);
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
    use alloc::{
        collections::{BTreeMap, BTreeSet},
        string::{String, ToString},
        vec,
        vec::Vec,
    };

    use engine::{
        ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_TABLES_STORAGE, RowIdentity,
    };
    use futures::executor::block_on;

    use super::{
        MAX_MESSAGE_BYTES, SyncError, account_message, catalog_order, consume_recovery_snapshot,
        insert_table_identity, receive_message, retained_message_bytes, send_message,
        validate_hello, validate_recovery_complete, validate_state_batch,
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
    fn table_identity_index_is_bounded_and_selects_the_greatest_id() {
        let mut index = BTreeMap::new();
        let mut used_bytes = 0;
        insert_table_identity(&mut index, &mut used_bytes, "people", &[1], 263)
            .expect("insert table name");
        let first_size = used_bytes;
        insert_table_identity(&mut index, &mut used_bytes, "people", &[2], 263)
            .expect("replace older concurrent table identity");
        assert_eq!(used_bytes, first_size);
        assert_eq!(index.get("people"), Some(&vec![2]));

        let error = insert_table_identity(&mut index, &mut used_bytes, "animals", &[3], 263)
            .expect_err("reject name index beyond its byte budget");
        assert!(error.to_string().contains("validation limit"));
        assert_eq!(index.len(), 1);
        assert_eq!(used_bytes, first_size);
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

        let remaining = || requests.iter().cloned().collect::<BTreeSet<_>>();
        let mut received = remaining();
        assert!(consume_recovery_snapshot(&mut received, &snapshot(1)).is_ok());
        assert!(validate_recovery_complete(&received).is_err());

        let mut received = remaining();
        assert!(consume_recovery_snapshot(&mut received, &snapshot(1)).is_ok());
        assert!(consume_recovery_snapshot(&mut received, &snapshot(2)).is_ok());
        assert!(validate_recovery_complete(&received).is_ok());

        let mut received = remaining();
        assert!(consume_recovery_snapshot(&mut received, &snapshot(1)).is_ok());
        assert!(consume_recovery_snapshot(&mut received, &snapshot(1)).is_err());

        let mut received = remaining();
        assert!(consume_recovery_snapshot(&mut received, &snapshot(3)).is_err());
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
    fn retained_record_estimate_counts_decoded_overhead_against_session_budget() {
        let message = SyncMessage::State(vec![SyncStateUnit::new(
            SyncKey::Row {
                table: String::from("rows"),
                row: RowIdentity::user(uuid::Uuid::from_bytes([1; 16])),
            },
            vec![1; 600_000],
            Vec::new(),
        )]);
        let retained = retained_message_bytes(&message);
        assert!(retained > 600_000);

        let mut transport = Transport::default();
        let error = block_on(account_message(
            &mut transport,
            0,
            600_000,
            retained,
            MAX_MESSAGE_BYTES,
        ))
        .expect_err("decoded storage must count against the bounded session budget");
        assert!(matches!(error, SyncError::SessionTooLarge { .. }));
        assert!(transport.outbound.is_some(), "send an abort to the peer");
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
