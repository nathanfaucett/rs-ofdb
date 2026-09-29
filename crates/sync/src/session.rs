use alloc::{
    boxed::Box,
    collections::BTreeMap,
    format,
    string::{String, ToString},
    vec::Vec,
};

use engine::{
    ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
    ENGINE_TABLES_STORAGE, Engine, EngineError, Kernel, KernelTransaction, RowIdentity,
    RowMutation,
};
use thiserror::Error;
use value::Row;

use crate::{
    PROTOCOL_VERSION, SyncHello, SyncIncrementalChange, SyncKey, SyncManifest, SyncMessage,
    SyncRowCodec, SyncRowInventory, SyncSnapshotRequest, SyncStateUnit, SyncTransport,
};

/// Maximum serialized sync message size (1 MiB). Transports must also bound reads
/// before buffering frames; the session checks received frames only afterward.
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRole {
    Initiator,
    Responder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionConfig {
    pub max_units_per_frame: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_units_per_frame: 64,
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
    if config.max_units_per_frame == 0 {
        return Err(SyncError::InvalidConfiguration);
    }

    exchange_hello(engine, transport, role).await?;

    let remote_manifest = exchange_manifest(engine, transport, role).await?;
    let local_units = export_sync_state_for(engine).await?;

    let local_inventories = inventories_for(engine, &local_units).await?;

    let remote_inventories =
        exchange_inventories(transport, role, local_inventories.clone()).await?;

    let remote_inventories = remote_inventories
        .into_iter()
        .map(|inventory| (inventory.key(), inventory))
        .collect::<BTreeMap<_, _>>();
    let outbound = build_outbound(
        engine,
        local_units,
        remote_manifest.clone(),
        remote_inventories,
    )
    .await?;

    let (sent, mut received) = match role {
        SyncRole::Initiator => {
            let sent = send_outbound(transport, &outbound, config.max_units_per_frame).await?;
            let received = receive_outbound(engine, transport, &remote_manifest).await?;
            (sent, received)
        }
        SyncRole::Responder => {
            let received = receive_outbound(engine, transport, &remote_manifest).await?;
            let sent = send_outbound(transport, &outbound, config.max_units_per_frame).await?;
            (sent, received)
        }
    };

    let remote_requests =
        exchange_snapshot_requests(transport, role, core::mem::take(&mut received.requests))
            .await?;
    let recovery = recovery_snapshots(engine, remote_requests).await?;
    let (sent_recovery, (received_recovery, retried_changes)) = match role {
        SyncRole::Initiator => {
            let sent = send_snapshots(transport, &recovery, config.max_units_per_frame).await?;
            let received =
                receive_recovery(engine, transport, &mut received, &remote_manifest).await?;
            (sent, received)
        }
        SyncRole::Responder => {
            let received =
                receive_recovery(engine, transport, &mut received, &remote_manifest).await?;
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
}

pub async fn sync_manifest_for<K, R>(engine: &Engine<K, R>) -> Result<SyncManifest, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    Ok(SyncManifest::new(
        export_sync_state_for(engine)
            .await?
            .into_iter()
            .map(|unit| (unit.key, unit.digest))
            .collect(),
    ))
}

pub async fn export_sync_state_for<K, R>(
    engine: &Engine<K, R>,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
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
        let rows = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move { codec.row_ids(transaction, &table_for_rows).await })
            })
            .await?;
        for row in rows {
            let table_for_row = table.clone();
            let row_for_state = row.clone();
            let (state, metadata) = engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move {
                        Ok((
                            codec
                                .export_state(transaction, &table_for_row, row_for_state.clone())
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
    apply_received_batch(engine, batch, Vec::new(), None).await
}

async fn apply_received_batch<K, R>(
    engine: &Engine<K, R>,
    mut snapshots: Vec<SyncStateUnit>,
    mut changes: Vec<SyncIncrementalChange>,
    manifest: Option<&SyncManifest>,
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    validate_state_batch(&snapshots)?;
    validate_change_batch(&changes)?;
    sort_state_batch(&mut snapshots);
    changes.sort_by_key(|change| catalog_order(&change.table));
    let rows = snapshots
        .iter()
        .map(|unit| {
            let SyncKey::Row { table, row } = &unit.key;
            (table.clone(), row.clone())
        })
        .chain(
            changes
                .iter()
                .map(|change| (change.table.clone(), change.row.clone())),
        )
        .collect::<Vec<_>>();
    let touched = rows.clone();
    let expected = manifest.cloned();
    let (catalog_snapshots, user_snapshots): (Vec<_>, Vec<_>) =
        snapshots.into_iter().partition(|unit| {
            let SyncKey::Row { table, .. } = &unit.key;
            catalog_order(table) < 4
        });
    let (catalog_changes, user_changes): (Vec<_>, Vec<_>) = changes
        .into_iter()
        .partition(|change| catalog_order(&change.table) < 4);
    let capacity = rows.len();
    engine
        .mutate_rows(&rows, |codec, transaction| {
            Box::pin(async move {
                let mut mutations = Vec::with_capacity(capacity);
                for unit in catalog_snapshots {
                    mutations.push(merge_snapshot(codec, transaction, unit).await?);
                }
                for change in catalog_changes {
                    mutations.push(merge_change(codec, transaction, change).await?);
                }
                for unit in user_snapshots {
                    mutations.push(merge_snapshot(codec, transaction, unit).await?);
                }
                for change in user_changes {
                    mutations.push(merge_change(codec, transaction, change).await?);
                }
                validate_catalog(codec, transaction, &touched, expected.as_ref()).await?;
                Ok(((), mutations))
            })
        })
        .await
}

async fn merge_snapshot<T, R>(
    codec: &R,
    transaction: &mut T,
    unit: SyncStateUnit,
) -> Result<RowMutation, EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    let SyncKey::Row { table, row } = unit.key;
    let old = codec.get_row(transaction, &table, &row).await?;
    if !unit.metadata.is_empty() {
        codec
            .merge_metadata(transaction, &table, row.clone(), &unit.metadata)
            .await?;
    }
    let new = if unit.state.is_empty() {
        None
    } else {
        codec
            .merge_state(transaction, &table, row.clone(), &unit.state)
            .await?
    };
    if new.is_none() && !codec.row_is_deleted(transaction, &table, &row).await? {
        return Err(EngineError::custom(format!(
            "Sync state for {} row {} produced no row",
            table, row
        )));
    }
    Ok(RowMutation {
        table,
        row,
        old,
        new,
    })
}

async fn merge_change<T, R>(
    codec: &R,
    transaction: &mut T,
    change: SyncIncrementalChange,
) -> Result<RowMutation, EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    let old = codec
        .get_row(transaction, &change.table, &change.row)
        .await?;
    let new = codec
        .apply_change(
            transaction,
            &change.table,
            change.row.clone(),
            &change.id,
            &change.payload,
        )
        .await?;
    Ok(RowMutation {
        table: change.table,
        row: change.row,
        old,
        new,
    })
}

async fn validate_catalog<T, R>(
    codec: &R,
    transaction: &T,
    touched: &[(String, RowIdentity)],
    manifest: Option<&SyncManifest>,
) -> Result<(), EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    if touched.is_empty() {
        return Ok(());
    }
    let mut catalog = BTreeMap::<(String, RowIdentity), Row>::new();
    for table in [
        ENGINE_TABLES_STORAGE,
        ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_INDICES_STORAGE,
        ENGINE_INDEX_FIELDS_STORAGE,
    ] {
        for id in codec.row_ids(transaction, table).await? {
            if let Some(row) = codec.get_row(transaction, table, &id).await? {
                catalog.insert((String::from(table), id), row);
            }
        }
    }
    if let Some(manifest) = manifest {
        for (key, _) in &manifest.entries {
            let SyncKey::Row { table, row } = key;
            if catalog_order(table) < 4
                && !catalog.contains_key(&(table.clone(), row.clone()))
                && !codec.row_is_deleted(transaction, table, row).await?
            {
                return Err(EngineError::custom(format!(
                    "Incomplete catalog: missing {} row {} advertised in manifest",
                    table, row
                )));
            }
        }
    }
    for ((table, id), value) in &catalog {
        let RowIdentity::Catalog(bytes) = id else {
            return Err(EngineError::custom(format!(
                "Invalid catalog identity in {}",
                table
            )));
        };
        let parent = match table.as_str() {
            ENGINE_TABLES_STORAGE if bytes.len() == 16 => None,
            ENGINE_TABLE_FIELDS_STORAGE | ENGINE_INDICES_STORAGE if bytes.len() == 32 => {
                Some((ENGINE_TABLES_STORAGE, 16))
            }
            ENGINE_INDEX_FIELDS_STORAGE if bytes.len() == 48 => Some((ENGINE_INDICES_STORAGE, 32)),
            _ => {
                return Err(EngineError::custom(format!(
                    "Invalid catalog identity length in {}",
                    table
                )));
            }
        };
        if let Some((storage, size)) = parent {
            let parent_id = RowIdentity::catalog(bytes[..size].to_vec());
            if !catalog.contains_key(&(String::from(storage), parent_id.clone()))
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
        let valid = match table.as_str() {
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
    for (table, _) in touched {
        if catalog_order(table) == 4
            && !catalog.iter().any(|((storage, _), value)| {
                storage == ENGINE_TABLES_STORAGE && value.values[0].as_text() == Some(table)
            })
        {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: data table {} has no definition",
                table
            )));
        }
    }
    for ((table, id), value) in &catalog {
        if table == ENGINE_TABLES_STORAGE {
            let RowIdentity::Catalog(parent) = id else {
                unreachable!()
            };
            let has_fields = catalog.keys().any(|(storage, child)| {
                storage == ENGINE_TABLE_FIELDS_STORAGE
                    && matches!(child, RowIdentity::Catalog(bytes) if bytes.starts_with(parent))
            });
            if touched.iter().any(|(storage, row)| {
                (storage == ENGINE_TABLES_STORAGE && row == id)
                    || (storage == ENGINE_TABLE_FIELDS_STORAGE
                        && matches!(row, RowIdentity::Catalog(bytes) if bytes.starts_with(parent)))
                    || (storage == value.values[0].as_text().expect("validated table name"))
            }) {
                if !has_fields {
                    return Err(EngineError::custom(format!(
                        "Incomplete catalog: table {} has no fields",
                        value.values[0].as_text().expect("validated table name")
                    )));
                }
                if !catalog.iter().any(|((storage, child), field)| {
                    storage == ENGINE_TABLE_FIELDS_STORAGE
                        && matches!(child, RowIdentity::Catalog(bytes) if bytes.starts_with(parent))
                        && field.values[1].to_type() == Some(value::ValueType::Uuid)
                        && field.values[4].to_bool() == Some(true)
                }) {
                    return Err(EngineError::custom(format!(
                        "Incomplete catalog: table {} has no UUID primary key",
                        value.values[0].as_text().expect("validated table name")
                    )));
                }
            }
        }
        if table == ENGINE_INDICES_STORAGE {
            let name = value.values[1].as_text().expect("validated index table");
            let parent = catalog.iter().find(|((storage, _), row)| {
                storage == ENGINE_TABLES_STORAGE && row.values[0].as_text() == Some(name)
            });
            let Some(((_, RowIdentity::Catalog(table_id)), _)) = parent else {
                return Err(EngineError::custom(format!(
                    "Incomplete catalog: index {} references missing table {}",
                    id, name
                )));
            };
            let RowIdentity::Catalog(index_id) = id else {
                unreachable!()
            };
            if !index_id.starts_with(table_id) {
                return Err(EngineError::custom(format!(
                    "Invalid catalog: index {} is not scoped to table {}",
                    id, name
                )));
            }
            let fields = catalog
                .iter()
                .filter(|((storage, row), _)| {
                    storage == ENGINE_INDEX_FIELDS_STORAGE
                        && matches!(row, RowIdentity::Catalog(bytes) if bytes.starts_with(index_id))
                })
                .collect::<Vec<_>>();
            if fields.is_empty() {
                return Err(EngineError::custom(format!(
                    "Incomplete catalog: index {} has no fields",
                    id
                )));
            }
            for (_, field) in fields {
                let column = field.values[1]
                    .as_text()
                    .expect("validated index field column");
                if !catalog.iter().any(|((storage, row), value)| {
                    storage == ENGINE_TABLE_FIELDS_STORAGE
                        && matches!(row, RowIdentity::Catalog(bytes) if bytes.starts_with(table_id))
                        && value.values[0].as_text() == Some(column)
                }) {
                    return Err(EngineError::custom(format!(
                        "Incomplete catalog: index {} references missing column {}",
                        id, column
                    )));
                }
            }
        }
    }
    Ok(())
}

fn sort_state_batch(batch: &mut [SyncStateUnit]) {
    batch.sort_by_key(|unit| {
        let SyncKey::Row { table, .. } = &unit.key;
        catalog_order(table)
    });
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
    apply_received_batch(engine, Vec::new(), batch.to_vec(), None).await
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
) -> Result<Vec<SyncRowInventory>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut result = Vec::new();
    for unit in units {
        let SyncKey::Row { table, row } = unit.key.clone();
        result.push(SyncRowInventory {
            table: table.clone(),
            row: row.clone(),
            changes: engine
                .read_transaction(|codec, transaction| {
                    Box::pin(async move { codec.change_inventory(transaction, &table, row).await })
                })
                .await?,
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
    match receive_message(transport).await? {
        SyncMessage::Inventory(inventory) => Ok(inventory),
        _ => Err(SyncError::UnexpectedMessage),
    }
}

async fn build_outbound<K, R>(
    engine: &Engine<K, R>,
    units: Vec<SyncStateUnit>,
    remote_manifest: SyncManifest,
    remote_inventories: BTreeMap<SyncKey, SyncRowInventory>,
) -> Result<Outbound, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut outbound = Outbound::default();
    for unit in units {
        let SyncKey::Row { table, row } = unit.key.clone();
        if remote_manifest.contains(&unit.key, unit.digest) {
            continue;
        }
        let Some(remote) = remote_inventories.get(&unit.key) else {
            outbound.snapshots.push(unit);
            continue;
        };
        let table_for_inventory = table.clone();
        let row_for_inventory = row.clone();
        let local = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    codec
                        .change_inventory(transaction, &table_for_inventory, row_for_inventory)
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
            if let Some(payload) = engine
                .read_transaction(move |codec, transaction| {
                    Box::pin(async move {
                        codec
                            .export_change(
                                transaction,
                                &table_for_export,
                                row_for_export,
                                &export_id,
                            )
                            .await
                    })
                })
                .await?
            {
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
    local: Vec<SyncSnapshotRequest>,
) -> Result<Vec<SyncSnapshotRequest>, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let message = SyncMessage::RequestSnapshots(local);
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
    match receive_message(transport).await? {
        SyncMessage::RequestSnapshots(requests) => Ok(requests),
        _ => Err(SyncError::UnexpectedMessage),
    }
}

async fn recovery_snapshots<K, R>(
    engine: &Engine<K, R>,
    requests: Vec<SyncSnapshotRequest>,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut snapshots = Vec::new();
    for request in requests {
        let snapshot = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let state = codec
                        .export_state(transaction, &request.table, request.row.clone())
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
    manifest: &SyncManifest,
) -> Result<(usize, usize), SyncError<T::Error>>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let mut snapshots = Vec::new();
    loop {
        match receive_message(transport).await? {
            SyncMessage::State(batch) => {
                snapshots.extend(batch);
            }
            SyncMessage::Done => break,
            SyncMessage::Abort(reason) => return Err(SyncError::RemoteAbort(reason)),
            _ => return Err(SyncError::UnexpectedMessage),
        }
    }
    let count = snapshots.len();
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
        core::mem::take(&mut received.pending_snapshots),
        Vec::new(),
        Some(manifest),
    )
    .await
    {
        let reason = format!("{} (recovery snapshot batch)", error);
        let _ = send_message(transport, &SyncMessage::Abort(reason)).await;
        return Err(error.into());
    }

    // A recovery snapshot is the sender's complete current row state. It includes
    // the history needed by the pending changes and supersedes their payloads.
    received.pending.clear();
    Ok((count, 0))
}

async fn receive_outbound<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    manifest: &SyncManifest,
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
    };
    let mut snapshots = Vec::new();
    let mut changes = Vec::new();
    loop {
        match receive_message(transport).await? {
            SyncMessage::State(batch) => snapshots.extend(batch),
            SyncMessage::Changes(batch) => changes.extend(batch),
            SyncMessage::Done => {
                count.snapshots = snapshots.len();
                if let Err(error) =
                    apply_received_batch(engine, snapshots.clone(), changes.clone(), Some(manifest))
                        .await
                {
                    if matches!(error, EngineError::SyncDependencyUnavailable) {
                        count.pending_snapshots = snapshots;
                        for change in changes {
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
                    count.changes = changes.len();
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
    let hello = SyncMessage::Hello(SyncHello {
        protocol_version: PROTOCOL_VERSION,
        manifest: sync_manifest_for(engine).await?,
    });
    match role {
        SyncRole::Initiator => {
            send_message(transport, &hello).await?;
            validate_hello(receive_message(transport).await?)
        }
        SyncRole::Responder => {
            validate_hello(receive_message(transport).await?)?;
            send_message(transport, &hello).await
        }
    }
}

fn validate_hello<E>(message: SyncMessage) -> Result<(), SyncError<E>> {
    let SyncMessage::Hello(hello) = message else {
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
    let message = SyncMessage::Manifest(sync_manifest_for(engine).await?);
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
    match receive_message(transport).await? {
        SyncMessage::Manifest(manifest) => Ok(manifest),
        _ => Err(SyncError::UnexpectedMessage),
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

async fn receive_message<T>(transport: &mut T) -> Result<SyncMessage, SyncError<T::Error>>
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
    postcard::from_bytes(&frame).map_err(|error| SyncError::Protocol(error.to_string()))
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
        MAX_MESSAGE_BYTES, SyncError, catalog_order, receive_message, send_message,
        sort_state_batch, validate_hello, validate_state_batch,
    };
    use crate::{
        PROTOCOL_VERSION, SyncHello, SyncKey, SyncManifest, SyncMessage, SyncStateUnit,
        SyncTransport,
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
    fn catalog_rows_are_ordered_before_user_rows() {
        let mut units = [
            "user_table",
            ENGINE_INDEX_FIELDS_STORAGE,
            ENGINE_TABLE_FIELDS_STORAGE,
            ENGINE_INDICES_STORAGE,
            ENGINE_TABLES_STORAGE,
        ]
        .map(|table| {
            SyncStateUnit::new(
                SyncKey::Row {
                    table: table.into(),
                    row: RowIdentity::catalog(vec![1]),
                },
                vec![2],
                vec![],
            )
        });
        sort_state_batch(&mut units);
        let tables = units.map(|unit| {
            let SyncKey::Row { table, .. } = unit.key;
            table
        });
        assert_eq!(
            tables,
            [
                ENGINE_TABLES_STORAGE,
                ENGINE_TABLE_FIELDS_STORAGE,
                ENGINE_INDICES_STORAGE,
                ENGINE_INDEX_FIELDS_STORAGE,
                "user_table",
            ]
        );
        assert_eq!(catalog_order("user_table"), 4);
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
        assert_eq!(
            block_on(receive_message(&mut transport)).expect("boundary message must decode"),
            message
        );
    }
}
