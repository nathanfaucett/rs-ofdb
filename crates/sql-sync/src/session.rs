use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet},
    format,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};

use engine::{
    DocumentSummary, ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE,
    ENGINE_TABLE_FIELDS_STORAGE, ENGINE_TABLES_STORAGE, Engine, EngineError, Kernel,
    KernelTransaction, MutationSummary, Uuid, validate_row_id,
};
use futures::{FutureExt, StreamExt, pin_mut};
use thiserror::Error;
use value::Value;

use crate::pre_apply::{ApplyError, Check, CheckFuture, check_apply};
use crate::scope::TableScope;
use crate::{
    PROTOCOL_VERSION, SyncDirection, SyncHello, SyncIncrementalChange, SyncKey, SyncManifest,
    SyncMessage, SyncRowCodec, SyncRowInventory, SyncSnapshotRequest, SyncStage, SyncStageFactory,
    SyncStateUnit, SyncTransport,
};

// Construct outside the caller's poll frame without erasing conditional auto-Send.
#[inline(never)]
fn sync_stage<F: core::future::Future>(make: impl FnOnce() -> F) -> core::pin::Pin<Box<F>> {
    Box::pin(make())
}

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
    /// Scoped peers must declare complementary directions. Non-bidirectional
    /// unscoped sessions reject; role only determines turn ordering.
    pub direction: SyncDirection,
    pub max_units_per_frame: usize,
    pub max_session_bytes: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            direction: SyncDirection::Bidirectional,
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
    #[error("pre-apply check failed: {0}")]
    PreApply(String),

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

/// Unchecked sync: the caller is responsible for admission of every atomic apply.
/// Production callers with changing authorization or cancellation use the checked variant.
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
    synchronize_with_stage_factory_checked(engine, transport, config, role, stage_factory, || {
        core::future::ready(Ok(()))
    })
    .await
}

pub async fn synchronize_with_stage_factory_checked<K, R, T, F, Q, U>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    stage_factory: &F,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send,
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Stage: 'static,
{
    sync_stage(|| {
        synchronize_scoped(
            engine,
            transport,
            config,
            role,
            stage_factory,
            check,
            None,
            None,
        )
    })
    .await
}

/// Checked selected discovery. Both peers must use this entry point. Capture each
/// peer's pending work before calling; acknowledge only on success. All is required
/// on bootstrap/reconnect/gaps. Remote-only commits require caller-driven wakeups.
/// Selection is not an authorization scope; scoped validators remain separate APIs.
#[allow(clippy::too_many_arguments)]
pub async fn synchronize_selected_with_stage_factory_checked<K, R, T, F, Q, U>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    stage_factory: &F,
    selection: crate::SyncSelection,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send,
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Stage: 'static,
{
    if config.max_units_per_frame == 0
        || config.max_session_bytes == 0
        || config.direction != SyncDirection::Bidirectional
    {
        return Err(SyncError::InvalidConfiguration);
    }
    let message = SyncMessage::Selection {
        protocol_version: PROTOCOL_VERSION,
        selection: selection.clone(),
    };
    let remote = match role {
        SyncRole::Initiator => {
            send_message(transport, &message).await?;
            receive_message(transport).await?.0
        }
        SyncRole::Responder => {
            let remote = receive_message(transport).await?.0;
            send_message(transport, &message).await?;
            remote
        }
    };
    let SyncMessage::Selection {
        protocol_version,
        selection: remote,
    } = remote
    else {
        return Err(SyncError::UnexpectedMessage);
    };
    if protocol_version != PROTOCOL_VERSION {
        return Err(SyncError::IncompatibleProtocol(protocol_version));
    }
    let selection = selection.union(remote);
    sync_stage(|| {
        synchronize_scoped(
            engine,
            transport,
            config,
            role,
            stage_factory,
            check,
            None,
            Some(selection),
        )
    })
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn synchronize_tables_with_stage_factory_checked<K, R, T, F, Q, U>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    stage_factory: &F,
    tables: &[&str],
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send,
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Stage: 'static,
{
    synchronize_tables_with_stage_factory_validated_checked(
        engine,
        transport,
        config,
        role,
        stage_factory,
        tables,
        |_, _, _| Ok(()),
        check,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn synchronize_tables_with_stage_factory_validated_checked<K, R, T, F, V, Q, U>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    stage_factory: &F,
    tables: &[&str],
    validate: V,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    V: Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync,
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send,
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Stage: 'static,
{
    sync_stage(|| {
        synchronize_tables_validators_checked(
            engine,
            transport,
            config,
            role,
            stage_factory,
            tables,
            validate,
            None,
            check,
        )
    })
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn synchronize_tables_with_stage_factory_candidate_validated_checked<
    K,
    R,
    T,
    F,
    V,
    C,
    Q,
    U,
>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    stage_factory: &F,
    tables: &[&str],
    validate: V,
    validate_candidate: C,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    V: Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync,
    C: Fn(&crate::SyncCandidateView) -> Result<(), String> + Send + Sync,
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send,
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Stage: 'static,
{
    sync_stage(|| {
        synchronize_tables_validators_checked(
            engine,
            transport,
            config,
            role,
            stage_factory,
            tables,
            validate,
            Some(&validate_candidate),
            check,
        )
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn synchronize_tables_validators_checked<K, R, T, F, V, Q, U>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    stage_factory: &F,
    tables: &[&str],
    validate: V,
    validate_candidate: Option<&crate::candidate::CandidateValidator<'_>>,
    check: Q,
) -> Result<SyncResult, SyncError<T::Error>>
where
    V: Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync,
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send,
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
    let mut scope = sync_stage(|| TableScope::new(engine, tables, &validate)).await?;
    scope.validate_candidate = validate_candidate;
    scope.direction = config.direction;
    let frame = postcard::to_allocvec(&(&scope.names, scope.direction))
        .map_err(|e| SyncError::Protocol(e.to_string()))?;
    if frame.len() > MAX_MESSAGE_BYTES {
        return Err(SyncError::InvalidConfiguration);
    }
    let remote = match role {
        SyncRole::Initiator => {
            transport.send(frame).await.map_err(SyncError::Transport)?;
            transport.receive().await.map_err(SyncError::Transport)?
        }
        SyncRole::Responder => {
            let remote = transport.receive().await.map_err(SyncError::Transport)?;
            transport.send(frame).await.map_err(SyncError::Transport)?;
            remote
        }
    };
    if remote.len() > MAX_MESSAGE_BYTES {
        return Err(SyncError::InvalidConfiguration);
    }
    let ((names, direction), remaining): ((Vec<String>, SyncDirection), _) =
        postcard::take_from_bytes(&remote).map_err(|e| SyncError::Protocol(e.to_string()))?;
    if !remaining.is_empty() {
        return Err(SyncError::Protocol(String::from(
            "trailing bytes in table scope",
        )));
    }
    if names != scope.names {
        return Err(SyncError::Protocol(String::from("table scopes differ")));
    }
    if !scope.direction.complements(direction) {
        return Err(SyncError::Protocol(String::from(
            "incompatible scoped sync directions",
        )));
    }
    sync_stage(|| {
        synchronize_scoped(
            engine,
            transport,
            config,
            role,
            stage_factory,
            check,
            Some(scope),
            None,
        )
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn synchronize_scoped<K, R, T, F, Q, U>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    stage_factory: &F,
    mut check: Q,
    mut scope: Option<TableScope<'_>>,
    selection: Option<crate::SyncSelection>,
) -> Result<SyncResult, SyncError<T::Error>>
where
    Q: FnMut() -> U + Send,
    U: core::future::Future<Output = Result<(), String>> + Send,
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
    F: SyncStageFactory,
    F::Stage: 'static,
{
    if scope.is_none() && config.direction != SyncDirection::Bidirectional {
        return Err(SyncError::InvalidConfiguration);
    }
    let authority = scope.clone();
    let mut check = || {
        let pending = sync_stage(&mut check);
        let authority = authority.as_ref();
        sync_stage(|| async move {
            pending.await?;
            if let Some(scope) = authority {
                sync_stage(|| scope.unchanged(engine))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        }) as CheckFuture<'_>
    };
    if config.max_units_per_frame == 0 || config.max_session_bytes == 0 {
        return Err(SyncError::InvalidConfiguration);
    }

    if scope.is_some() && config.direction.sends_history() {
        // Preserve complete outgoing scoped history/candidate validation before publication.
        with_engine_abort(
            transport,
            sync_stage(|| {
                export_sync_state_limited(engine, config.max_session_bytes, scope.as_ref())
            })
            .await,
        )
        .await?;
    }
    exchange_hello(transport, role).await?;

    let summaries = with_engine_abort(
        transport,
        discover(engine, config.max_session_bytes, scope.as_ref(), selection).await,
    )
    .await?;
    let local_manifest = SyncManifest::new(
        summaries
            .iter()
            .map(|(key, summary)| (key.clone(), summary.digest))
            .collect(),
    );
    let remote_manifest = exchange_manifest_value(transport, role, local_manifest).await?;
    let local_inventories = summaries
        .iter()
        .map(|(key, summary)| {
            let SyncKey::Row { table, row } = key;
            SyncRowInventory {
                table: table.clone(),
                row: *row,
                changes: summary.changes.clone(),
            }
        })
        .collect();

    let remote_inventories = exchange_inventories(transport, role, local_inventories).await?;

    if let Some(scope) = &scope {
        for inventory in &remote_inventories {
            if !scope.permits_address(&inventory.table) {
                return Err(SyncError::Engine(EngineError::InvalidQuery(
                    "Inventory outside table scope",
                )));
            }
        }
        for (key, _) in &remote_manifest.entries {
            let SyncKey::Row { table, .. } = key;
            if !scope.permits_address(table) {
                return Err(SyncError::Engine(EngineError::InvalidQuery(
                    "Manifest outside table scope",
                )));
            }
        }
    }
    let remote_inventories = remote_inventories
        .into_iter()
        .map(|inventory| (inventory.key(), inventory))
        .collect::<BTreeMap<_, _>>();
    let outbound = if !config.direction.sends_history() {
        Outbound::default()
    } else {
        with_engine_abort(
            transport,
            sync_stage(|| {
                build_outbound(
                    engine,
                    summaries,
                    &remote_manifest,
                    remote_inventories,
                    config.max_session_bytes,
                    scope.as_ref(),
                )
            })
            .await,
        )
        .await?
    };

    let (sent, mut received) = match role {
        SyncRole::Initiator => {
            let sent = send_outbound(transport, &outbound, config.max_units_per_frame).await?;
            let received = sync_stage(|| {
                receive_outbound(
                    engine,
                    transport,
                    &remote_manifest,
                    config.max_session_bytes,
                    stage_factory,
                    &mut check,
                    scope.as_mut(),
                )
            })
            .await?;
            (sent, received)
        }
        SyncRole::Responder => {
            let received = sync_stage(|| {
                receive_outbound(
                    engine,
                    transport,
                    &remote_manifest,
                    config.max_session_bytes,
                    stage_factory,
                    &mut check,
                    scope.as_mut(),
                )
            })
            .await?;
            let sent = send_outbound(transport, &outbound, config.max_units_per_frame).await?;
            (sent, received)
        }
    };

    let local_requests = core::mem::take(&mut received.requests);
    let remote_requests = exchange_snapshot_requests(transport, role, &local_requests).await?;
    if !config.direction.sends_history() && !remote_requests.is_empty() {
        return with_engine_abort(
            transport,
            Err(EngineError::InvalidQuery(
                "Reverse recovery publication is forbidden",
            )),
        )
        .await;
    }
    let recovery = if !config.direction.sends_history() {
        Vec::new()
    } else {
        with_engine_abort(
            transport,
            sync_stage(|| {
                recovery_snapshots(
                    engine,
                    remote_requests,
                    config.max_session_bytes,
                    scope.as_ref(),
                )
            })
            .await,
        )
        .await?
    };
    let (sent_recovery, (received_recovery, retried_changes)) = match role {
        SyncRole::Initiator => {
            let sent = send_snapshots(transport, &recovery, config.max_units_per_frame).await?;
            let received = sync_stage(|| {
                receive_recovery(
                    engine,
                    transport,
                    &mut received,
                    &local_requests,
                    &remote_manifest,
                    config.max_session_bytes,
                    stage_factory,
                    &mut check,
                    scope.as_mut(),
                )
            })
            .await?;
            (sent, received)
        }
        SyncRole::Responder => {
            let received = sync_stage(|| {
                receive_recovery(
                    engine,
                    transport,
                    &mut received,
                    &local_requests,
                    &remote_manifest,
                    config.max_session_bytes,
                    stage_factory,
                    &mut check,
                    scope.as_mut(),
                )
            })
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

async fn discover<K, R>(
    engine: &Engine<K, R>,
    max_bytes: usize,
    scope: Option<&TableScope<'_>>,
    selection: Option<crate::SyncSelection>,
) -> Result<Vec<(SyncKey, DocumentSummary)>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut selection = selection.unwrap_or(crate::SyncSelection::All);
    if let crate::SyncSelection::Selected { tables, rows } = &selection {
        for key in rows {
            let SyncKey::Row { table, row } = key;
            validate_row_id(table, row)?;
        }
        if tables
            .iter()
            .any(|table| engine::catalog_table_for_storage(table).is_some())
            || rows.iter().any(|key| {
                let SyncKey::Row { table, .. } = key;
                engine::catalog_table_for_storage(table).is_some()
            })
        {
            selection = crate::SyncSelection::All;
        }
    }
    let (tables, mut keys) = match selection {
        crate::SyncSelection::All => {
            let mut tables = Vec::from([
                String::from(ENGINE_TABLES_STORAGE),
                String::from(ENGINE_TABLE_FIELDS_STORAGE),
                String::from(ENGINE_INDICES_STORAGE),
                String::from(ENGINE_INDEX_FIELDS_STORAGE),
            ]);
            if let Some(scope) = scope {
                tables.extend(scope.names.iter().cloned());
            } else {
                tables.extend(engine.table_names().await?);
            }
            (tables, BTreeSet::new())
        }
        crate::SyncSelection::Selected { tables, rows } => (tables, rows.into_iter().collect()),
    };
    let scope_keys = scope.map(|scope| (scope.names.clone(), scope.keys.clone()));
    engine
        .read_transaction(move |codec, transaction| {
            Box::pin(async move {
                for table in tables {
                    let rows = codec.row_ids(transaction, &table);
                    pin_mut!(rows);
                    while let Some(row) = rows.next().await {
                        keys.insert(SyncKey::Row {
                            table: table.clone(),
                            row: row?,
                        });
                        // Bound identity discovery even for absent-state/hostile codecs.
                        if keys.len() > max_bytes / 32 {
                            return Err(EngineError::InvalidQuery(
                                "Sync discovery budget exceeded",
                            ));
                        }
                    }
                }
                let mut summaries = Vec::new();
                let mut used = 0;
                for key in keys {
                    let SyncKey::Row { table, row } = &key;
                    if scope_keys
                        .as_ref()
                        .is_some_and(|(names, keys)| !names.contains(table) && !keys.contains(&key))
                    {
                        continue;
                    }
                    used = account_engine_payload(used, table.len().saturating_add(80), max_bytes)?;
                    if let Some(summary) = codec
                        .history_summary(transaction, table, *row, max_bytes.saturating_sub(used))
                        .await?
                    {
                        used = account_engine_payload(
                            used,
                            summary.metadata.len().saturating_add(
                                summary
                                    .changes
                                    .iter()
                                    .map(|id| id.0.len().saturating_add(32))
                                    .sum::<usize>(),
                            ),
                            max_bytes,
                        )?;
                        summaries.push((key, summary));
                    }
                }
                Ok(summaries)
            })
        })
        .await
}

pub async fn sync_manifest_for<K, R>(engine: &Engine<K, R>) -> Result<SyncManifest, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    sync_manifest_limited(engine, usize::MAX, None).await
}

async fn sync_manifest_limited<K, R>(
    engine: &Engine<K, R>,
    max_bytes: usize,
    scope: Option<&TableScope<'_>>,
) -> Result<SyncManifest, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    Ok(SyncManifest::new(
        discover(engine, max_bytes, scope, None)
            .await?
            .into_iter()
            .map(|(key, summary)| (key, summary.digest))
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
    export_sync_state_limited(engine, usize::MAX, None).await
}

async fn export_sync_state_limited<K, R>(
    engine: &Engine<K, R>,
    max_bytes: usize,
    scope: Option<&TableScope<'_>>,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut tables = [
        ENGINE_TABLES_STORAGE,
        ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_INDICES_STORAGE,
        ENGINE_INDEX_FIELDS_STORAGE,
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    if let Some(scope) = scope {
        tables.extend(scope.names.iter().cloned());
    } else {
        tables.extend(sync_stage(|| engine.table_names()).await?);
    }
    tables.sort_unstable();
    tables.dedup();
    let scope_for_rows = scope.map(|s| (s.names.clone(), s.keys.clone()));
    let candidate_tables = scope.and_then(TableScope::candidate_tables);
    let (mut units, view) = sync_stage(|| {
        engine.read_transaction(|codec, transaction| {
            sync_stage(|| async move {
                let mut used_bytes = 0;
                let mut units = Vec::new();
                for table_for_rows in tables {
                    let rows = codec.row_ids(transaction, &table_for_rows);
                    pin_mut!(rows);
                    while let Some(row) = rows.next().await {
                        let row = row?;
                        if scope_for_rows.as_ref().is_some_and(|(names, keys)| {
                            !names.contains(&table_for_rows)
                                && !keys.contains(&SyncKey::Row {
                                    table: table_for_rows.clone(),
                                    row,
                                })
                        }) {
                            continue;
                        }
                        let state_budget = max_bytes
                            .saturating_sub(used_bytes)
                            .saturating_sub(table_for_rows.len())
                            .saturating_sub(row.as_bytes().len())
                            .saturating_sub(64);
                        let state = codec
                            .export_state(transaction, &table_for_rows, row, state_budget)
                            .await?
                            .unwrap_or_default();
                        let metadata = codec
                            .export_metadata(transaction, &table_for_rows, row)
                            .await?;
                        if !state.is_empty() || !metadata.is_empty() {
                            let state_bytes = state
                                .len()
                                .saturating_add(metadata.len())
                                .saturating_add(table_for_rows.len())
                                .saturating_add(row.as_bytes().len())
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
                }
                let view = crate::candidate::read_candidate_view(
                    codec,
                    transaction,
                    units.iter().map(|unit| unit.key.clone()).collect(),
                    crate::SyncValidationDirection::Outgoing,
                    candidate_tables,
                )
                .await?;
                Ok((units, view))
            })
        })
    })
    .await?;
    if let Some(scope) = scope {
        scope.validate_candidates(&view)?;
    }
    units.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    if let Some(scope) = scope {
        let mut scope = scope.clone();
        for unit in &units {
            sync_stage(|| scope.validate(engine, unit)).await?;
        }
    }
    Ok(units)
}

/// Direct unchecked apply. The caller must authorize each atomic catalog/data apply;
/// use checked sync for received production operations with changing decisions.
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

/// Direct unchecked apply; this batch may contain multiple atomic operations.
/// The caller is responsible for authorizing each operation; prefer checked sync.
pub async fn apply_sync_state_batch_for<K, R>(
    engine: &Engine<K, R>,
    batch: Vec<SyncStateUnit>,
) -> Result<(), EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    apply_received_batch(
        engine,
        Arc::new(batch),
        Arc::new(Vec::new()),
        None,
        false,
        None,
        &mut || Box::pin(core::future::ready(Ok(()))),
    )
    .await
    .map_err(ApplyError::into_engine)
}

async fn apply_received_batch<K, R>(
    engine: &Engine<K, R>,
    snapshots: Arc<Vec<SyncStateUnit>>,
    changes: Arc<Vec<SyncIncrementalChange>>,
    manifest: Option<&SyncManifest>,
    catalog_already_validated: bool,
    scope: Option<&TableScope<'_>>,
    check: &mut Check<'_>,
) -> Result<(), ApplyError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    if scope.is_some_and(|scope| !scope.direction.receives_history())
        && (!snapshots.is_empty() || !changes.is_empty())
    {
        return Err(EngineError::InvalidQuery("Reverse history apply is forbidden").into());
    }
    validate_state_batch(&snapshots)?;
    validate_change_batch(&changes)?;
    validate_row_addresses(&snapshots, &changes)?;
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
        check_apply(check).await?;
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
        let mut rows = BTreeMap::<(String, Uuid), (Vec<usize>, Vec<usize>, usize)>::new();
        for (index, unit) in snapshots.iter().enumerate() {
            let SyncKey::Row { table, row } = &unit.key;
            if catalog_order(table) == 4 {
                let entry = rows.entry((table.clone(), *row)).or_default();
                entry.0.push(index);
                entry.2 = entry.2.saturating_add(sync_state_unit_estimate(unit));
            }
        }
        for (index, change) in changes.iter().enumerate() {
            if catalog_order(&change.table) == 4 {
                let entry = rows.entry((change.table.clone(), change.row)).or_default();
                entry.1.push(index);
                entry.2 = entry
                    .2
                    .saturating_add(sync_incremental_change_estimate(change));
            }
        }
        let mut batch = Vec::new();
        let mut batch_bytes = 0_usize;
        for ((table, _), (snapshot_indices, change_indices, bytes)) in rows {
            if bytes > MAX_APPLY_BATCH_BYTES {
                return Err(EngineError::custom(format!(
                    "Sync row batch is {bytes} bytes, exceeds {MAX_APPLY_BATCH_BYTES} bytes"
                ))
                .into());
            }
            if !batch.is_empty() && batch_bytes.saturating_add(bytes) > MAX_APPLY_BATCH_BYTES {
                Box::pin(apply_data_row_batch(
                    engine,
                    Arc::clone(&snapshots),
                    Arc::clone(&changes),
                    core::mem::take(&mut batch),
                    scope,
                    check,
                ))
                .await?;
                batch_bytes = 0;
            }
            batch_bytes = batch_bytes.saturating_add(bytes);
            batch.push((table, snapshot_indices, change_indices));
        }
        if !batch.is_empty() {
            Box::pin(apply_data_row_batch(
                engine, snapshots, changes, batch, scope, check,
            ))
            .await?;
        }
    }

    Ok(())
}

async fn apply_data_row_batch<K, R>(
    engine: &Engine<K, R>,
    snapshots: Arc<Vec<SyncStateUnit>>,
    changes: Arc<Vec<SyncIncrementalChange>>,
    rows: Vec<(String, Vec<usize>, Vec<usize>)>,
    scope: Option<&TableScope<'_>>,
    check: &mut Check<'_>,
) -> Result<(), ApplyError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut tables = BTreeSet::new();
    for (table, _, _) in &rows {
        tables.insert(table.clone());
    }
    let tables = tables.into_iter().collect::<Vec<_>>();
    check_apply(check).await?;
    let candidate_tables = scope.and_then(TableScope::candidate_tables);
    engine
        .mutate_tables_validated(
            tables.as_slice(),
            move |codec, transaction| {
                Box::pin(async move {
                    let mut mutations = MutationSummary::default();
                    let mut candidates = Vec::new();
                    for (table, snapshot_indices, change_indices) in rows {
                        for index in &snapshot_indices {
                            candidates.push(snapshots[*index].key.clone());
                        }
                        for index in &change_indices {
                            candidates.push(SyncKey::Row {
                                table: table.clone(),
                                row: changes[*index].row,
                            });
                        }
                        for index in snapshot_indices {
                            merge_snapshot(codec, transaction, &snapshots[index]).await?;
                            mutations.record_table(&table);
                        }
                        for index in change_indices {
                            merge_change(codec, transaction, &changes[index]).await?;
                            mutations.record_table(&table);
                        }
                    }
                    let view = crate::candidate::read_candidate_view(
                        codec,
                        transaction,
                        candidates,
                        crate::SyncValidationDirection::Incoming,
                        candidate_tables,
                    )
                    .await?;
                    Ok((view, mutations))
                })
            },
            |view| {
                if let Some(scope) = scope {
                    scope.validate_candidates(view)?;
                }
                Ok(())
            },
        )
        .await
        .map(|_| ())
        .map_err(ApplyError::from)
}

fn sync_state_unit_estimate(unit: &SyncStateUnit) -> usize {
    let SyncKey::Row { table, row } = &unit.key;
    core::mem::size_of::<SyncStateUnit>()
        .saturating_add(table.len())
        .saturating_add(row_identity_estimate(row))
        .saturating_add(unit.state.len())
        .saturating_add(unit.metadata.len())
}

fn sync_incremental_change_estimate(change: &SyncIncrementalChange) -> usize {
    core::mem::size_of::<SyncIncrementalChange>()
        .saturating_add(change.table.len())
        .saturating_add(row_identity_estimate(&change.row))
        .saturating_add(change.id.0.len())
        .saturating_add(change.payload.len())
}

fn row_identity_estimate(_: &Uuid) -> usize {
    16
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
            .merge_metadata(transaction, table, *row, &unit.metadata)
            .await?;
    }
    let new = if unit.state.is_empty() {
        None
    } else {
        codec
            .merge_state(transaction, table, *row, &unit.state)
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
            change.row,
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
    let mut field_status: BTreeMap<Uuid, BTreeMap<String, bool>> = BTreeMap::new();
    let mut table_columns: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut index_columns: BTreeMap<Uuid, Vec<String>> = BTreeMap::new();
    let mut lookup_bytes = 0usize;
    for table in catalog_tables {
        let rows = codec.scan_row_states(transaction, table);
        pin_mut!(rows);
        while let Some(state) = rows.next().await {
            let (id, value, deleted) = state?;
            if deleted {
                continue;
            }

            let values = &value.values;
            let valid = match table {
                ENGINE_TABLES_STORAGE => values.len() == 1 && values[0].as_text().is_some(),
                ENGINE_TABLE_FIELDS_STORAGE => {
                    values.len() == 7
                        && values[0].to_uuid().is_some()
                        && values[1].as_text().is_some()
                        && values[2].as_text().is_some()
                        && values[3].to_type().is_some()
                        && values[5].to_integer().is_some_and(|position| position >= 0)
                        && values[6].to_bool().is_some()
                }
                ENGINE_INDICES_STORAGE => {
                    values.len() == 4
                        && values[0].as_text().is_some()
                        && values[1].as_text().is_some()
                        && values[2].to_bool().is_some()
                        && values[3].to_uuid().is_some()
                }
                ENGINE_INDEX_FIELDS_STORAGE => {
                    values.len() == 4
                        && values[0].to_uuid().is_some()
                        && values[1].to_integer().is_some_and(|position| position >= 0)
                        && values[2].as_text().is_some()
                        && values[3].to_uuid().is_some()
                }
                _ => false,
            };
            if !valid {
                return Err(EngineError::custom(format!(
                    "Invalid catalog row {} in {}",
                    id, table
                )));
            }
            match table {
                ENGINE_TABLE_FIELDS_STORAGE => {
                    let table_id = values[0]
                        .to_uuid()
                        .expect("catalog field table identity was validated");
                    let name = values[1]
                        .as_text()
                        .expect("catalog field table name was validated");
                    let column = values[2]
                        .as_text()
                        .expect("catalog field column was validated");
                    let next_bytes = lookup_bytes
                        .saturating_add(name.len().saturating_mul(2))
                        .saturating_add(column.len())
                        .saturating_add(1024);
                    if next_bytes > MAX_CATALOG_LOOKUP_BYTES {
                        return Err(EngineError::custom(format!(
                            "Catalog field lookup exceeds the {MAX_CATALOG_LOOKUP_BYTES}-byte validation limit"
                        )));
                    }
                    lookup_bytes = next_bytes;
                    let has_primary_key = field_status
                        .entry(table_id)
                        .or_default()
                        .entry(name.to_string())
                        .or_default();
                    if values[6].to_bool() == Some(true) {
                        if values[3].to_type() == Some(value::ValueType::Null) {
                            return Err(EngineError::custom(format!(
                                "Invalid catalog: table {name} has a null-type primary key"
                            )));
                        }
                        *has_primary_key = true;
                    }
                    table_columns
                        .entry(name.to_string())
                        .or_default()
                        .insert(column.to_string());
                }
                ENGINE_INDEX_FIELDS_STORAGE => {
                    let index_id = values[0]
                        .to_uuid()
                        .expect("catalog index field identity was validated");
                    let column = values[2]
                        .as_text()
                        .expect("catalog index field column was validated");
                    let next_bytes = lookup_bytes
                        .saturating_add(column.len())
                        .saturating_add(256);
                    if next_bytes > MAX_CATALOG_LOOKUP_BYTES {
                        return Err(EngineError::custom(format!(
                            "Catalog field lookup exceeds the {MAX_CATALOG_LOOKUP_BYTES}-byte validation limit"
                        )));
                    }
                    lookup_bytes = next_bytes;
                    index_columns
                        .entry(index_id)
                        .or_default()
                        .push(column.to_string());
                }
                _ => {}
            }
        }
    }
    let mut table_names = BTreeSet::new();
    let mut table_lookup_bytes = 0usize;
    let tables = codec.scan_row_states(transaction, ENGINE_TABLES_STORAGE);
    pin_mut!(tables);
    while let Some(state) = tables.next().await {
        let (table_id, value, deleted) = state?;
        if deleted {
            continue;
        }

        let name = value.values[0]
            .as_text()
            .expect("catalog table name was validated");
        let next_bytes = table_lookup_bytes
            .saturating_add(name.len())
            .saturating_add(256);
        if next_bytes > MAX_CATALOG_LOOKUP_BYTES {
            return Err(EngineError::custom(format!(
                "Catalog table-name index exceeds the {MAX_CATALOG_LOOKUP_BYTES}-byte validation limit"
            )));
        }
        table_lookup_bytes = next_bytes;
        table_names.insert(name.to_string());
        let has_primary_key = field_status
            .get(&table_id)
            .and_then(|names| names.get(name));
        let Some(has_primary_key) = has_primary_key else {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: table {} has no fields",
                name
            )));
        };
        if !has_primary_key {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: table {} has no primary key",
                name
            )));
        }
    }
    for table in touched_tables(snapshots, changes) {
        if catalog_order(table) == 4
            && !table_names.contains(table)
            && codec
                .table_drop_events(transaction, table)
                .await?
                .is_empty()
        {
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
        let table_id = value.values[3]
            .to_uuid()
            .expect("catalog index table identity was validated");
        if codec
            .table_definition_was_dropped(transaction, &table_id)
            .await?
        {
            continue;
        }
        if !table_names.contains(name) {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: index {} references missing table {}",
                id, name
            )));
        }
        let Some(columns) = index_columns.get(&id) else {
            return Err(EngineError::custom(format!(
                "Incomplete catalog: index {} has no fields",
                id
            )));
        };
        let available_columns = table_columns.get(name);
        for column in columns {
            if !available_columns.is_some_and(|columns| columns.contains(column)) {
                return Err(EngineError::custom(format!(
                    "Incomplete catalog: index {} references missing column {}",
                    id, column
                )));
            }
        }
    }
    Ok(())
}

async fn catalog_has_active_row<T, R>(
    codec: &R,
    transaction: &T,
    table: &str,
    target: &Uuid,
) -> Result<bool, EngineError>
where
    T: KernelTransaction,
    R: SyncRowCodec<T>,
{
    Ok(codec.get_row(transaction, table, target).await?.is_some())
}

fn touched_tables<'a>(
    snapshots: &'a [SyncStateUnit],
    changes: &'a [SyncIncrementalChange],
) -> impl Iterator<Item = &'a str> + 'a {
    snapshots
        .iter()
        .map(|unit| match &unit.key {
            SyncKey::Row { table, .. } => table.as_str(),
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
        row: *row,
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

fn validate_row_addresses(
    snapshots: &[SyncStateUnit],
    changes: &[SyncIncrementalChange],
) -> Result<(), EngineError> {
    for unit in snapshots {
        let SyncKey::Row { table, row } = &unit.key;
        validate_row_id(table, row)?;
    }
    for change in changes {
        validate_row_id(&change.table, &change.row)?;
    }
    Ok(())
}

fn validate_manifest_row_addresses(manifest: &SyncManifest) -> Result<(), EngineError> {
    for (key, _) in &manifest.entries {
        let SyncKey::Row { table, row } = key;
        validate_row_id(table, row)?;
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

/// Direct unchecked apply; caller authorization is required for each atomic operation.
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
        None,
        &mut || Box::pin(core::future::ready(Ok(()))),
    )
    .await
    .map_err(ApplyError::into_engine)
}

fn validate_change_batch(batch: &[SyncIncrementalChange]) -> Result<(), EngineError> {
    for change in batch {
        if change.id.0.is_empty() || change.payload.is_empty() {
            return Err(EngineError::custom("Invalid incremental change frame"));
        }
    }
    Ok(())
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
    summaries: Vec<(SyncKey, DocumentSummary)>,
    remote_manifest: &SyncManifest,
    remote_inventories: BTreeMap<SyncKey, SyncRowInventory>,
    max_bytes: usize,
    scope: Option<&TableScope<'_>>,
) -> Result<Outbound, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut used_bytes = 0;
    let mut outbound = Outbound::default();
    for (unit_key, summary) in summaries {
        let SyncKey::Row { table, row } = unit_key.clone();
        if remote_manifest.contains(&unit_key, summary.digest) {
            continue;
        }
        let remote = remote_inventories.get(&unit_key);
        // Metadata is outside document history. Never select document-only deltas
        // when metadata must travel with them.
        if remote.is_none() || !summary.metadata.is_empty() {
            for unit in recovery_snapshots(
                engine,
                Vec::from([SyncSnapshotRequest { table, row }]),
                max_bytes.saturating_sub(used_bytes),
                scope,
            )
            .await?
            {
                used_bytes =
                    account_engine_payload(used_bytes, state_unit_payload_size(&unit), max_bytes)?;
                outbound.snapshots.push(unit);
            }
            continue;
        }
        let remote = remote.expect("inventory checked above");
        let local = summary.changes;
        let has_history = !local.is_empty();
        let row_changes_start = outbound.changes.len();
        let row_bytes_start = used_bytes;
        let mut snapshot_required = false;
        for key in local {
            if remote.changes.iter().any(|candidate| candidate == &key) {
                continue;
            }
            let export_id = key.clone();
            let table_for_export = table.clone();
            let row_for_export = row;
            let export_budget = max_bytes.saturating_sub(
                used_bytes
                    .saturating_add(key.0.len())
                    .saturating_add(table.len())
                    .saturating_add(row.as_bytes().len())
                    .saturating_add(64),
            );
            let candidate_tables = scope.and_then(TableScope::candidate_tables);
            let (payload, view) = engine
                .read_transaction(move |codec, transaction| {
                    Box::pin(async move {
                        let payload = codec
                            .export_change(
                                transaction,
                                &table_for_export,
                                row_for_export,
                                &export_id,
                                export_budget,
                            )
                            .await?;
                        let view = if payload.is_some() {
                            crate::candidate::read_candidate_view(
                                codec,
                                transaction,
                                Vec::from([SyncKey::Row {
                                    table: table_for_export,
                                    row: row_for_export,
                                }]),
                                crate::SyncValidationDirection::Outgoing,
                                candidate_tables,
                            )
                            .await?
                        } else {
                            None
                        };
                        Ok((payload, view))
                    })
                })
                .await?;
            if let Some(scope) = scope {
                scope.validate_candidates(&view)?;
            }
            if let Some(payload) = payload {
                if let Some(scope) = scope {
                    scope
                        .clone()
                        .validate(
                            engine,
                            &SyncStateUnit::new(
                                SyncKey::Row {
                                    table: table.clone(),
                                    row,
                                },
                                payload.clone(),
                                Vec::new(),
                            ),
                        )
                        .await?;
                }
                used_bytes = account_engine_payload(
                    used_bytes,
                    payload.len() + key.0.len() + table.len() + row.as_bytes().len() + 64,
                    max_bytes,
                )?;
                outbound.changes.push(SyncIncrementalChange {
                    table: table.clone(),
                    row,
                    id: key,
                    payload,
                });
            } else {
                snapshot_required = true;
                break;
            }
        }
        if !snapshot_required && has_history && outbound.changes.len() == row_changes_start {
            continue;
        }
        if snapshot_required || outbound.changes.len() == row_changes_start {
            outbound.changes.truncate(row_changes_start);
            used_bytes = row_bytes_start;
            let snapshots = recovery_snapshots(
                engine,
                Vec::from([SyncSnapshotRequest { table, row }]),
                max_bytes.saturating_sub(used_bytes),
                scope,
            )
            .await?;
            for snapshot in snapshots {
                used_bytes = account_engine_payload(
                    used_bytes,
                    state_unit_payload_size(&snapshot),
                    max_bytes,
                )?;
                outbound.snapshots.push(snapshot);
            }
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
    // Preserve each codec inventory's dependency order within its row.
    outbound.changes.sort_by(|left, right| {
        catalog_order(&left.table)
            .cmp(&catalog_order(&right.table))
            .then_with(|| left.table.cmp(&right.table))
            .then_with(|| left.row.cmp(&right.row))
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
    scope: Option<&TableScope<'_>>,
) -> Result<Vec<SyncStateUnit>, EngineError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let mut used_bytes = 0;
    let mut snapshots = Vec::new();
    for request in requests {
        if scope.is_some_and(|scope| {
            !scope.permits(&SyncKey::Row {
                table: request.table.clone(),
                row: request.row,
            })
        }) {
            return Err(EngineError::InvalidQuery(
                "Recovery request outside table scope",
            ));
        }
        let state_budget = max_bytes.saturating_sub(used_bytes);
        let candidate_tables = scope.and_then(TableScope::candidate_tables);
        let (snapshot, view) = engine
            .read_transaction(|codec, transaction| {
                Box::pin(async move {
                    let state = codec
                        .export_state(transaction, &request.table, request.row, state_budget)
                        .await?
                        .unwrap_or_default();
                    let metadata = codec
                        .export_metadata(transaction, &request.table, request.row)
                        .await?;
                    let snapshot = if state.is_empty() && metadata.is_empty() {
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
                    };
                    let candidates = snapshot.iter().map(|unit| unit.key.clone()).collect();
                    let view = crate::candidate::read_candidate_view(
                        codec,
                        transaction,
                        candidates,
                        crate::SyncValidationDirection::Outgoing,
                        candidate_tables,
                    )
                    .await?;
                    Ok((snapshot, view))
                })
            })
            .await?;
        if let Some(scope) = scope {
            scope.validate_candidates(&view)?;
        }
        if let Some(snapshot) = snapshot {
            if let Some(scope) = scope {
                scope.clone().validate(engine, &snapshot).await?;
            }
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
    check: &mut Check<'_>,
    mut scope: Option<&mut TableScope<'_>>,
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
        if scope
            .as_deref()
            .is_some_and(|scope| !scope.direction.receives_history())
            && matches!(&message, SyncMessage::State(_) | SyncMessage::Changes(_))
        {
            with_engine_abort(
                transport,
                Err::<(), _>(EngineError::InvalidQuery(
                    "Reverse history apply is forbidden",
                )),
            )
            .await?;
        }
        match message {
            SyncMessage::State(batch) => {
                for snapshot in batch {
                    if let Some(scope) = scope.as_deref_mut() {
                        with_engine_abort(transport, scope.validate(engine, &snapshot).await)
                            .await?;
                    }
                    if let Err(error) = consume_recovery_snapshot(&mut remaining, &snapshot) {
                        let _ = send_message(
                            transport,
                            &SyncMessage::Abort(format!("{} (recovery snapshot batch)", error)),
                        )
                        .now_or_never();
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
        .now_or_never();
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
        if let Err(error) = Box::pin(apply_received_batch(
            engine,
            Arc::new(snapshot),
            Arc::new(Vec::new()),
            Some(manifest),
            false,
            scope.as_deref(),
            check,
        ))
        .await
        {
            let reason = format!("{} (recovery snapshot batch)", error);
            let _ = send_message(transport, &SyncMessage::Abort(reason)).now_or_never();
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
    check: &mut Check<'_>,
) -> Result<(), ApplyError>
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
    check_apply(check).await?;
    sync_stage(|| {
        engine.mutate_tables(&tables, move |codec, transaction| {
            sync_stage(|| async move {
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
                sync_stage(|| validate_catalog(codec, transaction, &[], &[], Some(&manifest)))
                    .await?;
                Ok(((), mutations))
            })
        })
    })
    .await
    .map_err(ApplyError::from)
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
    scope: Option<&TableScope<'_>>,
    check: &mut Check<'_>,
) -> Result<StagedDataApply, ApplyError>
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
    let mut pending_snapshots = Vec::new();
    let mut pending_snapshot_bytes = 0_usize;
    let mut pending_changes = Vec::new();
    let mut pending_change_bytes = 0_usize;
    while let Some(record) = stage
        .next_record()
        .await
        .map_err(|error| EngineError::custom(error.to_string()))?
    {
        let message: SyncMessage = postcard::from_bytes(&record)
            .map_err(|error| EngineError::custom(error.to_string()))?;
        match (snapshots, message) {
            (true, SyncMessage::State(units)) => {
                if units.is_empty() {
                    return Err(EngineError::custom("Empty state record in data stage").into());
                }
                for unit in units {
                    let bytes = sync_state_unit_estimate(&unit);
                    if bytes > MAX_APPLY_BATCH_BYTES {
                        return Err(EngineError::custom(format!(
                            "Sync row batch is {bytes} bytes, exceeds {MAX_APPLY_BATCH_BYTES} bytes"
                        ))
                        .into());
                    }
                    if !pending_snapshots.is_empty()
                        && pending_snapshot_bytes.saturating_add(bytes) > MAX_APPLY_BATCH_BYTES
                    {
                        let completed = apply_staged_snapshot_batch(
                            engine,
                            manifest,
                            core::mem::take(&mut pending_snapshots),
                            scope,
                            check,
                        )
                        .await?;
                        merge_staged_apply(&mut applied, completed);
                        pending_snapshot_bytes = 0;
                    }
                    pending_snapshot_bytes = pending_snapshot_bytes.saturating_add(bytes);
                    pending_snapshots.push(unit);
                }
            }
            (false, SyncMessage::Changes(changes)) => {
                let Some(first) = changes.first() else {
                    return Err(EngineError::custom("Empty change record in data stage").into());
                };
                if changes
                    .iter()
                    .any(|change| change.table != first.table || change.row != first.row)
                {
                    return Err(EngineError::custom(
                        "Change record in data stage spans multiple rows",
                    )
                    .into());
                }
                let bytes = changes.iter().fold(0_usize, |total, change| {
                    total.saturating_add(sync_incremental_change_estimate(change))
                });
                if bytes > MAX_APPLY_BATCH_BYTES {
                    return Err(EngineError::custom(format!(
                        "Sync row batch is {bytes} bytes, exceeds {MAX_APPLY_BATCH_BYTES} bytes"
                    ))
                    .into());
                }
                if !pending_changes.is_empty()
                    && pending_change_bytes.saturating_add(bytes) > MAX_APPLY_BATCH_BYTES
                {
                    let completed = apply_staged_change_batch(
                        engine,
                        manifest,
                        core::mem::take(&mut pending_changes),
                        scope,
                        check,
                    )
                    .await?;
                    merge_staged_apply(&mut applied, completed);
                    pending_change_bytes = 0;
                }
                pending_change_bytes = pending_change_bytes.saturating_add(bytes);
                pending_changes.extend(changes);
            }
            _ => return Err(EngineError::custom("Unexpected message in data stage").into()),
        }
    }
    if !pending_snapshots.is_empty() {
        let completed = apply_staged_snapshot_batch(
            engine,
            manifest,
            core::mem::take(&mut pending_snapshots),
            scope,
            check,
        )
        .await?;
        merge_staged_apply(&mut applied, completed);
    }
    if !pending_changes.is_empty() {
        let completed = apply_staged_change_batch(
            engine,
            manifest,
            core::mem::take(&mut pending_changes),
            scope,
            check,
        )
        .await?;
        merge_staged_apply(&mut applied, completed);
    }
    Ok(applied)
}

fn merge_staged_apply(target: &mut StagedDataApply, source: StagedDataApply) {
    target.snapshots = target.snapshots.saturating_add(source.snapshots);
    target.changes = target.changes.saturating_add(source.changes);
    for request in source.requests {
        if !target
            .requests
            .iter()
            .any(|existing| existing.table == request.table && existing.row == request.row)
        {
            target.requests.push(request);
        }
    }
}

async fn apply_staged_snapshot_batch<K, R>(
    engine: &Engine<K, R>,
    manifest: &SyncManifest,
    units: Vec<SyncStateUnit>,
    scope: Option<&TableScope<'_>>,
    check: &mut Check<'_>,
) -> Result<StagedDataApply, ApplyError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let count = units.len();
    let units = Arc::new(units);
    match Box::pin(apply_received_batch(
        engine,
        Arc::clone(&units),
        Arc::new(Vec::new()),
        Some(manifest),
        true,
        scope,
        check,
    ))
    .await
    {
        Ok(()) => Ok(StagedDataApply {
            snapshots: count,
            changes: 0,
            requests: Vec::new(),
        }),
        Err(ApplyError::Engine(EngineError::SyncDependencyUnavailable)) => {
            let units = Arc::try_unwrap(units).unwrap_or_else(|shared| shared.as_ref().clone());
            let mut rows = BTreeMap::<(String, Uuid), Vec<SyncStateUnit>>::new();
            for unit in units {
                let SyncKey::Row { table, row } = &unit.key;
                rows.entry((table.clone(), *row)).or_default().push(unit);
            }
            let mut applied = StagedDataApply {
                snapshots: 0,
                changes: 0,
                requests: Vec::new(),
            };
            for (_, units) in rows {
                let request = snapshot_request(&units[0]);
                let count = units.len();
                match Box::pin(apply_received_batch(
                    engine,
                    Arc::new(units),
                    Arc::new(Vec::new()),
                    Some(manifest),
                    true,
                    scope,
                    check,
                ))
                .await
                {
                    Ok(()) => applied.snapshots += count,
                    Err(ApplyError::Engine(EngineError::SyncDependencyUnavailable)) => {
                        applied.requests.push(request);
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(applied)
        }
        Err(error) => Err(error),
    }
}

async fn apply_staged_change_batch<K, R>(
    engine: &Engine<K, R>,
    manifest: &SyncManifest,
    changes: Vec<SyncIncrementalChange>,
    scope: Option<&TableScope<'_>>,
    check: &mut Check<'_>,
) -> Result<StagedDataApply, ApplyError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    let count = changes.len();
    let changes = Arc::new(changes);
    match Box::pin(apply_received_batch(
        engine,
        Arc::new(Vec::new()),
        Arc::clone(&changes),
        Some(manifest),
        true,
        scope,
        check,
    ))
    .await
    {
        Ok(()) => Ok(StagedDataApply {
            snapshots: 0,
            changes: count,
            requests: Vec::new(),
        }),
        Err(ApplyError::Engine(EngineError::SyncDependencyUnavailable)) => {
            let changes = Arc::try_unwrap(changes).unwrap_or_else(|shared| shared.as_ref().clone());
            let mut rows = BTreeMap::<(String, Uuid), Vec<SyncIncrementalChange>>::new();
            for change in changes {
                rows.entry((change.table.clone(), change.row))
                    .or_default()
                    .push(change);
            }
            let mut applied = StagedDataApply {
                snapshots: 0,
                changes: 0,
                requests: Vec::new(),
            };
            for ((table, row), changes) in rows {
                let request = SyncSnapshotRequest { table, row };
                let count = changes.len();
                match Box::pin(apply_received_batch(
                    engine,
                    Arc::new(Vec::new()),
                    Arc::new(changes),
                    Some(manifest),
                    true,
                    scope,
                    check,
                ))
                .await
                {
                    Ok(()) => applied.changes += count,
                    Err(ApplyError::Engine(EngineError::SyncDependencyUnavailable)) => {
                        applied.requests.push(request);
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(applied)
        }
        Err(error) => Err(error),
    }
}

fn snapshot_request(unit: &SyncStateUnit) -> SyncSnapshotRequest {
    match &unit.key {
        SyncKey::Row { table, row } => SyncSnapshotRequest {
            table: table.clone(),
            row: *row,
        },
    }
}

async fn receive_outbound<K, R, T, F>(
    engine: &Engine<K, R>,
    transport: &mut T,
    manifest: &SyncManifest,
    max_session_bytes: usize,
    stage_factory: &F,
    check: &mut Check<'_>,
    mut scope: Option<&mut TableScope<'_>>,
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
    let mut current_change_key: Option<(String, Uuid)> = None;
    let mut previous_change_key: Option<(String, Uuid)> = None;
    let mut current_change_ids = BTreeSet::new();
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
        if scope
            .as_deref()
            .is_some_and(|scope| !scope.direction.receives_history())
            && matches!(&message, SyncMessage::State(_) | SyncMessage::Changes(_))
        {
            with_engine_abort(
                transport,
                Err::<(), _>(EngineError::InvalidQuery(
                    "Reverse history apply is forbidden",
                )),
            )
            .await?;
        }
        match message {
            SyncMessage::State(batch) => {
                for unit in batch {
                    if let Some(scope) = scope.as_deref_mut() {
                        with_engine_abort(transport, scope.validate(engine, &unit).await).await?;
                    }
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
                    if let Some(scope) = scope.as_deref_mut() {
                        let unit = SyncStateUnit::new(
                            SyncKey::Row {
                                table: change.table.clone(),
                                row: change.row,
                            },
                            change.payload.clone(),
                            Vec::new(),
                        );
                        with_engine_abort(transport, scope.validate(engine, &unit).await).await?;
                    }
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
                        let key = (change.table.clone(), change.row);
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
                            current_change_ids.clear();
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
                        if !current_change_ids.insert(change.id.clone()) {
                            return Err(SyncError::Protocol(String::from(
                                "duplicate row change ID",
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
                if (catalog_snapshots > 0 || catalog_changes > 0)
                    && let Err(error) =
                        sync_stage(|| apply_catalog_stages(engine, catalog_stages, manifest, check))
                            .await
                {
                    return abort(transport, error, String::from("catalog batch")).await;
                }
                let staged_snapshots = match apply_staged_data(
                    engine,
                    &mut data_snapshot_stage,
                    manifest,
                    true,
                    scope.as_deref(),
                    check,
                )
                .await
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
                    scope.as_deref(),
                    check,
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
            SyncMessage::Selection { .. }
            | SyncMessage::Hello(_)
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
    error: ApplyError,
    context: String,
) -> Result<Received, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let reason = format!("{} ({})", error, context);
    let _ = send_message(transport, &SyncMessage::Abort(reason)).now_or_never();
    Err(error.into())
}

async fn exchange_hello<T>(transport: &mut T, role: SyncRole) -> Result<(), SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let hello = SyncMessage::Hello(SyncHello {
        protocol_version: PROTOCOL_VERSION,
        manifest: SyncManifest::default(),
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

async fn exchange_manifest_value<T>(
    transport: &mut T,
    role: SyncRole,
    manifest: SyncManifest,
) -> Result<SyncManifest, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
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
        SyncMessage::Manifest(manifest) => {
            validate_manifest_row_addresses(&manifest)?;
            Ok(manifest)
        }
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
            let _ = send_message(transport, &SyncMessage::Abort(error.to_string())).now_or_never();
            Err(error.into())
        }
    }
}

fn incremental_change_estimate(change: &SyncIncrementalChange) -> usize {
    let identity_bytes = change.row.as_bytes().len();
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
        .saturating_add(row.as_bytes().len())
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
                        .saturating_add(row.as_bytes().len())
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
                        .saturating_add(change.row.as_bytes().len())
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
                        .saturating_add(request.row.as_bytes().len()),
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
        let _ = send_message(transport, &SyncMessage::Abort(error.to_string())).now_or_never();
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
    use alloc::{collections::BTreeSet, string::String, vec, vec::Vec};

    use engine::{
        ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_TABLES_STORAGE, Uuid,
    };
    use futures::executor::block_on;

    use super::{
        MAX_MESSAGE_BYTES, SyncError, account_message, catalog_order, consume_recovery_snapshot,
        receive_message, retained_message_bytes, send_message, validate_hello,
        validate_recovery_complete, validate_state_batch,
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
        let requests = [
            SyncSnapshotRequest {
                table: String::from("people"),
                row: Uuid::from_bytes([1; 16]),
            },
            SyncSnapshotRequest {
                table: String::from("people"),
                row: Uuid::from_bytes([2; 16]),
            },
        ];
        let snapshot = |row| {
            SyncStateUnit::new(
                SyncKey::Row {
                    table: String::from("people"),
                    row: Uuid::from_bytes([row; 16]),
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
            row: Uuid::from_bytes([1; 16]),
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
                row: Uuid::from_bytes([1; 16]),
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
