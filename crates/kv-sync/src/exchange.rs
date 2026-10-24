use core::{
    future::Future,
    pin::pin,
    task::{Context, Waker},
};
use std::collections::{BTreeMap, BTreeSet};

use btree::{BTree, BTreeResult};
use kv::{ChangeApply, KvInvalidation, KvStore, NativeChange, decode_document_id};

use crate::{
    Config, Error, KvChange, KvChanges, KvInventory, KvSnapshot, Message, SyncRole, SyncTransport,
    decode_frame, encode_frame, protocol::VERSION,
};

pub async fn synchronize<B, T>(
    store: &KvStore<B>,
    transport: &mut T,
    role: SyncRole,
    config: Config,
) -> Result<(), Error<T::Error>>
where
    B: BTree<Vec<u8>, Vec<u8>>,
    T: SyncTransport,
{
    synchronize_checked(store, transport, role, config, || {
        core::future::ready(Ok(()))
    })
    .await
}

/// Broad discovery, native incremental transfer, and merge-preserving snapshot recovery.
/// Checks run before complete atomic incoming batches. Discard transport after failure.
pub async fn synchronize_checked<B, T, Q, U>(
    store: &KvStore<B>,
    transport: &mut T,
    role: SyncRole,
    config: Config,
    check: Q,
) -> Result<(), Error<T::Error>>
where
    B: BTree<Vec<u8>, Vec<u8>>,
    T: SyncTransport,
    Q: FnMut() -> U,
    U: Future<Output = Result<(), String>>,
{
    synchronize_selected_checked(
        store,
        &[KvInvalidation::All],
        transport,
        role,
        config,
        check,
    )
    .await
}

/// Select work before opening read snapshots. Both peers compare the union of their
/// keys; either peer's All forces broad discovery. Acknowledge selected work only
/// after success. New peers, reconnects and unknown/gapped work must select All.
/// Local subscriptions cannot observe remote-only writes: callers must arrange a
/// remote wakeup/receive loop or explicitly reconcile, even when local work is empty.
pub async fn synchronize_selected_checked<B, T, Q, U>(
    store: &KvStore<B>,
    invalidations: &[KvInvalidation],
    transport: &mut T,
    role: SyncRole,
    config: Config,
    mut check: Q,
) -> Result<(), Error<T::Error>>
where
    B: BTree<Vec<u8>, Vec<u8>>,
    T: SyncTransport,
    Q: FnMut() -> U,
    U: Future<Output = Result<(), String>>,
{
    let local = if invalidations.contains(&KvInvalidation::All) {
        None
    } else {
        Some(
            invalidations
                .iter()
                .filter_map(|item| match item {
                    KvInvalidation::Generation { key, .. } => Some(key.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>(),
        )
    };
    let remote;
    match role {
        SyncRole::Initiator => {
            send(transport, Message::Hello { version: VERSION }, config).await?;
            hello(receive(transport, config).await?)?;
            send(transport, Message::Selection(local.clone()), config).await?;
            remote = selection(receive(transport, config).await?)?;
        }
        SyncRole::Responder => {
            hello(receive(transport, config).await?)?;
            send(transport, Message::Hello { version: VERSION }, config).await?;
            remote = selection(receive(transport, config).await?)?;
            send(transport, Message::Selection(local.clone()), config).await?;
        }
    }
    let keys = match (local, remote) {
        (Some(a), Some(b)) => Some(
            a.into_iter()
                .chain(b)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>(),
        ),
        _ => None,
    };
    match role {
        SyncRole::Initiator => {
            send_direction(store, transport, keys.as_deref(), config).await?;
            receive_direction(store, transport, config, &mut check).await?;
            send(transport, Message::Finished, config).await?;
            if receive(transport, config).await? != Message::Finished {
                return Err(Error::UnexpectedMessage);
            }
        }
        SyncRole::Responder => {
            receive_direction(store, transport, config, &mut check).await?;
            send_direction(store, transport, keys.as_deref(), config).await?;
            if receive(transport, config).await? != Message::Finished {
                return Err(Error::UnexpectedMessage);
            }
            send(transport, Message::Finished, config).await?;
        }
    }
    Ok(())
}

fn selection<E>(message: Message) -> Result<Option<Vec<String>>, Error<E>> {
    if let Message::Selection(keys) = message {
        Ok(keys)
    } else {
        Err(Error::UnexpectedMessage)
    }
}
fn hello<E>(message: Message) -> Result<(), Error<E>> {
    match message {
        Message::Hello { version: VERSION } => Ok(()),
        Message::Hello { version } => Err(Error::UnsupportedVersion(version)),
        _ => Err(Error::UnexpectedMessage),
    }
}

async fn inventory<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    keys: Option<&[String]>,
) -> BTreeResult<Vec<KvInventory>> {
    let tx = store.transaction().await?;
    let histories = if let Some(keys) = keys {
        let mut histories = Vec::new();
        for key in keys {
            if let Some(history) = tx.history(key).await? {
                histories.push(history);
            }
        }
        histories
    } else {
        tx.histories().await?
    };
    tx.rollback().await?;
    Ok(histories
        .into_iter()
        .map(|history| KvInventory {
            id: history.id,
            hashes: history
                .changes
                .into_iter()
                .map(|change| change.hash)
                .collect(),
        })
        .collect())
}
async fn send_inventory<T: SyncTransport>(
    transport: &mut T,
    items: &[KvInventory],
    config: Config,
) -> Result<(), Error<T::Error>> {
    for batch in items.chunks(config.max_snapshots_per_batch.max(1)) {
        send(transport, Message::Inventory(batch.to_vec()), config).await?;
    }
    send(transport, Message::End, config).await
}
async fn receive_inventory<T: SyncTransport>(
    transport: &mut T,
    config: Config,
) -> Result<Vec<KvInventory>, Error<T::Error>> {
    let mut items = Vec::new();
    let mut ids = BTreeSet::new();
    let mut bytes = 0usize;
    loop {
        let frame = transport.receive().await.map_err(Error::Transport)?;
        match decode_frame(&frame, config)? {
            Message::Inventory(batch) => {
                bytes = bytes.saturating_add(frame.len());
                if bytes > config.max_inventory_bytes
                    || items.len().saturating_add(batch.len()) > config.max_inventory_entries
                {
                    return Err(Error::FrameTooLarge);
                }
                for item in &batch {
                    if !ids.insert(item.id.clone()) {
                        return Err(Error::InvalidFrame);
                    }
                }
                items.extend(batch);
            }
            Message::End => return Ok(items),
            _ => return Err(Error::UnexpectedMessage),
        }
    }
}

async fn send_direction<B: BTree<Vec<u8>, Vec<u8>>, T: SyncTransport>(
    store: &KvStore<B>,
    transport: &mut T,
    keys: Option<&[String]>,
    config: Config,
) -> Result<(), Error<T::Error>> {
    let local = inventory(store, keys).await.map_err(storage_error)?;
    send_inventory(transport, &local, config).await?;
    let remote = receive_inventory(transport, config)
        .await?
        .into_iter()
        .map(|item| (item.id, item.hashes))
        .collect::<BTreeMap<_, _>>();
    // Each complete batch is prepared in one transaction and released before network I/O.
    for batch in local.chunks(config.max_snapshots_per_batch.max(1)) {
        let tx = store.transaction().await.map_err(storage_error)?;
        let mut snapshots = Vec::new();
        let mut deltas = Vec::new();
        let mut delta_keys = Vec::new();
        for item in batch {
            let (key, _) = decode_document_id(&item.id).map_err(storage_error)?;
            let Some(current) = tx.history(&key).await.map_err(storage_error)? else {
                continue;
            };
            if let Some(known) = remote.get(&current.id) {
                let changes = tx
                    .export_changes(&current.id, known)
                    .await
                    .map_err(storage_error)?;
                if !changes.is_empty() {
                    delta_keys.push(key);
                    deltas.push(KvChanges {
                        id: current.id,
                        changes: changes
                            .into_iter()
                            .map(|change| KvChange {
                                hash: change.hash,
                                deps: change.deps,
                                payload: change.payload,
                            })
                            .collect(),
                    });
                }
            } else if let Some((key, payload)) =
                tx.export_snapshot(&key).await.map_err(storage_error)?
            {
                snapshots.push(KvSnapshot { key, payload });
            }
        }
        tx.rollback().await.map_err(storage_error)?;
        if !snapshots.is_empty() {
            send(transport, Message::Snapshots(snapshots), config).await?;
            if receive(transport, config).await? != Message::Applied {
                return Err(Error::UnexpectedMessage);
            }
        }
        if !deltas.is_empty() {
            send(transport, Message::Changes(deltas), config).await?;
            match receive(transport, config).await? {
                Message::Applied => {}
                Message::Recover => {
                    let tx = store.transaction().await.map_err(storage_error)?;
                    let mut snapshots = Vec::new();
                    for key in delta_keys {
                        if let Some((key, payload)) =
                            tx.export_snapshot(&key).await.map_err(storage_error)?
                        {
                            snapshots.push(KvSnapshot { key, payload });
                        }
                    }
                    tx.rollback().await.map_err(storage_error)?;
                    send(transport, Message::Snapshots(snapshots), config).await?;
                    if receive(transport, config).await? != Message::Applied {
                        return Err(Error::UnexpectedMessage);
                    }
                }
                _ => return Err(Error::UnexpectedMessage),
            }
        }
    }
    send(transport, Message::End, config).await
}

async fn receive_direction<B, T, Q, U>(
    store: &KvStore<B>,
    transport: &mut T,
    config: Config,
    check: &mut Q,
) -> Result<(), Error<T::Error>>
where
    B: BTree<Vec<u8>, Vec<u8>>,
    T: SyncTransport,
    Q: FnMut() -> U,
    U: Future<Output = Result<(), String>>,
{
    let advertised = receive_inventory(transport, config).await?;
    let keys = advertised
        .iter()
        .map(|item| decode_document_id(&item.id).map(|(key, _)| key))
        .collect::<BTreeResult<Vec<_>>>()
        .map_err(storage_error)?;
    send_inventory(
        transport,
        &inventory(store, Some(&keys)).await.map_err(storage_error)?,
        config,
    )
    .await?;
    let mut recovery = Vec::new();
    loop {
        let message = receive(transport, config).await?;
        if message == Message::End && recovery.is_empty() {
            return Ok(());
        }
        if !matches!(message, Message::Snapshots(_) | Message::Changes(_))
            || (!recovery.is_empty() && !matches!(message, Message::Snapshots(_)))
        {
            return Err(Error::UnexpectedMessage);
        }
        if let Err(reason) = check().await {
            let mut notification = pin!(send(transport, Message::Abort, config));
            let _ = notification
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            return Err(Error::PreApply(reason));
        }
        let applied = apply_batch(store, &message, &recovery)
            .await
            .map_err(storage_error)?;
        if applied {
            recovery.clear();
        } else if let Message::Changes(items) = message {
            recovery = items;
        }
        send(
            transport,
            if applied {
                Message::Applied
            } else {
                Message::Recover
            },
            config,
        )
        .await?;
    }
}

async fn apply_batch<B: BTree<Vec<u8>, Vec<u8>>>(
    store: &KvStore<B>,
    message: &Message,
    recovery: &[KvChanges],
) -> BTreeResult<bool> {
    let mut tx = store.transaction().await?;
    let result: BTreeResult<bool> = async {
        match message {
            Message::Snapshots(items) => {
                for item in items {
                    tx.import_snapshot((item.key.clone(), item.payload.clone()))
                        .await?;
                }
            }
            Message::Changes(items) => {
                for item in items {
                    let changes = item
                        .changes
                        .iter()
                        .map(|change| NativeChange {
                            hash: change.hash,
                            deps: change.deps.clone(),
                            payload: change.payload.clone(),
                        })
                        .collect::<Vec<_>>();
                    if tx.import_changes(&item.id, &changes).await?
                        == ChangeApply::MissingDependencies
                    {
                        return Ok(false);
                    }
                }
            }
            _ => unreachable!("only complete data batches reach apply"),
        }
        for required in recovery {
            let (key, generation) = decode_document_id(&required.id)?;
            let current = tx
                .history(&key)
                .await?
                .ok_or(btree::BTreeError::InvalidDocument)?;
            let (_, current_generation) = decode_document_id(&current.id)?;
            if current_generation < generation
                || (current_generation == generation
                    && required.changes.iter().any(|change| {
                        !current
                            .changes
                            .iter()
                            .any(|known| known.hash == change.hash)
                    }))
            {
                return Err(btree::BTreeError::InvalidDocument);
            }
        }
        Ok(true)
    }
    .await;
    match result {
        Ok(true) => {
            tx.commit().await?;
            Ok(true)
        }
        Ok(false) => {
            tx.rollback().await?;
            Ok(false)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}
async fn send<T: SyncTransport>(
    transport: &mut T,
    message: Message,
    config: Config,
) -> Result<(), Error<T::Error>> {
    let frame = encode_frame(&message, config).map_err(|error| match error {
        Error::FrameTooLarge => Error::FrameTooLarge,
        _ => Error::InvalidFrame,
    })?;
    transport.send(frame).await.map_err(Error::Transport)
}
async fn receive<T: SyncTransport>(
    transport: &mut T,
    config: Config,
) -> Result<Message, Error<T::Error>> {
    decode_frame(
        &transport.receive().await.map_err(Error::Transport)?,
        config,
    )
}
fn storage_error<E: core::fmt::Debug, T>(error: E) -> Error<T> {
    Error::Storage(format!("{error:?}"))
}
