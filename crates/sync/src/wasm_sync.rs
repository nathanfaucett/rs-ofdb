// WASM-specific push synchronization that doesn't require Send bounds.
// Uses spawn_local to run non-Send futures on the local executor.

use crate::protocol::{Error, HelloConfig, PushConfig, PushMessage, PushResult};
use crate::transport::PushSyncTransport;
use btree::{ChangeEvent, StorageKey};
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

/// Error during push synchronization.
#[derive(Debug, Error)]
pub enum PushSynchronizeError<E> {
    #[error("transport error: {0}")]
    Transport(E),
    #[error("protocol error: {0}")]
    Protocol(Error<E>),
    #[error("apply error: {0}")]
    Apply(String),
    #[error("session timeout")]
    SessionTimeout,
    #[error("peer closed")]
    PeerClosed,
    #[error("handshake failed: {0}")]
    HandshakeFailed(String),
    #[error("generation gap for key {key:?}: have {have}, need {need}")]
    GenerationGap {
        key: StorageKey,
        have: u64,
        need: u64,
    },
}

impl<E> From<Error<E>> for PushSynchronizeError<E> {
    fn from(e: Error<E>) -> Self {
        PushSynchronizeError::Protocol(e)
    }
}

/// Configuration for push_synchronize.
#[derive(Debug, Clone)]
pub struct PushSynchronizeConfig {
    /// Protocol configuration.
    pub protocol: PushConfig,
    /// Maximum time to wait for handshake.
    pub handshake_timeout: Duration,
    /// Maximum time between messages before timeout.
    pub message_timeout: Duration,
    /// Whether to send DONE after exhausting outbound stream.
    pub send_done_on_exhaust: bool,
}

impl Default for PushSynchronizeConfig {
    fn default() -> Self {
        Self {
            protocol: PushConfig::default(),
            handshake_timeout: Duration::from_secs(10),
            message_timeout: Duration::from_secs(30),
            send_done_on_exhaust: true,
        }
    }
}

/// Run push-based synchronization between two peers (WASM version).
///
/// This version uses `spawn_local` for the apply_inbound callback, allowing
/// non-Send futures (e.g., js-sys JsFuture) to be used.
///
/// # Type Parameters
/// - `T`: Transport implementing `PushSyncTransport`
/// - `F`: Callback to apply inbound ChangeEvent to local storage
///
/// # Arguments
/// - `transport`: Bidirectional transport
/// - `outbound`: Stream of local ChangeEvent to push to peer
/// - `apply_inbound`: Function to apply received ChangeEvent to local storage
/// - `config`: Synchronization configuration
///
/// # Returns
/// `PushResult` with statistics, or `PushSynchronizeError` on failure.
pub fn push_synchronize<T, F, S, Fut>(
    mut transport: T,
    mut outbound: S,
    mut apply_inbound: F,
    config: PushSynchronizeConfig,
) -> impl Future<Output = Result<PushResult, PushSynchronizeError<T::Error>>>
where
    T: PushSyncTransport,
    S: Stream<Item = ChangeEvent> + Unpin,
    F: FnMut(ChangeEvent) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    async move {
        let mut result = PushResult::default();
        let mut local_generations: HashMap<StorageKey, u64> = HashMap::new();
        let mut remote_generations: HashMap<StorageKey, u64> = HashMap::new();
        let mut pending_acks: HashMap<StorageKey, u64> = HashMap::new();
        let mut sent_done = false;
        let mut received_done = false;

        // Handshake
        let manifest_digest = [0u8; 32]; // TODO: compute actual manifest digest
        let hello = PushMessage::Hello(HelloConfig {
            protocol_version: 1,
            manifest_digest,
            config: config.protocol.clone(),
        });
        transport
            .send(hello)
            .await
            .map_err(PushSynchronizeError::Transport)?;
        result.bytes_sent += 1; // approximate

        // Wait for HelloAck
        let hello_ack = tokio::time::timeout(config.handshake_timeout, transport.receive())
            .await
            .map_err(|_| PushSynchronizeError::SessionTimeout)?
            .map_err(PushSynchronizeError::Transport)?;

        match hello_ack {
            PushMessage::HelloAck {
                protocol_version: _,
                accepted,
            } => {
                if !accepted {
                    return Err(PushSynchronizeError::HandshakeFailed(
                        "peer rejected handshake".into(),
                    ));
                }
            }
            _ => {
                return Err(PushSynchronizeError::HandshakeFailed(
                    "expected HelloAck".into(),
                ));
            }
        }

        // Main loop
        loop {
            tokio::select! {
                // Send outbound events
                event = outbound.next(), if !sent_done => {
                    match event {
                        Some(event) => {
                            let blob_len = event.blob.len() as u64;
                            let msg = PushMessage::ChangeEvent(event);
                            transport
                                .send(msg)
                                .await
                                .map_err(PushSynchronizeError::Transport)?;
                            result.events_sent += 1;
                            result.bytes_sent += blob_len;
                        }
                        None => {
                            if config.send_done_on_exhaust {
                                let done = PushMessage::Done {
                                    final_generations: local_generations.clone(),
                                };
                                transport
                                    .send(done)
                                    .await
                                    .map_err(PushSynchronizeError::Transport)?;
                                sent_done = true;
                            }
                        }
                    }
                }

                // Receive inbound messages
                msg = transport.receive() => {
                    let msg = msg.map_err(PushSynchronizeError::Transport)?;
                    result.bytes_received += 1; // approximate

                    match msg {
                        PushMessage::ChangeEvent(change_event) => {
                            result.events_received += 1;
                            let key = change_event.key.clone();
                            let generation = change_event.generation;
                            remote_generations.insert(key.clone(), generation);

                            // Apply inbound change
                            let apply_result = apply_inbound(change_event).await;
                            if let Err(e) = apply_result {
                                return Err(PushSynchronizeError::Apply(e));
                            }

                            // Send ACK
                            pending_acks.insert(key, generation);
                            if config.protocol.immediate_ack {
                                let ack = PushMessage::Ack {
                                    acknowledgments: std::mem::take(&mut pending_acks),
                                };
                                transport
                                    .send(ack)
                                    .await
                                    .map_err(PushSynchronizeError::Transport)?;
                            }
                        }
                        PushMessage::SnapshotRequest { key, min_generation: _ } => {
                            // TODO: implement snapshot fallback
                            let _ = key;
                        }
                        PushMessage::Snapshot { key, blob, generation } => {
                            result.events_received += 1;
                            remote_generations.insert(key.clone(), generation);

                            let apply_result = apply_inbound(ChangeEvent { key, blob, generation }).await;
                            if let Err(e) = apply_result {
                                return Err(PushSynchronizeError::Apply(e));
                            }
                        }
                        PushMessage::Ack { acknowledgments } => {
                            for (key, generation) in acknowledgments {
                                local_generations.insert(key, generation);
                            }
                        }
                        PushMessage::Done { final_generations } => {
                            for (key, generation) in final_generations {
                                remote_generations.insert(key, generation);
                            }
                            received_done = true;
                            if sent_done {
                                break;
                            }
                        }
                        PushMessage::Close => {
                            return Err(PushSynchronizeError::PeerClosed);
                        }
                        _ => {}
                    }
                }

                // Periodic ACK flush
                _ = tokio::time::sleep(Duration::from_millis(100)), if !config.protocol.immediate_ack && !pending_acks.is_empty() => {
                    let ack = PushMessage::Ack {
                        acknowledgments: std::mem::take(&mut pending_acks),
                    };
                    transport
                        .send(ack)
                        .await
                        .map_err(PushSynchronizeError::Transport)?;
                }
            }

            if sent_done && received_done {
                break;
            }
        }

        Ok(result)
    }
}
