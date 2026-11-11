use crate::protocol::{Error, PushConfig, PushMessage, PushResult};
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

/// Run push-based synchronization between two peers.
///
/// This function implements the push sync protocol:
/// 1. Handshake (Hello/HelloAck)
/// 2. Bidirectional streaming of ChangeEvent
/// 3. Snapshot fallback for missing dependencies
/// 4. Acknowledgment of received events
/// 5. Graceful close (Done/Close)
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
pub async fn push_synchronize<T, F, S, Fut>(
    mut transport: T,
    mut outbound: S,
    mut apply_inbound: F,
    config: PushSynchronizeConfig,
) -> Result<PushResult, PushSynchronizeError<T::Error>>
where
    T: PushSyncTransport,
    S: Stream<Item = ChangeEvent> + Unpin + Send,
    F: FnMut(ChangeEvent) -> Fut + Send,
    Fut: Future<Output = Result<(), String>> + Send,
{
    let mut result = PushResult::default();
    let mut local_generations: HashMap<StorageKey, u64> = HashMap::new();
    let mut remote_generations: HashMap<StorageKey, u64> = HashMap::new();
    let mut pending_acks: HashMap<StorageKey, u64> = HashMap::new();
    let mut sent_done = false;
    let mut _received_done = false;

    // Handshake
    let manifest_digest = [0u8; 32]; // TODO: compute actual manifest digest
    let hello = PushMessage::Hello(crate::protocol::HelloConfig {
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

    let accepted = match hello_ack {
        PushMessage::HelloAck { accepted, .. } => accepted,
        msg => {
            return Err(PushSynchronizeError::HandshakeFailed(format!(
                "expected HelloAck, got {:?}",
                msg
            )));
        }
    };

    if !accepted {
        return Err(PushSynchronizeError::HandshakeFailed(
            "peer rejected handshake".into(),
        ));
    }

    // Main bidirectional loop
    let mut outbound_exhausted = false;
    let mut inbound_closed = false;

    loop {
        tokio::select! {
            biased;
            // Outbound: send local ChangeEvents
            event = async {
                if outbound_exhausted {
                    futures::future::pending().await
                } else {
                    outbound.next().await
                }
            }, if !outbound_exhausted => {
                match event {
                    Some(event) => {
                        // Track local generation
                        let generation = event.generation;
                        local_generations.insert(event.key.clone(), generation);

                        let msg = PushMessage::ChangeEvent(event);
                        transport.send(msg).await
                            .map_err(PushSynchronizeError::Transport)?;
                        result.events_sent += 1;
                        result.bytes_sent += 1; // approximate
                    }
                    None => {
                        outbound_exhausted = true;
                        if config.send_done_on_exhaust && !sent_done {
                            let done = PushMessage::Done {
                                final_generations: local_generations.clone(),
                            };
                            transport.send(done).await
                                .map_err(PushSynchronizeError::Transport)?;
                            sent_done = true;
                        }
                    }
                }
            }
            // Inbound: receive and process messages
            msg = async {
                if inbound_closed {
                    futures::future::pending().await
                } else {
                    tokio::time::timeout(config.message_timeout, transport.receive()).await
                }
            }, if !inbound_closed => {
                match msg {
                    Ok(Ok(message)) => {
                        result.bytes_received += 1; // approximate
                        match message {
                            PushMessage::ChangeEvent(event) => {
                                // Track remote generation
                                let generation = event.generation;
                                let key = event.key.clone();
                                remote_generations.insert(key.clone(), generation);

                                // Apply to local storage
                                apply_inbound(event).await
                                    .map_err(PushSynchronizeError::Apply)?;
                                result.events_received += 1;

                                // Queue acknowledgment
                                pending_acks.insert(key, generation);

                                // Send ACK if immediate
                                if config.protocol.immediate_ack {
                                    let ack = PushMessage::Ack {
                                        acknowledgments: std::mem::take(&mut pending_acks),
                                    };
                                    transport.send(ack).await
                                        .map_err(PushSynchronizeError::Transport)?;
                                }
                            }
                            PushMessage::SnapshotRequest { key: _, min_generation: _ } => {
                                // TODO: Implement snapshot serving
                                // For now, send Close to indicate we can't serve
                                let close = PushMessage::Close;
                                transport.send(close).await
                                    .map_err(PushSynchronizeError::Transport)?;
                            }
                            PushMessage::Snapshot { key, blob, generation } => {
                                // Apply snapshot
                                let event = ChangeEvent { key, blob, generation };
                                apply_inbound(event).await
                                    .map_err(PushSynchronizeError::Apply)?;
                                result.snapshots_received += 1;
                            }
                            PushMessage::Ack { acknowledgments } => {
                                // Peer acknowledged our events
                                for (key, _generation) in acknowledgments {
                                    pending_acks.remove(&key);
                                }
                            }
                            PushMessage::Done { final_generations } => {
                                _received_done = true;
                                // Update remote generations with final values
                                remote_generations.extend(final_generations);
                            }
                            PushMessage::Close => {
                                inbound_closed = true;
                            }
                            PushMessage::HelloAck { .. } => {
                                // Ignore duplicate hello ack
                            }
                            PushMessage::Hello { .. } => {
                                // Ignore duplicate hello
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        return Err(PushSynchronizeError::Protocol(Error::Transport(e)));
                    }
                    Err(_) => {
                        return Err(PushSynchronizeError::SessionTimeout);
                    }
                }
            }
            // Check termination conditions
            _ = async {}, if outbound_exhausted && inbound_closed => {
                break;
            }
        }

        // Send batched ACKs if not immediate
        if !config.protocol.immediate_ack && !pending_acks.is_empty() {
            let ack = PushMessage::Ack {
                acknowledgments: std::mem::take(&mut pending_acks),
            };
            transport
                .send(ack)
                .await
                .map_err(PushSynchronizeError::Transport)?;
        }
    }

    // Send final DONE if not already sent
    if !sent_done {
        let done = PushMessage::Done {
            final_generations: local_generations,
        };
        transport
            .send(done)
            .await
            .map_err(PushSynchronizeError::Transport)?;
    }

    // Send final CLOSE
    let close = PushMessage::Close;
    let _ = transport.send(close).await;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PushConfig;
    use crate::transport::mock::MockTransport;
    use btree::{ChangeEvent, StorageKey};
    use futures::stream;

    #[tokio::test]
    async fn push_synchronize_compiles() {
        let config = PushConfig::default();
        let transport = MockTransport::new(config.clone());
        let _peer = transport.peer();

        // Create a simple outbound stream with one event
        let event = ChangeEvent {
            key: StorageKey::kv(b"key1".to_vec()),
            blob: vec![1, 2, 3],
            generation: 1,
        };
        let outbound = stream::iter(vec![event]);

        let sync_config = PushSynchronizeConfig {
            protocol: config.clone(),
            handshake_timeout: std::time::Duration::from_secs(1),
            message_timeout: std::time::Duration::from_secs(1),
            send_done_on_exhaust: true,
        };

        // Just testing the function compiles - mock doesn't fully implement protocol
        let _ =
            push_synchronize(transport, outbound, |_e| async move { Ok(()) }, sync_config).await;
    }
}
