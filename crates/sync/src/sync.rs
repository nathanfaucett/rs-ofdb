use crate::protocol::{Config, Error, Message, SyncResult};
use crate::transport::SyncTransport;
use btree::{ChangeEvent, StorageKey};
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

/// Error during synchronization.
#[derive(Debug, Error)]
pub enum SynchronizeError<E> {
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

impl<E> From<Error<E>> for SynchronizeError<E> {
    fn from(e: Error<E>) -> Self {
        SynchronizeError::Protocol(e)
    }
}

/// Configuration for synchronize.
#[derive(Debug, Clone)]
pub struct SynchronizeConfig {
    pub role: crate::SyncRole,
    /// Protocol configuration.
    pub protocol: Config,
    /// Maximum time to wait for handshake.
    pub handshake_timeout: Duration,
    /// Maximum time between messages before timeout.
    pub message_timeout: Duration,
    /// Whether to send DONE after exhausting outbound stream.
    pub send_done_on_exhaust: bool,
}

impl Default for SynchronizeConfig {
    fn default() -> Self {
        Self {
            role: crate::SyncRole::Initiator,
            protocol: Config::default(),
            handshake_timeout: Duration::from_secs(10),
            message_timeout: Duration::from_secs(30),
            send_done_on_exhaust: true,
        }
    }
}

/// Run synchronization between two peers.
///
/// This function implements the sync protocol:
/// 1. Handshake (Hello/HelloAck)
/// 2. Bidirectional streaming of ChangeEvent
/// 3. Snapshot fallback for missing dependencies
/// 4. Acknowledgment of received events
/// 5. Graceful close (Done/Close)
///
/// # Type Parameters
/// - `T`: Transport implementing `SyncTransport`
/// - `F`: Callback to apply inbound ChangeEvent to local storage
///
/// # Arguments
/// - `transport`: Bidirectional transport
/// - `outbound`: Stream of local ChangeEvent to push to peer
/// - `apply_inbound`: Function to apply received ChangeEvent to local storage
/// - `config`: Synchronization configuration
///
/// # Returns
/// `SyncResult` with statistics, or `SynchronizeError` on failure.
pub async fn synchronize<T, F, S, Fut>(
    mut transport: T,
    mut outbound: S,
    mut apply_inbound: F,
    config: SynchronizeConfig,
) -> Result<SyncResult, SynchronizeError<T::Error>>
where
    T: SyncTransport,
    S: Stream<Item = ChangeEvent> + Unpin + Send,
    F: FnMut(ChangeEvent) -> Fut + Send,
    Fut: Future<Output = Result<(), String>> + Send,
{
    let mut result = SyncResult::default();
    let mut local_generations: HashMap<StorageKey, u64> = HashMap::new();
    let mut remote_generations: HashMap<StorageKey, u64> = HashMap::new();
    let mut pending_acks: HashMap<StorageKey, u64> = HashMap::new();
    let mut outstanding = HashMap::new();
    let mut sent_done = false;
    let mut _received_done = false;

    let hello = Message::Hello(crate::protocol::HelloConfig {
        protocol_version: 1,
        manifest_digest: [0; 32],
        config: config.protocol.clone(),
    });
    let handshake = async {
        match config.role {
            crate::SyncRole::Initiator => {
                transport
                    .send(hello)
                    .await
                    .map_err(SynchronizeError::Transport)?;
                match transport
                    .receive()
                    .await
                    .map_err(SynchronizeError::Transport)?
                {
                    Message::HelloAck {
                        protocol_version: 1,
                        accepted: true,
                    } => Ok(()),
                    _ => Err(SynchronizeError::HandshakeFailed(
                        "expected accepted version-one HelloAck".into(),
                    )),
                }
            }
            crate::SyncRole::Responder => {
                let accepted = matches!(
                    transport
                        .receive()
                        .await
                        .map_err(SynchronizeError::Transport)?,
                    Message::Hello(crate::protocol::HelloConfig {
                        protocol_version: 1,
                        ..
                    })
                );
                transport
                    .send(Message::HelloAck {
                        protocol_version: 1,
                        accepted,
                    })
                    .await
                    .map_err(SynchronizeError::Transport)?;
                if accepted {
                    Ok(())
                } else {
                    Err(SynchronizeError::HandshakeFailed(
                        "unsupported Hello".into(),
                    ))
                }
            }
        }
    };
    tokio::time::timeout(config.handshake_timeout, handshake)
        .await
        .map_err(|_| SynchronizeError::SessionTimeout)??;

    // Main bidirectional loop
    let mut outbound_exhausted = false;
    let inbound_closed = false;

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
                        outstanding.insert(event.key.clone(), generation);

                        let msg = Message::ChangeEvent(event);
                        transport.send(msg).await
                            .map_err(SynchronizeError::Transport)?;
                        result.events_sent += 1;
                        result.bytes_sent += 1; // approximate
                    }
                    None => {
                        outbound_exhausted = true;
                        if config.send_done_on_exhaust && !sent_done {
                            let done = Message::Done {
                                final_generations: local_generations.clone(),
                            };
                            transport.send(done).await
                                .map_err(SynchronizeError::Transport)?;
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
                            Message::ChangeEvent(event) => {
                                // Track remote generation
                                let generation = event.generation;
                                let key = event.key.clone();
                                remote_generations.insert(key.clone(), generation);

                                // Apply to local storage
                                apply_inbound(event).await
                                    .map_err(SynchronizeError::Apply)?;
                                result.events_received += 1;

                                // Queue acknowledgment
                                pending_acks.insert(key, generation);

                                // Send ACK if immediate
                                if config.protocol.immediate_ack {
                                    let ack = Message::Ack {
                                        acknowledgments: std::mem::take(&mut pending_acks),
                                    };
                                    transport.send(ack).await
                                        .map_err(SynchronizeError::Transport)?;
                                }
                            }
                            Message::SnapshotRequest { key: _, min_generation: _ } => {
                                // TODO: Implement snapshot serving
                                // For now, send Close to indicate we can't serve
                                let close = Message::Close;
                                transport.send(close).await
                                    .map_err(SynchronizeError::Transport)?;
                            }
                            Message::Snapshot { key, blob, generation } => {
                                // Apply snapshot
                                let event = ChangeEvent { key, blob, generation };
                                apply_inbound(event).await
                                    .map_err(SynchronizeError::Apply)?;
                                result.snapshots_received += 1;
                            }
                            Message::Ack { acknowledgments } => {
                                // Peer acknowledged our events
                                for (key, generation) in acknowledgments {
                                    if outstanding.get(&key).is_some_and(|pending| *pending <= generation) { outstanding.remove(&key); }
                                }
                            }
                            Message::Done { final_generations } => {
                                _received_done = true;
                                // Update remote generations with final values
                                remote_generations.extend(final_generations);
                            }
                            Message::Close => { break; }
                            Message::HelloAck { .. } => {
                                // Ignore duplicate hello ack
                            }
                            Message::Hello { .. } => {
                                // Ignore duplicate hello
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        return Err(SynchronizeError::Protocol(Error::Transport(e)));
                    }
                    Err(_) => {
                        return Err(SynchronizeError::SessionTimeout);
                    }
                }
            }
            // Check termination conditions
            _ = async {}, if outbound_exhausted && outstanding.is_empty() && (_received_done || inbound_closed) => {
                break;
            }
        }

        // Send batched ACKs if not immediate
        if !config.protocol.immediate_ack && !pending_acks.is_empty() {
            let ack = Message::Ack {
                acknowledgments: std::mem::take(&mut pending_acks),
            };
            transport
                .send(ack)
                .await
                .map_err(SynchronizeError::Transport)?;
        }
    }

    // Send final DONE if not already sent
    if !sent_done {
        let done = Message::Done {
            final_generations: local_generations,
        };
        transport
            .send(done)
            .await
            .map_err(SynchronizeError::Transport)?;
    }

    // Send final CLOSE
    let close = Message::Close;
    let _ = transport.send(close).await;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{channel::mpsc, stream};
    use std::sync::{Arc, Mutex};

    struct Channel(
        mpsc::UnboundedSender<Message>,
        mpsc::UnboundedReceiver<Message>,
    );
    impl SyncTransport for Channel {
        type Error = String;
        async fn send(&mut self, message: Message) -> Result<(), String> {
            self.0
                .unbounded_send(message)
                .map_err(|error| error.to_string())
        }
        async fn receive(&mut self) -> Result<Message, String> {
            self.1.next().await.ok_or_else(|| "closed".into())
        }
    }
    #[tokio::test]
    async fn initiator_and_responder_exchange_changes_and_finish() {
        let (a, ar) = mpsc::unbounded();
        let (b, br) = mpsc::unbounded();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let left_seen = seen.clone();
        let right_seen = seen.clone();
        let event = |key: &[u8], byte| ChangeEvent {
            key: StorageKey::kv(key.to_vec()),
            blob: vec![byte],
            generation: 1,
        };
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                synchronize(
                    Channel(a, br),
                    stream::iter([event(b"left", 7)]),
                    move |change| {
                        left_seen.lock().expect("observations").push(change.blob);
                        async { Ok(()) }
                    },
                    SynchronizeConfig {
                        role: crate::SyncRole::Initiator,
                        ..Default::default()
                    }
                ),
                synchronize(
                    Channel(b, ar),
                    stream::iter([event(b"right", 9)]),
                    move |change| {
                        right_seen.lock().expect("observations").push(change.blob);
                        async { Ok(()) }
                    },
                    SynchronizeConfig {
                        role: crate::SyncRole::Responder,
                        ..Default::default()
                    }
                ),
            )
        })
        .await
        .expect("finite sessions must complete");
        assert_eq!(result.0.expect("initiator exchange").events_received, 1);
        assert_eq!(result.1.expect("responder exchange").events_received, 1);
        let mut seen = seen.lock().expect("observations").clone();
        seen.sort();
        assert_eq!(seen, vec![vec![7], vec![9]]);
    }
}
