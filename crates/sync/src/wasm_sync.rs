// WASM-specific push synchronization using web-sys timers and futures::select!
// Does not require tokio runtime.

use crate::protocol::{Config, Error, HelloConfig, Message, SyncResult};
use crate::transport::SyncTransport;
use btree::{ChangeEvent, StorageKey};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::select;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use web_sys::window;

// Type aliases for WASM compatibility (expected by ofdb-wasm crate)
pub type PushConfig = Config;
pub type PushMessage = Message;
pub type PushResult = SyncResult;
pub use crate::transport::SyncTransport as PushSyncTransport;

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
    /// Role in the synchronization (Initiator or Responder).
    pub role: crate::SyncRole,
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
            role: crate::SyncRole::Initiator,
            protocol: PushConfig::default(),
            handshake_timeout: Duration::from_secs(10),
            message_timeout: Duration::from_secs(30),
            send_done_on_exhaust: true,
        }
    }
}

/// Timer future using web-sys setTimeout/clearTimeout.
struct WasmSleep {
    duration: Duration,
    handle: Option<i32>,
    done: bool,
}

impl WasmSleep {
    fn new(duration: Duration) -> Self {
        Self {
            duration,
            handle: None,
            done: false,
        }
    }
}

impl Future for WasmSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            return Poll::Ready(());
        }

        if self.handle.is_none() {
            let window = window().expect("no window available");
            let waker = cx.waker().clone();
            let millis = self.duration.as_millis().min(i32::MAX as u128) as i32;

            let closure = Closure::once_into_js(move || {
                waker.wake();
            });

            let handle = window
                .set_timeout_with_callback_and_timeout_and_arguments_0(
                    closure.as_ref().unchecked_ref(),
                    millis,
                )
                .expect("setTimeout failed");

            self.handle = Some(handle);
        }

        Poll::Pending
    }
}

impl Drop for WasmSleep {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            if let Some(window) = window() {
                let _ = window.clear_timeout_with_handle(handle);
            }
        }
    }
}

/// Sleep for a duration using web-sys timers.
fn wasm_sleep(duration: Duration) -> impl Future<Output = ()> {
    WasmSleep::new(duration)
}

/// Timeout wrapper using web-sys timers and futures::select.
/// Does not require the future to be Unpin.
async fn wasm_timeout<F, T>(duration: Duration, future: F) -> Result<T, ()>
where
    F: Future<Output = T>,
{
    let timer = wasm_sleep(duration);
    futures::pin_mut!(future, timer);
    select! {
        res = future.fuse() => Ok(res),
        _ = timer.fuse() => Err(()),



    }
}

/// Run push-based synchronization between two peers (WASM version).
///
/// This version uses web-sys timers and futures::select!, allowing
/// non-Send futures to be used without a tokio runtime.
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
        let mut outstanding = HashMap::new();
        let mut sent_done = false;
        let mut received_done = false;
        let mut outbound_exhausted = false;
        let mut inbound_closed = false;

        let hello = Message::Hello(crate::protocol::HelloConfig {
            protocol_version: 1,
            manifest_digest: [0; 32],
            config: config.protocol.clone(),
        });
        // Perform handshake with timeout
        let handshake_result = wasm_timeout(config.handshake_timeout, async {
            match config.role {
                crate::SyncRole::Initiator => {
                    transport
                        .send(hello)
                        .await
                        .map_err(PushSynchronizeError::Transport)?;
                    match transport
                        .receive()
                        .await
                        .map_err(PushSynchronizeError::Transport)?
                    {
                        Message::HelloAck {
                            protocol_version: 1,
                            accepted: true,
                        } => Ok(()),
                        _ => Err(PushSynchronizeError::HandshakeFailed(
                            "expected accepted version-one HelloAck".into(),
                        )),
                    }
                }
                crate::SyncRole::Responder => {
                    let accepted = matches!(
                        transport
                            .receive()
                            .await
                            .map_err(PushSynchronizeError::Transport)?,
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
                        .map_err(PushSynchronizeError::Transport)?;
                    if accepted {
                        Ok(())
                    } else {
                        Err(PushSynchronizeError::HandshakeFailed(
                            "unsupported Hello".into(),
                        ))
                    }
                }
            }
        })
        .await;
        let _ = handshake_result.map_err(|_| PushSynchronizeError::SessionTimeout)?;

        // Main bidirectional loop using futures::select!
        loop {
            select! {
                // Outbound: send local ChangeEvents
                event = async {
                    if outbound_exhausted {
                        futures::future::pending().await
                    } else {
                        outbound.next().await
                    }
                }.fuse() => {
                    match event {
                        Some(event) => {
                            // Track local generation
                            let generation = event.generation;
                            local_generations.insert(event.key.clone(), generation);
                        outstanding.insert(event.key.clone(), generation);

                            let msg = Message::ChangeEvent(event);
                            transport.send(msg).await
                                .map_err(PushSynchronizeError::Transport)?;
                            result.events_sent += 1;
                            result.bytes_sent += 1; // approximate
                        }
                        None => {
                            outbound_exhausted = true;
                            if config.send_done_on_exhaust && !sent_done {
                                let done = Message::Done { final_generations: local_generations.clone() };
                                transport.send(done).await
                                    .map_err(PushSynchronizeError::Transport)?;
                                sent_done = true;
                            }
                        }
                    }
                }

                // Inbound: receive and process messages with message timeout
                msg = async {
                    if inbound_closed {
                        futures::future::pending().await
                    } else {
                        wasm_timeout(config.message_timeout, transport.receive()).await
                    }
                }.fuse() => {
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
                                        .map_err(PushSynchronizeError::Apply)?;
                                    result.events_received += 1;

                                    // Queue acknowledgment
                                    pending_acks.insert(key, generation);

                                    // Send ACK if immediate
                                    if config.protocol.immediate_ack {
                                        let ack = Message::Ack {
                                            acknowledgments: std::mem::take(&mut pending_acks),
                                        };
                                        transport.send(ack).await
                                            .map_err(PushSynchronizeError::Transport)?;
                                    }
                                }
                                Message::SnapshotRequest { key: _, min_generation: _ } => {
                                    // TODO: Implement snapshot serving
                                    // For now, send Close to indicate we can't serve
                                    let close = Message::Close;
                                    transport.send(close).await
                                        .map_err(PushSynchronizeError::Transport)?;
                                }
                                Message::Snapshot { key, blob, generation } => {
                                    // Apply snapshot
                                    let event = ChangeEvent { key, blob, generation };
                                    apply_inbound(event).await
                                        .map_err(PushSynchronizeError::Apply)?;
                                    result.snapshots_received += 1;
                                }
                                Message::Ack { acknowledgments } => {
                                    // Peer acknowledged our events
                                    for (key, _generation) in acknowledgments {
                                        pending_acks.remove(&key);
                                    }
                                }
                                Message::Done { final_generations } => {
                                    received_done = true;
                                    // Update remote generations with final values
                                    remote_generations.extend(final_generations);
                                }
                                Message::Close => {
                                    inbound_closed = true;
                                }
                                Message::HelloAck { .. } => {
                                    // Ignore duplicate hello ack
                                }
                                Message::Hello { .. } => {
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

                // Periodic ACK flush if not immediate ack
                _ = async {
                    if config.protocol.immediate_ack || pending_acks.is_empty() {
                        futures::future::pending().await
                    } else {
                        wasm_sleep(Duration::from_millis(100)).await
                    }
                }.fuse() => {
                    let ack = Message::Ack {
                        acknowledgments: std::mem::take(&mut pending_acks),
                    };
                    transport.send(ack).await
                        .map_err(PushSynchronizeError::Transport)?;
                }

                // Termination check
                _ = async {
                    if outbound_exhausted && outstanding.is_empty() && (received_done || inbound_closed) {
                        futures::future::ready(()).await
                    } else {
                        futures::future::pending().await
                    }
                }.fuse() => {
                    break;
                }
            }

            if sent_done && received_done {
                break;
            }
        }

        // Send final DONE if not already sent
        if !sent_done {
            let done = Message::Done {
                final_generations: local_generations.clone(),
            };
            transport
                .send(done)
                .await
                .map_err(PushSynchronizeError::Transport)?;
        }

        // Send final CLOSE
        let close = Message::Close;
        let _ = transport.send(close).await;

        Ok(result)
    }
}

/// Configuration for push_synchronize (re-export with expected name).
pub type SynchronizeConfig = PushSynchronizeConfig;

/// Type alias for error (re-export with expected name).
pub type SynchronizeError<E> = PushSynchronizeError<E>;

/// Run push-based synchronization (re-export with expected name).
pub fn synchronize<T, F, S, Fut>(
    transport: T,
    outbound: S,
    apply_inbound: F,
    config: SynchronizeConfig,
) -> impl Future<Output = Result<PushResult, PushSynchronizeError<T::Error>>>
where
    T: PushSyncTransport,
    S: Stream<Item = ChangeEvent> + Unpin,
    F: FnMut(ChangeEvent) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    push_synchronize(transport, outbound, apply_inbound, config)
}
