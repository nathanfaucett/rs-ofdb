use core::fmt;
use std::collections::HashMap;

use btree::{ChangeEvent, StorageKey};
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const PROTOCOL_VERSION: u8 = 1;

/// Maximum message size in bytes (1 MiB).
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// Configuration for the sync protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// Maximum frame size in bytes.
    pub max_frame_bytes: usize,
    /// Maximum number of pending unacknowledged events per key.
    pub max_pending_per_key: usize,
    /// Maximum total pending events across all keys.
    pub max_total_pending: usize,
    /// Session timeout in milliseconds.
    pub session_timeout_ms: u64,
    /// Whether to send ACK immediately or batch.
    pub immediate_ack: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_frame_bytes: 8 * 1024 * 1024, // 8 MiB
            max_pending_per_key: 1024,
            max_total_pending: 65536,
            session_timeout_ms: 30_000,
            immediate_ack: true,
        }
    }
}

/// Configuration sent in Hello message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloConfig {
    pub protocol_version: u8,
    pub manifest_digest: [u8; 32],
    pub config: Config,
}

/// Configuration for ACK behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckConfig {
    /// Maximum number of generations to acknowledge in one message.
    pub max_generations_per_ack: usize,
    /// Whether to send ACK immediately or batch.
    pub immediate_ack: bool,
}

impl Default for AckConfig {
    fn default() -> Self {
        Self {
            max_generations_per_ack: 256,
            immediate_ack: true,
        }
    }
}

/// Result of a synchronization session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SyncResult {
    pub events_sent: u64,
    pub events_received: u64,
    pub snapshots_sent: u64,
    pub snapshots_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

/// Sync protocol messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// Initial handshake with protocol version and manifest.
    Hello(HelloConfig),
    /// Acknowledges Hello and confirms protocol compatibility.
    HelloAck {
        protocol_version: u8,
        accepted: bool,
    },
    /// Incremental change event with Automerge blob.
    ChangeEvent(ChangeEvent),
    /// Request for a full snapshot of a key (missing dependencies).
    SnapshotRequest {
        key: StorageKey,
        /// Minimum generation the receiver has for this key.
        min_generation: u64,
    },
    /// Full document/row snapshot response.
    Snapshot {
        key: StorageKey,
        blob: Vec<u8>,
        generation: u64,
    },
    /// Acknowledges receipt of change events up to a generation per key.
    Ack {
        /// Per-key highest generation acknowledged.
        acknowledgments: HashMap<StorageKey, u64>,
    },
    /// Signals end of current change stream (sender has no more events).
    Done {
        /// Final generation per key for this session.
        final_generations: HashMap<StorageKey, u64>,
    },
    /// Terminates the session gracefully.
    Close,
}

/// Error types for sync.
#[derive(Debug, Error)]
pub enum Error<E> {
    #[error("transport error: {0}")]
    Transport(E),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("invalid frame")]
    InvalidFrame,
    #[error("frame too large: {0} bytes > limit {1}")]
    FrameTooLarge(usize, usize),
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    #[error("unexpected message: expected {expected}, got {got}")]
    UnexpectedMessage { expected: String, got: String },
    #[error("storage error: {0}")]
    Storage(String),
    #[error("session timeout")]
    SessionTimeout,
    #[error("peer closed connection")]
    PeerClosed,
    #[error("peer rejected handshake: {0}")]
    HandshakeRejected(String),
    #[error("invalid message sequence")]
    InvalidSequence,
    #[error("generation gap for key {key:?}: have {have}, need {need}")]
    GenerationGap {
        key: StorageKey,
        have: u64,
        need: u64,
    },
}

pub type Result<T, E = core::convert::Infallible> = core::result::Result<T, Error<E>>;

/// Encode a sync message to bytes.
pub fn encode_message(
    message: &Message,
    config: &Config,
) -> Result<Vec<u8>, core::convert::Infallible> {
    let bytes = to_allocvec(message).map_err(|error| Error::Codec(error.to_string()))?;
    if bytes.len() > config.max_frame_bytes {
        return Err(Error::FrameTooLarge(bytes.len(), config.max_frame_bytes));
    }
    Ok(bytes)
}

/// Decode bytes to a sync message.
pub fn decode_message<E>(bytes: &[u8], config: &Config) -> Result<Message, E> {
    if bytes.len() > config.max_frame_bytes {
        return Err(Error::FrameTooLarge(bytes.len(), config.max_frame_bytes));
    }
    let message: Message = from_bytes(bytes).map_err(|_| Error::InvalidFrame)?;
    Ok(message)
}

/// Create a Hello message for initiating a session.
pub fn make_hello(manifest_digest: [u8; 32], config: Config) -> Message {
    Message::Hello(HelloConfig {
        protocol_version: PROTOCOL_VERSION,
        manifest_digest,
        config,
    })
}

/// Create a HelloAck message.
pub fn make_hello_ack(accepted: bool) -> Message {
    Message::HelloAck {
        protocol_version: PROTOCOL_VERSION,
        accepted,
    }
}

/// Create an Ack message.
pub fn make_ack(acknowledgments: HashMap<StorageKey, u64>) -> Message {
    Message::Ack { acknowledgments }
}

/// Create a Done message.
pub fn make_done(final_generations: HashMap<StorageKey, u64>) -> Message {
    Message::Done { final_generations }
}

/// Create a SnapshotRequest message.
pub fn make_snapshot_request(key: StorageKey, min_generation: u64) -> Message {
    Message::SnapshotRequest {
        key,
        min_generation,
    }
}

/// Create a Snapshot message.
pub fn make_snapshot(key: StorageKey, blob: Vec<u8>, generation: u64) -> Message {
    Message::Snapshot {
        key,
        blob,
        generation,
    }
}

/// Create a Close message.
pub fn make_close() -> Message {
    Message::Close
}

/// Unique session identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btree::StorageKey;

    #[test]
    fn message_round_trip() {
        let config = Config::default();
        let event = ChangeEvent {
            key: StorageKey::kv(b"test-key".to_vec()),
            blob: vec![1, 2, 3],
            generation: 1,
        };
        let message = Message::ChangeEvent(event.clone());
        let encoded = encode_message(&message, &config).unwrap();
        let decoded = decode_message::<()>(&encoded, &config).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn hello_round_trip() {
        let config = Config::default();
        let message = make_hello([7; 32], config.clone());
        let encoded = encode_message(&message, &config).unwrap();
        let decoded = decode_message::<()>(&encoded, &config).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn frame_too_large() {
        let config = Config {
            max_frame_bytes: 10,
            ..Default::default()
        };
        let event = ChangeEvent {
            key: StorageKey::kv(b"test".to_vec()),
            blob: vec![0; 100],
            generation: 1,
        };
        let message = Message::ChangeEvent(event);
        let result = encode_message(&message, &config);
        assert!(matches!(result, Err(Error::FrameTooLarge(_, _))));
    }

    #[test]
    fn protocol_version_constant() {
        assert_eq!(PROTOCOL_VERSION, 1);
    }
}
