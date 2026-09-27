use core::future::Future;

use btree_automerge::DocumentChangeKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvSnapshot {
    pub key: DocumentChangeKey,
    pub payload: Vec<u8>,
}
const VERSION: u8 = 1;

pub trait SyncTransport {
    type Error;

    fn receive(&mut self) -> impl Future<Output = Result<Vec<u8>, Self::Error>>;
    fn send(&mut self, frame: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncRole {
    Initiator,
    Responder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub max_frame_bytes: usize,
    pub max_snapshots_per_batch: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_frame_bytes: 8 * 1024 * 1024,
            max_snapshots_per_batch: 128,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error<E> {
    Transport(E),
    Codec(String),
    InvalidFrame,
    FrameTooLarge,
    UnsupportedVersion(u8),
    UnexpectedMessage,
    Storage(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    Hello { version: u8 },

    Snapshots(Vec<KvSnapshot>),
    End,
    Finished,
    Abort,
}

pub fn encode_frame(
    message: &Message,
    config: Config,
) -> Result<Vec<u8>, Error<core::convert::Infallible>> {
    let bytes = postcard::to_allocvec(message).map_err(|error| Error::Codec(error.to_string()))?;
    if bytes.len() > config.max_frame_bytes {
        return Err(Error::FrameTooLarge);
    }
    Ok(bytes)
}

pub fn decode_frame<E>(bytes: &[u8], config: Config) -> Result<Message, Error<E>> {
    if bytes.len() > config.max_frame_bytes {
        return Err(Error::FrameTooLarge);
    }
    let message: Message = postcard::from_bytes(bytes).map_err(|_| Error::InvalidFrame)?;
    if let Message::Snapshots(snapshots) = &message {
        if snapshots.len() > config.max_snapshots_per_batch {
            return Err(Error::FrameTooLarge);
        }
        for snapshot in snapshots {
            kv::decode_document_id(&snapshot.key.id).map_err(|_| Error::InvalidFrame)?;
        }
    }
    if let Message::Hello { version } = message {
        if version != VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        return Ok(Message::Hello { version });
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_round_trip_and_frame_limit() {
        let config = Config::default();
        let frame = encode_frame(&Message::Hello { version: VERSION }, config).unwrap();
        assert_eq!(
            decode_frame::<()>(frame.as_slice(), config),
            Ok(Message::Hello { version: VERSION })
        );
        assert_eq!(
            decode_frame::<()>(
                &frame,
                Config {
                    max_frame_bytes: 1,
                    ..config
                }
            ),
            Err(Error::FrameTooLarge)
        );
    }

    #[test]
    fn snapshot_wire_type_round_trips_and_requires_valid_document_ids() {
        let id = kv::encode_document_id(
            "wire-key",
            uuid::Uuid::from_u128((1 << 80) | (7 << 76) | (2 << 62)),
        );
        let snapshot = KvSnapshot {
            key: DocumentChangeKey::new_snapshot(id, [7; 32]),
            payload: vec![1, 2, 3],
        };
        let message = Message::Snapshots(vec![snapshot.clone()]);
        let frame = encode_frame(&message, Config::default()).unwrap();
        assert_eq!(decode_frame::<()>(&frame, Config::default()), Ok(message));

        let invalid = Message::Snapshots(vec![KvSnapshot {
            key: DocumentChangeKey::new_snapshot(vec![0], [7; 32]),
            payload: vec![],
        }]);
        let frame = postcard::to_allocvec(&invalid).unwrap();
        assert_eq!(
            decode_frame::<()>(&frame, Config::default()),
            Err(Error::InvalidFrame)
        );
    }

    #[test]
    fn rejects_unknown_protocol_version() {
        let frame = postcard::to_allocvec(&Message::Hello {
            version: VERSION + 1,
        })
        .unwrap();
        assert_eq!(
            decode_frame::<()>(&frame, Config::default()),
            Err(Error::UnsupportedVersion(VERSION + 1))
        );
    }
}
