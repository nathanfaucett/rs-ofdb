use alloc::vec::Vec;
use std::io::{Error, ErrorKind};

use iroh::endpoint::{RecvStream, SendStream};

use crate::{MAX_MESSAGE_BYTES, SyncTransport};

/// Length-prefixed sync frames over an already-authorized Iroh bidirectional stream.
/// Each frame is a big-endian u32 byte length followed by exactly that many bytes.
#[derive(Debug)]
pub struct IrohTransport {
    send: SendStream,
    recv: RecvStream,
}

impl IrohTransport {
    pub fn new(send: SendStream, recv: RecvStream) -> Self {
        Self { send, recv }
    }
}

impl SyncTransport for IrohTransport {
    type Error = Error;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        let mut prefix = [0; 4];
        self.recv
            .read_exact(&mut prefix)
            .await
            .map_err(Error::other)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > MAX_MESSAGE_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "sync frame exceeds 1 MiB",
            ));
        }
        let mut frame = alloc::vec![0; length];
        self.recv
            .read_exact(&mut frame)
            .await
            .map_err(Error::other)?;
        Ok(frame)
    }

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        if frame.len() > MAX_MESSAGE_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "sync frame exceeds 1 MiB",
            ));
        }
        self.send
            .write_all(&(frame.len() as u32).to_be_bytes())
            .await
            .map_err(Error::other)?;
        self.send.write_all(&frame).await.map_err(Error::other)
    }
}
