use core::future::Future;
use std::io::{Error, ErrorKind};

use iroh::endpoint::{RecvStream, SendStream};

use crate::protocol::{
    Error as ProtocolError, MAX_MESSAGE_BYTES, PushConfig, PushMessage, decode_message,
    encode_message,
};
use crate::transport::PushSyncTransport;

/// Length-prefixed push sync frames over an already-authorized Iroh bidirectional stream.
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

impl PushSyncTransport for IrohTransport {
    type Error = Error;

    fn send(&mut self, msg: PushMessage) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let config = PushConfig::default();
        let bytes =
            encode_message(&msg, &config).map_err(|e| Error::new(ErrorKind::InvalidData, e));
        let send = &mut self.send;
        async move {
            let bytes = bytes?;
            if bytes.len() > MAX_MESSAGE_BYTES {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "sync frame exceeds 1 MiB",
                ));
            }
            send.write_all(&(bytes.len() as u32).to_be_bytes())
                .await
                .map_err(Error::other)?;
            send.write_all(&bytes).await.map_err(Error::other)
        }
    }

    fn receive(&mut self) -> impl Future<Output = Result<PushMessage, Self::Error>> + Send {
        let config = PushConfig::default();
        let recv = &mut self.recv;
        async move {
            let mut prefix = [0; 4];
            recv.read_exact(&mut prefix).await.map_err(Error::other)?;
            let length = u32::from_be_bytes(prefix) as usize;
            if length > MAX_MESSAGE_BYTES {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "sync frame exceeds 1 MiB",
                ));
            }
            let mut frame = vec![0; length];
            recv.read_exact(&mut frame).await.map_err(Error::other)?;
            decode_message::<ProtocolError<Error>>(&frame, &config)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
        }
    }
}
