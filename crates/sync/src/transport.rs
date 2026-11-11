#![allow(dead_code, unused_variables)]
use crate::protocol::PushMessage;
use core::future::Future;

/// Bidirectional transport for push-based synchronization.
///
/// Implementations handle the underlying I/O (TCP, WebSocket, Iroh, etc.)
/// and provide async send/receive operations for PushMessage frames.
pub trait PushSyncTransport {
    /// Transport-specific error type.
    type Error: core::fmt::Debug + Send + Sync + 'static;

    /// Send a push sync message.
    ///
    /// The transport is responsible for framing and serialization.
    /// Returns an error if the send fails.
    #[cfg(not(target_arch = "wasm32"))]
    fn send(&mut self, msg: PushMessage) -> impl Future<Output = Result<(), Self::Error>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn send(&mut self, msg: PushMessage) -> impl Future<Output = Result<(), Self::Error>>;

    /// Receive the next push sync message.
    ///
    /// Returns an error if the connection is closed, times out, or
    /// the received frame is invalid.
    #[cfg(not(target_arch = "wasm32"))]
    fn receive(&mut self) -> impl Future<Output = Result<PushMessage, Self::Error>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn receive(&mut self) -> impl Future<Output = Result<PushMessage, Self::Error>>;

    /// Optional: Get the peer's session ID if available.
    fn session_id(&self) -> Option<crate::protocol::SessionId> {
        None
    }
}

/// Wrapper that adds framing to any async read/write stream.
#[cfg(feature = "std")]
pub mod framed {
    use super::*;
    use crate::protocol::PushConfig;
    use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use std::io;

    /// Length-prefixed frame codec for push sync messages.
    pub struct FramedTransport<S> {
        stream: S,
        config: PushConfig,
        read_buf: Vec<u8>,
    }

    impl<S> FramedTransport<S>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        pub fn new(stream: S, config: PushConfig) -> Self {
            Self {
                stream,
                config,
                read_buf: Vec::new(),
            }
        }

        pub fn into_inner(self) -> S {
            self.stream
        }

        pub fn config(&self) -> &PushConfig {
            &self.config
        }

        pub fn config_mut(&mut self) -> &mut PushConfig {
            &mut self.config
        }
    }

    impl<S> PushSyncTransport for FramedTransport<S>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        type Error = io::Error;

        async fn send(&mut self, msg: PushMessage) -> Result<(), Self::Error> {
            let bytes = crate::protocol::encode_message(&msg, &self.config)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let len = bytes.len() as u32;
            self.stream.write_all(&len.to_le_bytes()).await?;
            self.stream.write_all(&bytes).await?;
            self.stream.flush().await?;
            Ok(())
        }

        async fn receive(&mut self) -> Result<PushMessage, Self::Error> {
            let mut len_bytes = [0u8; 4];
            self.stream.read_exact(&mut len_bytes).await?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            if len > self.config.max_frame_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "frame too large: {} bytes > limit {}",
                        len, self.config.max_frame_bytes
                    ),
                ));
            }

            self.read_buf.resize(len, 0);
            self.stream.read_exact(&mut self.read_buf).await?;
            crate::protocol::decode_message::<io::Error>(&self.read_buf, &self.config)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }
    }
}

/// Mock transport for testing.
#[cfg(test)]
pub mod mock {
    use super::*;
    use crate::protocol::{PushConfig, PushMessage};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    /// In-memory mock transport for testing.
    #[derive(Debug, Clone)]
    pub struct MockTransport {
        inbound: Arc<Mutex<VecDeque<PushMessage>>>,
        outbound: Arc<Mutex<VecDeque<PushMessage>>>,
        config: PushConfig,
        closed: Arc<Mutex<bool>>,
    }

    impl MockTransport {
        pub fn new(config: PushConfig) -> Self {
            Self {
                inbound: Arc::new(Mutex::new(VecDeque::new())),
                outbound: Arc::new(Mutex::new(VecDeque::new())),
                config,
                closed: Arc::new(Mutex::new(false)),
            }
        }

        pub fn peer(&self) -> Self {
            Self {
                inbound: self.outbound.clone(),
                outbound: self.inbound.clone(),
                config: self.config.clone(),
                closed: self.closed.clone(),
            }
        }

        pub fn push_inbound(&self, msg: PushMessage) {
            self.inbound.lock().unwrap().push_back(msg);
        }

        pub fn pop_outbound(&self) -> Option<PushMessage> {
            self.outbound.lock().unwrap().pop_front()
        }

        pub fn outbound_len(&self) -> usize {
            self.outbound.lock().unwrap().len()
        }

        pub fn close(&self) {
            *self.closed.lock().unwrap() = true;
        }

        pub fn is_closed(&self) -> bool {
            *self.closed.lock().unwrap()
        }
    }

    impl PushSyncTransport for MockTransport {
        type Error = std::io::Error;

        async fn send(&mut self, msg: PushMessage) -> Result<(), Self::Error> {
            if *self.closed.lock().unwrap() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "peer closed",
                ));
            }
            self.outbound.lock().unwrap().push_back(msg);
            Ok(())
        }

        async fn receive(&mut self) -> Result<PushMessage, Self::Error> {
            if *self.closed.lock().unwrap() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "peer closed",
                ));
            }
            if let Some(msg) = self.inbound.lock().unwrap().pop_front() {
                return Ok(msg);
            }
            // In real implementation this would wait on a condvar or channel
            // For mock, we return error to indicate no more messages
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "peer closed",
            ))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::protocol::{PushConfig, make_hello};

        #[tokio::test]
        async fn mock_transport_send_receive() {
            let config = PushConfig::default();
            let mut transport = MockTransport::new(config.clone());
            let mut peer = transport.peer();

            let hello = make_hello([1; 32], config);
            transport.send(hello.clone()).await.unwrap();

            let received = peer.receive().await.unwrap();
            assert_eq!(received, hello);
        }

        #[tokio::test]
        async fn mock_transport_bidirectional() {
            let config = PushConfig::default();
            let mut transport = MockTransport::new(config.clone());
            let mut peer = transport.peer();

            // Client sends hello
            let hello = make_hello([1; 32], config.clone());
            transport.send(hello.clone()).await.unwrap();

            // Server receives hello
            let received = peer.receive().await.unwrap();
            assert_eq!(received, hello);

            // Server sends hello ack
            let ack = crate::protocol::make_hello_ack(true);
            peer.send(ack.clone()).await.unwrap();

            // Client receives ack
            let received = transport.receive().await.unwrap();
            assert_eq!(received, ack);
        }
    }
}
