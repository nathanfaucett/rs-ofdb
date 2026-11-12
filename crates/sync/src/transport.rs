#![allow(dead_code, unused_variables)]
use crate::protocol::Message;
use core::future::Future;

/// Bidirectional transport for synchronization.
///
/// Implementations handle the underlying I/O (TCP, WebSocket, Iroh, etc.)
/// and provide async send/receive operations for Message frames.
pub trait SyncTransport {
    /// Transport-specific error type.
    type Error: core::fmt::Debug + Send + Sync + 'static;

    /// Send a sync message.
    ///
    /// The transport is responsible for framing and serialization.
    /// Returns an error if the send fails.
    #[cfg(not(target_arch = "wasm32"))]
    fn send(&mut self, msg: Message) -> impl Future<Output = Result<(), Self::Error>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn send(&mut self, msg: Message) -> impl Future<Output = Result<(), Self::Error>>;

    /// Receive the next sync message.
    ///
    /// Returns an error if the connection is closed, times out, or
    /// the received frame is invalid.
    #[cfg(not(target_arch = "wasm32"))]
    fn receive(&mut self) -> impl Future<Output = Result<Message, Self::Error>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn receive(&mut self) -> impl Future<Output = Result<Message, Self::Error>>;

    /// Optional: Get the peer's session ID if available.
    fn session_id(&self) -> Option<crate::protocol::SessionId> {
        None
    }
}

/// Mock transport for testing.
#[cfg(test)]
pub mod mock {
    use super::*;
    use crate::protocol::{Config, Message};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    /// In-memory mock transport for testing.
    #[derive(Debug, Clone)]
    pub struct MockTransport {
        inbound: Arc<Mutex<VecDeque<Message>>>,
        outbound: Arc<Mutex<VecDeque<Message>>>,
        config: Config,
        closed: Arc<Mutex<bool>>,
    }

    impl MockTransport {
        pub fn new(config: Config) -> Self {
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

        pub fn push_inbound(&self, msg: Message) {
            self.inbound.lock().unwrap().push_back(msg);
        }

        pub fn pop_outbound(&self) -> Option<Message> {
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

    impl SyncTransport for MockTransport {
        type Error = std::io::Error;

        async fn send(&mut self, msg: Message) -> Result<(), Self::Error> {
            if *self.closed.lock().unwrap() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "peer closed",
                ));
            }
            self.outbound.lock().unwrap().push_back(msg);
            Ok(())
        }

        async fn receive(&mut self) -> Result<Message, Self::Error> {
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
        use crate::protocol::{Config, make_hello};

        #[tokio::test]
        async fn mock_transport_send_receive() {
            let config = Config::default();
            let mut transport = MockTransport::new(config.clone());
            let mut peer = transport.peer();

            let hello = make_hello([1; 32], config);
            transport.send(hello.clone()).await.unwrap();

            let received = peer.receive().await.unwrap();
            assert_eq!(received, hello);
        }

        #[tokio::test]
        async fn mock_transport_bidirectional() {
            let config = Config::default();
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
