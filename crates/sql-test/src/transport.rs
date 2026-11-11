use core::fmt;

use futures::{StreamExt, channel::mpsc};

use sync::{PushMessage, PushSyncTransport};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InMemoryTransportError {
    Closed,
    Interrupted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportDirection {
    LeftToRight,
    RightToLeft,
}

impl fmt::Display for InMemoryTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Closed => "channel closed",
            Self::Interrupted => "transport interrupted",
        })
    }
}

impl std::error::Error for InMemoryTransportError {}

pub struct InMemoryTransport {
    receiver: mpsc::UnboundedReceiver<PushMessage>,
    sender: Option<mpsc::UnboundedSender<PushMessage>>,
    fail_at: Option<usize>,
    sent: usize,
}

impl PushSyncTransport for InMemoryTransport {
    type Error = InMemoryTransportError;

    async fn send(&mut self, msg: PushMessage) -> Result<(), Self::Error> {
        if self.fail_at == Some(self.sent) {
            self.sender = None;
            return Err(InMemoryTransportError::Interrupted);
        }
        self.sent += 1;
        self.sender
            .as_ref()
            .ok_or(InMemoryTransportError::Closed)?
            .unbounded_send(msg)
            .map_err(|_| InMemoryTransportError::Closed)
    }

    async fn receive(&mut self) -> Result<PushMessage, Self::Error> {
        self.receiver
            .next()
            .await
            .ok_or(InMemoryTransportError::Closed)
    }
}

pub fn in_memory_transport_pair() -> (InMemoryTransport, InMemoryTransport) {
    in_memory_transport_pair_failing_at(None)
}

pub fn in_memory_transport_pair_failing_at(
    fail_at: Option<usize>,
) -> (InMemoryTransport, InMemoryTransport) {
    in_memory_transport_pair_failing(None, fail_at)
}

pub fn in_memory_transport_pair_failing(
    direction: Option<TransportDirection>,
    fail_at: Option<usize>,
) -> (InMemoryTransport, InMemoryTransport) {
    let (left_sender, right_receiver) = mpsc::unbounded();
    let (right_sender, left_receiver) = mpsc::unbounded();
    let left_fails = direction != Some(TransportDirection::RightToLeft);
    let right_fails = direction == Some(TransportDirection::RightToLeft);
    (
        InMemoryTransport {
            receiver: left_receiver,
            sender: Some(left_sender),
            fail_at: left_fails.then_some(fail_at).flatten(),
            sent: 0,
        },
        InMemoryTransport {
            receiver: right_receiver,
            sender: Some(right_sender),
            fail_at: right_fails.then_some(fail_at).flatten(),
            sent: 0,
        },
    )
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::{InMemoryTransportError, PushSyncTransport, in_memory_transport_pair_failing_at};

    #[test]
    fn failed_send_closes_the_peer() {
        block_on(async {
            let (mut left, mut right) = in_memory_transport_pair_failing_at(Some(0));

            assert_eq!(
                left.send(sync::PushMessage::Close).await,
                Err(InMemoryTransportError::Interrupted)
            );
            assert_eq!(right.receive().await, Err(InMemoryTransportError::Closed));
        });
    }
}
