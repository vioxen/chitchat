use std::io;
use std::net::SocketAddr;

use anyhow::{Context, ensure};
use async_trait::async_trait;
use tracing::{debug, warn};

use crate::MAX_UDP_DATAGRAM_PAYLOAD_SIZE;
use crate::message::ChitchatEnvelope;
use crate::transport::{RecvOutcome, SendOutcome, Socket, Transport};

pub struct UdpTransport;

#[async_trait]
impl Transport for UdpTransport {
    async fn open(&self, bind_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let udp_socket = UdpSocket::open(bind_addr).await?;
        Ok(Box::new(udp_socket))
    }
}

pub struct UdpSocket {
    buf_send: Vec<u8>,
    // One extra byte lets us distinguish an exact-limit datagram from a
    // truncated oversized datagram. Reading into an exact-limit buffer would
    // make both cases appear to have the same length.
    buf_recv: Box<[u8; MAX_UDP_DATAGRAM_PAYLOAD_SIZE + 1]>,
    socket: tokio::net::UdpSocket,
}

impl UdpSocket {
    pub async fn open(bind_addr: SocketAddr) -> anyhow::Result<UdpSocket> {
        let socket = tokio::net::UdpSocket::bind(bind_addr)
            .await
            .with_context(|| format!("failed to bind to {bind_addr}/UDP for gossip"))?;
        Ok(UdpSocket {
            buf_send: Vec::with_capacity(MAX_UDP_DATAGRAM_PAYLOAD_SIZE),
            buf_recv: Box::new([0u8; MAX_UDP_DATAGRAM_PAYLOAD_SIZE + 1]),
            socket,
        })
    }
}

fn is_transient_io_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::OutOfMemory
            // On Windows, sending to a closed peer causes recv_from to fail
            // with ConnectionReset (WSAECONNRESET / 10054).
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
    )
}

fn is_oversized_payload(payload_len: usize) -> bool {
    payload_len > MAX_UDP_DATAGRAM_PAYLOAD_SIZE
}

#[async_trait]
impl Socket for UdpSocket {
    fn local_addr(&self) -> anyhow::Result<SocketAddr> {
        self.socket
            .local_addr()
            .context("failed to get UDP socket address")
    }

    async fn send(
        &mut self,
        to_addr: SocketAddr,
        envelope: ChitchatEnvelope,
    ) -> anyhow::Result<SendOutcome> {
        self.buf_send.clear();
        envelope.serialize(&mut self.buf_send);
        ensure!(
            self.buf_send.len() <= MAX_UDP_DATAGRAM_PAYLOAD_SIZE,
            "serialized chitchat envelope exceeds the UDP payload limit"
        );
        let num_bytes_sent = self.send_bytes(to_addr, &self.buf_send).await?;
        Ok(SendOutcome { num_bytes_sent })
    }

    /// Recv needs to be cancellable.
    async fn recv(&mut self) -> anyhow::Result<RecvOutcome> {
        loop {
            if let Some(envelope) = self.receive_one().await? {
                return Ok(envelope);
            }
        }
    }
}

impl UdpSocket {
    async fn receive_one(&mut self) -> anyhow::Result<Option<RecvOutcome>> {
        let (num_bytes_received, from_addr) =
            match self.socket.recv_from(&mut self.buf_recv[..]).await {
                Ok(result) => result,
                Err(err) if is_transient_io_error(&err) => {
                    warn!(error=%err, "transient UDP recv error");
                    return Ok(None);
                }
                Err(err) => {
                    return Err(err).context("fatal UDP recv error");
                }
            };
        if is_oversized_payload(num_bytes_received) {
            debug!(
                payload_len = num_bytes_received,
                max_payload_len = MAX_UDP_DATAGRAM_PAYLOAD_SIZE,
                from = %from_addr,
                "oversized-chitchat-payload"
            );
            return Ok(None);
        }
        let mut buf = &self.buf_recv[..num_bytes_received];
        match ChitchatEnvelope::deserialize(&mut buf) {
            Ok(envelope) if buf.is_empty() => Ok(Some(RecvOutcome {
                from_addr,
                envelope,
                num_bytes_received,
            })),
            Ok(_) => {
                debug!(
                    payload_len = num_bytes_received,
                    trailing_len = buf.len(),
                    from = %from_addr,
                    "trailing-chitchat-payload"
                );
                Ok(None)
            }
            Err(err) => {
                warn!(payload_len=num_bytes_received, from=%from_addr, err=%err, "invalid-chitchat-payload");
                Ok(None)
            }
        }
    }

    pub(crate) async fn send_bytes(
        &self,
        to_addr: SocketAddr,
        payload: &[u8],
    ) -> anyhow::Result<usize> {
        let num_bytes = self
            .socket
            .send_to(payload, to_addr)
            .await
            .context("failed to send chitchat message to peer")?;
        Ok(num_bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::net::UdpSocket as TokioUdpSocket;

    use super::*;
    use crate::digest::Digest;

    async fn sockets() -> (TokioUdpSocket, UdpSocket, SocketAddr) {
        let sender = TokioUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let receiver = UdpSocket::open(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let receiver_addr = receiver.local_addr().unwrap();
        (sender, receiver, receiver_addr)
    }

    #[tokio::test]
    async fn receive_buffer_has_oversize_sentinel() {
        let (_sender, receiver, _receiver_addr) = sockets().await;

        assert_eq!(receiver.buf_recv.len(), MAX_UDP_DATAGRAM_PAYLOAD_SIZE + 1);
        assert!(!is_oversized_payload(MAX_UDP_DATAGRAM_PAYLOAD_SIZE));
        assert!(is_oversized_payload(MAX_UDP_DATAGRAM_PAYLOAD_SIZE + 1));
    }

    #[tokio::test]
    async fn trailing_bytes_are_rejected() {
        let (sender, mut receiver, receiver_addr) = sockets().await;
        let mut payload = ChitchatEnvelope::new_syn_v0("cluster".to_string(), Digest::default())
            .serialize_to_vec();
        payload.push(0);

        sender.send_to(&payload, receiver_addr).await.unwrap();

        assert!(receiver.receive_one().await.unwrap().is_none());
    }
}
