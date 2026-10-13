use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::types::Result;
use crate::types::NodeID;

#[derive(Clone)]
pub struct Contact {
    pub id: NodeID,
    pub socket: Arc<UdpSocket>,
}

impl Contact {
    pub async fn new(bind_addr: &str, id: NodeID) -> Result<Self> {
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);

        Ok(Self {
            id, 
            socket: socket, 
        })
    }

    pub async fn with_node(id: NodeID, bind_addr: &str) -> Result<Self> {
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);

        Ok(Self {
            id, 
            socket, 
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }

    pub async fn send(&self, to: SocketAddr, bytes: &[u8]) -> Result<()> {
        self.socket.send_to(bytes, to).await?;
        Ok(())
    }

    pub async fn receive(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        Ok(self.socket.recv_from(buf).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    // Verify the send and receive functions work properly
    #[tokio::test]
    async fn send_then_recv_on_loopback() {
        let a = Contact::with_node(NodeID([1; 32]), "127.0.0.1:0").await.unwrap();
        let b = Contact::with_node(NodeID([2; 32]), "127.0.0.1:0").await.unwrap();

        a.send(b.local_addr().unwrap(), b"Hello").await.unwrap();

        let mut buffer = [0u8; 2048];
        let (len, from) = timeout(Duration::from_secs(1), b.receive(&mut buffer))
            .await
            .expect("timed out waiting for packet")
            .unwrap();

        assert_eq!(&buffer[..len], b"Hello");
        assert_eq!(from, a.local_addr().unwrap());
    }
}