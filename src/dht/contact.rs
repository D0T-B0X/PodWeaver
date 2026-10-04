use rand::Rng;
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::error::Error;
use crate::types::Result;
use crate::types::NodeID;
use crate::storage::boot::*;

#[derive(Clone)]
pub struct Contact {
    pub id: NodeID,
    pub socket: Arc<UdpSocket>,
}

impl Contact {
    pub async fn new(bind_addr: &str) -> Result<Self> {
        let node_id = load_or_create_id()?;
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);

        Ok(Self {
            id: node_id, 
            socket: socket, 
        })
    }

    pub async fn with_node(id: NodeID, bind_addr: String) -> Result<Self> {
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

fn load_or_create_id() -> Result<NodeID> {
    // read the id from the file or create a new id and store it in the file
    match read_file("node_id") {
        Ok(extracted_id) => {
            let id_bytes = extracted_id
                .try_into()
                .map_err(|_| Error::CorruptedNodeID)?;

            Ok(NodeID(id_bytes))
        },

        Err(Error::FileError(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut id = [0u8; 32];
            rand::rng().fill_bytes(&mut id);
            save_to_disk("node_id", &id)?;
            Ok(NodeID(id))
        }

        Err(Error::FileError(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            Err(Error::UserLacksPrivileges)
        }

        _ => {
            Err(Error::FileReadFailed)
        }
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
        let a = Contact::with_node(NodeID([1; 32]), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let b = Contact::with_node(NodeID([2; 32]), "127.0.0.1:0".parse().unwrap()).await.unwrap();

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