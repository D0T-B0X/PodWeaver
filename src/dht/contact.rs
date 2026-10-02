use std::sync::Arc;

use rand::Rng;
use tokio::net::UdpSocket;

use crate::error::*;
use crate::types::*;
use crate::storage::boot::*;

pub struct Contact {
    pub id: NodeID,
    pub socket: Arc<UdpSocket>,
}

impl Contact {
    pub async fn new(bind_addr: &str) -> Result<Arc<Self>> {
        let node_id = load_or_create_id()?;
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);

        let contact = Arc::new(Self {
            id: node_id, 
            socket: socket, 
        });

        Ok(contact)
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