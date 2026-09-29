use std::sync::Arc;
use std::time::Instant;
use std::path::{PathBuf, Path};

use rand::{RngExt, rng};
use tokio::net::UdpSocket;

use crate::storage::*;
use crate::types::*;
use crate::error::*;

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
}

fn load_or_create_id() -> Result<NodeID> {
    // read the id from the file or create a new id and store it in the file
    match read_file("node_id") {
        Ok(extracted_id) => {
            if extracted_id.len() == 32 {
                Ok(NodeID(extracted_id.try_into().map_err(|_| Error::CorruptedNodeID)?))
            } else {
                Err(Error::CorruptedNodeID)
            }
        },

        Err(Error::FileError(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut id = [0u8; 32];
            rng().fill(&mut id);
            save_to_disk("node_id", &id)?;
            Ok(NodeID(id))
        }

        Err(Error::FileError(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            Err(Error::UserLacksPrivilages)
        }

        _ => {
            Err(Error::FileReadFailed)
        }
    }
}