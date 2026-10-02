use std::time::Instant;
use std::net::SocketAddr;

use crate::error::*;
use crate::types::*;

#[derive(Clone)]
pub struct Node {
    pub id: NodeID,    
    pub address: SocketAddr,
    pub last_seen: Instant,
    pub status: NodeStatus
}

impl Node {
    pub fn new(id: NodeID, sockaddr: SocketAddr) -> Result<Self> {
        Ok(Self {
            id: id, 
            address: sockaddr,  
            last_seen: Instant::now(), 
            status: NodeStatus::Good 
        })
    }
}