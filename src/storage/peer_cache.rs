use std::net::Ipv4Addr;
use std::net:: SocketAddr;
use std::net::SocketAddrV4;

use crate::types::Result;
use super::disk::save_to_disk;
use crate::storage::disk::read_or_create_file;

const PEER_CACHE_FILENAME: &str = "peer_cache.silk";
const MAX_PEERS: usize = 40;

pub struct PeerCache;

impl PeerCache {
    pub fn extract_peers() -> Result<Option<Vec<SocketAddr>>> {
        let bytes = read_or_create_file(PEER_CACHE_FILENAME)?;
        if bytes.len() < 6 { return Ok(None); }

        Ok(Some(bytes
            .chunks_exact(6)
            .map(|chunk| {
                let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);

                SocketAddr::V4(SocketAddrV4::new(ip, port))
            }).collect()))
    }

    pub fn save_peers(peers: &[SocketAddr]) -> Result<()> {
        if peers.is_empty() { return Ok(()); }
        let peers = &peers[..peers.len().min(MAX_PEERS)];

        let mut bytes = Vec::with_capacity(peers.len() * 6);
        for peer in peers {
            if let SocketAddr::V4(addr) = peer {
                bytes.extend_from_slice(&addr.ip().octets());
                bytes.extend_from_slice(&addr.port().to_be_bytes());
            }
        }
    
        save_to_disk(PEER_CACHE_FILENAME, &bytes)
    }
}