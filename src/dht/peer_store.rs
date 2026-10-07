use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashMap;

use crate::types::InfoHash;

const MAX_PEERS_PER_HASH: usize = 100;

#[derive(Clone)]
pub struct PeerEntry {
    pub addr: SocketAddr,
    pub tcp_port: u16,
    pub last_seen: Instant,
}

#[derive(Clone)]
pub struct PeerStore {
    pub store: HashMap<InfoHash, Vec<PeerEntry>>,
    pub ttl: Duration,
}

impl PeerStore {
    pub fn new(ttl: Duration) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            store: HashMap::new(),
            ttl
        }))
    }

    pub fn announce(&mut self, info_hash: &InfoHash, peer: SocketAddr) {
        let peers = self.store.entry(*info_hash).or_default();

        if let Some(entry) = peers.iter_mut().find(|e| e.addr == peer) {
            entry.last_seen = Instant::now();
        } else if peers.len() < MAX_PEERS_PER_HASH {
            peers.push(PeerEntry { addr: peer, tcp_port: 0, last_seen: Instant::now() });
        }
    }

    pub fn get_peers(&mut self, info_hash: &InfoHash) -> Option<Vec<SocketAddr>> {
        let now = Instant::now();
        let ttl = self.ttl;

        if let Some(peers) = self.store.get_mut(info_hash) {
            peers.retain(|entry| now.duration_since(entry.last_seen) < ttl);

            if !peers.is_empty() {
                return Some(peers.iter().map(|e| e.addr).collect());
            }
        }
        None
    }
}