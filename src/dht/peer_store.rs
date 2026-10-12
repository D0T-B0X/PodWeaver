use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashMap;

use crate::types::InfoHash;

const MAX_PEERS_PER_HASH: usize = 100;

#[derive(Clone)]
pub struct PeerStore {
    pub store: HashMap<InfoHash, HashMap<SocketAddr, Instant>>,
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
        peers.insert(peer, Instant::now());

        if peers.len() > MAX_PEERS_PER_HASH {
            if let Some(oldest) = peers
                .iter()
                .min_by_key(|(_, v)| **v)
                .map(|(a, _)| *a) 
            {
                peers.remove(&oldest);
            }
        }
    }

    pub fn get_peers(&mut self, info_hash: &InfoHash) -> Option<Vec<SocketAddr>> {
        let now = Instant::now();
        let ttl = self.ttl;

        if let Some(peers) = self.store.get_mut(info_hash) {
            peers.retain(|_, last_seen| now.duration_since(*last_seen) < ttl);

            if !peers.is_empty() {
                return Some(peers.keys().copied().collect());
            }
        }
        None
    }
}