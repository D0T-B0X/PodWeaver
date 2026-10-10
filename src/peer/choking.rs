use std::time::Instant;
use std::net::SocketAddr;
use std::collections::HashMap;

use rand::seq::IndexedRandom;

use crate::error::Error;
use crate::types::Result;
use super::piece::Bitfield;

const PEER_SLOTS: u8 = 6;

const SLICE_COUNT: usize = 3;
const NOMINAL_SLICE_SECS: f64 = 10.0;

#[derive(Clone, Copy)]
pub struct ChokeState {
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub slices: [u64; SLICE_COUNT],
    pub slice_index: usize,
    pub slices_filled: usize,
    pub download_rate: f64,
}

impl ChokeState {
    pub fn new() -> Self {
        ChokeState { 
            am_choking: true, 
            am_interested: false, 
            peer_choking: true, 
            peer_interested: false, 
            slices: [0; SLICE_COUNT],
            slice_index: 0,
            slices_filled: 0,
            download_rate: 0.0,
        }
    }
}

pub struct ChokeManager {
    peers: HashMap<SocketAddr, ChokeState>,
    last_unchoke: Instant,
    last_optimistic_unchoke: Instant,
    current_optimistic: Option<SocketAddr>,
}

impl ChokeManager {
    pub fn new() -> Self {
        let peers = HashMap::new();
        let last_unchoke = Instant::now();
        let last_optimistic_unchoke = Instant::now();

        Self {
            peers,
            last_unchoke,
            last_optimistic_unchoke,
            current_optimistic: None,
        }
    }

    pub fn add_peer(&mut self, from: SocketAddr) {
        self.peers.insert(from, ChokeState::new());
    }

    pub fn remove_peer(&mut self, addr: SocketAddr) {
        self.peers.remove(&addr);
        if self.current_optimistic == Some(addr) {
            self.current_optimistic = None;
        }
    }

    pub fn set_am_choking(&mut self, addr: SocketAddr, v: bool) -> Result<()> {
        self.peers.get_mut(&addr).ok_or(Error::PeerNotFound)?.am_choking = v;
        Ok(())
    }

    pub fn set_am_interested(&mut self, addr: SocketAddr, v: bool) -> Result<()> {
        self.peers.get_mut(&addr).ok_or(Error::PeerNotFound)?.am_interested = v;
        Ok(())
    }

    pub fn set_peer_choking(&mut self, addr: SocketAddr, v: bool) -> Result<()> {
        self.peers.get_mut(&addr).ok_or(Error::PeerNotFound)?.peer_choking = v;
        Ok(())
    }

    pub fn set_peer_interested(&mut self, addr: SocketAddr, v: bool) -> Result<()> {
        self.peers.get_mut(&addr).ok_or(Error::PeerNotFound)?.peer_interested = v;
        Ok(())
    }

    pub fn record_download(&mut self, addr: SocketAddr, bytes: u64) -> Result<()> {
        let state = self.peers.get_mut(&addr).ok_or(Error::PeerNotFound)?;
        state.slices[state.slice_index] += bytes;
        Ok(())
    }

    fn roll_slices(&mut self) {
        for (_, state) in self.peers.iter_mut() {
            state.slice_index = (state.slice_index + 1) % SLICE_COUNT;
            state.slices[state.slice_index] = 0;
            if state.slices_filled < SLICE_COUNT {
                state.slices_filled += 1;
            }
        } 
    }

    pub fn should_be_interested(remote_bitfield: &Bitfield, my_bitfield: &Bitfield) -> bool {
        (0..my_bitfield.piece_count).any(|i| remote_bitfield.has(i) && !my_bitfield.has(i))
    }

    pub fn decide_unchokes(&mut self) -> Vec<(SocketAddr, bool)> {
        self.last_unchoke = Instant::now();
        self.roll_slices();

        // compute download rates from the ring of slices
        for (_, state) in self.peers.iter_mut() {
            let total: u64 = state.slices.iter().sum();
            state.download_rate = if state.slices_filled > 0 {
                total as f64 / (state.slices_filled as f64 * NOMINAL_SLICE_SECS)
            } else {
                0.0
            };
        }

        // unchoking decision is based on the download rate
        let mut peers_with_download_rates: Vec<(SocketAddr, bool, f64)> = self.peers
            .iter()
            .map(|(addr, state)| (*addr, state.peer_interested, state.download_rate))
            .collect();

        peers_with_download_rates.sort_by(|a,b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        let mut peer_unchokes = Vec::new();

        let mut selected = 0;
        for (addr, interested, _) in peers_with_download_rates.iter() {
            // the top 6 interested peers are selected
            let optimistic = Some(*addr) == self.current_optimistic;
            let decision = selected < PEER_SLOTS && *interested || optimistic;
            let state = self.peers.get_mut(addr).unwrap();
            selected += (!optimistic && *interested) as u8;

            // only add on peer state change
            if state.am_choking != !decision {
                // update peer state
                state.am_choking = !decision;
                peer_unchokes.push((*addr, decision));
            }
        }

        peer_unchokes
    }

    pub fn optimistic_unchoke(&mut self) -> Option<SocketAddr> {
        self.last_optimistic_unchoke = Instant::now();

        let choked: Vec<SocketAddr> = self.peers.iter()
            .filter(|(_, s)| s.am_choking && s.peer_interested)
            .map(|(addr, _)| *addr)
            .collect();

        if choked.is_empty() { return None; }

        // pick a random peer to unchoke
        let chosen = *choked.choose(&mut rand::rng())?;

        self.peers.get_mut(&chosen).unwrap().am_choking = false;
        self.current_optimistic = Some(chosen);
        Some(chosen)
    }

    pub fn can_request(&self, addr: SocketAddr) -> Result<bool> {
        if let Some(peer) = self.peers.get(&addr) {
            return Ok(!peer.peer_choking);
        }

        Err(Error::PeerNotFound)
    }

    pub fn am_uploading_to(&self, addr: SocketAddr) -> Result<bool> {
        if let Some(peer) = self.peers.get(&addr) {
            return Ok(!peer.am_choking);
        }

        Err(Error::PeerNotFound)
    }

    pub fn peer_is_interested(&self, addr: SocketAddr) -> Result<bool> {
        if let Some(peer) = self.peers.get(&addr) {
            return Ok(peer.peer_interested);
        }

        Err(Error::PeerNotFound)
    }

    pub fn rate_of(&self, addr: SocketAddr) -> Result<f64> {
        if let Some(peer) = self.peers.get(&addr) {
            return Ok(peer.download_rate);
        }

        Err(Error::PeerNotFound)
    }

    pub fn secs_since_last_unchoke(&self) -> u64 {
        self.last_unchoke.elapsed().as_secs()
    }

    pub fn secs_since_last_optimistic_unchoke(&self) -> u64 {
        self.last_optimistic_unchoke.elapsed().as_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> SocketAddr {
        format!("127.0.0.1:{}", 9000 + n as u16).parse().unwrap()
    }

    #[test]
    fn decide_unchokes_only_returns_changes() {
        let mut mgr = ChokeManager::new();
        for i in 0..10 {
            mgr.add_peer(addr(i));
            mgr.set_peer_interested(addr(i), true).unwrap()
        }

        // First call: everyone was choking, top 4 become unchoked → 4 changes
        let changes = mgr.decide_unchokes();
        assert_eq!(changes.len(), 6);
        assert!(changes.iter().all(|(_, unchoke)| *unchoke));

        let changes2 = mgr.decide_unchokes();
        assert_eq!(changes2.len(), 0);
    }

    #[test]
    fn record_download_accumulates() {
        let mut mgr = ChokeManager::new();
        let a = addr(1);
        mgr.add_peer(a);

        mgr.record_download(a, 1000).unwrap();
        mgr.record_download(a, 2000).unwrap();

        let state = mgr.peers.get(&a).unwrap();
        assert_eq!(state.slices.iter().sum::<u64>(), 3000);
    }

    #[test]
    fn optimistic_unchoke_keeps_peer_unchoked_through_decide() {
        let mut mgr = ChokeManager::new();
        for i in 0..10 {
            mgr.add_peer(addr(i));
            mgr.set_peer_interested(addr(i), true).unwrap();
        }

        let chosen = mgr.optimistic_unchoke().unwrap();

        let changes = mgr.decide_unchokes();
        // The optimistic peer should NOT appear in the changes (it stays unchoked)
        assert!(!changes.iter().any(|(a, unchoke)| *a == chosen && !*unchoke));
    }
}