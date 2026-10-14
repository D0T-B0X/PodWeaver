use std::net::SocketAddr;
use std::collections::HashSet;

use rand::seq::IteratorRandom;

const MAX_RECENT_DROPS: usize = 50;

#[derive(Clone)]
pub struct Pex {
    pub known_addrs: HashSet<SocketAddr>,
    pub me: SocketAddr,
    max_pool: usize,
    recent_drops: Vec<SocketAddr>,
}

impl Pex {
    pub fn new(me: SocketAddr, max_size: usize) -> Self {
        Self {
            known_addrs: HashSet::new(),
            me,
            max_pool: max_size,
            recent_drops: Vec::new(),
        }
    }

    /// insert a list of addresses received from a remote pod
    pub fn add(&mut self, addrs: impl IntoIterator<Item = SocketAddr>) {
        for addr in addrs {
            if self.known_addrs.len() >= self.max_pool { break; }
            if addr == self.me { continue; }
            if self.known_addrs.insert(addr) {
                // A peer we just learned about is no longer "dropped".
                self.recent_drops.retain(|a| *a != addr);
            }
        }
    }

    /// remove addresses that are useless as indicated by a response from a remote pod
    pub fn remove(&mut self, addrs: impl IntoIterator<Item = SocketAddr>) {
        for addr in addrs {
            if self.known_addrs.remove(&addr) {
                self.record_drop(addr);
            }
        }
    }

    /// get a list of socket addresses without removing them from the PEX set
    pub fn sample(&self, n: usize) -> Vec<SocketAddr> {
        let mut rng = rand::rng();
        self.known_addrs.iter().copied().sample(&mut rng, n)
    }

    pub fn sample_drops(&self, n: usize) -> Vec<SocketAddr> {
        let mut rng = rand::rng();
        self.recent_drops.iter().copied().sample(&mut rng, n)
    }

    /// extract and remove n addresses from the pex set
    pub fn take_for_dial(&mut self, n: usize) -> Vec<SocketAddr> {
        let mut rng = rand::rng();
        let picked = self.known_addrs
            .iter()
            .copied()
            .sample(&mut rng, n);

        for addr in &picked {
            self.known_addrs.remove(addr);
        }

        picked
    }

    pub fn len(&self) -> usize {
        self.known_addrs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.known_addrs.is_empty()
    }

    fn record_drop(&mut self, addr: SocketAddr) {
        if self.recent_drops.contains(&addr) { return; }
        self.recent_drops.push(addr);
        if self.recent_drops.len() > MAX_RECENT_DROPS {
            self.recent_drops.remove(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> SocketAddr {
        format!("127.0.0.1:{}", 9000 + n as u16).parse().unwrap()
    }

    #[test]
    fn add_skips_self() {
        let me = addr(1);
        let mut pex = Pex::new(me, 100);
        pex.add(vec![me, addr(2), addr(3)]);
        assert_eq!(pex.len(), 2);
        assert!(!pex.known_addrs.contains(&me));
    }

    #[test]
    fn add_respects_cap() {
        let mut pex = Pex::new(addr(0), 100);
        let addrs: Vec<_> = (1..=200).map(addr).collect();
        pex.add(addrs);
        assert_eq!(pex.len(), 100);
    }

    #[test]
    fn sample_does_not_remove() {
        let mut pex = Pex::new(addr(0), 100);
        pex.add((1..=10).map(addr));
        let picked = pex.sample(5);
        assert!(picked.len() <= 5);
        assert_eq!(pex.len(), 10);   // unchanged
    }

    #[test]
    fn take_removes_what_it_returns() {
        let mut pex = Pex::new(addr(0), 100);
        pex.add((1..=10).map(addr));
        let picked = pex.take_for_dial(5);
        assert_eq!(picked.len(), 5);
        assert_eq!(pex.len(), 5);
        for a in &picked {
            assert!(!pex.known_addrs.contains(a));
        }
    }

    #[test]
    fn remove_drops_addrs() {
        let mut pex = Pex::new(addr(0), 100);
        pex.add((1..=5).map(addr));
        pex.remove(vec![addr(2), addr(4)]);
        assert_eq!(pex.len(), 3);
    }

    #[test]
    fn take_on_empty_pool_returns_empty() {
        let mut pex = Pex::new(addr(0), 100);
        assert!(pex.take_for_dial(5).is_empty());
    }

#[test]
    fn remove_records_drop_for_sampling() {
        let mut pex = Pex::new(addr(0), 100);
        pex.add((1..=5).map(addr));
        pex.remove(vec![addr(2), addr(4)]);

        let drops = pex.sample_drops(10);
        assert_eq!(drops.len(), 2);
        assert!(drops.contains(&addr(2)));
        assert!(drops.contains(&addr(4)));
    }

    #[test]
    fn removing_unknown_addr_is_not_gossipped() {
        let mut pex = Pex::new(addr(0), 100);
        pex.add((1..=3).map(addr));
        pex.remove(vec![addr(7)]);   // never in set
        assert!(pex.sample_drops(10).is_empty());
    }

    #[test]
    fn re_add_clears_pending_drop() {
        let mut pex = Pex::new(addr(0), 100);
        pex.add(vec![addr(2)]);
        pex.remove(vec![addr(2)]);
        assert_eq!(pex.sample_drops(10).len(), 1);

        pex.add(vec![addr(2)]);
        assert!(pex.sample_drops(10).is_empty());
    }

    #[test]
    fn recent_drops_bounded() {
        let mut pex = Pex::new(addr(0), 1000);
        pex.add((0u8..=99).map(addr));
        // Remove 100, but the cap is 50 — only the newest 50 survive.
        pex.remove((0u8..=99).map(addr));
        assert_eq!(pex.sample_drops(200).len(), MAX_RECENT_DROPS);
    }
}