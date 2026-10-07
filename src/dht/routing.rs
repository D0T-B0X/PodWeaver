use std::sync::{Mutex, Arc};

use crate::types::*;
use crate::dht::node::*;

pub const K: usize = 20;
const BIT_SPACE: usize = 256;

pub struct Bucket{
    pub nodes: Vec<Node>,
}

impl Bucket {
    pub fn new() -> Self {
        Self { nodes: Vec::new(), }
    }

    pub fn insert(&mut self, node: Node) {
        if let Some(pos) = self.nodes.iter().position(|n| n.id == node.id) {
            self.nodes.remove(pos);
            self.nodes.push(node);
            return;
        }

        if self.nodes.len() < K {
            self.nodes.push(node);
            return;
        }

        if  let Some(last_node) = self.nodes.last() {
            match last_node.status {
                NodeStatus::Good => return,
                _ => { self.nodes.pop(); }
            }
        }

        self.nodes.push(node);
    }

    pub fn closest(&self) -> &[Node] {
        &self.nodes
    }

    pub fn exists(&self, target: &NodeID) -> bool {
        self.nodes.iter().any(|n| n.id == *target)
    }

    pub fn shake(&self) -> Vec<Node> {
        self.nodes.clone()
    }

    pub fn pluck(&mut self, target: &NodeID) {
        if let Some(pos) = self.nodes.iter().position(|n| n.id == *target) {
            self.nodes.remove(pos);
        }
    }
}

pub struct RoutingTable {
    pub id: NodeID,
    pub buckets: Vec<Bucket>, 
}

impl RoutingTable {
    pub fn new(self_id: NodeID) -> Arc<Mutex<Self>> {
        Arc::new(
            Mutex::new(
                Self {
                    id: self_id,
                    buckets: (0..BIT_SPACE).map(|_| Bucket::new()).collect(),
                }
            )
        )
    }

    pub fn insert(&mut self, node: Node) {
        if node.id == self.id { return; }
        let b_idx = bucket_index(&node.id, &self.id);
        self.buckets[b_idx].insert(node);
    }

    pub fn closest(&self, target: &NodeID) -> Vec<Node> {
        let target_id = NodeID(target.0);
        let closest_bucket = bucket_index(&target_id, &self.id);

        let mut closest_nodes: Vec<Node> = Vec::with_capacity(K * 2);
        closest_nodes.extend(self.buckets[closest_bucket].nodes.iter().cloned());

        let mut it = 1;
        while closest_nodes.len() < K && (closest_bucket > it || closest_bucket + it < BIT_SPACE) {
            if it <= closest_bucket {
                closest_nodes.extend(self.buckets[closest_bucket - it].nodes.iter().cloned());
            }
            
            if closest_bucket + it < BIT_SPACE {
                closest_nodes.extend(self.buckets[closest_bucket + it].nodes.iter().cloned());
            }
            
            it += 1;
        }

        closest_nodes.sort_by_key(|n| xor_nodes(&n.id, &target_id));
        closest_nodes
    }

    pub fn contains(&self, target: &NodeID) -> bool {
        self.buckets.iter().any(|bucket| bucket.exists(target))
    }

    pub fn shake(&self) -> Vec<Node> {
        self.buckets
            .iter()
            .flat_map(|b| b.shake())
            .collect()
    }

    pub fn pluck(&mut self, target: &NodeID) {
        let bucket_pos = bucket_index(&self.id, target);
        self.buckets[bucket_pos].pluck(target);
    }
}

fn xor_nodes(a: &NodeID, b: &NodeID) -> [u8; 32] {
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = a.0[i] ^ b.0[i];
    }
    out
}

fn bucket_index(node: &NodeID, self_node: &NodeID) -> usize {
    if node.0 == self_node.0 {
        return 0;
    }

    let xor_dist = xor_nodes(node, self_node);
    for (byte_idx, &byte) in xor_dist.iter().enumerate() {
        if byte != 0 {
            let leading_zeroes = byte.leading_zeros() as usize;
            return byte_idx * 8 + leading_zeroes;
        }
    }

    return 0;
}