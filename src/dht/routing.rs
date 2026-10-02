use std::sync::{Mutex, Arc};

use crate::error::*;
use crate::types::*;
use crate::dht::node::*;

const K: u8 = 20;
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

        // TODO => Update after PING implementation
        if self.nodes.len() == K as usize {
            self.nodes.remove(0);
            self.nodes.push(node);
            return;
        }

        self.nodes.push(node);
    }

    pub fn closest(&self) -> &[Node] {
        &self.nodes
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

    pub fn insert(&mut self, node: Node) -> Result<()> {
        if node.id == self.id { return Ok(()); }
        let b_idx = bucket_index(&node.id, &self.id)?;
        self.buckets[b_idx].insert(node);
        Ok(())
    }

    pub fn closest(&self, target: &InfoHash, node_count: usize) -> Result<Vec<Node>> {
        let target_id = NodeID(target.0);
        let closest_bucket = bucket_index(&target_id, &self.id)?;

        let mut closest_nodes: Vec<Node> = Vec::with_capacity(node_count * 2);
        closest_nodes.extend(self.buckets[closest_bucket].nodes.iter().cloned());

        let mut it = 1;
        while closest_nodes.len() < node_count && (closest_bucket > it || closest_bucket + it < BIT_SPACE) {
            if it <= closest_bucket {
                closest_nodes.extend(self.buckets[closest_bucket - it].nodes.iter().cloned());
            }
            
            if closest_bucket + it < BIT_SPACE {
                closest_nodes.extend(self.buckets[closest_bucket + it].nodes.iter().cloned());
            }
            
            it += 1;
        }

        closest_nodes.sort_by_key(|n| xor_nodes(&n.id, &target_id));
        closest_nodes.truncate(node_count);
        Ok(closest_nodes)
    }
}

fn xor_nodes(a: &NodeID, b: &NodeID) -> [u8; 32] {
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = a.0[i] ^ b.0[i];
    }
    out
}

fn bucket_index(node: &NodeID, self_node: &NodeID) -> Result<usize> {
    if node.0 == self_node.0 {
        return Err(Error::IdenticalKeys);
    }

    let xor_dist = xor_nodes(node, self_node);
    for (byte_idx, &byte) in xor_dist.iter().enumerate() {
        if byte != 0 {
            let leading_zeroes = byte.leading_zeros() as usize;
            return Ok(byte_idx * 8 + leading_zeroes);
        }
    }

    Err(Error::UnexpectedIDSearchExit) // What the fuck is going on dawg???
}