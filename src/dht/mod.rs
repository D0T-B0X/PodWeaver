pub mod node;
pub mod krpc;
mod peer_store;
pub mod routing;
mod token_store;

use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashSet;

use routing::*;
use node::Node;
use krpc::QueryPayload;
use tokio::sync::watch;
use crate::error::Error;
use crate::types::Result;
use crate::types::NodeID;
use peer_store::PeerStore;
use krpc::ResponsePayload;
use crate::types::InfoHash;
use token_store::TokenStore;
use crate::transport::Transport;
use crate::transport::IncomingQuery;

pub enum GetPeersOutput {
    Peers {
        token: Vec<u8>,
        peers: Vec<SocketAddr>,
    },
    Closest {
        token: Vec<u8>,
        nodes: Vec<Node>,
    }
}

#[derive(Clone)]
pub struct DHT {
    pub transport: Transport,
    pub routing_table: Arc<Mutex<RoutingTable>>,
    pub peer_store: Arc<Mutex<PeerStore>>,
    pub token_store: Arc<Mutex<TokenStore>>,
}

impl DHT {
    pub fn new(id: NodeID, transport: Transport) -> Result<Self> {
        let routing_table = RoutingTable::new(id);
        let peer_store = PeerStore::new(Duration::from_secs(30 * 60));
        let token_store = TokenStore::new();
        
        Ok(Self {
            transport,
            routing_table,
            peer_store,
            token_store
        })
    }

    fn touch(&self, id: NodeID, addr: SocketAddr) {
        if id == self.id() { return; }

        let node = Node::new(id, addr);
        self.routing_table.lock().unwrap().insert(node);
    }

    fn id(&self) -> NodeID {
        *self.transport.id()
    }

    // Initialize the system with at most 10 bootstrap nodes that kickstart the DHT engine
    pub async fn bootstrap(&self, start_nodes: Vec<SocketAddr>) -> usize {
        let mut seen: HashSet<SocketAddr> = HashSet::new();
        let mut online: usize = 0;

        for node in start_nodes.into_iter() {

            // ping bootstrap nodes and them to DHT if online
            if !seen.insert(node) { continue; }
            if self.ping(node).await.is_ok() {
                online += 1;
                if online >= 10 { return online; }
            }

            // if 10 nodes have not been found, expand the search using those nodes
            if let Ok(nodes) = self.find_node(self.id(), node).await {
                for(_id, addr) in nodes {
                    if !seen.insert(addr) { continue; }
                    if self.ping(addr).await.is_ok() {
                        online += 1;
                        if online >= 10 { return online; }
                    }
                }
            }
        };

        online
    }

    // ---------------
    // Query response
    // ---------------

    fn answer(&self, sender: &SocketAddr, payload: &QueryPayload) -> Result<Option<ResponsePayload>> {
        match payload {
            QueryPayload::Ping { .. } => Ok(Some(ResponsePayload::Id { id: *self.transport.id() })),

            QueryPayload::FindNode { target, .. } => {
                let nodes = self.routing_table.lock().unwrap()
                    .closest(target)
                    .into_iter()
                    .map(|n| (n.id, n.address))
                    .collect();

                Ok(Some(ResponsePayload::Nodes { id: *self.transport.id(), nodes }))
            },

            QueryPayload::GetPeers { info_hash, .. } => {
                let token = self.token_store.lock().unwrap().new_token(sender.ip());

                if let Some(peers) = self.peer_store.lock().unwrap().get_peers(info_hash) {
                    Ok(Some(ResponsePayload::Peers { id: self.id(), token, values: peers }))
                } else {
                    let closest_nodes = self.routing_table.lock().unwrap().closest(&info_hash.into());
                    let closest = closest_nodes.iter().map(|n| (n.id, n.address)).collect();
                    Ok(Some(ResponsePayload::Closer { id: self.id(), token, nodes: closest }))
                }
            },

            QueryPayload::AnnouncePeer { port, info_hash, token, .. } => {
                let is_valid = self.token_store.lock().unwrap().validate_token(token, sender.ip());
                if !is_valid {
                    return Err(Error::InvalidPeerToken);
                }

                let peer = SocketAddr::new(sender.ip(), *port);

                self.peer_store.lock().unwrap().announce(info_hash, peer);
                Ok(Some(ResponsePayload::Id { id: self.id() }))
            }
        }
    }

    // --------------------------------
    // Query Creation and Transmission
    // --------------------------------

    // Async runtime
    pub async fn run(self, mut queries: mpsc::Receiver<IncomingQuery>, mut stop: watch::Receiver<bool>) -> Result<()> {
        
        if *stop.borrow() {
            return Ok(());
        }

        loop {
            tokio::select! {
                _ = stop.changed() => return Ok(()),
                Some(q) = queries.recv() => {
                    let _ = self.touch(q.payload.sender_id(), q.from);
                    if let Ok(Some(resp)) = self.answer(&q.from, &q.payload) {
                        let _ = self.transport.respond(q.from, q.transaction_id, resp).await;
                    }
                }
                else => return Ok(()),
            }
        }
    }

    /**
     * 
     */
    pub async fn ping(&self, addr: SocketAddr) -> Result<()> {
        let resp = self.transport.query(addr,QueryPayload::Ping { id: self.id() }).await?;
        self.touch(resp.sender_id(), addr);
        Ok(())
    }

    pub async fn find_node(&self, id: NodeID, addr: SocketAddr) -> Result<Vec<(NodeID, SocketAddr)>> {
        let resp = self.transport
            .query(addr, QueryPayload::FindNode { id: self.id(), target: id })
            .await?;
        self.touch(resp.sender_id(), addr);

        match resp {
            ResponsePayload::Nodes { nodes, .. } | ResponsePayload::Closer { nodes, .. } => Ok(nodes),
            _ => Err(Error::UnexpectedFormat)
        }
    }

    pub async fn get_peers(&self, addr: SocketAddr, info_hash: InfoHash) -> Result<GetPeersOutput> {
        let resp = self.transport
            .query(addr, QueryPayload::GetPeers { id: *self.transport.id(), info_hash })
            .await?;
        self.touch(resp.sender_id(), addr);

        match resp {
            ResponsePayload::Peers { token, values, .. } => {
                Ok(GetPeersOutput::Peers { 
                    token, 
                    peers: values 
                })
            },

            ResponsePayload::Closer { token, nodes, .. } => {
                let nodes = nodes
                    .into_iter()
                    .map(|(id, addr)| Node::new(id, addr))
                    .collect();
                
                Ok(GetPeersOutput::Closest { 
                    token, 
                    nodes 
                })
            },

            _ => Err(Error::UnexpectedFormat)
        }
    }

    pub async fn announce_peer(
        &self, 
        addr: SocketAddr,
        info_hash: InfoHash, 
        port: u16, 
        token: Vec<u8>
    ) -> Result<()> {
        let resp = self
            .transport
            .query(addr, QueryPayload::AnnouncePeer { id: self.id(), info_hash, port, token })
            .await?;

            let resp_id = resp.sender_id().clone();

            match resp {
            ResponsePayload::Id { .. } => {
                self.touch(resp_id, addr);
                Ok(())
            }
            _ => Err(Error::UnexpectedFormat)
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use crate::transport::Transport;
    use crate::transport::contact::Contact;
    use crate::transport::pending_map::PendingMap;

use super::*;
    
    async fn spawn_dht(id: NodeID) -> Result<(DHT, SocketAddr, watch::Sender<bool>)> {
        let contact = Contact::with_node(id, "127.0.0.1:0").await?;
        let addr = contact.local_addr().unwrap();
        let transport = Transport::new(contact, PendingMap::new());
        let dht = DHT::new(*transport.id(), transport.clone())?;

        let (qtx, qrx) = mpsc::channel(32);
        let (stop_tx, stop_rx) = watch::channel(false);
        tokio::spawn(transport.run(qtx, stop_rx.clone()));
        tokio::spawn(dht.clone().run(qrx, stop_rx));

        Ok((dht, addr, stop_tx))
    }

    #[tokio::test]
    async fn ping_fills_both_routing_tables() {
        let (a, _, _stop_a) = spawn_dht(NodeID([1; 32])).await.unwrap();
        let (b, b_addr, _stop_b) = spawn_dht(NodeID([2; 32])).await.unwrap();

        a.ping(b_addr).await.unwrap();

        assert!(a.routing_table.lock().unwrap().contains(&NodeID([2; 32])));
        assert!(b.routing_table.lock().unwrap().contains(&NodeID([1; 32])));
    }
}   