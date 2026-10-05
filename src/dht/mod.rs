pub mod node;
pub mod krpc;
pub mod contact;
pub mod routing;

use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use std::net::SocketAddr;

use routing::*;
use node::Node;
use krpc::QueryPayload;
use tokio::sync::watch;
use crate::error::Error;
use crate::types::Result;
use crate::types::NodeID;
use krpc::ResponsePayload;
use crate::transport::Transport;
use crate::transport::IncomingQuery;

#[derive(Clone)]
pub struct DHT {
    pub transport: Transport,
    pub routing_table: Arc<Mutex<RoutingTable>>,
}

impl DHT {
    pub fn new(id: NodeID, transport: Transport) -> Result<Self> {
        let routing_table = RoutingTable::new(id);
        
        Ok(Self {
            transport,
            routing_table,
        })
    }

    fn touch(&self, id: NodeID, addr: SocketAddr) -> Result<()> {
        let node = Node::new(id, addr);
        self.routing_table.lock().unwrap().insert(node)
    }

    // ---------------
    // Query response
    // ---------------

    fn answer(&self, payload: &QueryPayload) -> Option<ResponsePayload> {
        match payload {
            QueryPayload::Ping { .. } => Some(ResponsePayload::Id { id: *self.transport.id() }),

            QueryPayload::FindNode { target, .. } => {
                let nodes = self.routing_table.lock().unwrap()
                    .closest(target, K.into())
                    .into_iter()
                    .map(|n| (n.id, n.address))
                    .collect();

                Some(ResponsePayload::Nodes { id: *self.transport.id(), nodes })
            },

            _ => None,
        }
    }

    fn id(&self) -> NodeID {
        *self.transport.id()
    }

    // ----------------
    // Query servicing
    // ----------------

    // Async runtime
    pub async fn run(self, mut queries: mpsc::Receiver<IncomingQuery>, mut stop: watch::Receiver<bool>) -> Result<()> {
        loop {
            tokio::select! {
                _ = stop.changed() => return Ok(()),
                Some(q) = queries.recv() => {
                    self.touch(q.payload.sender_id(), q.from)?;
                    if let Some(resp) = self.answer(&q.payload) {
                        let _ = self.transport.respond(q.from, q.transaction_id, resp).await;
                    }
                }
                else => return Ok(()),
            }
        }
    }

    pub async fn ping(&self, addr: SocketAddr) -> Result<()> {
        let resp = self.transport.query(addr,QueryPayload::Ping { id: self.id() }).await?;
        self.touch(resp.sender_id(), addr)
    }

    pub async fn find_node(&self, target: &Node) -> Result<Vec<(NodeID, SocketAddr)>> {
        let resp = self.transport
            .query(target.address, QueryPayload::FindNode { id: self.id(), target: target.id })
            .await?;
        self.touch(resp.sender_id(), target.address)?;

        match resp {
            ResponsePayload::Nodes { nodes, .. } | ResponsePayload::Closer { nodes, .. } => Ok(nodes),
            _ => Err(Error::UnexpectedFormat)
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use contact::Contact;
    use crate::transport::Transport;
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