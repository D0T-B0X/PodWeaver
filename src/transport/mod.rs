pub mod pending_map;

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use std::time::Duration;
use tokio::time::timeout;
use std::net::SocketAddr;
use tokio::sync::oneshot;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicU16;

use crate::error::Error;
use crate::types::Result;
use crate::types::NodeID;
use pending_map::PendingMap;
use crate::types::TransactionID;
use crate::dht::contact::Contact;
use crate::dht::krpc::KRPCMessage;
use crate::dht::krpc::QueryPayload;
use crate::dht::krpc::encode_message;
use crate::dht::krpc::decode_message;
use crate::dht::krpc::ResponsePayload;

const QUERY_TIMEOUT: Duration = Duration::from_secs(10); // 10 second query timeout

pub struct PendingQuery {
    pub to : SocketAddr,
    pub reply: oneshot::Sender<Result<ResponsePayload>>,
}

pub struct IncomingQuery {
    pub from: SocketAddr,
    pub transaction_id: TransactionID,
    pub payload: QueryPayload,
}

#[derive(Clone)]
pub struct Transport {
    contact: Contact,
    pending_map: PendingMap<TransactionID, PendingQuery>,
    next_tid: Arc<AtomicU16>,
}

impl Transport {
    pub fn new(contact: Contact, pending_map: PendingMap<TransactionID, PendingQuery>) -> Self {
        Self { 
            contact, 
            pending_map, 
            next_tid: Arc::new(AtomicU16::new(0)) 
        }
    }

    pub fn id(&self) -> &NodeID {
        &self.contact.id
    }

    fn alloc_tid(&self) -> TransactionID {
        TransactionID(self.next_tid.fetch_add(1, Ordering::Relaxed).to_be_bytes())
    }

    pub async fn query(&self, to: SocketAddr, payload: QueryPayload) -> Result<ResponsePayload> {
        let tid = self.alloc_tid();
        let message = KRPCMessage::Query { transaction_id: tid, payload };
        let bytes = encode_message(message)?;

        let (tx, rx) = oneshot::channel();
        self.pending_map.insert(tid, PendingQuery { to, reply: tx });
        
        if let Err(e) = self.contact.send(to, &bytes).await {
            self.pending_map.remove(&tid);
            return Err(e);
        }

        match timeout(QUERY_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            _ => {
                self.pending_map.remove(&tid);
                Err(Error::TransactionExpired)
            }
        }
    }

    pub async fn respond(
        &self, 
        to: SocketAddr, 
        tid: TransactionID, 
        payload: ResponsePayload
    ) -> Result<()> {
        let bytes = encode_message(KRPCMessage::Response { transaction_id: tid, payload })?;
        self.contact.send(to, &bytes).await
    }

    pub async fn run(
        self, 
        queries: mpsc::Sender<IncomingQuery>, 
        mut stop: watch::Receiver<bool>
    ) {
        // 2kB buffer
        let mut buffer = [0u8; 2048];
        loop {
            tokio::select! {
                _ = stop.changed() => break,
                res = self.contact.receive(&mut buffer) => {
                    let Ok((len, from)) = res else { continue };
                    let Ok(msg) = decode_message(&buffer[..len]) else { continue };
                    self.dispatch(from, msg, &queries);
                }
            }
        }
    }

    fn dispatch(&self, from: SocketAddr, msg: KRPCMessage, queries: &mpsc::Sender<IncomingQuery>) {
        match msg {
            KRPCMessage::Response { transaction_id, payload } => 
                self.complete(from, transaction_id, Ok(payload)),
            KRPCMessage::Error { transaction_id, code, message } =>
                self.complete(from, transaction_id, Err(Error::RemoteError(code, message))),
            KRPCMessage::Query { transaction_id, payload } => {
                let _ = queries.try_send(IncomingQuery { from, transaction_id, payload });
            }
        }
    }

    fn complete(&self, from: SocketAddr, tid: TransactionID, result: Result<ResponsePayload>) {
        if let Some(p) = self.pending_map.remove_if(&tid, |p| p.to == from) {
            let _ = p.reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeID;

    async fn start(id: NodeID) -> (Transport, SocketAddr, mpsc::Receiver<IncomingQuery>, watch::Sender<bool>) {
        let contact = Contact::with_node(id, "127.0.0.1:0".to_string()).await.unwrap();
        let addr = contact.local_addr().unwrap();
        let t = Transport::new(contact, PendingMap::new());
        let (qtx, qrx) = mpsc::channel(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        tokio::spawn(t.clone().run(qtx, stop_rx));
        (t, addr, qrx, stop_tx)
    }

    #[tokio::test]
    async fn ping_gets_a_reply() {
        let (a, _a_addr, _a_q, _a_stop) = start(NodeID([1; 32])).await;
        let (b, b_addr, mut b_queries, _b_stop) = start(NodeID([2; 32])).await;

        // b's stand-in for Dht: answer every ping
        tokio::spawn(async move {
            while let Some(q) = b_queries.recv().await {
                if let QueryPayload::Ping { .. } = q.payload {
                    let _ = b.respond(q.from, q.transaction_id, ResponsePayload::Id { id: NodeID([2; 32]) }).await;
                }
            }
        });

        let resp = a.query(b_addr, QueryPayload::Ping { id: NodeID([1; 32]) }).await.unwrap();
        assert!(matches!(resp, ResponsePayload::Id { id } if id == NodeID([2; 32])));
    }

    #[tokio::test]
    async fn silent_peer_times_out() {
        let (a, _, _q, _stop) = start(NodeID([1; 32])).await;
        let dead = "127.0.0.1:9".parse().unwrap();   // nothing listening
        let r = a.query(dead, QueryPayload::Ping { id: NodeID([1; 32]) }).await;
        assert!(matches!(r, Err(Error::TransactionExpired)));
    }
}