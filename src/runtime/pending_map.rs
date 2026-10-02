use std::sync::Mutex;
use tokio::sync::oneshot;
use std::collections::HashMap;

use crate::dht::krpc::*;
use crate::types::*;
use crate::error::*;

pub struct PendingMap {
    queries: Mutex<HashMap<TransactionID, oneshot::Sender<KRPCMessage>>>,
}

impl PendingMap {
    pub fn new() -> Self {
        Self { queries: Mutex::new(HashMap::new()) }
    }

    pub fn register(&self, tid: TransactionID) -> oneshot::Receiver<KRPCMessage> {
        let mut q = self.queries.lock().unwrap();
        let (tx, rx) = oneshot::channel();
        q.insert(tid, tx);
        rx
    }

    pub fn respond(&self, tid: &TransactionID, message: KRPCMessage) -> Result<()> {
        let mut q = self.queries.lock().unwrap();
        if let Some(tx) = q.remove(tid) {
            tx.send(message).map_err(|_| Error::TransactionExpired)?;
            Ok(())
        } else {
            Err(Error::TransactionIDMissing)
        }
    }

    pub fn cancel(&self, tid: &TransactionID) {
        self.queries.lock().unwrap().remove(tid);
    }
}