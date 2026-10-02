use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::oneshot;
use std::collections::HashMap;

use crate::error::*;

pub struct PendingMap<K, V> {
    queries: Arc<Mutex<HashMap<K, oneshot::Sender<V>>>>,
}

impl<K, V> Clone for PendingMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            queries: Arc::clone(&self.queries),
        }
    }
}

impl<K, V> PendingMap<K, V> 
where 
    K: std::hash::Hash + Eq + Send + 'static,
    V: Send + 'static
{
    pub fn new() -> Self {
        Self { queries: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn insert(&self, key: K) -> oneshot::Receiver<V> {
        let (tx, rx) = oneshot::channel();
        let mut q = self.queries.lock().unwrap();
        q.insert(key, tx);
        rx
    }

    pub fn respond(&self, key: K, message: V) -> Result<()> {
        let mut q = self.queries.lock().unwrap();
        if let Some(tx) = q.remove(&key) {
            tx.send(message).map_err(|_| Error::TransactionExpired)?;
            Ok(())
        } else {
            Err(Error::TransactionIDMissing)
        }
    }

    pub fn cancel(&self, key: &K) {
        self.queries.lock().unwrap().remove(key);
    }
}