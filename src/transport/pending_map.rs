use std::sync::Arc;
use std::sync::Mutex;
use std::collections::HashMap;

pub struct PendingMap<K, V> {
    queries: Arc<Mutex<HashMap<K, V>>>,
}

impl<K: Eq + std::hash::Hash, V> Clone for PendingMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            queries: Arc::clone(&self.queries),
        }
    }
}

impl<K, V> PendingMap<K, V> 
where 
    K: std::hash::Hash + Eq,
{
    pub fn new() -> Self {
        Self { queries: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.queries.lock().unwrap().insert(key, value)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.queries.lock().unwrap().remove(key)
    }

    pub fn remove_if(&self, key: &K, cond: impl FnOnce(&V) -> bool) -> Option<V> {
        let mut map = self.queries.lock().unwrap();
        if map.get(key).is_some_and(cond) {
            map.remove(key)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.queries.lock().unwrap().len()
    }
}

impl<K: Eq + std::hash::Hash, V> Default for PendingMap<K, V> {
    fn default() -> Self { Self::new() }
} 

#[cfg(test)]
mod tests {
    use crate::types::TransactionID;

    use super::*;
    use tokio::sync::oneshot;

    #[test]
    fn insert_and_remove() {
        let map: PendingMap<u16, &str> = PendingMap::new();
        assert!(map.insert(1, "a").is_none());
        assert_eq!(map.remove(&1), Some("a"));
        assert_eq!(map.remove(&1), None);
    }

    #[test]
    fn remove_if_respects_the_check() {
        let map: PendingMap<u16, u32> = PendingMap::new();
        map.insert(1, 10);

        assert_eq!(map.remove_if(&1, |v| *v == 99), None);   // check fails: stays
        assert_eq!(map.len(), 1);
        assert_eq!(map.remove_if(&1, |v| *v == 10), Some(10)); // check passes: removed
        assert_eq!(map.len(), 0);
    }

    #[tokio::test]
    async fn reply_reaches_the_waiter() {
        let map: PendingMap<TransactionID, oneshot::Sender<&'static str>> = PendingMap::new();
        let (tx, rx) = oneshot::channel();
        map.insert(TransactionID([0u8; 2]), tx);

        let waiter = tokio::spawn(async move { rx.await.unwrap() });

        map.remove(&TransactionID([0u8; 2])).unwrap().send("pong").unwrap();
        assert_eq!(waiter.await.unwrap(), "pong");
    }
}