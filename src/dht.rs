mod node;
mod contact;
mod routing;
pub mod krpc;

use std::sync::Arc;
use std::sync::Mutex;

use contact::*;
use routing::*;
use crate::error::*;
use crate::runtime::pending_map::*;

pub struct DHT {
    pub contact: Arc<Contact>,
    pub routing_table: Arc<Mutex<RoutingTable>>,
    pub task_map: PendingMap,
}

impl DHT {
    pub async fn new() -> Result<Self> {
        // TODO => Change to a dynamic port
        let contact = Contact::new("0.0.0.0:10001").await?;
        let routing_table = RoutingTable::new(contact.id.clone());
        let task_map = PendingMap::new();
        
        Ok(Self {
            contact,
            routing_table,
            task_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = 4;
        assert_eq!(result, 4);
    }
}
