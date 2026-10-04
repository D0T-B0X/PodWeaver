mod node;
mod krpc;
mod contact;
mod routing;
mod transport;

use std::sync::Arc;
use std::sync::Mutex;

use contact::*;
use routing::*;
use super::types::Result;

#[derive(Clone)]
pub struct DHT {
    pub contact: Contact,
    pub routing_table: Arc<Mutex<RoutingTable>>,
}

impl DHT {
    pub async fn new() -> Result<Self> {
        // TODO => Change to a dynamic port
        let contact = Contact::new("0.0.0.0:10001").await?;
        let routing_table = RoutingTable::new(contact.id.clone());
        
        Ok(Self {
            contact,
            routing_table,
        })
    }


}