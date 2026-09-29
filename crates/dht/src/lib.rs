mod node;
mod types;
mod error;
mod contact;
mod routing;
mod storage;
mod metainfo;

use crate::{types::*, error::*, node::*, contact::*};

pub struct DHT {
    pub contact: Contact,
}

impl DHT {
    pub async fn new() -> Result<Self> {
        // TODO => Change to a dynamic port
        let contact = Contact::new("0.0.0.0:10001").await?;
        
        todo!();
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
