pub use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeID(pub [u8; 32]);
pub struct InfoHash(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionID(pub [u8; 2]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Good,
    Questionable,
    Bad,
}
