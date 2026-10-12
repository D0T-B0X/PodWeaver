pub use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeID(pub [u8; 32]);


impl From<InfoHash> for NodeID {
    fn from(info_hash: InfoHash) -> Self { NodeID(info_hash.0) }
}
impl From<&InfoHash> for NodeID {
    fn from(info_hash: &InfoHash) -> Self { NodeID(info_hash.0) }
}
impl TryFrom<&Vec<u8>> for NodeID {
    type Error = Error;

    fn try_from(bytes: &Vec<u8>) -> Result<Self> {
        let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| Error::UnexpectedFormat)?;
        Ok(NodeID(arr))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InfoHash(pub [u8; 32]);
impl TryFrom<&Vec<u8>> for InfoHash {
    type Error = Error;

    fn try_from(bytes: &Vec<u8>) -> Result<Self> {
        let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| Error::UnexpectedFormat)?;
        Ok(InfoHash(arr))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionID(pub [u8; 2]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Good,
    Questionable,
    Bad,
}
