#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeID(pub [u8; 32]);
pub struct InfoHash(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Good,
    Questionable,
    Bad,
}
