use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error, Could not establish a connection")]
    ConnectionFailed,
    #[error("Error, PodWeaver home not found")]
    HomeNotFound,
    #[error("Unexpected, error when accessing PodWeaver files")]
    FileError(#[from] std::io::Error),
    #[error("What? User Home does not exist")]
    UserHomeMissing, // The fuck
    #[error("Error, Morphed NodeID")]
    CorruptedNodeID,
    #[error("Error, File could not be read")]
    FileReadFailed,
    #[error("Error, User privilege insufficient for file access")]
    UserLacksPrivilages,
    #[error("Error, NodeIDs are identical, it is the same node")]
    IdenticalKeys,
    #[error("Unexpected, ID bucket search exited")]
    UnexpectedIDSearchExit
}

pub type Result<T> = std::result::Result<T, Error>;