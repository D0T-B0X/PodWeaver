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
    UserLacksPrivileges,
    #[error("Error, NodeIDs are identical, it is the same node")]
    IdenticalKeys,
    #[error("Unexpected, ID bucket search exited")]
    UnexpectedIDSearchExit,
    #[error("Error, Could not find Transaction ID in Task Queue")]
    TransactionIDMissing,
    #[error("Error, Transaction could not be serviced before timeout")]
    TransactionExpired,
    #[error("Error, Port number beyond valid range")]
    InvalidPortNumber,
    #[error("Error, RPC does not recognize the command")]
    MethodUnknown,
    #[error("Error, The RPC message format is invalid")]
    InvalidRPCFormat,
    #[error("Unexpected, Pieces are incomplete")]
    IncompletePieces,
    #[error("Unexpected, The format of the dictionary is invalid")]
    UnexpectedFormat,
    #[error("Error, InfoHash encode failed")]
    EncodeFail,
    #[error("Critical, Info subdictionary not present")]
    InfoNotFound,
    #[error("Unexpected end of input")]
    UnexpectedEOF,
    #[error("Unexpected, Code does not match Bencode")]
    InvalidCode,
    #[error("Unexpected, String delimiter not found")]
    InvalidStringFormat,
    #[error("Unexpected, missing delimiter in code")]
    MissingDelimiter(u8),
    #[error("Error, Dictionary keys are not in lexical order")]
    InvalidKeyOrder,
}

pub type Result<T> = std::result::Result<T, Error>;