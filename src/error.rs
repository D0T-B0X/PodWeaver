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
    #[error("Error of remote origin")]
    RemoteError(i32, String),
    #[error("Error, Peer token is either invalid or expired")]
    InvalidPeerToken,
    #[error("Error, Query could not be sent")]
    SendFailed,
    #[error("Error, yarn creation failed due to tokio task panic")]
    YarnCreationPanicked(String),
    #[error("Error, the file provided is empty")]
    EmptyFile,
    #[error("Error, the file provided does not exist")]
    FileNotFound,
    #[error("Error, IPv6 addresses are not yet supported")]
    IPv6NotSupported,
    #[error("Error, Request type is unrecognised")]
    InvalidRequestType,
    #[error("Error, An attempt was made to mark a missing piece complete without any requests")]
    PieceCompletionImpossibleWithoutRequest,
    #[error("Error, this piece was already complete")]
    PieceAlreadyComplete,
    #[error("Error, An attempt was made to mark a complete or missing piece as failed")]
    InvalidFailAttempt,
    #[error("Error, An operation was attempted on a piece that is not InFlight")]
    InvalidPieceOperation,
    #[error("Error, the queried block is already complete")]
    BlockAlreadyReceived,
    #[error("Error, The queried peer does not exist in the choke manager's table")]
    PeerNotFound,
    #[error("Error, The incoming frame is too large")]
    FrameTooLarge,
    #[error("Error, Unexpected response from remote pod")]
    InvalidPodResponse,
    #[error("Error, Response took too long")]
    ResponseTimedOut,
    #[error("Error, both pods attempted a connection at the same time")]
    MutualDial,
    #[error("Error, the message could not be sent because of some reason (wow really, what a great error message")]
    MessageNotSent,
    #[error("Error, the piece is not complete, there is atleast one block missing")]
    BlockMissing,
    #[error("Error, could not write bytes to file")]
    FileWriteFailed,
    #[error("Error, mismatched hash, likely a bad actor")]
    MaliciousPeer,
    #[error("Error, requested yarn does not exist on this pod")]
    YarnNotFound,
    #[error("Error, Bitfield length does not match piece count")]
    InvalidBitfieldLength,
}