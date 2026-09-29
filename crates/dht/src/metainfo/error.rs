use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
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
    #[error("Error, could not access file")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
