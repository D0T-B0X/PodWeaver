pub mod bencode;

use std::fs;
use sha2::Digest;
use sha2::Sha256;
use std::path::Path;
use std::collections::BTreeMap;

use bencode::*;
use crate::error::*;
use crate::types::*;
use crate::storage::spool::Spool;

pub struct Info {
    pub filename: String,
    pub total_length: u64,
    pub piece_length: u32,
    pub pieces: Vec<[u8; 32]>,
}

impl Info {
    pub fn to_bencode(&self) -> BencodeMap {
        let mut info_dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
        info_dict.insert(b"filename".to_vec(), BencodeMap::ByteString(self.filename.as_bytes().to_vec()));
        info_dict.insert(b"total_length".to_vec(), BencodeMap::Integer(self.total_length as i64));
        info_dict.insert(b"piece_length".to_vec(), BencodeMap::Integer(self.piece_length as i64));
        info_dict.insert(b"pieces".to_vec(), BencodeMap::ByteString(self.pieces.concat()));

        BencodeMap::Dict(info_dict)
    }
}

pub struct Yarn {
    pub author: Option<String>,
    pub creation_date: Option<String>,
    pub comments: Option<String>,
    pub info: Info,
}

impl Yarn {
    pub async fn from_file(
        author: Option<String>,
        creation_date: Option<String>,
        comments: Option<String>,
        filepath: String
    ) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::from_file_blocking(author, creation_date, comments, &filepath))
            .await
            .map_err(|e| Error::YarnCreationPanicked(e.to_string()))?
    }

    fn from_file_blocking(
        author: Option<String>,
        creation_date: Option<String>,
        comments: Option<String>,
        filepath: &str
    ) -> Result<Self> {
        let metadata = fs::metadata(filepath)?;
        let file_size = metadata.len();

        // dynamic piece sizing
        const MAX_PIECE_COUNT: u64 = 4096;

        let file_size_kb = file_size / 1024;
        let piece_len_kb = file_size_kb / MAX_PIECE_COUNT;
        let mut piece_len_bytes= piece_len_kb.next_power_of_two() * 1024;

        piece_len_bytes = piece_len_bytes.clamp(32_768, 524_288);
        let hash_pieces: Vec<[u8; 32]> = get_piece_hashes(filepath, piece_len_bytes, file_size)?;

        let filename = Path::new(filepath)
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or(Error::UnexpectedFormat)?
            .to_string();

        Ok(Self {
            author,
            creation_date,
            comments,
            info: Info {
                pieces: hash_pieces,
                filename: filename,
                total_length: file_size,
                piece_length: piece_len_bytes as u32,
            },
        }) 
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bencode_dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();

        if let Some(a) = &self.author {
            bencode_dict.insert(b"author".to_vec(), BencodeMap::ByteString(a.as_bytes().to_vec()));
        }

        if let Some(c) = &self.comments {
            bencode_dict.insert(b"comments".to_vec(), BencodeMap::ByteString(c.as_bytes().to_vec()));
        }

        if let Some(d) = &self.creation_date {
            bencode_dict.insert(b"creation_date".to_vec(), BencodeMap::ByteString(d.as_bytes().to_vec()));
        }

        bencode_dict.insert(b"info".to_vec(), self.info.to_bencode());

        encode_to_bytes(&BencodeMap::Dict(bencode_dict)).map_err(|_| Error::EncodeFail)
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        let BencodeMap::Dict(bencode_dict) = decode_to_bencode(input)?.0 else {
            return Err(Error::UnexpectedFormat);
        };

        Ok(Self {
            author: get_optional_string(&bencode_dict, "author")?,
            creation_date: get_optional_string(&bencode_dict, "creation_date")?,
            comments: get_optional_string(&bencode_dict, "comments")?,
            info: bencode_to_info(&bencode_dict)?,
        })
    }

    pub fn info_hash(&self) -> Result<InfoHash> {
        let bytes = encode_to_bytes(&self
            .info
            .to_bencode())
            .map_err(|_| Error::EncodeFail)?;
        
        Ok(InfoHash(hash_bytes(&bytes)))
    }
}

fn get_piece_hashes(filepath: &str, piece_len: u64, total_len: u64) -> Result<Vec<[u8; 32]>> {
    let mut spool = Spool::new(filepath, piece_len, total_len)?;
    let mut pieces: Vec<[u8; 32]> = Vec::new();

    loop {
        let bytes = spool.read_next()?;
        if bytes.is_empty() {
            return Ok(pieces);
        }

        let hash = hash_bytes(&bytes);
        pieces.push(hash);
    }
}

fn bencode_to_info(dict: &BTreeMap<Vec<u8>, BencodeMap>) -> Result<Info> {
    let info_val = dict.get(b"info".as_slice()).ok_or(Error::InfoNotFound)?;

    let BencodeMap::Dict(info_dict) = info_val else {
        return Err(Error::UnexpectedFormat);
    };

    let filename = get_required_string(info_dict, "filename")?;
    let total_length = get_required_integer(info_dict, "total_length")?;
    let piece_length = get_required_integer(info_dict, "piece_length")?;
    let pieces = get_info_pieces(info_dict)?;

    Ok(Info {
        filename,
        total_length: total_length as u64,
        piece_length: piece_length as u32,
        pieces,
    })
}

fn get_optional_string(
    dict: &BTreeMap<Vec<u8>, BencodeMap>,
    key: &str,
) -> Result<Option<String>> {
    match dict.get(key.as_bytes()) {
        Some(BencodeMap::ByteString(bytes)) => {
            Ok(Some(String::from_utf8(bytes.clone()).map_err(|_| Error::UnexpectedFormat)?))
        }
        Some(_) => Err(Error::UnexpectedFormat),
        None => Ok(None),
    }
}

fn get_required_string(
    dict: &BTreeMap<Vec<u8>, BencodeMap>,
    key: &str,
) -> Result<String> {
    match dict.get(key.as_bytes()) {
        Some(BencodeMap::ByteString(bytes)) => {
            String::from_utf8(bytes.clone()).map_err(|_| Error::UnexpectedFormat)
        }
        _ => Err(Error::UnexpectedFormat),
    }
}

fn get_required_integer(
    dict: &BTreeMap<Vec<u8>, BencodeMap>,
    key: &str,
) -> Result<i64> {
    match dict.get(key.as_bytes()) {
        Some(BencodeMap::Integer(integer)) => Ok(*integer),
        _ => Err(Error::UnexpectedFormat),
    }
}

fn get_info_pieces(
    dict: &BTreeMap<Vec<u8>, BencodeMap>,
) -> Result<Vec<[u8; 32]>> {
    let Some(BencodeMap::ByteString(bytes)) = dict.get(b"pieces".as_slice()) else {
        return Err(Error::IncompletePieces);
    };

    if bytes.len() % 32 != 0 {
        return Err(Error::IncompletePieces);
    }

    Ok(bytes
        .chunks_exact(32)
        .map(|chunk| {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(chunk);
            arr
        })
        .collect())
}

fn hash_bytes(piece: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(piece);
    hasher.finalize().into()
}