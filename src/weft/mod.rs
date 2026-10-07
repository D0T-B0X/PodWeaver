pub mod bencode;

use std::fs;
use std::path::Path;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{BufReader, Read};

use bencode::*;
use crate::error::*;
use crate::types::*;

pub struct Weft {
    pub author: Option<String>,
    pub creation_date: Option<String>,
    pub comments: Option<String>,
    pub info: Info,
}

pub struct Info {
    pub filename: String,
    pub total_length: u64,
    pub piece_length: u32,
    pub pieces: Vec<[u8; 32]>,
}

impl Weft {
    pub fn new(
        author: Option<String>,
        creation_date: Option<String>,
        comments: Option<String>,
        filename: &Path,
    ) -> Result<Self> {
        let filepath = filename;
        let metadata = fs::metadata(filepath)?;
        let file_size = metadata.len();

        // dynamic piece sizing
        const MAX_PIECE_COUNT: u64 = 4096;

        let file_size_kb = file_size / 1024;
        let piece_len_kb = file_size_kb / MAX_PIECE_COUNT;
        let mut piece_len_bytes = piece_len_kb.next_power_of_two() * 1024;

        piece_len_bytes = std::cmp::min(piece_len_bytes, 524_288);
        piece_len_bytes = std::cmp::max(piece_len_bytes, 32_768);
        let num_pieces = file_size.div_ceil(piece_len_bytes);

        let hash_pieces: Vec<[u8; 32]> = get_pieces(filepath, piece_len_bytes as u32)?;

        if hash_pieces.len() != num_pieces as usize {
            return Err(Error::IncompletePieces);
        }

        Ok(Self {
            author,
            creation_date,
            comments,
            info: Info {
                pieces: hash_pieces,
                filename: String::from(filepath.to_str().unwrap()),
                total_length: file_size,
                piece_length: piece_len_bytes as u32,
            },
        })
    }

    pub fn from_file() -> Self {
        todo!();
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

        let mut info_dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
        info_dict.insert(b"filename".to_vec(), BencodeMap::ByteString(self.info.filename.as_bytes().to_vec()));
        info_dict.insert(b"total_length".to_vec(), BencodeMap::Integer(self.info.total_length as i64));
        info_dict.insert(b"piece_length".to_vec(), BencodeMap::Integer(self.info.piece_length as i64));
        info_dict.insert(b"pieces".to_vec(), BencodeMap::ByteString(self.info.pieces.concat()));

        bencode_dict.insert(b"info".to_vec(), BencodeMap::Dict(info_dict));

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
        let mut info_dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
        info_dict.insert(b"filename".to_vec(), BencodeMap::ByteString(self.info.filename.as_bytes().to_vec()));
        info_dict.insert(b"total_length".to_vec(), BencodeMap::Integer(self.info.total_length as i64));
        info_dict.insert(b"piece_length".to_vec(), BencodeMap::Integer(self.info.piece_length as i64));
        info_dict.insert(b"pieces".to_vec(), BencodeMap::ByteString(self.info.pieces.concat()));

        let bytes = encode_to_bytes(&BencodeMap::Dict(info_dict)).map_err(|_| Error::EncodeFail)?;
        Ok(InfoHash(Sha256::digest(&bytes).into()))
    }
}

fn get_pieces(filepath: &Path, piece_len: u32) -> Result<Vec<[u8; 32]>> {
    let file = fs::File::open(filepath)?;
    let mut reader = BufReader::new(file);
    let piece_len = piece_len as usize;
    let mut buf = vec![0u8; piece_len];
    let mut pieces: Vec<[u8; 32]> = Vec::new();

    loop {
        let mut filled = 0;
        while filled < piece_len {
            let n = reader.read(&mut buf[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }

        if filled == 0 {
            break;
        }

        let hash: [u8; 32] = Sha256::digest(&buf[..filled]).into();
        pieces.push(hash);

        if filled < piece_len {
            break;
        }
    }

    Ok(pieces)
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