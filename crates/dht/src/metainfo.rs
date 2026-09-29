mod bencode;
mod error;

use std::fs;
use sha2::{Sha256, Digest};
use std::io::{Read, BufReader};
use std::collections::BTreeMap;

use crate::metainfo::{error::*, bencode::*};
use crate::types::InfoHash;

pub struct MetaInfo {
    pub author: Option<String>,
    pub creation_date: Option<String>,
    pub comments: Option<String>,
    pub info: Info, 
}

pub struct Info {
    pub filename: String,
    pub total_length: u64,
    pub piece_length: u32,
    pieces: Vec<[u8; 32]>,
}

impl MetaInfo {

    pub fn new(
        author: Option<String>,
        creation_date : Option<String>,
        comments: Option<String>,
        filename: &str
    ) -> Result<Self> {
        let filepath = filename;
        let metadata = fs::metadata(&filepath)?;
        let file_size = metadata.len();

        const MAX_PIECE_COUNT: u64 = 4096;

        // piece lengths are dynamically scaled keeping max piece count 
        // below 4096, if the piece length is < 512kb
        let file_size_kb = file_size / 1024;
        let piece_len_kb = file_size_kb / MAX_PIECE_COUNT;
        let mut piece_len_bytes = piece_len_kb.next_power_of_two() * 1024;
        
        // The piece length is a power of 2 in [32kB, 512kB]
        piece_len_bytes = std::cmp::min(piece_len_bytes, 524288);
        piece_len_bytes = std::cmp::max(piece_len_bytes, 32768);
        let num_pieces = file_size.div_ceil(piece_len_bytes);

        let hash_pieces: Vec<[u8; 32]> = get_pieces(&filepath, piece_len_bytes as u32)?;

        // ensure all pieces are computed
        if hash_pieces.len() != num_pieces as usize { return Err(Error::IncompletePieces); }

        let metainfo = MetaInfo {
            author: author,
            creation_date: creation_date,
            comments: comments,
            info: Info {
                pieces: hash_pieces,
                filename: filepath.to_string(),
                total_length: file_size,
                piece_length: piece_len_bytes as u32,
            }
        };

        Ok(metainfo)
    }

    pub fn encode(self: &Self) -> Result<Vec<u8>> {
        let mut bencode_dict: BTreeMap<&str, BencodeMap> = BTreeMap::new();

        // check all optional fields and insert them if they are present
        if let Some(a) = &self.author {
            bencode_dict.insert("author", BencodeMap::ByteString(a.as_bytes()));
        }

        if let Some(c) = &self.comments {
            bencode_dict.insert("comments", BencodeMap::ByteString(c.as_bytes()));
        }

        if let Some(d) = &self.creation_date {
            bencode_dict.insert("creation_date", BencodeMap::ByteString(d.as_bytes()));
        }

        // convert the info subdictionary BTreeMap into a BencodeMap::Dict
        let mut info_dict: BTreeMap<&str, BencodeMap> = BTreeMap::new();
        info_dict.insert("filename", BencodeMap::ByteString(self.info.filename.as_bytes()));
        info_dict.insert("total_length", BencodeMap::Integer(self.info.total_length as i64));
        info_dict.insert("piece_length", BencodeMap::Integer(self.info.piece_length as i64));
        info_dict.insert("pieces", BencodeMap::ByteString(self.info.pieces.as_flattened()));

        bencode_dict.insert("info", BencodeMap::Dict(info_dict));

        encode_to_bytes(&BencodeMap::Dict(bencode_dict)).map_err(|_| Error::EncodeFail)
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        let BencodeMap::Dict(bencode_dict) = decode_to_bencode(input)?.0 else {
            return Err(Error::UnexpectedFormat)
        };

        Ok (Self {
            author: get_optional_string(&bencode_dict, "author")?,
            creation_date: get_optional_string(&bencode_dict, "creation_date")?,
            comments: get_optional_string(&bencode_dict, "comments")?,
            info: bencode_to_info(&bencode_dict)?
        })
    }

    pub fn info_hash(&self) -> Result<InfoHash> {
        let mut info_dict: BTreeMap<&str, BencodeMap> = BTreeMap::new();
        info_dict.insert("filename", BencodeMap::ByteString(self.info.filename.as_bytes()));
        info_dict.insert("total_length", BencodeMap::Integer(self.info.total_length as i64));
        info_dict.insert("piece_length", BencodeMap::Integer(self.info.piece_length as i64));
        info_dict.insert("pieces", BencodeMap::ByteString(self.info.pieces.as_flattened()));

        let bytes = encode_to_bytes(&BencodeMap::Dict(info_dict)).map_err(|_| Error::EncodeFail)?;
        Ok(InfoHash(Sha256::digest(&bytes).into()))
    }
} // impl MetaInfo

fn get_pieces(filepath: &str, piece_len: u32) -> Result<Vec<[u8; 32]>> {
    let file = fs::File::open(filepath)?;
    let mut reader = BufReader::new(file);
    let piece_len = piece_len as usize;
    let mut buf = vec![0u8; piece_len];
    let mut pieces: Vec<[u8; 32]> = Vec::new();

    loop {
        let mut filled = 0;
        while filled < piece_len {
            let n = reader.read(&mut buf[filled..])?;
            if n == 0 { break; }
            filled += n;
        }

        if filled == 0 { break; }

        let hash: [u8; 32] = Sha256::digest(&buf[..filled]).into();
        pieces.push(hash);

        if filled < piece_len { break; }
    }

    Ok(pieces)
}

fn bencode_to_info(dict: &BTreeMap<&str, BencodeMap>) -> Result<Info> {
    let info_val = dict.get("info").ok_or(Error::InfoNotFound)?;

    let BencodeMap::Dict(info_dict) = info_val else {
        return Err(Error::UnexpectedFormat);  
    };

    let filename = get_required_string(info_dict, "filename")?;
    let total_length = get_required_integer(info_dict, "total_length")?;  
    let piece_length = get_required_integer(info_dict, "piece_length")?;
    let pieces = get_info_pieces(info_dict)?;

    Ok(Info{
        filename: filename, 
        total_length: total_length as u64, 
        piece_length: piece_length as u32, 
        pieces: pieces
    })
}

fn get_optional_string(
    dict: &BTreeMap<&str, BencodeMap>,
    key: &str
) -> Result<Option<String>> {
    match dict.get(key) {
        Some(BencodeMap::ByteString(bytes)) => {
            Ok(Some(String::from_utf8(bytes.to_vec()).map_err(|_| Error::UnexpectedFormat)?))
        }
        Some(_) => return Err(Error::UnexpectedFormat),
        _ => Ok(None),
    }
}

fn get_required_string(
    dict: &BTreeMap<&str, BencodeMap>, 
    key: &str
) -> Result<String> {
    match dict.get(key) {
        Some(BencodeMap::ByteString(bytes)) => {
            String::from_utf8(bytes.to_vec()).map_err(|_| Error::UnexpectedFormat)
        }
        _ => Err(Error::UnexpectedFormat),
    }
}

fn get_required_integer(
    dict: &BTreeMap<&str, BencodeMap>, 
    key: &str
) -> Result<i64> {
    match dict.get(key) {
        Some(BencodeMap::Integer(integer)) => Ok(*integer),
        _ => Err(Error::UnexpectedFormat)
    }
}

fn get_info_pieces(
    dict: &BTreeMap<&str, BencodeMap>
) -> Result<Vec<[u8; 32]>> {
    let Some(BencodeMap::ByteString(bytes)) = dict.get("pieces") else {
        return Err(Error::IncompletePieces);
    };

    if bytes.len() % 32 != 0 { return Err(Error::IncompletePieces); }

    Ok(bytes.chunks_exact(32).map(|chunk| {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(chunk);
        arr
    }).collect())
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
