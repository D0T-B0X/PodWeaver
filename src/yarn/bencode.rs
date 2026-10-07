use std::collections::BTreeMap;
use std::net::SocketAddr;

use crate::error::*;
use crate::types::*;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BencodeMap {
    Integer(i64),
    ByteString(Vec<u8>),
    List(Vec<BencodeMap>),
    Dict(BTreeMap<Vec<u8>, BencodeMap>), 
}

impl From<&str> for BencodeMap {
    fn from(s: &str) -> Self { BencodeMap::ByteString(s.to_string().as_bytes().to_vec()) }
}
impl From<BTreeMap<Vec<u8>, BencodeMap>> for BencodeMap {
    fn from(dict: BTreeMap<Vec<u8>, BencodeMap>) -> Self { BencodeMap::Dict(dict) }
}
impl From<InfoHash> for BencodeMap {
    fn from(infohash: InfoHash) -> Self { BencodeMap::ByteString(infohash.0.to_vec()) }
}
impl From<NodeID> for BencodeMap {
    fn from(id: NodeID) -> Self { BencodeMap::ByteString(id.0.to_vec()) }
}
impl From<&[u8]> for BencodeMap {
    fn from(bytes: &[u8]) -> Self { BencodeMap::ByteString(bytes.to_vec()) }
}
impl From<u16> for BencodeMap {
    fn from(int: u16) -> Self { BencodeMap::Integer(int as i64) }
}
impl From<Vec<u8>> for BencodeMap {
    fn from(v: Vec<u8>) -> Self { BencodeMap::ByteString(v) }
}
impl From<String> for BencodeMap {
    fn from(s: String) -> Self { BencodeMap::ByteString(s.into_bytes()) }
}
impl From<i32> for BencodeMap {
    fn from(n: i32) -> Self { BencodeMap::Integer(n as i64) }
}
impl From<TransactionID> for BencodeMap {
    fn from(t: TransactionID) -> Self { BencodeMap::ByteString(t.0.to_vec()) }
}
impl From<Vec<BencodeMap>> for BencodeMap {
    fn from(v: Vec<BencodeMap>) -> Self { BencodeMap::List(v) }
}
impl From<Vec<(NodeID, SocketAddr)>> for BencodeMap {
    fn from(nodes: Vec<(NodeID, SocketAddr)>) -> Self {
        BencodeMap::ByteString(nodes.into_iter()
            .filter_map(|(id, addr)| match addr {
                SocketAddr::V4(v4) => Some((id, v4)),
                _ => None,                
            })
            .flat_map(|(id, v4)| {
                let mut buf = [0u8; 38];
                buf[..32].copy_from_slice(&id.0.to_vec());
                buf[32..36].copy_from_slice(&v4.ip().octets());
                buf[36..38].copy_from_slice(&v4.port().to_be_bytes());
                buf
            })
            .collect())
    }
}

// *************************** 
// ****** Public APIs  *******
// ***************************

pub fn decode_to_bencode(input_code: &[u8]) -> Result<(BencodeMap, &[u8])> {
    let (&first, rest) = input_code.split_first().ok_or(Error::UnexpectedEOF)?;

    match first {
        b'i' => parse_int(rest),
        b'l' => parse_list(rest),
        b'd' => parse_dict(rest),
        b'0'..=b'9' => parse_str(input_code),
        _ => Err(Error::InvalidCode),
    }
}

pub fn encode_to_bytes(input: &BencodeMap) -> Result<Vec<u8>> {
    let mut buffer: Vec<u8> = Vec::new();
    encode_value(input, &mut buffer);
    Ok(buffer)
}

// -------- Private Functions --------

fn parse_int(input: &[u8]) -> Result<(BencodeMap, &[u8])> {
    let end_pos = input
        .iter()
        .position(|&b| b == b'e')
        .ok_or(Error::MissingDelimiter(b'e'))?;

    let int_bytes = &input[..end_pos];
    let remainder = &input[end_pos + 1..];

    if int_bytes.is_empty() {
        return Err(Error::InvalidCode);
    }

    // Check for leading zeros: "01", "-0", "-01" are invalid
    if int_bytes.len() > 1 && int_bytes[0] == b'0' {
        return Err(Error::UnexpectedFormat);
    }
    if int_bytes.starts_with(b"-0") {
        return Err(Error::UnexpectedFormat);
    }

    let int_str = std::str::from_utf8(int_bytes).map_err(|_| Error::InvalidCode)?;
    let integer: i64 = int_str.parse().map_err(|_| Error::InvalidCode)?;

    Ok((BencodeMap::Integer(integer), remainder))
}

fn parse_str(input: &[u8]) -> Result<(BencodeMap, &[u8])> {
    let colon_pos = input
        .iter()
        .position(|&b| b == b':')
        .ok_or(Error::InvalidStringFormat)?;

    let len_bytes = &input[..colon_pos];

    // Reject leading zeros in string lengths (e.g. "04:test")
    if len_bytes.len() > 1 && len_bytes[0] == b'0' {
        return Err(Error::InvalidStringFormat);
    }

    let len_str = std::str::from_utf8(len_bytes).map_err(|_| Error::InvalidCode)?;
    let len: usize = len_str.parse().map_err(|_| Error::InvalidCode)?;

    let str_start = colon_pos + 1;
    let str_end = str_start.checked_add(len).ok_or(Error::UnexpectedEOF)?;

    if input.len() < str_end {
        return Err(Error::UnexpectedEOF);
    }

    let value = &input[str_start..str_end];
    let remainder = &input[str_end..];

    Ok((BencodeMap::ByteString(value.to_vec()), remainder))
}

fn parse_list(mut input: &[u8]) -> Result<(BencodeMap, &[u8])> {
    let mut list: Vec<BencodeMap> = Vec::new();

    while !input.starts_with(b"e") {
        if input.is_empty() {
            return Err(Error::UnexpectedEOF);
        }

        let (val, rest) = decode_to_bencode(input)?;
        list.push(val);
        input = rest;
    }

    Ok((BencodeMap::List(list), &input[1..]))
}

fn parse_dict(mut input: &[u8]) -> Result<(BencodeMap, &[u8])> {
    let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
    let mut last_key: Option<Vec<u8>> = None;

    while !input.starts_with(b"e") {
        if input.is_empty() {
            return Err(Error::UnexpectedEOF);
        }

        let (key, rest) = decode_to_bencode(input)?;
        input = rest;

        let key_bytes = match key {
            BencodeMap::ByteString(b) => b,
            _ => return Err(Error::InvalidCode),
        };

        // Enforce strict Bencode lexicographical key ordering
        if let Some(ref prev) = last_key {
            if prev >= &key_bytes {
                return Err(Error::InvalidKeyOrder);
            }
        }
        last_key = Some(key_bytes.clone());

        let (val, rest) = decode_to_bencode(input)?;
        dict.insert(key_bytes, val);
        input = rest;
    }

    Ok((BencodeMap::Dict(dict), &input[1..]))
}

fn encode_value(input: &BencodeMap, buf: &mut Vec<u8>) {
    match input {
        BencodeMap::Dict(dict) => {
            buf.push(b'd');
            for (k, v) in dict {
                buf.extend_from_slice(k.len().to_string().as_bytes());
                buf.push(b':');
                buf.extend_from_slice(k);
                encode_value(v, buf);
            }
            buf.push(b'e');
        }

        BencodeMap::List(list) => {
            buf.push(b'l');
            for item in list {
                encode_value(item, buf);
            }
            buf.push(b'e');
        }

        BencodeMap::ByteString(bstr) => {
            buf.extend_from_slice(bstr.len().to_string().as_bytes());
            buf.push(b':');
            buf.extend_from_slice(bstr);
        }

        BencodeMap::Integer(int) => {
            buf.push(b'i');
            buf.extend_from_slice(int.to_string().as_bytes());
            buf.push(b'e');
        }
    }
}