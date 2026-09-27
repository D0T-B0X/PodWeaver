use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BencodeMap<'a> {
    Integer(i64),
    ByteString(&'a [u8]),
    List(Vec<BencodeMap<'a>>),
    Dict(BTreeMap<&'a str, BencodeMap<'a>>),
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Unexpected end of input")]
    UnexpectedEOF,
    #[error("Unexpected ASCII or UTF-8 Formatting")]
    UnexpectedFormat,
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

// *************************** 
// ****** Public APIs  *******
// ***************************

pub fn decode_to_bencode<'a>(input_code: &'a [u8]) -> Result<(BencodeMap<'a>, &'a [u8])> {
    let (&first, rest) = input_code.split_first().ok_or(Error::UnexpectedEOF)?;

    match first {
        b'i' => parse_int(rest),
        b'l' => parse_list(rest),
        b'd' => parse_dict(rest),
        b'0'..=b'9' => parse_str(input_code),
        _ => Err(Error::InvalidCode),
    }
}

pub fn encode_to_bytes<'a>(input_dict: &BencodeMap<'a>) -> Result<Vec<u8>> {
    let mut buffer: Vec<u8> = Vec::new();
    encode_value(input_dict, &mut buffer);
    Ok(buffer)
}

// -------- Private Functions --------

fn parse_int<'a>(input: &'a [u8]) -> Result<(BencodeMap<'a>, &'a [u8])> {
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

fn parse_str<'a>(input: &'a [u8]) -> Result<(BencodeMap<'a>, &'a [u8])> {
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

    Ok((BencodeMap::ByteString(value), remainder))
}

fn parse_list<'a>(mut input: &'a [u8]) -> Result<(BencodeMap<'a>, &'a [u8])> {
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

fn parse_dict<'a>(mut input: &'a [u8]) -> Result<(BencodeMap<'a>, &'a [u8])> {
    let mut dict: BTreeMap<&str, BencodeMap> = BTreeMap::new();
    let mut last_key: Option<&str> = None;

    while !input.starts_with(b"e") {
        if input.is_empty() {
            return Err(Error::UnexpectedEOF);
        }

        let (key, rest) = decode_to_bencode(input)?;
        input = rest;

        let key_bytes = match key {
            BencodeMap::ByteString(b) => b,
            _ => return Err(Error::InvalidCode), // returns error instead of panicking
        };

        let key_str = std::str::from_utf8(key_bytes).map_err(|_| Error::InvalidStringFormat)?;

        if let Some(prev) = last_key {
            if prev >= key_str {
                return Err(Error::InvalidKeyOrder);
            }
        }
        last_key = Some(key_str);

        let (val, rest) = decode_to_bencode(input)?;
        dict.insert(key_str, val);
        input = rest;
    }

    Ok((BencodeMap::Dict(dict), &input[1..]))
}

fn encode_value<'a>(input_dict: &BencodeMap<'a>, buf: &mut Vec<u8>) {
    match input_dict {
        BencodeMap::Dict(dict) => {
            buf.push(b'd');
            for (k, v) in dict {
                buf.extend_from_slice(k.len().to_string().as_bytes());
                buf.push(b':');
                buf.extend_from_slice(k.as_bytes());
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