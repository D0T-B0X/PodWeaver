use crate::types::Result;
use crate::yarn::bencode::BencodeMap;
use crate::yarn::bencode::decode_to_bencode;
use crate::yarn::bencode::encode_to_bytes;

pub fn encode_message(message: BencodeMap) -> Result<Vec<u8>> {
    let payload = encode_to_bytes(&message)?;
    let mut bytes = Vec::with_capacity(4 + payload.len());
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode_message(bytes: &[u8]) -> Result<BencodeMap> {
    let (value, _) = decode_to_bencode(bytes)?;
    Ok(value)
}