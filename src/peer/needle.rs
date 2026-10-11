use crate::error::Error;
use crate::types::Result;
use crate::yarn::bencode::BencodeMap;
use crate::yarn::bencode::decode_to_bencode;
use crate::yarn::bencode::encode_to_bytes;

// A Piece would be the largest incoming Message
// 16 Kb for block data and 96 bytes for offset and index
// So I am allocating 32 kb for shits and giggles
const MAX_FRAME_SIZE: u32 = 32_768;

pub fn encode_message(message: &BencodeMap) -> Result<Vec<u8>> {
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

pub fn parse_length(header: [u8; 4]) -> Result<u32> {
    let len = u32::from_be_bytes(header);
    if len > MAX_FRAME_SIZE {
        return Err(Error::FrameTooLarge);
    }
    
    Ok(len)
}