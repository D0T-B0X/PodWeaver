use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::collections::BTreeMap;
use std::net::SocketAddrV4;

use crate::error::Error;
use crate::types::Result;
use crate::types::NodeID;
use crate::types::InfoHash;
use crate::yarn::bencode::BencodeMap;

pub enum Message {
    // Establish a connection with another peer
    Handshake { 
        node_id: NodeID, 
        info_hash: InfoHash, 
        listen_port: u16,
    },

    // Peer preference control
    Choke, Unchoke, Interested, NotInterested,

    // New peer-2-peer information sharing
    Have { 
        piece_index: u32, 
    }, 
    Bitfield { 
        bits: Vec<u8>,
    },
    
    // Data transfer requests
    Request { 
        piece_index: u32, 
        offset: u64, 
        length: u64, 
    },
    Piece { 
        piece_index: u32, 
        offset: u64,
        data: Vec<u8>,
    },
    Cancel { 
        piece_index: u32, 
        offset: u64, 
        length: u64,
    },

    // PEX commands
    PexRequest,
    PexResponse { 
        added: Vec<SocketAddr>, 
        dropped: Vec<SocketAddr>,
    }
}

impl Message {
    pub fn to_bencode(self) -> BencodeMap {
        match self {
            Self::Handshake { node_id, info_hash, listen_port } => {
                let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
                dict.insert("type".into(), "handshake".into());
                dict.insert("node_id".into(), node_id.into());
                dict.insert("info_hash".into(), info_hash.into());
                dict.insert("listen_port".into(), listen_port.into());

                dict.into()
            },

            Self::Choke => get_named_dict("choke"),
            Self::Unchoke => get_named_dict("unchoke"),
            Self::Interested => get_named_dict("interested"),
            Self::NotInterested => get_named_dict("not_interested"),

            Self::Have { piece_index } => {
                let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
                dict.insert("type".into(), "have".into());
                dict.insert("piece_index".into(), piece_index.into());

                dict.into()
            },
            Self::Bitfield { bits } => {
                let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
                dict.insert("type".into(), "bits".into());
                dict.insert("bits".into(), bits.into());

                dict.into()
            },

            Self::Request { piece_index, offset, length } => {
                let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
                dict.insert("type".into(), "request".into());
                dict.insert("piece_index".into(), piece_index.into());
                dict.insert("offset".into(), offset.into());
                dict.insert("length".into(), length.into());

                dict.into()
            },
            Self::Piece { piece_index, offset, data } => {
                let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
                dict.insert("type".into(), "piece".into());
                dict.insert("piece_index".into(), piece_index.into());
                dict.insert("offset".into(), offset.into());
                dict.insert("data".into(), data.into());

                dict.into()
            },
            Self::Cancel { piece_index, offset, length } => {
                let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
                dict.insert("type".into(), "piece".into());
                dict.insert("piece_index".into(), piece_index.into());
                dict.insert("offset".into(), offset.into());
                dict.insert("length".into(), length.into());

                dict.into()
            },

            Self::PexRequest => get_named_dict("pex_request"),
            Self::PexResponse { added, dropped } => {
                let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
                dict.insert("type".into(), "pex_response".into());
                dict.insert("added".into(), added.into());
                dict.insert("dropped".into(), dropped.into());

                dict.into()
            },
        }
    }

    pub fn from_bencode(v: &BencodeMap) -> Result<Self> {
        let dict = match v {
            BencodeMap::Dict(d) => d,
            _ => return Err(Error::UnexpectedFormat),
        };

        let type_field = dict.get(b"type".as_slice()).ok_or(Error::UnexpectedFormat)?;

        let message_type = match type_field {
            BencodeMap::ByteString(m) => m,
            _ => return Err(Error::InvalidRequestType),
        };

        match message_type.as_slice() {
            b"handshake" => {
                let id = dict.get(b"node_id".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let infohash = dict.get(b"info_hash".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let port = dict.get(b"listen_port".as_slice()).ok_or(Error::UnexpectedFormat)?;

                let node_id = match id {
                    BencodeMap::ByteString(b) => NodeID::try_from(b)?,
                    _ => return Err(Error::UnexpectedFormat),
                };

                let info_hash = match infohash {
                    BencodeMap::ByteString(b) => InfoHash::try_from(b)?,
                    _ => return Err(Error::UnexpectedFormat),
                };

                let listen_port = match port {
                    BencodeMap::Integer(p) => u16::try_from(*p).map_err(|_| Error::InvalidPortNumber)?,
                    _ => return Err(Error::UnexpectedFormat),
                };

                Ok(Self::Handshake { node_id, info_hash, listen_port })
            },

            b"choke" => Ok(Self::Choke),
            b"unchoke" => Ok(Self::Unchoke),
            b"interested" => Ok(Self::Interested),
            b"not_interested" => Ok(Self::NotInterested),

            b"have" => {
                let idx = dict.get(b"piece_index".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let piece_index = match idx {
                    BencodeMap::Integer(i) => u32::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };

                Ok(Self::Have { piece_index })
            }

            b"bitfield" => {
                let bits = dict.get(b"bits".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let bits = match bits {
                    BencodeMap::ByteString(b) => b.clone(),
                    _ => return Err(Error::UnexpectedFormat),
                };

                Ok(Self::Bitfield { bits })
            }

            b"request" => {
                let idx = dict.get(b"piece_index".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let off = dict.get(b"offset".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let len = dict.get(b"length".as_slice()).ok_or(Error::UnexpectedFormat)?;

                let piece_index = match idx {
                    BencodeMap::Integer(i) => u32::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };
                let offset = match off {
                    BencodeMap::Integer(i) => u64::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };
                let length = match len {
                    BencodeMap::Integer(i) => u64::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };

                Ok(Self::Request { piece_index, offset, length })
            }

            b"piece" => {
                let idx = dict.get(b"piece_index".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let off = dict.get(b"offset".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let data = dict.get(b"data".as_slice()).ok_or(Error::UnexpectedFormat)?;

                let piece_index = match idx {
                    BencodeMap::Integer(i) => u32::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };
                let offset = match off {
                    BencodeMap::Integer(i) => u64::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };
                let data = match data {
                    BencodeMap::ByteString(b) => b.clone(),
                    _ => return Err(Error::UnexpectedFormat),
                };
                Ok(Self::Piece { piece_index, offset, data })
            }

            b"cancel" => {
                let idx = dict.get(b"piece_index".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let off = dict.get(b"offset".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let len = dict.get(b"length".as_slice()).ok_or(Error::UnexpectedFormat)?;

                let piece_index = match idx {
                    BencodeMap::Integer(i) => u32::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };
                let offset = match off {
                    BencodeMap::Integer(i) => u64::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };
                let length = match len {
                    BencodeMap::Integer(i) => u64::try_from(*i).map_err(|_| Error::UnexpectedFormat)?,
                    _ => return Err(Error::UnexpectedFormat),
                };
                Ok(Self::Cancel { piece_index, offset, length })
            }

            b"pex_request" => Ok(Self::PexRequest),

            b"pex_response" => {
                let added_bytes = dict.get(b"added".as_slice()).ok_or(Error::UnexpectedFormat)?;
                let dropped_bytes = dict.get(b"dropped".as_slice()).ok_or(Error::UnexpectedFormat)?;
                
                let added = match added_bytes {
                    BencodeMap::ByteString(a) => decode_addrs(a),
                    _ => return Err(Error::UnexpectedFormat),
                };

                let dropped = match dropped_bytes {
                    BencodeMap::ByteString(d) => decode_addrs(d),
                    _ => return Err(Error::UnexpectedFormat),
                };
                
                Ok(Self::PexResponse { added, dropped })
            }

            _ => Err(Error::InvalidRequestType),
        }
    }
}

fn get_named_dict(name: &str) -> BencodeMap {
    let mut dict: BTreeMap<Vec<u8>, BencodeMap> = BTreeMap::new();
    dict.insert("type".into(), name.into());
    dict.into()
}

fn decode_addrs(bytes: &Vec<u8>) -> Vec<SocketAddr> {
    bytes.chunks_exact(6).map(|b| {
        let ip = Ipv4Addr::from_octets([b[0], b[1], b[2], b[3]]);
        let port = u16::from_be_bytes([b[3], b[4]]);

        SocketAddr::V4(SocketAddrV4::new(ip, port))
    }).collect()
}