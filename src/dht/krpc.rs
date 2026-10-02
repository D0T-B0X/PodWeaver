use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::collections::BTreeMap;

use crate::types::*;
use crate::error::*;
use crate::weft::bencode::*;

pub enum QueryPayload {
    Ping { id: NodeID },
    FindNode { id: NodeID, target: NodeID },
    GetPeers { id: NodeID, info_hash: InfoHash },
    AnnouncePeer { id: NodeID, info_hash: InfoHash, port: u16, token: Vec<u8> },
}

impl QueryPayload {
    fn into_parts(self) -> (BencodeMap, BencodeMap) {
        match self {
            Self::Ping { id } => ("ping".into(), dict([("id", id.into())])),

            Self::FindNode { id, target } => ("find_node".into(), dict([
                ("id", id.into()),
                ("target", target.into())
            ])),

            Self::GetPeers { id, info_hash } => ("get_peers".into(), dict([
                ("id", id.into()),
                ("info_hash", info_hash.into()),
            ])),

            Self::AnnouncePeer { id, info_hash, port, token } => ("announce_peer".into(), dict([
                ("id", id.into()),
                ("info_hash", info_hash.into()),
                ("port", port.into()),
                ("token", token.into())
            ]))
        }
    }
}

pub enum ResponsePayload {
    Id { id: NodeID },
    Nodes { id: NodeID, nodes: Vec<u8> },
    Peers { id: NodeID, token: Vec<u8>, values: Vec<std::net::SocketAddr> },
    Closer { id: NodeID, token: Vec<u8>, nodes: Vec<u8> },
}

impl ResponsePayload {
    fn into_args(self) -> BencodeMap {
        match self {
            Self::Id { id } => dict([("id", id.into())]),

            Self::Nodes { id, nodes } => dict([
                ("id", id.into()),
                ("nodes", nodes.into()),
            ]),

            Self::Closer { id, token, nodes } => dict([
                ("id", id.into()),
                ("token", token.into()),
                ("nodes", nodes.into()),
            ]),

            Self::Peers { id, token, values } => {
                let values: Vec<BencodeMap> = values.iter()
                    .filter_map(compact_v4)
                    .map(BencodeMap::from)
                    .collect();

                dict([
                    ("id", id.into()),
                    ("token", token.into()),
                    ("values", values.into()),
                ])
            }
        }
    }
}

pub enum KRPCMessage {
    Query {
        transaction_id: TransactionID,
        payload: QueryPayload
    },

    Response {
        transaction_id: TransactionID,
        payload: ResponsePayload
    },

    Error {
        transaction_id: TransactionID,
        code: i32,
        message: String
    }
}

pub fn encode_message(message: KRPCMessage) -> Result<Vec<u8>> {
    let top: BencodeMap = match message {
        KRPCMessage::Query { transaction_id, payload } => {

            let (query_name, args) = payload.into_parts();

            dict([
                ("t", transaction_id.into()),
                ("y", "q".into()),
                ("q", query_name),
                ("a", args),
            ])
        },

        KRPCMessage::Response { transaction_id, payload } => {
            let args: BencodeMap = payload.into_args();
        
            dict([
                ("t", transaction_id.into()),
                ("y", "r".into()),
                ("r", args),
            ])
        },

        KRPCMessage::Error { transaction_id, code, message } => dict([
            ("t", transaction_id.into()),
            ("y", "e".into()),
            ("e", BencodeMap::List(vec![code.into(), message.into()]))
        ])
    };

    Ok(encode_to_bytes(&top)?)
}

pub fn decode_message(message: Vec<u8>) -> Result<KRPCMessage> {
    let BencodeMap::Dict(dict) = decode_to_bencode(&message)?.0 else {
        return Err(Error::UnexpectedFormat);
    };

    let y = bytes_from_dict(&dict, b"y")?;

    match y {
        b"q" => {
            Ok(decode_query(&dict)?)
        }

        b"r" => {
            Ok(decode_response(&dict)?)
        }

        b"e" => {
            Ok(decode_error(&dict)?)
        }

        _ => Err(Error::InvalidRPCFormat)
    }
}

fn dict<const N: usize>(pairs: [(&str, BencodeMap); N]) -> BencodeMap {
    BencodeMap::Dict(
        pairs.into_iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v))
            .collect()
    )
}

fn compact_v4(addr: &SocketAddr) -> Option<Vec<u8>> {
    let SocketAddr::V4(a) = addr else { return None };
    let mut out = a.ip().octets().to_vec();
    out.extend_from_slice(&a.port().to_be_bytes());
    Some(out)
}

fn decode_query(dict: &BTreeMap<Vec<u8>, BencodeMap>) -> Result<KRPCMessage> {
    let tid = bytes_to_type(
        bytes_from_dict(dict, b"t")?, 
        TransactionID
    )?;

    let query_type = bytes_from_dict(dict, b"q")?;
    let args_dict = subdict_from_dict(dict, b"a")?;

    let id = bytes_to_type(
        bytes_from_dict(args_dict, b"id")?,
        NodeID
    )?;

    let payload: QueryPayload = match query_type {
        b"ping" => QueryPayload::Ping { id },

        b"find_node" => {
            let target = bytes_to_type(
                bytes_from_dict(args_dict, b"target")?, 
                NodeID
            )?;

            QueryPayload::FindNode { id, target }
        }

        b"get_peers" => {
            let info_hash = bytes_to_type(
                bytes_from_dict(args_dict, b"info_hash")?,
                InfoHash 
            )?;

            QueryPayload::GetPeers { id, info_hash }
        }

        b"announce_peer" => {
            let info_hash = bytes_to_type(
                bytes_from_dict(args_dict, b"info_hash")?,
                InfoHash 
            )?;

            let port_int = int_from_dict(args_dict, b"port")?;
            let port = u16::try_from(port_int).map_err(|_| Error::InvalidPortNumber)?;

            let token = bytes_from_dict(args_dict, b"token")?.to_vec();

            QueryPayload::AnnouncePeer { id, info_hash, port, token }  
        }

        _ => return Err(Error::MethodUnknown)
    };

    Ok(KRPCMessage::Query { 
        transaction_id: tid, 
        payload, 
    })
}

fn decode_response(dict: &BTreeMap<Vec<u8>, BencodeMap>) -> Result<KRPCMessage> {
    let transaction_id = bytes_to_type(
        bytes_from_dict(dict, b"t")?, 
        TransactionID
    )?;

    let args_dict = subdict_from_dict(dict, b"r")?;
    let id = bytes_to_type(
        bytes_from_dict(args_dict, b"id")?, 
        NodeID
    )?;

    let payload = if args_dict.contains_key(b"values".as_slice()) {
        let token = bytes_from_dict(args_dict, b"token")?.to_vec();
        let values_list = list_from_dict(args_dict, b"values")?;
        let values = parse_compact_socket_addrs(values_list)?;

        ResponsePayload::Peers { id, token, values }
    } else if args_dict.contains_key(b"token".as_slice()) {
        let token = bytes_from_dict(args_dict, b"token")?.to_vec();
        let nodes = bytes_from_dict(args_dict, b"nodes")?.to_vec();

        ResponsePayload::Closer { id, token, nodes }
    } else if args_dict.contains_key(b"nodes".as_slice()) {
        let nodes = bytes_from_dict(args_dict, b"nodes")?.to_vec();

        ResponsePayload::Nodes { id, nodes }
    } else {
        ResponsePayload::Id { id }
    };

    Ok(KRPCMessage::Response { 
        transaction_id, 
        payload
    })
}

fn decode_error(dict: &BTreeMap<Vec<u8>, BencodeMap>) -> Result<KRPCMessage> {
    let transaction_id = bytes_to_type(
        bytes_from_dict(dict, b"t")?,
        TransactionID
    )?;
    let e = list_from_dict(dict, b"e")?;

    if e.len() < 2 {
        return Err(Error::InvalidRPCFormat);
    }

    let code = match e[0] {
        BencodeMap::Integer(int) => i32::try_from(int).map_err(|_| Error::UnexpectedFormat),
        _ => return Err(Error::UnexpectedFormat)
    }?;

    let message = match &e[1] {
        BencodeMap::ByteString(bytes) => {
            String::from_utf8(bytes.to_vec()).map_err(|_| Error::InvalidRPCFormat)?
        },
        _ => return Err(Error::UnexpectedFormat)
    };
    
    Ok(KRPCMessage::Error { 
        transaction_id, 
        code, 
        message 
    })
}

fn bytes_from_dict<'a>(dict: &'a BTreeMap<Vec<u8>, BencodeMap>, key: &'a [u8]) -> Result<&'a [u8]> {
    dict
        .get(key)
        .and_then(|val| match val {
            BencodeMap::ByteString(bytes) => Some(bytes.as_slice()),
            _ => None
        })
        .ok_or(Error::UnexpectedFormat)
}

fn int_from_dict<'a>(dict: &'a BTreeMap<Vec<u8>, BencodeMap>, key: &'a [u8]) -> Result<i64> {
    dict
        .get(key)
        .and_then(|val| match val {
            BencodeMap::Integer(int) => Some(int),
            _ => None
        })
        .ok_or(Error::UnexpectedFormat).cloned()
}

fn subdict_from_dict<'a>(
    dict: &'a BTreeMap<Vec<u8>, BencodeMap>, 
    key: &'a [u8]
) -> Result<&'a BTreeMap<Vec<u8>, BencodeMap>> {
    dict
    .get(key)
    .and_then(|map| match map {
        BencodeMap::Dict(m) => Some(m),
        _ => None
    })
    .ok_or(Error::UnexpectedFormat)
}

fn list_from_dict<'a>(dict: &'a BTreeMap<Vec<u8>, BencodeMap>, key: &'a [u8]) -> Result<&'a Vec<BencodeMap>> {
    dict.get(key)
        .and_then(|ls| match ls {
            BencodeMap::List(ls) => Some(ls),
            _ => None
        })
        .ok_or(Error::UnexpectedFormat)
}

fn bytes_to_type<const N: usize, T>(bytes: &[u8], constructor: fn([u8; N]) -> T) -> Result<T> {
    bytes.try_into()
        .map(constructor)
        .map_err(|_| Error::UnexpectedFormat)
}

fn parse_compact_socket_addrs<'a>(list: &'a Vec<BencodeMap>) -> Result<Vec<SocketAddr>> {
    let mut addrs = Vec::with_capacity(list.len());

    for addr in list {
        let bytes = match addr {
            BencodeMap::ByteString(bytes) if bytes.len() == 6 => bytes.as_slice(),
            _ => return Err(Error::UnexpectedFormat)
        };

        let ip = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
        let port = u16::from_be_bytes([bytes[4], bytes[5]]);

        addrs.push(SocketAddr::V4(SocketAddrV4::new(ip, port)));
    };

    Ok(addrs)
}