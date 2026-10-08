# Objective

Pods in the PodWeaver fabric communicate with each other by receiving and transmitting standardized `message` packets. This file defines those message standards and exposes functions to translate them between `BencodeMap` and `Message`.

## Message

PodWeaver supports Peer exchange and choking which adds extra message variants to the list. The message variants are broadly divided into 5 categories - `Connection`, `Peer control`, `Peer Inventory`, `Data Transfer` and `Peer Exchange`. `Peer control` handles choking, while `Peer Inventory` is state management for data between peers.

```rs
pub enum Message {
    // Peer Connection
    Handshake,

    // Peer Control
    Choke, 
    Unchoke, 
    Interested, 
    NotInterested,

    // Peer Inventory
    Have,
    Bitfield,

    // Peer Data Transfer
    Request,
    Piece,
    Cancel,

    // Peer Exchange
    PexRequest,
    PexResponse,
}
```

### Variant Parameters

| Variant         | Parameters                                                   |
|-----------------|--------------------------------------------------------------|
| `Handshake`     | `node_id: NodeID`, `info_hash: InfoHash`, `listen_port: u16` |
| `Have`          | `piece_index: u32`                                           |
| `Bitfield`      | `bits: Vec<u8>`                                              |
| `Choke`         | —                                                            |
| `Unchoke`       | —                                                            |
| `Interested`    | —                                                            |
| `NotInterested` | —                                                            |
| `Request`       | `piece_index: u32`, `offset: u64`, `length: u64`             |
| `Piece`         | `piece_index: u32`, `offset: u64`, `data: Vec<u8>`           |
| `Cancel`        | `piece_index: u32`, `offset: u64`, `length: u64`             |
| `PexRequest`    | —                                                            |
| `PexResponse`   | `added: Vec<SocketAddr>`, `dropped: Vec<SocketAddr>`         |

### Message Categories

1. **Connection:** `Handshake`, One time transmission to a remote pod familiarising it with the sender.
2. **Peer Control:** Sender Peers declare interests or dis interest in specific pieces and state of service between the two pods.
3. **Peer Inventory:** Declares what pieces the sender holds.
4. **Data Transfer:** Block level data request control.
5. **Peer Exchange:** Gossip protocol to share known peer addresses in the swarm.

## API

Two protocol APIs are available to peer network manager: 

1. **`to_bencode:`** Converts a `Message` variant to its `BencodeMap` equivalent, which can be transmitted across the network to fellow pods.
2. **`from_bencode:`** Accepts a `BencodeMap` and converts it into its correct `Message` variant based on the `type` field of the input. In case the input is malformed or incorrect, the method returns an `UnexpectedFormat` error.