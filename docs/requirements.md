# PodWeaver

PodWeaver is a decentralized Peer-2-Peer file-sharing network (similar to BitTorrent) implemented in Rust. It lets you share files reliably; in a serverless, concurrent manner while protecting you from malicious actors. 

***

## Scope

 - Serverless peer discovery through Kademlia's Distributed Hash Table. 
 - Concurrent peer count limited only by the peer's system capabilities .
 - Easy to connect for any public device.
 - Direct peer-to-peer data transfer.
 - A User Interface for easier access.
 - Security among all peers from malicious
 actors.

***

## Operating Environment

The application will be built with a primary focus on the Linux ecosystem. An executable for windows might be produced if the time frame allows that.

***

## Functional Requirements

 - **Peer Discovery:** Peers must be able to discover other peers using Kademlia's DHT and not a central discovery server.
 - **Peer connections:** Once discovered, a peer should make a TCP connection with that peer.
 - **Transfer Rate Fairness:** Every peer must manage its own bandwidth fairness using a fairness algorithm to ensure proportional upload/download rates with each peer.
 - **Metainfo generation:** Peers can generate a metainfo file for the file the wish to transfer.
 - **Security:** Peers should be protected against all malicious users, each chunk must be verified against their encrypted chhecksums.
 - **File sharing:** A file of any size can be uploaded or downloaded using the application.
 - **File chunking:** All files will be divided into equal sized chunks (size TBD) making it easier to manage fairness and congestion.
 - **User Interface:** A simple GUI application to generate the metainfo file and upload/download files.
 - **Peer Disconnects:** Disconnection from connected peers should not result in crashes or corruption.
 - **File State Management:** Each file should be stateful and only mark a chunk complete once all its sub-chunks have been received and verified.
 - **Multiple Concurrent Shares:** The system should handle more than one files shared concurrently.

***

## Non-Functional Requirements

 - Each peer should be able to maintain atleast 10 concurrent peers.
 - The custom protocol should maintain a < 1% overhead of the payload size.