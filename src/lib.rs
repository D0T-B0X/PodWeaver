pub mod dht;
pub mod peer;
pub mod yarn;
pub mod error;
pub mod types;
pub mod runtime;
pub mod storage;
pub mod transport;

use std::sync::Arc;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::collections::HashSet;

use rand::Rng;
use tokio::sync::mpsc;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use dht::DHT;
use yarn::Yarn;
use error::Error;
use types::NodeID;
use types::Result;
use types::InfoHash;
use peer::PodManager;
use peer::YarnFabric;
use dht::GetPeersOutput;
use transport::Transport;
use dht::routing::xor_nodes;
use runtime::RuntimeManager;
use storage::disk::save_to_disk;
use transport::contact::Contact;
use runtime::tracker::TaskSpawner;
use storage::peer_cache::PeerCache;
use transport::pending_map::PendingMap;
use runtime::shutdown::ShutdownController;

use crate::storage::disk::read_or_create_file;

/// lighthouse is hosted at `10.20.0.1:9001`
const LIGHTHOUSE_ADDR: SocketAddr = SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(10, 20, 0, 1)),
    9001
);

const UDP_PORT: u16 = 16001;
const TCP_PORT: u16 = 20001;
const PING_INTERVAL: Duration = Duration::from_secs(120);

pub enum LighthouseRequest {
    // Get the list of all online nodes
    Get { port: u16 },

    // Ping the server and pass your own SocketAddr
    Ping { port: u16 },
}

pub struct Engine {
    runtime: RuntimeManager,
    shutdown: ShutdownController,
    pod_manager: Arc<PodManager>,
    dht: Arc<DHT>,
    dht_addr: SocketAddr,
    peer_addr: SocketAddr,
    listen_port: u16,
    node_id: NodeID,
}

impl Engine {
    pub async fn new() -> Result<Self> {
        let (runtime, shutdown) = RuntimeManager::new();
        let spawner = runtime.spawner();

        // Disk reads for user data
        // get user Node ID
        let id = load_or_create_id()?;

        // Initialize the Pod manager
        let tcp_addr: SocketAddr = format!("0.0.0.0:{}", TCP_PORT)
            .parse()
            .map_err(|_| Error::InvalidPortNumber)?;
        let pod_manager_stop = shutdown.child_token();
        let pod_manager = Arc::new(PodManager::new(tcp_addr, id, pod_manager_stop).await?);
        let listen_port = pod_manager.listen_port;

        let pm = pod_manager.clone();
        spawner.spawn(move |_| async move {
            let _ = pm.run().await;
        });

        let pm = pod_manager.clone();
        spawner.spawn(move |cancel_token| async move {
            pm.discovery_loop(cancel_token).await;
        });

        // Initialize the DHT
        let udp_bind = format!("0.0.0.0:{}", UDP_PORT);        
        let transport = Transport::new(
            Contact::new(&udp_bind, id).await?, 
            PendingMap::new()
        );        
        let dht_addr = transport.addr()?;
        let dht = Arc::new(DHT::new(id, transport.clone())?);

        // spawn transport async runtime to handle incoming queries on the dht socket
        let (query_tx, query_rx) = mpsc::channel(64);
        let t = transport.clone();
        spawner.spawn(move |cancel_token| async move {
            t.run(query_tx, cancel_token).await;
        });

        let d = (*dht).clone();
        spawner.spawn(move |cancel_token| async move {
            let _ = d.run(query_rx, cancel_token).await;
        });

        // bootstrap peer extraction 

        // get cached peer addresses
        // if the cache is empty, ping the lighthouse
        let mut cached_nodes = PeerCache::extract_peers()?.unwrap_or_default();
        if cached_nodes.is_empty() {
            if let Ok(Some(nodes)) = lighthouse_request(LighthouseRequest::Get { port: UDP_PORT }).await {
                cached_nodes = nodes;
            }
        }

        // bootstrap dht with seed nodes
        let _ = dht.bootstrap(cached_nodes).await;

        // lighthouse keep-alive pings
        let _ = lighthouse_request(LighthouseRequest::Ping { port: UDP_PORT }).await;
        spawner.spawn(move |cancel_token| async move {
            lighthouse_ping(cancel_token).await;
        });

        // Signal handler: Ctrl-C / SIGTERM flips the root token.
        let sc = shutdown.clone();
        spawner.spawn(move |_| async move { sc.listen_for_signals().await });

        Ok(Self {
            runtime,
            shutdown,
            pod_manager,
            dht,
            dht_addr,
            peer_addr: tcp_addr,
            listen_port,
            node_id: id,
        })
    }

    pub fn node_id(&self) -> NodeID { self.node_id }
    pub fn listen_port(&self) -> u16 { self.listen_port }
    pub fn dht_addr(&self) -> SocketAddr { self.dht_addr }
    pub fn peer_addr(&self) -> SocketAddr { self.peer_addr }
    pub fn spawner(&self) -> &TaskSpawner { self.runtime.spawner() }
    pub fn cancel_token(&self) -> CancellationToken { self.runtime.cancel_token() }

    /// Register a yarn with the pod. Announce to our
    /// current routing-table neighbours so the swarm can find us.
    pub async fn add_yarn(
        &self,
        author: Option<String>,
        creation_date: Option<String>,
        comments: Option<String>,
        filepath: String, 
    ) -> Result<InfoHash> {
        let fabric = YarnFabric::from_source_file(
            author,
            creation_date,
            comments,
            filepath,
            self.spawner(), 
            self.peer_addr(),
            self.cancel_token(),
        ).await?;

        let info_hash = fabric.info_hash;
        self.pod_manager.add_yarn(fabric.clone());

        let dht = self.dht.clone();
        let listen_port = self.listen_port;
        let pm = self.pod_manager.clone();

        let my_addr = self.peer_addr;
        self.spawner().spawn(move |cancel_token| async move {
            Self::iterative_lookup_and_announce(&dht, &pm, fabric.clone(), listen_port, my_addr).await;
        
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    _ = ticker.tick() => {
                        Self::iterative_lookup_and_announce(&dht, &pm, fabric.clone(), listen_port, my_addr).await;
                    }
                }
            }
        });

        Ok(info_hash)
    }

    pub async fn download_yarn(
        &self, 
        yarn_bytes: Vec<u8>, 
        output_path: Option<PathBuf>, 
        filename: &str
    ) -> Result<Arc<YarnFabric>> {
        // build yarn from bytes
        let yarn = Arc::new(Yarn::decode(&yarn_bytes)?);
        // pre allocate space on the disk
        let out = storage::disk::allocate_file_space(
            output_path, 
            filename, 
            yarn.info.total_length
        )?.to_str().unwrap().to_string();

        // yarn specific runtime setup
        
        // yarn state 
        let fabric = YarnFabric::new(
            yarn, 
            out,
            self.spawner(),
            self.peer_addr,
            false, // not a seed
            self.cancel_token()
        )?;

        self.pod_manager.add_yarn(fabric.clone());

        let dht = self.dht.clone();
        let listen_port = self.listen_port;
        let pm = self.pod_manager.clone();

        let fab = fabric.clone();
        let my_addr = self.peer_addr;
        self.spawner().spawn(move |cancel_token| async move {
            Self::iterative_lookup_and_announce(&dht, &pm, fab.clone(), listen_port, my_addr).await;

            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    _ = ticker.tick() => {
                        Self::iterative_lookup_and_announce(&dht, &pm, fab.clone(), listen_port, my_addr).await;
                    } 
                }
            }
        });

        Ok(fabric)
    }

    async fn iterative_lookup_and_announce(
        dht: &DHT,
        pm: &PodManager,
        fabric: Arc<YarnFabric>,
        listen_port: u16,
        my_tcp: SocketAddr,
    ) {
        let info_hash = fabric.info_hash;
        let target_id: NodeID = info_hash.into();

        let mut visited: HashSet<NodeID> = HashSet::new();
        let mut announce_targets: Vec<(SocketAddr, Vec<u8>)> = Vec::new();

        let mut candidates = {
            let rt = dht.routing_table.lock().unwrap();
            rt.closest(&target_id)
        };

        let alpha = 3;

        while !candidates.is_empty() && visited.len() < 20 {
            candidates.retain(|n| !visited.contains(&n.id));
            candidates.truncate(8);

            let mut to_query = Vec::new();
            for node in candidates.drain(..candidates.len().min(alpha)) {
                visited.insert(node.id);
                to_query.push(node);
            }

            if to_query.is_empty() {
                break;
            }

            for node in to_query {
                if let Ok(output) = dht.get_peers(node.address, info_hash).await {
                    match output {
                        GetPeersOutput::Peers { token, peers } => {
                            announce_targets.push((node.address, token));
                            for peer_addr in peers {
                                if peer_addr != my_tcp && fabric.peer_count() < 30 {
                                    let _ = pm.dial(peer_addr, info_hash).await;
                                }
                            }
                        }
                        GetPeersOutput::Closest { token, nodes } => {
                            announce_targets.push((node.address, token));
                            for next_node in nodes {
                                if !visited.contains(&next_node.id) {
                                    candidates.push(next_node);
                                }
                            }
                        }
                    }
                }
            }

            candidates.sort_by_key(|n| xor_nodes(&n.id, &target_id));
        }

        // Announce our TCP port to the 8 closest nodes that gave us tokens
        for (target_addr, token) in announce_targets.into_iter().take(8) {
            let _ = dht.announce_peer(target_addr, info_hash, listen_port, token).await;
        }
    }

    pub async fn shutdown(self) {
        let nodes = self.dht.routing_table.lock().unwrap().shake();
        let peers: Vec<SocketAddr> = nodes.into_iter().map(|n| n.address).collect();
        let _ = PeerCache::save_peers(&peers);

        self.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), self.runtime.wait_for_tasks()).await;
    }
}

fn load_or_create_id() -> Result<NodeID> {
    // read the id from the file or create a new id and store it in the file
    match read_or_create_file("node_id.silk") {
        Ok(bytes) if bytes.is_empty() => {
            let mut id = [0u8; 32];
            rand::rng().fill_bytes(&mut id);
            save_to_disk("node_id.silk", &id)?;
            Ok(NodeID(id))
        }

        Ok(bytes) if bytes.len() == 32 => {
            let id_bytes = bytes
                .try_into()
                .map_err(|_| Error::CorruptedNodeID)?;

            Ok(NodeID(id_bytes))
        },

        // non 0, non 32 len bytes imply corrupted node
        Ok(_) => Err(Error::CorruptedNodeID),

        Err(Error::FileError(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut id = [0u8; 32];
            rand::rng().fill_bytes(&mut id);
            save_to_disk("node_id.silk", &id)?;
            Ok(NodeID(id))
        }

        Err(Error::FileError(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            Err(Error::UserLacksPrivileges)
        }

        _ => {
            Err(Error::FileReadFailed)
        }
    }
}

async fn lighthouse_request(request: LighthouseRequest) -> Result<Option<Vec<SocketAddr>>> {
    use tokio::io::AsyncWriteExt;

    let stream = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(LIGHTHOUSE_ADDR),
    ).await.map_err(|_| Error::ConnectionFailed)??;

    let (mut r, mut w) = stream.into_split();

    match request {
        LighthouseRequest::Get { port } => {
            let port_bytes = port.to_be_bytes();
            let payload = [0xAAu8, port_bytes[0], port_bytes[1]];
            w.write_all(&payload).await?;

            let mut buf = [0u8; 1];
            tokio::time::timeout(
                Duration::from_secs(3), 
                r.read_exact(&mut buf)
            ).await.map_err(|_| Error::ResponseTimedOut)??;

            let peer_count = buf[0] as usize;
            if peer_count == 0 {
                return Ok(None);
            }

            let mut peer_buf = vec![0u8; peer_count * 6];
            tokio::time::timeout(
                Duration::from_secs(3), 
                r.read_exact(&mut peer_buf)
            ).await.map_err(|_| Error::ResponseTimedOut)??;

            if peer_buf.len() < 6 {
                return Ok(None);
            }

            let peers = peer_buf.chunks_exact(6).map(|b| {
                let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                let port = u16::from_be_bytes([b[4], b[5]]);
                SocketAddr::V4(SocketAddrV4::new(ip, port))
            }).collect();

            Ok(Some(peers))
        },

        LighthouseRequest::Ping { port } => {
            // 2 for Port + 1 for opcode
            let port_bytes = port.to_be_bytes();
            let bytes = vec![0xFFu8, port_bytes[0], port_bytes[1]];

            w.write_all(&bytes).await?;

            let mut status = [0u8; 1];
            tokio::time::timeout(
                Duration::from_secs(3),
                r.read_exact(&mut status), 
            ).await.map_err(|_| Error::ResponseTimedOut)??;

            Ok(None)
        }
    }
}

async fn lighthouse_ping(cancel_token: CancellationToken) {
    let mut tick = tokio::time::interval(PING_INTERVAL);
    tick.tick().await;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = tick.tick() => {
                let _ = lighthouse_request(LighthouseRequest::Ping{ port: UDP_PORT }).await;
            }
        }
    }
}