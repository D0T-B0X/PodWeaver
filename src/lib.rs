#[cfg(feature = "ui")]
pub mod ui;
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
use sha2::Digest;
use sha2::Sha256;
use std::path::Path;
use tokio::sync::mpsc;
use peer::piece::Bitfield;
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

/// Lighthouse is hosted at `10.20.0.1:9001`
const LIGHTHOUSE_ADDR: SocketAddr = SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(10, 99, 0, 2)),
    9001
);

const UDP_PORT: u16 = 16001;
const TCP_PORT: u16 = 20001;
const PING_INTERVAL: Duration = Duration::from_secs(120);
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(60);

// Opcodes / status bytes — keep in sync with `src/bin/lighthouse.rs`.
const OP_GET: u8 = 0xAA;
const OP_PING: u8 = 0xFF;
const STATUS_OK: u8 = 0x01;

pub enum LighthouseRequest {
    Get { port: u16 },
    /// Register / refresh: tell the lighthouse both of our ports so it can
    /// probe the TCP one for liveness and hand the UDP one to bootstrappers.
    Ping { udp_port: u16, tcp_port: u16 },
}

pub enum LighthouseResponse {
    Peers(Vec<SocketAddr>),
    /// The address at which the lighthouse observed *this* node. Returned
    /// by `Ping`, used as the value of `Engine::peer_addr`.
    Observed(SocketAddr),
}

pub struct Engine {
    runtime: RuntimeManager,
    shutdown: ShutdownController,
    pod_manager: Arc<PodManager>,
    dht: Arc<DHT>,
    dht_addr: SocketAddr,
    /// The TCP listener's bind address (may be `0.0.0.0:port`).
    bind_addr: SocketAddr,
    /// The routable address others should reach us at. Falls back to
    /// `bind_addr` when the lighthouse was unreachable.
    peer_addr: SocketAddr,
    listen_port: u16,
    node_id: NodeID,
}

impl Engine {
    pub async fn new() -> Result<Self> {
        let (runtime, shutdown) = RuntimeManager::new();
        let spawner = runtime.spawner();

        // Disk reads for user data
        let id = load_or_create_id()?;

        // TCP listener for peer connections.
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

        // DHT over UDP.
        let udp_bind = format!("0.0.0.0:{}", UDP_PORT);
        let transport = Transport::new(
            Contact::new(&udp_bind, id).await?,
            PendingMap::new()
        );
        let dht_addr = transport.addr()?;
        let dht = Arc::new(DHT::new(id, transport.clone())?);

        let (query_tx, query_rx) = mpsc::channel(64);
        let t = transport.clone();
        spawner.spawn(move |cancel_token| async move {
            t.run(query_tx, cancel_token).await;
        });

        let d = (*dht).clone();
        spawner.spawn(move |cancel_token| async move {
            let _ = d.run(query_rx, cancel_token).await;
        });

        // Register with the lighthouse and learn our own external address.
        // Without this, `peer_addr` would be `0.0.0.0:port`, PEX would gossip
        // it back to us, and we'd try to dial ourselves on every sweep.
        let peer_addr = match lighthouse_request(LighthouseRequest::Ping {
            udp_port: UDP_PORT,
            tcp_port: listen_port,
        })
        .await
        {
            Ok(LighthouseResponse::Observed(addr)) => addr,
            _ => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), listen_port),
        };

        // Bootstrap the DHT: prefer the on-disk cache, fall back to the
        // lighthouse on a cold start.
        let mut cached_nodes = PeerCache::extract_peers()?.unwrap_or_default();
        if cached_nodes.is_empty() {
            if let Ok(LighthouseResponse::Peers(nodes)) =
                lighthouse_request(LighthouseRequest::Get { port: UDP_PORT }).await
            {
                cached_nodes = nodes;
            }
        }
        let _ = dht.bootstrap(cached_nodes).await;

        // Lighthouse keep-alive pings.
        spawner.spawn(move |cancel_token| async move {
            lighthouse_ping(cancel_token, listen_port).await;
        });

        // Signal handler. Pass the child token so it can actually be cancelled
        // — otherwise `wait_for_tasks()` in `shutdown()` always hits its
        // timeout instead of shutting down cleanly.
        let sc = shutdown.clone();
        spawner.spawn(move |cancel_token| async move {
            tokio::select! {
                _ = cancel_token.cancelled() => {}
                _ = sc.listen_for_signals() => {}
            }
        });

        Ok(Self {
            runtime,
            shutdown,
            pod_manager,
            dht,
            dht_addr,
            bind_addr: tcp_addr,
            peer_addr,
            listen_port,
            node_id: id,
        })
    }

    pub fn node_id(&self) -> NodeID { self.node_id }
    pub fn listen_port(&self) -> u16 { self.listen_port }
    pub fn dht_addr(&self) -> SocketAddr { self.dht_addr }
    /// External routable address. Use this for PEX identity, dial filters,
    /// and anything that gets gossiped to other nodes.
    pub fn peer_addr(&self) -> SocketAddr { self.peer_addr }
    /// Local bind address. Only useful for diagnostics.
    pub fn bind_addr(&self) -> SocketAddr { self.bind_addr }
    pub fn spawner(&self) -> &TaskSpawner { self.runtime.spawner() }
    pub fn cancel_token(&self) -> CancellationToken { self.runtime.cancel_token() }
    pub fn child_token(&self) -> CancellationToken { self.shutdown.child_token() }

    /// Register a yarn with the pod and keep announcing it to the DHT.
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
            self.child_token(),
        )
        .await?;

        let info_hash = fabric.info_hash;
        self.pod_manager.add_yarn(fabric.clone());

        self.spawn_announce_loop(fabric);
        Ok(info_hash)
    }

    pub async fn download_yarn(
        &self,
        yarn: Arc<Yarn>,
        output_path: PathBuf,
    ) -> Result<Arc<YarnFabric>> {
        let out_path = storage::disk::allocate_file_space(
            Some(output_path),
            &yarn.info.filename,
            yarn.info.total_length,
        )?;
        let out = out_path.to_string_lossy().into_owned();

        let resume_bitfield = {
            let path = out_path.clone();
            let yarn = yarn.clone();
            tokio::task::spawn_blocking(move || scan_existing_pieces(&path, &yarn))
                .await
                .map_err(|_| Error::FileReadFailed)??
        };

        let fabric = YarnFabric::new(
            yarn,
            out,
            self.spawner(),
            self.peer_addr(),
            Some(resume_bitfield),
            self.child_token(),
        )?;

        self.pod_manager.add_yarn(fabric.clone());
        self.spawn_announce_loop(fabric.clone());
        Ok(fabric)
    }

    /// Spawn the per-yarn task that periodically runs an iterative DHT
    /// lookup against our neighbours and re-announces our TCP port.
    fn spawn_announce_loop(&self, fabric: Arc<YarnFabric>) {
        let dht = self.dht.clone();
        let pm = self.pod_manager.clone();
        let listen_port = self.listen_port;
        let my_addr = self.peer_addr;

        self.spawner().spawn(move |_| async move {
            let mut ticker = tokio::time::interval(ANNOUNCE_INTERVAL);
            ticker.tick().await; // consume the immediate tick

            loop {
                let did_work = Self::iterative_lookup_and_announce(
                    &dht,
                    &pm,
                    fabric.clone(),
                    listen_port,
                    my_addr,
                )
                .await;

                if !did_work {
                    if let Ok(LighthouseResponse::Peers(seed)) =
                        lighthouse_request(LighthouseRequest::Get { port: UDP_PORT }).await
                    {
                        let _ = dht.bootstrap(seed).await;
                    }
                }

                tokio::select! {
                    _ = fabric.stop.cancelled() => break,
                    _ = ticker.tick() => {}
                }
            }
        });
    }

    /// Returns `true` if the routing table had any candidates to work with.
    async fn iterative_lookup_and_announce(
        dht: &DHT,
        pm: &PodManager,
        fabric: Arc<YarnFabric>,
        listen_port: u16,
        my_tcp: SocketAddr,
    ) -> bool {
        let info_hash = fabric.info_hash;
        let target_id: NodeID = info_hash.into();

        let mut visited: HashSet<NodeID> = HashSet::new();
        let mut announce_targets: Vec<(SocketAddr, Vec<u8>)> = Vec::new();

        let mut candidates = {
            let rt = dht.routing_table.lock().unwrap();
            rt.closest(&target_id)
        };

        if candidates.is_empty() {
            return false;
        }

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
                                // Now that `my_tcp` is the real external address,
                                // this filter actually excludes us.
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

        // Announce our TCP port to the closest nodes that gave us tokens.
        for (target_addr, token) in announce_targets.into_iter().take(8) {
            let _ = dht.announce_peer(target_addr, info_hash, listen_port, token).await;
        }

        true
    }

    pub async fn shutdown(&self) {
        let nodes = self.dht.routing_table.lock().unwrap().shake();
        let peers: Vec<SocketAddr> = nodes.into_iter().map(|n| n.address).collect();
        let _ = PeerCache::save_peers(&peers);

        self.shutdown.cancel();
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            self.runtime.wait_for_tasks(),
        )
        .await;
    }
}

fn load_or_create_id() -> Result<NodeID> {
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
        }

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

        _ => Err(Error::FileReadFailed),
    }
}

async fn lighthouse_request(request: LighthouseRequest) -> Result<LighthouseResponse> {
    use tokio::io::AsyncWriteExt;

    let stream = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(LIGHTHOUSE_ADDR),
    )
    .await
    .map_err(|_| Error::ConnectionFailed)??;

    let (mut r, mut w) = stream.into_split();

    match request {
        LighthouseRequest::Get { port } => {
            let port_bytes = port.to_be_bytes();
            let payload = [OP_GET, port_bytes[0], port_bytes[1]];
            w.write_all(&payload).await?;

            let mut buf = [0u8; 1];
            tokio::time::timeout(Duration::from_secs(3), r.read_exact(&mut buf))
                .await
                .map_err(|_| Error::ResponseTimedOut)??;

            let peer_count = buf[0] as usize;
            if peer_count == 0 {
                return Ok(LighthouseResponse::Peers(Vec::new()));
            }

            let mut peer_buf = vec![0u8; peer_count * 6];
            tokio::time::timeout(Duration::from_secs(3), r.read_exact(&mut peer_buf))
                .await
                .map_err(|_| Error::ResponseTimedOut)??;

            let peers = peer_buf
                .chunks_exact(6)
                .map(|b| {
                    let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                    let port = u16::from_be_bytes([b[4], b[5]]);
                    SocketAddr::V4(SocketAddrV4::new(ip, port))
                })
                .collect();

            Ok(LighthouseResponse::Peers(peers))
        }

        LighthouseRequest::Ping { udp_port, tcp_port } => {
            let udp = udp_port.to_be_bytes();
            let tcp = tcp_port.to_be_bytes();
            let payload = [OP_PING, udp[0], udp[1], tcp[0], tcp[1]];
            w.write_all(&payload).await?;

            // [status, ip0, ip1, ip2, ip3]
            let mut resp = [0u8; 5];
            tokio::time::timeout(Duration::from_secs(3), r.read_exact(&mut resp))
                .await
                .map_err(|_| Error::ResponseTimedOut)??;

            if resp[0] != STATUS_OK {
                return Err(Error::InvalidPodResponse);
            }

            let ip = Ipv4Addr::new(resp[1], resp[2], resp[3], resp[4]);
            let observed = SocketAddr::V4(SocketAddrV4::new(ip, tcp_port));
            Ok(LighthouseResponse::Observed(observed))
        }
    }
}

async fn lighthouse_ping(cancel_token: CancellationToken, tcp_port: u16) {
    let mut tick = tokio::time::interval(PING_INTERVAL);
    tick.tick().await;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = tick.tick() => {
                let _ = lighthouse_request(LighthouseRequest::Ping {
                    udp_port: UDP_PORT,
                    tcp_port,
                })
                .await;
            }
        }
    }
}

pub fn scan_existing_pieces(path: &Path, yarn: &Yarn) -> Result<Bitfield> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path)?;

    let piece_count = yarn.info.pieces.len();
    let mut bitfield = Bitfield::new(piece_count);

    let piece_length = yarn.info.piece_length as u64;
    let total_length = yarn.info.total_length;

    for idx in 0..piece_count {
        let piece_start = idx as u64 * piece_length;
        let this_len = (total_length - piece_start).min(piece_length);

        file.seek(SeekFrom::Start(piece_start))?;
        let mut buf = vec![0u8; this_len as usize];
        file.read_exact(&mut buf)?;

        let mut hasher = Sha256::new();
        hasher.update(&buf);
        let digest: [u8; 32] = hasher.finalize().into();

        if digest == yarn.info.pieces[idx] {
            bitfield.set(idx as u32);
        }
    }

    Ok(bitfield)
}