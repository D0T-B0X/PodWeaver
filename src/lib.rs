pub mod dht;
pub mod peer;
pub mod yarn;
pub mod error;
pub mod types;
pub mod runtime;
pub mod storage;
pub mod transport;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::dht::{DHT, GetPeersOutput};
use crate::error::Error;
use crate::peer::{PodManager, YarnFabric};
use crate::runtime::tracker::TaskSpawner;
use crate::runtime::RuntimeManager;
use crate::runtime::shutdown::ShutdownController;
use crate::storage::peer_cache::PeerCache;
use crate::transport::Transport;
use crate::transport::contact::Contact;
use crate::transport::pending_map::PendingMap;
use crate::types::{InfoHash, NodeID, Result};
use crate::yarn::Yarn;

pub struct Engine {
    runtime: RuntimeManager,
    shutdown: ShutdownController,
    pod_manager: Arc<PodManager>,
    dht: Option<Arc<DHT>>,
    dht_addr: Option<SocketAddr>,
    listen_port: u16,
    node_id: NodeID,
}

impl Engine {
    /// Bring up the node.
    ///
    /// * `udp_bind` — `Some("0.0.0.0:8000")` to run a DHT node, `None` for mesh-only.
    /// * `tcp_bind` — always required; this is the peer mesh listener.
    /// * `node_id`  — `Some` for an ephemeral identity (tests), `None` to persist to disk.
    /// * `bootstrap` — DHT seed addresses. Empty = skip DHT bootstrap.
    pub async fn new(
        udp_bind: Option<&str>,
        tcp_bind: &str,
        node_id: Option<NodeID>,
        bootstrap: Vec<SocketAddr>,
    ) -> Result<Self> {
        let (runtime, shutdown) = RuntimeManager::new();
        let spawner = runtime.spawner().clone();
        let root_token = shutdown.token();

        // --- DHT side (optional) ---
        let mut dht: Option<Arc<DHT>> = None;
        let mut dht_addr: Option<SocketAddr> = None;
        let mut resolved_id: Option<NodeID> = node_id;

        if let Some(bind) = udp_bind {
            let contact = match node_id {
                Some(id) => Contact::with_node(id, bind).await?,
                None => Contact::new(bind).await?,
            };
            dht_addr = Some(contact.local_addr()?);
            resolved_id = Some(contact.id);

            let transport = Transport::new(contact, PendingMap::new());
            let dht_obj = Arc::new(DHT::new(transport.id().clone(), transport.clone())?);

            // transport.run(queries_tx, token) -> dht.run(queries_rx, token)
            let (q_tx, q_rx) = mpsc::channel(64);

            let t = transport.clone();
            let tok = root_token.clone();
            spawner.spawn(move |_| async move { t.run(q_tx, tok).await });

            let d = (*dht_obj).clone();
            let tok = root_token.clone();
            spawner.spawn(move |_| async move {
                if let Err(e) = d.run(q_rx, tok).await {
                    eprintln!("dht.run exited: {e:?}");
                }
            });

            // Bootstrap: explicit addresses first, then peer cache.
            let mut seeds = bootstrap;
            if seeds.is_empty() {
                if let Ok(Some(cached)) = PeerCache::extract_peers() {
                    seeds = cached;
                }
            }
            if !seeds.is_empty() {
                let online = dht_obj.bootstrap(seeds).await;
                eprintln!("[dht] bootstrap: {online} node(s) online");
            }

            dht = Some(dht_obj);
        }

        // --- Peer mesh side (always) ---
        let id = resolved_id.unwrap_or_else(|| {
            use rand::Rng;
            let mut b = [0u8; 32];
            rand::rng().fill_bytes(&mut b);
            NodeID(b)
        });

        let tcp: SocketAddr = tcp_bind.parse().map_err(|_| Error::InvalidPortNumber)?;
        let pod_manager = Arc::new(PodManager::new(tcp, id, root_token.clone()).await?);
        let listen_port = pod_manager.listen_port;

        let pm = pod_manager.clone();
        let tok = root_token.clone();
        spawner.spawn(move |_| async move {
            if let Err(e) = pm.run().await {
                eprintln!("pod_manager.run exited: {e:?}");
            }
        });

        let pm = pod_manager.clone();
        let tok2 = root_token.clone();
        spawner.spawn(move |_| async move { pm.discovery_loop(tok2).await });

        // Signal handler: Ctrl-C / SIGTERM flips the root token.
        let sc = shutdown.clone();
        spawner.spawn(move |_| async move { sc.listen_for_signals().await });

        Ok(Self {
            runtime,
            shutdown,
            pod_manager,
            dht,
            dht_addr,
            listen_port,
            node_id: id,
        })
    }

    pub fn node_id(&self) -> NodeID { self.node_id }
    pub fn listen_port(&self) -> u16 { self.listen_port }
    pub fn dht_addr(&self) -> Option<SocketAddr> { self.dht_addr }
    pub fn spawner(&self) -> &TaskSpawner { self.runtime.spawner() }
    pub fn cancel_token(&self) -> CancellationToken { self.runtime.cancel_token() }

    /// Register a yarn with the mesh. If the DHT is up, announce to our
    /// current routing-table neighbours so the swarm can find us.
    pub async fn add_yarn(&self, fabric: Arc<YarnFabric>) -> Result<()> {
        let info_hash = fabric.info_hash;
        self.pod_manager.add_yarn(fabric);

        if let Some(dht) = &self.dht {
            for addr in self.known_dht_nodes(8) {
                if let Ok(out) = dht.get_peers(addr, info_hash).await {
                    let token = match out {
                        GetPeersOutput::Peers { token, .. } => token,
                        GetPeersOutput::Closest { token, .. } => token,
                    };
                    let _ = dht
                        .announce_peer(addr, info_hash, self.listen_port, token)
                        .await;
                }
            }
        }
        Ok(())
    }

    /// Ask the DHT who has `info_hash`, then dial each result.
    /// Returns the addresses we attempted.
    pub async fn find_and_dial(&self, info_hash: InfoHash) -> Result<Vec<SocketAddr>> {
        let dht = self.dht.as_ref().ok_or(Error::ConnectionFailed)?;
        let mut found = Vec::new();
        for addr in self.known_dht_nodes(8) {
            if let Ok(GetPeersOutput::Peers { peers, .. }) =
                dht.get_peers(addr, info_hash).await
            {
                for p in peers {
                    found.push(p);
                    let _ = self.pod_manager.dial(p, info_hash).await;
                }
            }
        }
        Ok(found)
    }

    /// Skip discovery entirely and dial a known peer. This is what you want
    /// for a two-process test.
    pub async fn dial(&self, addr: SocketAddr, info_hash: InfoHash) -> Result<()> {
        self.pod_manager.dial(addr, info_hash).await
    }

    fn known_dht_nodes(&self, limit: usize) -> Vec<SocketAddr> {
        let Some(dht) = &self.dht else { return Vec::new() };
        let rt = dht.routing_table.lock().unwrap();
        rt.buckets
            .iter()
            .flat_map(|b| b.nodes.iter())
            .map(|n| n.address)
            .take(limit)
            .collect()
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        // Give spawned tasks a moment to observe cancellation before the
        // process exits; TaskTracker::wait is behind &self and we own it here.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
}