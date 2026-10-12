pub mod pex;
pub mod piece;
pub mod needle;
pub mod choking;
pub mod protocol;
pub mod connection;

use std::sync::Arc;
use std::sync::Mutex;
use std::path::PathBuf;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::collections::HashSet;

use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use pex::Pex;
use piece::Bitfield;
use protocol::Message;
use piece::PieceManager;
use choking::ChokeManager;
use connection::Connection;

use crate::yarn::Yarn;
use crate::error::Error;
use crate::types::Result;
use crate::types::NodeID;
use crate::types::InfoHash;
use crate::storage::spool::Spool;
use crate::runtime::tracker::TaskSpawner;

const CHOKE_INTERVAL: Duration = Duration::from_secs(10);
const OPTIMISTIC_INTERVAL: Duration = Duration::from_secs(30);
const MAX_PEX_BUFFER: usize = 100;
const MAX_CONNECTIONS: usize = 30;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub enum FabricEvent {
    /// a connection wants to receive messages routed to its addr
    Register(SocketAddr, mpsc::Sender<Message>),

    /// closing connection
    Unregister(SocketAddr),

    /// pass a message to a specific connection
    Send(SocketAddr, Message),

    /// pass a message to all connections, with optional exception
    Broadcast(Message, Option<SocketAddr>),
}

#[derive(Clone)]
pub struct FabricHandle {
    pub tx: mpsc::Sender<FabricEvent>,
}

impl FabricHandle {
    pub fn register(&self, addr: SocketAddr, tx: mpsc::Sender<Message>) {
        let _ = self.tx.try_send(FabricEvent::Register(addr, tx));
    }

    pub fn unregister(&self, addr: SocketAddr) {
        let _ = self.tx.try_send(FabricEvent::Unregister(addr));
    }

    pub fn send(&self, to: SocketAddr, message: Message) {
        let _ = self.tx.try_send(FabricEvent::Send(to, message));
    }

    pub fn broadcast(&self, message: Message, except: Option<SocketAddr>) {
        let _ = self.tx.try_send(FabricEvent::Broadcast(message, except));
    }
}

/// The router task. Owns the map of registered connections and forwards
/// messages from the handle to the right connection's outbound channel.
async fn router_loop(
    mut rx: mpsc::Receiver<FabricEvent>,
    token: CancellationToken,
) {
    let mut senders: HashMap<SocketAddr, mpsc::Sender<Message>> = HashMap::new();

    loop {
        tokio::select! {
            _ = token.cancelled() => return,

            Some(event) = rx.recv() => match event {
                FabricEvent::Register(addr, tx) => {
                    senders.insert(addr, tx);
                }
                FabricEvent::Unregister(addr) => {
                    senders.remove(&addr);
                }
                FabricEvent::Send(addr, msg) => {
                    if let Some(tx) = senders.get(&addr) {
                        let _ = tx.try_send(msg);
                    }
                }
                FabricEvent::Broadcast(msg, except) => {
                    for (addr, tx) in &senders {
                        if Some(*addr) == except {
                            continue;
                        }
                        let _ = tx.try_send(msg.clone());
                    }
                }
            }
        }
    }
}

async fn pex_requester(fabric: Arc<YarnFabric>, token: CancellationToken) {
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    tick.tick().await;

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tick.tick() => {
                fabric.router.broadcast(Message::PexRequest, None);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// YarnFabric
// ---------------------------------------------------------------------------

/// Shared between all connections for a single Yarn (file)
#[derive(Clone)]
pub struct YarnFabric {
    pub yarn: Arc<Yarn>,
    pub info_hash: InfoHash,
    pub peers: Arc<Mutex<HashSet<SocketAddr>>>,
    pub piece_manager: Arc<Mutex<PieceManager>>,
    pub choke_manager: Arc<Mutex<ChokeManager>>,
    pub spool: Spool,
    pub have_tx: broadcast::Sender<u32>,
    pub router: FabricHandle,
    pub pex: Arc<Mutex<Pex>>,
    pub stop: CancellationToken,
}

impl YarnFabric {
    /// Build a fabric for an already-loaded yarn, writing downloads into
    /// `output_path`. The output file must already exist at the right size.
    pub fn new(
        yarn: Arc<Yarn>,
        output_path: PathBuf,
        spawner: &TaskSpawner,
        my_addr: SocketAddr,
        seed: bool,
        stop: CancellationToken,
    ) -> Result<Arc<Self>> {
        let info_hash = yarn.info_hash()?;
        let piece_manager = if seed {
            let mut bitfield = Bitfield::new(yarn.info.pieces.len());
            bitfield.full();
            Arc::new(Mutex::new(PieceManager::resume(&yarn, bitfield)?))
        } else {
            Arc::new(Mutex::new(PieceManager::new(&yarn)?))
        };
        let choke_manager = Arc::new(Mutex::new(ChokeManager::new()));
        let spool = Spool::new(
            output_path.to_str().ok_or(Error::UnexpectedFormat)?,
            yarn.info.piece_length as u64,
            yarn.info.total_length,
        )?;
        let peers = Arc::new(Mutex::new(HashSet::new()));
        let pex = Arc::new(Mutex::new(Pex::new(my_addr, MAX_PEX_BUFFER)));

        // Have broadcast: one sender (kept in fabric), many subscribers
        // (each connection subscribes).
        let (have_tx, _) = broadcast::channel(32);

        // Router channel: sender in the handle, receiver in the router task.
        let (router_tx, router_rx) = mpsc::channel::<FabricEvent>(64);
        let router = FabricHandle { tx: router_tx };

        let fabric = Arc::new(Self {
            yarn,
            info_hash,
            peers: peers.clone(),
            piece_manager,
            choke_manager,
            spool,
            have_tx,
            router,
            pex,
            stop,
        });

        // Spawn the router task for this yarn.
        spawner.spawn({
            let yarn_stop = fabric.stop.clone();
            move |_| async move {
                router_loop(router_rx, yarn_stop).await;
            }
        });

        // Spawn the choke scheduler for this yarn.
        spawner.spawn({
            let fabric = fabric.clone();
            let yarn_stop = fabric.stop.clone();
            move |_| async move {
                choke_scheduler(fabric, yarn_stop).await;
            }
        });

        // spawn the pex requester which checks for new pex addrs every 60 s
        spawner.spawn({
            let yarn_stop = fabric.stop.clone();
            let fabric = fabric.clone();
            move |_| async move {
                pex_requester(fabric, yarn_stop).await;
            }
        });

        Ok(fabric)
    }

    /// Convenience for the seeder case: build a yarn from a source file and
    /// use the same file for both reading and writing.
    pub async fn from_source_file(
        author: Option<String>,
        creation_date: Option<String>,
        comments: Option<String>,
        source_path: PathBuf,
        spawner: &TaskSpawner,
        my_addr: SocketAddr,
        stop: CancellationToken,
    ) -> Result<Arc<Self>> {
        let path_str = source_path
            .to_str()
            .ok_or(Error::UnexpectedFormat)?
            .to_string();
        let yarn = Arc::new(
            Yarn::from_file(author, creation_date, comments, path_str).await?,
        );
        Self::new(yarn, source_path, spawner, my_addr, true, stop)
    }

    pub fn contains(&self, addr: SocketAddr) -> bool {
        self.peers.lock().unwrap().get(&addr).is_some()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.lock().unwrap().len()
    }
}

// ---------------------------------------------------------------------------
// Choke scheduler
// ---------------------------------------------------------------------------

async fn choke_scheduler(fabric: Arc<YarnFabric>, token: CancellationToken) {
    let mut choke_tick = tokio::time::interval(CHOKE_INTERVAL);
    let mut optimistic_tick = tokio::time::interval(OPTIMISTIC_INTERVAL);

    // Consume the immediate first tick of each so we don't fire at t=0.
    choke_tick.tick().await;
    optimistic_tick.tick().await;

    loop {
        tokio::select! {
            _ = token.cancelled() => return,

            _ = choke_tick.tick() => {
                let changes = fabric
                    .choke_manager
                    .lock()
                    .unwrap()
                    .decide_unchokes();
                for (addr, unchoke) in changes {
                    let msg = if unchoke { Message::Unchoke } else { Message::Choke };
                    fabric.router.send(addr, msg);
                }
            }

            _ = optimistic_tick.tick() => {
                let chosen = fabric
                    .choke_manager
                    .lock()
                    .unwrap()
                    .optimistic_unchoke();
                if let Some(addr) = chosen {
                    fabric.router.send(addr, Message::Unchoke);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PodManager
// ---------------------------------------------------------------------------

pub struct PodManager {
    pub listener: TcpListener,
    pub id: NodeID,
    pub listen_port: u16,
    pub yarns: Arc<Mutex<HashMap<InfoHash, Arc<YarnFabric>>>>,
    pub stop: CancellationToken,
}

impl PodManager {
    pub async fn new(
        bind_addr: SocketAddr,
        id: NodeID,
        stop: CancellationToken,
    ) -> Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;
        let listen_port = listener.local_addr()?.port();

        Ok(Self {
            listener,
            id,
            listen_port,
            yarns: Arc::new(Mutex::new(HashMap::new())),
            stop,
        })
    }

    pub fn add_yarn(&self, yarn_fabric: Arc<YarnFabric>) {
        self.yarns
            .lock()
            .unwrap()
            .insert(yarn_fabric.info_hash, yarn_fabric);
    }

    pub fn get_yarn(&self, info_hash: &InfoHash) -> Option<Arc<YarnFabric>> {
        self.yarns.lock().unwrap().get(info_hash).cloned()
    }

    /// Dial a peer for the given yarn. Spawns the connection task and
    /// returns immediately.
    pub async fn dial(&self, addr: SocketAddr, info_hash: InfoHash) -> Result<()> {
        let fabric = self
            .get_yarn(&info_hash)
            .ok_or(Error::YarnNotFound)?;

        // Check-and-insert atomically. Either this dial is counted now,
        // or we skip because there's already a connection/dial in flight.
        {
            let mut peers = fabric.peers.lock().unwrap();
            if peers.contains(&addr) {
                return Ok(());
            }
            if peers.len() >= MAX_CONNECTIONS {
                return Ok(());
            }
            peers.insert(addr);
        }

        let id = self.id;
        let listen_port = self.listen_port;
        let stop = self.stop.clone();

        tokio::spawn(async move {
            if let Err(e) = Connection::connect(
                addr,
                id,
                listen_port,
                info_hash,
                fabric.yarn.clone(),
                fabric.piece_manager.clone(),
                fabric.choke_manager.clone(),
                fabric.spool.clone(),
                fabric.pex.clone(),
                fabric.have_tx.clone(),
                fabric.router.clone(),
                stop,
            )
            .await
            {
                eprintln!("outbound {} failed: {:?}", addr, e);
            }

            fabric.peers.lock().unwrap().remove(&addr);
        });

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => return Ok(()),

                result = self.listener.accept() => {
                    let (stream, pod_addr) = match result {
                        Ok(x) => x,
                        Err(_) => continue,
                    };

                    let id = self.id;
                    let listen_port = self.listen_port;
                    let yarns = self.yarns.clone();
                    let stop = self.stop.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_inbound(
                            stream, pod_addr, id, listen_port, yarns, stop,
                        ).await {
                            eprintln!("inbound {} failed: {:?}", pod_addr, e);
                        }
                    });
                }
            }
        }
    }

    pub async fn discovery_loop(&self, stop: CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        tick.tick().await;

        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tick.tick() => {
                    let yarns: Vec<_> = self
                        .yarns
                        .lock()
                        .unwrap()
                        .values()
                        .cloned()
                        .collect();

                    for fabric in yarns {
                        let candidates = fabric.pex.lock().unwrap().take_for_dial(5);

                        for addr in candidates {
                            if let Err(e) = self.dial(addr, fabric.info_hash).await {
                                eprintln!("dial {} failed: {:?}", addr, e);
                            }
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Inbound handling
// ---------------------------------------------------------------------------

async fn handle_inbound(
    mut stream: TcpStream,
    pod_addr: SocketAddr,
    id: NodeID,
    listen_port: u16,
    yarns: Arc<Mutex<HashMap<InfoHash, Arc<YarnFabric>>>>,
    stop: CancellationToken
) -> Result<()> {
    // Read the handshake frame so we know which yarn the peer wants.
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    let len = needle::parse_length(header)?;

    let mut payload = vec![0u8; len as usize];
    stream.read_exact(&mut payload).await?;

    let value = needle::decode_message(&payload)?;
    let handshake = Message::from_bencode(&value)?;

    let info_hash = match &handshake {
        Message::Handshake { info_hash, .. } => *info_hash,
        _ => return Err(Error::InvalidPodResponse),
    };

    // Look up the yarn. If we're not serving this file, close.
    let fabric = {
        let map = yarns.lock().unwrap();
        map.get(&info_hash).cloned().ok_or(Error::YarnNotFound)?
    };

    {
        let mut peers = fabric.peers.lock().unwrap();
        if peers.len() >= MAX_CONNECTIONS {
            return Ok(());
        }

        peers.insert(pod_addr);
    }

    // Hand off to a connection task, giving it the handshake we already read.
    let result = Connection::accept(
        stream,
        pod_addr,
        id,
        listen_port,
        info_hash,
        handshake,
        fabric.yarn.clone(),
        fabric.piece_manager.clone(),
        fabric.choke_manager.clone(),
        fabric.spool.clone(),
        fabric.pex.clone(),
        fabric.have_tx.clone(),
        fabric.router.clone(),
        stop,
    )
    .await;

    fabric.peers.lock().unwrap().remove(&pod_addr);
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Spawn a router task and return its handle + cancel token + join handle.
    fn spawn_router() -> (FabricHandle, CancellationToken, tokio::task::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel::<FabricEvent>(16);
        let handle = FabricHandle { tx };

        let token = CancellationToken::new();
        let task = tokio::spawn(router_loop(rx, token.clone()));

        (handle, token, task)
    }

    #[tokio::test]
    async fn pod_manager_binds_and_records_port() {
        let stop = CancellationToken::new();
        let mgr = PodManager::new(
            "127.0.0.1:0".parse().unwrap(),
            NodeID([1; 32]),
            stop,
        )
        .await
        .unwrap();

        assert!(mgr.listen_port > 0);
        assert_eq!(mgr.yarns.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn router_registers_and_routes() {
        let (handle, token, task) = spawn_router();

        let (conn_tx, mut conn_rx) = mpsc::channel::<Message>(4);
        let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        handle.register(addr, conn_tx);
        tokio::time::sleep(Duration::from_millis(20)).await;

        handle.send(addr, Message::Choke);

        let got = tokio::time::timeout(
            Duration::from_millis(200),
            conn_rx.recv(),
        )
        .await
        .expect("timeout")
        .unwrap();

        assert!(matches!(got, Message::Choke));

        token.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn router_broadcasts_to_all_except_sender() {
        let (handle, token, task) = spawn_router();

        let addr_a: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:2".parse().unwrap();

        let (a_tx, mut a_rx) = mpsc::channel::<Message>(4);
        let (b_tx, mut b_rx) = mpsc::channel::<Message>(4);
        handle.register(addr_a, a_tx);
        handle.register(addr_b, b_tx);
        tokio::time::sleep(Duration::from_millis(20)).await;

        handle.broadcast(Message::Have { piece_index: 3 }, Some(addr_a));

        let b_got = tokio::time::timeout(
            Duration::from_millis(200),
            b_rx.recv(),
        )
        .await
        .expect("b timeout")
        .unwrap();
        assert!(matches!(b_got, Message::Have { piece_index: 3 }));

        let a_got = tokio::time::timeout(
            Duration::from_millis(100),
            a_rx.recv(),
        )
        .await;
        assert!(a_got.is_err(), "A should not have received anything");

        token.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn unregister_stops_routing() {
        let (handle, token, task) = spawn_router();

        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let (conn_tx, mut conn_rx) = mpsc::channel::<Message>(4);
        handle.register(addr, conn_tx);
        tokio::time::sleep(Duration::from_millis(20)).await;

        handle.unregister(addr);
        tokio::time::sleep(Duration::from_millis(20)).await;

        handle.send(addr, Message::Choke);
        let got = tokio::time::timeout(
            Duration::from_millis(100),
            conn_rx.recv(),
        )
        .await;

        match got {
            Err(_) => {}
            Ok(None) => {}
            Ok(Some(msg)) => panic!("unregistered peer received {:?}", msg),
        }

        token.cancel();
        let _ = task.await;
    }
}