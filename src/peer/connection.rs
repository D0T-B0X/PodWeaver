use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::yarn::Yarn;
use crate::error::Error;
use crate::types::NodeID;
use crate::types::Result;
use crate::types::InfoHash;
use crate::storage::spool::Spool;

use super::needle;
use super::pex::Pex;
use super::choking::*;
use super::protocol::*;
use super::piece::Bitfield;
use super::piece::PieceManager;
use super::FabricHandle;

const MAX_IN_FLIGHT: u8 = 8;
const TICK: Duration = Duration::from_millis(500);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const BLOCK_LENGTH: usize = 16_384;
const PEX_INTERVAL_TICKS: u32 = 120; // 120 * 500ms = 60s

pub struct Connection {
    // connection identity
    pub our_id: NodeID,
    pub listen_port: u16,
    pub info_hash: InfoHash,

    // pod information
    pub pod_addr: SocketAddr,
    pub pod_bitfield: Bitfield,
    pub pod_id: NodeID,

    // process address of the remote pod
    pub pod_listen_addr: Option<SocketAddr>,

    // shared between pods
    pub yarn: Arc<Yarn>,
    pub choke_manager: Arc<Mutex<ChokeManager>>,
    pub piece_manager: Arc<Mutex<PieceManager>>,
    pub spool: Spool,
    pub have_tx: broadcast::Sender<u32>,

    // connection state
    pub in_flight: HashMap<(u32, u32), Instant>,
    pub last_activity: Instant,

    // pex
    pub pex: Arc<Mutex<Pex>>,

    // channels
    pub outbound_tx: mpsc::Sender<Message>,
    pub have_rx: broadcast::Receiver<u32>,
    pub router: FabricHandle,
    pub cancel_token: CancellationToken,

    // for the accept path: the handshake we already read
    pub pre_read_handshake: Option<Message>,

    // rate limiter
    rate_limiter: Option<RateLimiter>,
}

impl Connection {

    /// Connect to an online pod in the swarm
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        addr: SocketAddr,
        our_id: NodeID,
        listen_port: u16,
        info_hash: InfoHash,
        yarn: Arc<Yarn>,
        piece_manager: Arc<Mutex<PieceManager>>,
        choke_manager: Arc<Mutex<ChokeManager>>,
        spool: Spool,
        pex: Arc<Mutex<Pex>>,
        have_tx: broadcast::Sender<u32>,
        router: FabricHandle,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let stream = TcpStream::connect(addr).await?;
        let (mut connection, stream, outbound_rx) = Self::new(
            stream,
            addr,
            our_id,
            listen_port,
            info_hash,
            yarn,
            piece_manager,
            choke_manager,
            spool,
            pex,
            have_tx,
            router,
            cancel_token,
        );

        let (reader, writer) = stream.into_split();
        connection.run(reader, writer, outbound_rx).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn accept(
        stream: TcpStream,
        pod_addr: SocketAddr,
        our_id: NodeID,
        listen_port: u16,
        info_hash: InfoHash,
        pre_read_handshake: Message,
        yarn: Arc<Yarn>,
        piece_manager: Arc<Mutex<PieceManager>>,
        choke_manager: Arc<Mutex<ChokeManager>>,
        spool: Spool,
        pex: Arc<Mutex<Pex>>,
        have_tx: broadcast::Sender<u32>,
        router: FabricHandle,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let (mut connection, stream, outbound_rx) = Self::new(
            stream,
            pod_addr,
            our_id,
            listen_port,
            info_hash,
            yarn,
            piece_manager,
            choke_manager,
            spool,
            pex,
            have_tx,
            router,
            cancel_token,
        );
        connection.pre_read_handshake = Some(pre_read_handshake);

        let (reader, writer) = stream.into_split();
        connection.run(reader, writer, outbound_rx).await
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        stream: TcpStream,
        pod_addr: SocketAddr,
        our_id: NodeID,
        listen_port: u16,
        info_hash: InfoHash,
        yarn: Arc<Yarn>,
        piece_manager: Arc<Mutex<PieceManager>>,
        choke_manager: Arc<Mutex<ChokeManager>>,
        spool: Spool,
        pex: Arc<Mutex<Pex>>,
        have_tx: broadcast::Sender<u32>,
        router: FabricHandle,
        cancel_token: CancellationToken,
    ) -> (Self, TcpStream, mpsc::Receiver<Message>) {
        let (outbound_tx, outbound_rx) = mpsc::channel(64);
        let have_rx = have_tx.subscribe();
        let piece_count = yarn.info.pieces.len();

        let rate_limiter = std::env::var("DOWNLOAD_RATE")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .map(RateLimiter::new);

        (Self {
            our_id,
            listen_port,
            info_hash,
            pod_addr,
            pod_bitfield: Bitfield::new(piece_count),
            pod_id: NodeID([0u8; 32]), // dummy ID
            pod_listen_addr: None,     // changed after handshake
            yarn,
            choke_manager,
            piece_manager,
            spool,
            pex,
            have_tx,
            in_flight: HashMap::new(),
            last_activity: Instant::now(),
            outbound_tx,
            have_rx,
            router,
            cancel_token,
            pre_read_handshake: None,
            rate_limiter,
        }, stream, outbound_rx)
    }

    async fn run<R, W>(&mut self, mut reader: R, writer: W, rx: mpsc::Receiver<Message>) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        // initialize the writer task so the connection can send out messages
        let write_task = tokio::spawn(Self::write_loop(writer, rx));

        // perform pod handshake (may use a pre-read message from accept path)
        let pre_read = self.pre_read_handshake.take();
        self.handshake(&mut reader, pre_read).await?;

        // add the verified pod to the choke manager's table
        self.choke_manager.lock().unwrap().add_peer(self.pod_addr);

        // register with the fabric so the choke scheduler and other
        // connections can deliver messages to us
        self.router.register(self.pod_addr, self.outbound_tx.clone());

        // Register pod_listen_addr with PEX pool so it gets shared across the swarm
        if let Some(listen_addr) = self.pod_listen_addr {
            self.pex.lock().unwrap().add([listen_addr]);
        }

        self.pass_bitfield().await?;

        // initialize gossip
        let _ = self.outbound_tx.try_send(Message::PexRequest);

        let result = self.read_loop(reader).await;

        // cleanup
        let mut pm = self.piece_manager.lock().unwrap();
        for (&(piece, block), _) in &self.in_flight {
            pm.cancel_request(piece, block, self.pod_addr);
        }
        self.in_flight.clear();
        self.router.unregister(self.pod_addr);
        self.choke_manager.lock().unwrap().remove_peer(self.pod_addr);
        write_task.abort();

        result
    }

    async fn write_loop<W>(mut writer: W, mut rx: mpsc::Receiver<Message>) -> Result<()>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        while let Some(message) = rx.recv().await {
            Self::write_message(&mut writer, &message).await?;
        }
        Ok(())
    }

    async fn read_loop<R>(&mut self, reader: R) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let (msg_tx, mut msg_rx) = mpsc::channel::<Result<Message>>(16);

        let reader_handle = tokio::spawn(async move {
            let mut reader = reader;
            loop {
                match tokio::time::timeout(
                    Duration::from_secs(30),
                    Self::read_message(&mut reader),
                ).await {
                    Ok(Ok(m)) => {
                        if msg_tx.send(Ok(m)).await.is_err() { break; }
                    }
                    Ok(Err(e)) => {
                        let _ = msg_tx.send(Err(e)).await;
                        break;
                    }
                    Err(_elapsed) => break,   // read timed out
                }
            }
        });

        let mut ticker = tokio::time::interval(TICK);
        ticker.tick().await;
        let mut pex_ticks = 0u32;

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => break,

                msg = msg_rx.recv() => {
                    match msg {
                        Some(Ok(m)) => {
                            if self.handle_message(m).await.is_err() { break; }
                            self.last_activity = Instant::now();
                        }
                        Some(Err(_)) | None => break,
                    }
                }

                result = self.have_rx.recv() => {
                    if let Ok(piece_index) = result {
                        let _ = self.outbound_tx
                            .send(Message::Have { piece_index })
                            .await
                            .map_err(|_| Error::MessageNotSent);
                    }
                }

                _ = ticker.tick() => {
                    self.check_timeouts();
                    if self.last_activity.elapsed() > Duration::from_secs(120) {
                        break;
                    }
                    if self.refill_pipeline().await.is_err() { break; }

                    // Periodically send unsolicited PEX samples to keep swarm active
                    pex_ticks += 1;
                    if pex_ticks >= PEX_INTERVAL_TICKS {
                        pex_ticks = 0;
                        let (added, dropped) = {
                            let pex = self.pex.lock().unwrap();
                            (pex.sample(20), pex.sample_drops(20))
                        };
                        if !added.is_empty() || !dropped.is_empty() {
                            let _ = self.outbound_tx.send(Message::PexResponse {
                                added,
                                dropped,
                            }).await;
                        }
                    }
                }
            }
        }

        reader_handle.abort();
        Ok(())
    }

    async fn handle_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Handshake { .. } => {
                return Err(Error::InvalidPodResponse);
            },

            Message::Choke => {
                self.choke_manager.lock().unwrap()
                    .set_peer_choking(self.pod_addr, true)?;
                self.in_flight.clear();
            },

            Message::Unchoke => {
                self.choke_manager.lock().unwrap()
                    .set_peer_choking(self.pod_addr, false)?;
                self.refill_pipeline().await?;
            },

            Message::Interested => {
                self.choke_manager.lock().unwrap()
                    .set_peer_interested(self.pod_addr, true)?;
            },

            Message::NotInterested => {
                self.choke_manager.lock().unwrap()
                    .set_peer_interested(self.pod_addr, false)?;
            },

            Message::Have { piece_index } => {
                self.pod_bitfield.set(piece_index);
                self.maybe_send_interested().await?;
            },

            Message::Bitfield { bits } => {
                let piece_count = self.piece_manager.lock().unwrap().piece_count;
                self.pod_bitfield = Bitfield::from_bytes(bits, piece_count as usize)?;
                self.maybe_send_interested().await?;
            },

            Message::Request { piece_index, offset, length } => {
                self.handle_request(piece_index, offset, length).await?;
            },

            Message::Piece { piece_index, offset, data } => {
                self.handle_piece(piece_index, offset, data).await?;
            },

            Message::Cancel { .. } => {
                // Upload cancellation handled via internal task check
            },

            Message::PexRequest => {
                let (added, dropped) = {
                    let pex = self.pex.lock().unwrap();
                    (pex.sample(20), pex.sample_drops(20))
                };

                self.outbound_tx.send(Message::PexResponse {
                    added,
                    dropped,
                }).await.map_err(|_| Error::MessageNotSent)?;
            },

            Message::PexResponse { added, dropped } => {
                let mut pex = self.pex.lock().unwrap();
                pex.add(added);
                pex.remove(dropped);
            }
        };
        Ok(())
    }

    async fn maybe_send_interested(&mut self) -> Result<()> {
        let should = {
            let piece_manager = self.piece_manager.lock().unwrap();
            ChokeManager::should_be_interested(&self.pod_bitfield, &piece_manager.bitfield)
        };

        let current_interest = self
            .choke_manager
            .lock()
            .unwrap()
            .am_interested(self.pod_addr)?;

        if should && !current_interest {
            self.outbound_tx.send(Message::Interested)
                .await
                .map_err(|_| Error::MessageNotSent)?;
            self.choke_manager.lock().unwrap()
                .set_am_interested(self.pod_addr, true)?;
        } else if !should && current_interest {
            self.outbound_tx.send(Message::NotInterested)
                .await
                .map_err(|_| Error::MessageNotSent)?;
            self.choke_manager.lock().unwrap()
                .set_am_interested(self.pod_addr, false)?;
        }

        Ok(())
    }

    async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Message> {
        let mut header = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut header).await {
            return Err(Error::FileError(e));
        }
        let len = needle::parse_length(header)?;

        let mut payload = vec![0u8; len as usize];
        reader.read_exact(&mut payload).await?;

        let value = needle::decode_message(&payload)?;
        Message::from_bencode(&value)
    }

    async fn write_message<W: AsyncWrite + Unpin>(writer: &mut W, message: &Message) -> Result<()> {
        let bytes = needle::encode_message(&message.clone().to_bencode())?;
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn handshake<R>(&mut self, reader: &mut R, pre_read: Option<Message>) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        // perform a handshake with the remote pod
        self.outbound_tx.send(Message::Handshake {
            node_id: self.our_id,
            info_hash: self.info_hash,
            listen_port: self.listen_port,
        })
        .await
        .map_err(|_| Error::MessageNotSent)?;

        // wait for the peer's handshake — either pre-read by the accept path
        // or read from the wire here
        let pod_response = match pre_read {
            Some(m) => m,
            None => tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                Self::read_message(reader),
            )
            .await
            .map_err(|_| Error::ResponseTimedOut)??,
        };

        match pod_response {
            Message::Handshake { node_id, info_hash, listen_port } => {
                if info_hash != self.info_hash || node_id == self.our_id {
                    return Err(Error::InvalidPodResponse);
                }

                self.pod_listen_addr = if listen_port == 0 {
                    None
                } else {
                    Some(SocketAddr::new(self.pod_addr.ip(), listen_port))
                };
                self.pod_id = node_id;
            },
            _ => return Err(Error::InvalidPodResponse),
        };

        Ok(())
    }

    async fn pass_bitfield(&mut self) -> Result<()> {
        let bits = {
            let piece_manager = self.piece_manager.lock().unwrap();
            if piece_manager.bitfield.count_ones() == 0 {
                return Ok(());
            }
            piece_manager.bitfield.bits.clone()
        };
        let message = Message::Bitfield { bits };

        self.outbound_tx.send(message)
            .await
            .map_err(|_| Error::MessageNotSent)?;
        Ok(())
    }

    async fn handle_request(&mut self, piece_index: u32, offset: u64, length: u64) -> Result<()> {
        // verify the block exists
        if !self.piece_manager.lock().unwrap().exists(piece_index) {
            return Ok(());
        }

        let (piece_len, total_len) = {
            (self.yarn.info.piece_length, self.yarn.info.total_length)
        };

        // error checks
        if length == 0 || length > BLOCK_LENGTH as u64 { return Ok(()); }
        let piece_start = piece_index as u64 * piece_len as u64;
        if piece_start >= total_len { return Ok(()); }
        let this_piece_len = (total_len - piece_start).min(piece_len as u64);
        let end = match offset.checked_add(length) {
            Some(e) => e,
            None => return Ok(()),
        };
        if end > this_piece_len {
            return Ok(());
        }

        // STRICT CHOKE CHECK: Reject block requests if the peer is choked
        match self.choke_manager.lock().unwrap().am_uploading_to(self.pod_addr) {
            Ok(true) => {},
            _ => return Ok(()),
        }

        let global_offset = piece_start + offset;

        let spool = self.spool.clone();
        let data = tokio::task::spawn_blocking(move || {
            spool.read_block(global_offset, length)
        })
        .await
        .map_err(|_| Error::FileReadFailed)??;

        self.outbound_tx.send(Message::Piece {
            piece_index,
            offset,
            data,
        })
        .await
        .map_err(|_| Error::MessageNotSent)?;

        Ok(())
    }

    async fn handle_piece(&mut self, piece_index: u32, offset: u64, data: Vec<u8>) -> Result<()> {
        let block = offset / BLOCK_LENGTH as u64;

        // Always clear our own in-flight slot, even if the data is a duplicate.
        self.in_flight.remove(&(piece_index, block as u32));

        let (piece_len, total_len) = {
            let m = self.piece_manager.lock().unwrap();
            (m.piece_length as u64, m.total_length)
        };
        let piece_start = piece_index as u64 * piece_len;
        if piece_start >= total_len { return Ok(()); }
        let this_piece_len = (total_len - piece_start).min(piece_len);
        if offset >= this_piece_len { return Ok(()); }

        if self.piece_manager.lock().unwrap().block_exists(piece_index, block) {
            self.refill_pipeline().await?;
            return Ok(());
        }

        self.choke_manager.lock().unwrap()
            .record_download(self.pod_addr, data.len() as u64)?;

        let record_result = {
            let mut piece_manager = self.piece_manager.lock().unwrap();
            piece_manager.record_block(piece_index, block as u32, data, self.pod_addr)
        };

        let (bytes, cancel) = match record_result {
            Ok(r) => r,
            Err(Error::BlockAlreadyReceived) => {
                self.refill_pipeline().await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        if !cancel.is_empty() {
            let block_len = self.piece_manager.lock().unwrap()
                .block_length(piece_index, block as u32);
            for peer_addr in cancel {
                self.router.send(peer_addr, Message::Cancel {
                    piece_index,
                    offset,
                    length: block_len,
                });
            }
        }

        let Some(b) = bytes else {
            self.refill_pipeline().await?;
            return Ok(());
        };

        let expected_hash = self.yarn.info.pieces[piece_index as usize];
        if Self::hash(&b) != expected_hash {
            self.piece_manager.lock().unwrap().mark_failed(piece_index, self.pod_addr)?;
            return Err(Error::MaliciousPeer);
        }

        let global_offset = piece_index as u64 * self.yarn.info.piece_length as u64;
        let spool = self.spool.clone();
        tokio::task::spawn_blocking(move || {
            spool.write_block(global_offset, &b)
        })
        .await
        .map_err(|_| Error::FileWriteFailed)??;

        self.piece_manager.lock().unwrap().mark_complete(piece_index)?;
        let _ = self.have_tx.send(piece_index);

        self.refill_pipeline().await?;
        Ok(())
    }

    async fn refill_pipeline(&mut self) -> Result<()> {
        if !self.choke_manager.lock().unwrap().am_interested(self.pod_addr)? {
            return Ok(())
        }
        if !self.choke_manager.lock().unwrap().can_request(self.pod_addr)? {
            return Ok(());
        }

        while self.in_flight.len() < MAX_IN_FLIGHT as usize {
            let (piece, block) = {
                let pm = self.piece_manager.lock().unwrap();
                let endgame = pm.is_endgame();
                let Some(piece) = pm.choose_piece(&self.pod_bitfield, endgame) else { break; };
                let Some(block) = pm.choose_block(piece, self.pod_addr, endgame) else { break; };
                (piece, block)
            };

            if self.in_flight.contains_key(&(piece, block)) {
                break;
            }

            let (offset, length) = {
                let pm = self.piece_manager.lock().unwrap();
                (pm.block_offset(block as u64), pm.block_length(piece, block))
            };

            if let Some(rl) = self.rate_limiter.as_mut() {
                if !rl.try_consume(length) {
                    let wait = rl.time_until(length);
                    tokio::time::sleep(wait).await;
                    if !rl.try_consume(length) {
                        break;
                    }
                }
            }

            self.outbound_tx.send(Message::Request {
                piece_index: piece,
                offset,
                length,
            }).await.map_err(|_| Error::MessageNotSent)?;

            self.in_flight.insert((piece, block), Instant::now());
            self.piece_manager.lock().unwrap().mark_requested(piece, block, self.pod_addr);
        }

        Ok(())
    }

    fn check_timeouts(&mut self) {
        let now = Instant::now();
        let mut expired = Vec::new();
        self.in_flight.retain(|&(piece, block), sent_at| {
            if now.duration_since(*sent_at) < RESPONSE_TIMEOUT {
                true
            } else {
                expired.push((piece, block));
                false
            }
        });

        let mut piece_manager = self.piece_manager.lock().unwrap();
        for (piece, block) in expired {
            piece_manager.cancel_request(piece, block, self.pod_addr);
        }
    }

    fn hash(bytes: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hasher.finalize().into()
    }
}

struct RateLimiter {
    bytes_per_sec: u64,
    available: f64,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            available: bytes_per_sec as f64,
            last_refill: Instant::now(),
        }
    }

    fn try_consume(&mut self, bytes: u64) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.available = (self.available + elapsed * self.bytes_per_sec as f64)
            .min(self.bytes_per_sec as f64);
        if self.available >= bytes as f64 {
            self.available -= bytes as f64;
            true
        } else {
            false
        }
    }

    fn time_until(&self, bytes: u64) -> Duration {
        if self.available >= bytes as f64 {
            return Duration::ZERO;
        }
        let needed = bytes as f64 - self.available;
        Duration::from_secs_f64(needed / self.bytes_per_sec as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::FabricEvent;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::net::TcpListener;

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    fn unique_path(prefix: &str) -> String {
        let n = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        format!(
            "/tmp/podweaver_conn_test_{}_{}_{}",
            prefix,
            std::process::id(),
            n
        )
    }

    const TEST_FILE_SIZE: usize = 64 * 1024;
    const PEX_POOL_SIZE: usize = 100;

    fn test_content() -> Vec<u8> {
        (0..TEST_FILE_SIZE).map(|i| (i % 251) as u8).collect()
    }

    async fn build_yarn(content: &[u8]) -> (Arc<Yarn>, String) {
        let path = unique_path("src");
        std::fs::write(&path, content).unwrap();
        let yarn = Yarn::from_file(None, None, None, path.clone()).await.unwrap();
        (Arc::new(yarn), path)
    }

    fn dummy_pex() -> Arc<Mutex<Pex>> {
        let me: SocketAddr = "0.0.0.0:0".parse().unwrap();
        Arc::new(Mutex::new(Pex::new(me, PEX_POOL_SIZE)))
    }

    fn spawn_test_router() -> FabricHandle {
        let (tx, mut rx) = mpsc::channel::<FabricEvent>(64);
        tokio::spawn(async move {
            while let Some(_event) = rx.recv().await {
            }
        });
        FabricHandle { tx }
    }

    struct PeerTestEnv {
        leecher_choke: Arc<Mutex<ChokeManager>>,
        leecher_piece: Arc<Mutex<PieceManager>>,
        seeder_choke: Arc<Mutex<ChokeManager>>,
        seeder_stop: CancellationToken,
        leecher_stop: CancellationToken,
        listen_addr: SocketAddr,
        seeder_handle: Option<tokio::task::JoinHandle<Result<()>>>,
        leecher_handle: Option<tokio::task::JoinHandle<Result<()>>>,
        seeder_path: String,
        leecher_path: String,
    }

    impl PeerTestEnv {
        async fn wait_for_handshake(&self) -> bool {
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if self
                    .leecher_choke
                    .lock()
                    .unwrap()
                    .can_request(self.listen_addr)
                    .is_ok()
                {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            false
        }

        async fn teardown(mut self) {
            self.seeder_stop.cancel();
            self.leecher_stop.cancel();

            if let Some(h) = self.seeder_handle.take() {
                let _ = tokio::time::timeout(Duration::from_secs(2), h).await;
            }
            if let Some(h) = self.leecher_handle.take() {
                let _ = tokio::time::timeout(Duration::from_secs(2), h).await;
            }

            let _ = std::fs::remove_file(&self.seeder_path);
            let _ = std::fs::remove_file(&self.leecher_path);
        }
    }

    async fn setup_pair() -> PeerTestEnv {
        let content = test_content();
        let (yarn, seeder_path) = build_yarn(&content).await;
        let info_hash = yarn.info_hash().unwrap();

        let mut bf = Bitfield::new(yarn.info.pieces.len());
        bf.full();
        let seeder_piece = Arc::new(Mutex::new(PieceManager::resume(&yarn, bf).unwrap()));
        let seeder_choke = Arc::new(Mutex::new(ChokeManager::new()));
        let seeder_spool = Spool::new(
            &seeder_path,
            yarn.info.piece_length as u64,
            yarn.info.total_length,
        )
        .unwrap();
        let seeder_pex = dummy_pex();
        let (seeder_have_tx, _) = broadcast::channel(64);
        let seeder_cancel = CancellationToken::new();

        let leecher_path = unique_path("out");
        std::fs::write(&leecher_path, vec![0u8; yarn.info.total_length as usize]).unwrap();
        let leecher_piece = Arc::new(Mutex::new(PieceManager::new(&yarn).unwrap()));
        let leecher_choke = Arc::new(Mutex::new(ChokeManager::new()));
        let leecher_spool = Spool::new(
            &leecher_path,
            yarn.info.piece_length as u64,
            yarn.info.total_length,
        )
        .unwrap();
        let leecher_pex = dummy_pex();
        let (leecher_have_tx, _) = broadcast::channel(64);
        let leecher_cancel = CancellationToken::new();

        let leecher_choke_t = leecher_choke.clone();
        let leecher_piece_t = leecher_piece.clone();
        let seeder_choke_t = seeder_choke.clone();
        let seeder_stop_t = seeder_cancel.clone();
        let leecher_stop_t = leecher_cancel.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let listen_port = listen_addr.port();

        let seeder_id = NodeID([0xAA; 32]);
        let leecher_id = NodeID([0xBB; 32]);

        let seeder_router = spawn_test_router();
        let leecher_router = spawn_test_router();

        let seeder_cancel_cl = seeder_cancel.clone();
        let seeder_handle = tokio::spawn({
            let yarn = yarn.clone();
            async move {
                let (mut stream, peer_addr) = listener.accept().await.unwrap();

                let mut header = [0u8; 4];
                AsyncReadExt::read_exact(&mut stream, &mut header).await.unwrap();
                let len = needle::parse_length(header).unwrap();

                let mut payload = vec![0u8; len as usize];
                AsyncReadExt::read_exact(&mut stream, &mut payload).await.unwrap();

                let value = needle::decode_message(&payload).unwrap();
                let handshake = Message::from_bencode(&value).unwrap();

                Connection::accept(
                    stream,
                    peer_addr,
                    seeder_id,
                    listen_port,
                    info_hash,
                    handshake,
                    yarn,
                    seeder_piece,
                    seeder_choke,
                    seeder_spool,
                    seeder_pex,
                    seeder_have_tx,
                    seeder_router,
                    seeder_cancel_cl,
                )
                .await
            }
        });

        let leecher_cancel_cl = leecher_cancel.clone();
        let leecher_handle = tokio::spawn({
            let yarn = yarn.clone();
            async move {
                Connection::connect(
                    listen_addr,
                    leecher_id,
                    0,
                    info_hash,
                    yarn,
                    leecher_piece,
                    leecher_choke,
                    leecher_spool,
                    leecher_pex,
                    leecher_have_tx,
                    leecher_router,
                    leecher_cancel_cl,
                )
                .await
            }
        });

        PeerTestEnv {
            leecher_choke: leecher_choke_t,
            leecher_piece: leecher_piece_t,
            seeder_choke: seeder_choke_t,
            seeder_stop: seeder_stop_t,
            leecher_stop: leecher_stop_t,
            listen_addr,
            seeder_handle: Some(seeder_handle),
            leecher_handle: Some(leecher_handle),
            seeder_path,
            leecher_path,
        }
    }

    #[tokio::test]
    async fn handshake_completes_between_two_peers() {
        let env = setup_pair().await;

        assert!(
            env.wait_for_handshake().await,
            "handshake did not complete within 3s"
        );

        env.teardown().await;
    }

    #[tokio::test]
    async fn handshake_rejects_wrong_info_hash() {
        let content_a = test_content();
        let content_b: Vec<u8> = (0..TEST_FILE_SIZE)
            .map(|i| ((i * 7 + 13) % 251) as u8)
            .collect();

        let (yarn_a, path_a) = build_yarn(&content_a).await;
        let (yarn_b, path_b) = build_yarn(&content_b).await;
        let info_hash_a = yarn_a.info_hash().unwrap();
        let info_hash_b = yarn_b.info_hash().unwrap();
        assert_ne!(info_hash_a, info_hash_b, "test setup: hashes must differ");

        let mut bf = Bitfield::new(yarn_a.info.pieces.len());
        bf.full();
        let seeder_piece = Arc::new(Mutex::new(PieceManager::resume(&yarn_a, bf).unwrap()));
        let seeder_choke = Arc::new(Mutex::new(ChokeManager::new()));
        let seeder_spool = Spool::new(
            &path_a,
            yarn_a.info.piece_length as u64,
            yarn_a.info.total_length,
        )
        .unwrap();
        let seeder_pex = dummy_pex();
        let (seeder_have_tx, _) = broadcast::channel(16);
        let seeder_cancel = CancellationToken::new();

        let out_b = unique_path("out");
        std::fs::write(&out_b, vec![0u8; yarn_b.info.total_length as usize]).unwrap();
        let leecher_piece = Arc::new(Mutex::new(PieceManager::new(&yarn_b).unwrap()));
        let leecher_choke = Arc::new(Mutex::new(ChokeManager::new()));
        let leecher_spool = Spool::new(
            &out_b,
            yarn_b.info.piece_length as u64,
            yarn_b.info.total_length,
        )
        .unwrap();
        let leecher_pex = dummy_pex();
        let (leecher_have_tx, _) = broadcast::channel(16);
        let leecher_cancel = CancellationToken::new();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let listen_port = listen_addr.port();

        let seeder_router = spawn_test_router();
        let leecher_router = spawn_test_router();

        let seeder_cancel_cl = seeder_cancel.clone();
        let seeder_handle = tokio::spawn(async move {
            let (mut stream, peer_addr) = listener.accept().await.unwrap();

            let mut header = [0u8; 4];
            AsyncReadExt::read_exact(&mut stream, &mut header).await.unwrap();
            let len = needle::parse_length(header).unwrap();

            let mut payload = vec![0u8; len as usize];
            AsyncReadExt::read_exact(&mut stream, &mut payload).await.unwrap();

            let value = needle::decode_message(&payload).unwrap();
            let handshake = Message::from_bencode(&value).unwrap();

            Connection::accept(
                stream,
                peer_addr,
                NodeID([0xAA; 32]),
                listen_port,
                info_hash_a,
                handshake,
                yarn_a,
                seeder_piece,
                seeder_choke,
                seeder_spool,
                seeder_pex,
                seeder_have_tx,
                seeder_router,
                seeder_cancel_cl,
            )
            .await
        });

        let leecher_cancel_cl = leecher_cancel.clone();
        let leecher_handle = tokio::spawn(async move {
            Connection::connect(
                listen_addr,
                NodeID([0xBB; 32]),
                0,
                info_hash_b,
                yarn_b,
                leecher_piece,
                leecher_choke,
                leecher_spool,
                leecher_pex,
                leecher_have_tx,
                leecher_router,
                leecher_cancel_cl,
            )
            .await
        });

        let leecher_result = tokio::time::timeout(Duration::from_secs(3), leecher_handle)
            .await
            .expect("leecher timed out")
            .expect("leecher task panicked");

        assert!(
            leecher_result.is_err(),
            "expected handshake rejection, got {:?}",
            leecher_result
        );

        seeder_cancel.cancel();
        leecher_cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), seeder_handle).await;

        let _ = std::fs::remove_file(&path_a);
        let _ = std::fs::remove_file(&path_b);
        let _ = std::fs::remove_file(&out_b);
    }

    #[tokio::test]
    async fn leecher_downloads_whole_file() {
        let env = setup_pair().await;

        assert!(
            env.wait_for_handshake().await,
            "handshake did not complete"
        );

        let seeder_peers = env.seeder_choke.lock().unwrap().pod_addrs();
        assert!(
            !seeder_peers.is_empty(),
            "seeder did not register the leecher"
        );
        for addr in seeder_peers {
            env.seeder_choke
                .lock()
                .unwrap()
                .set_am_choking(addr, false)
                .unwrap();
        }

        env.leecher_choke
            .lock()
            .unwrap()
            .set_peer_choking(env.listen_addr, false)
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let (have, total) = env.leecher_piece.lock().unwrap().progress();
            if have == total {
                break;
            }
            if Instant::now() > deadline {
                panic!("download timed out at {}/{} pieces", have, total);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let source = std::fs::read(&env.seeder_path).unwrap();
        let output = std::fs::read(&env.leecher_path).unwrap();
        assert_eq!(source.len(), output.len(), "output length differs");
        assert_eq!(source, output, "output bytes differ");

        env.teardown().await;
    }
}