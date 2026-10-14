use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::collections::HashMap;

use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

const MAX_NODES: usize = 20;
const PING_INTERVAL: Duration = Duration::from_secs(120);
const PING_TIMEOUT: Duration = Duration::from_secs(3);

// Opcodes / status bytes shared with the client in `Engine::lighthouse_request`.
const OP_GET: u8 = 0xAA;
const OP_PING: u8 = 0xFF;
const STATUS_OK: u8 = 0x01;
const STATUS_FULL: u8 = 0x00;
const STATUS_BAD_OPCODE: u8 = 0x02;

#[derive(Clone, Copy)]
struct Entry {
    udp: u16,
    tcp: u16,
}

#[derive(Clone)]
struct Registry {
    nodes: Arc<Mutex<HashMap<Ipv4Addr, Entry>>>,
}

impl Registry {
    fn new() -> Self {
        Self { nodes: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Insert or refresh a node. Returns `false` only when the registry is
    /// full and this is a new IP.
    fn insert(&self, ip: Ipv4Addr, entry: Entry) -> bool {
        let mut n = self.nodes.lock().unwrap();
        if n.contains_key(&ip) {
            n.insert(ip, entry);
            return true;
        }
        if n.len() >= MAX_NODES {
            return false;
        }
        n.insert(ip, entry);
        true
    }

    fn snapshot(&self) -> Vec<(Ipv4Addr, Entry)> {
        self.nodes
            .lock()
            .unwrap()
            .iter()
            .map(|(ip, e)| (*ip, *e))
            .collect()
    }

    fn remove(&self, ip: Ipv4Addr) {
        self.nodes.lock().unwrap().remove(&ip);
    }

    fn len(&self) -> usize {
        self.nodes.lock().unwrap().len()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:9001".to_string());

    let reg = Registry::new();

    // Every PING_INTERVAL, dead nodes are evicted so their slot in the
    // MAX_NODES cap becomes available to fresh pings.
    {
        let reg = reg.clone();
        tokio::spawn(async move {
            registry_refresh_loop(reg).await;
        });
    }

    let listener = TcpListener::bind(&bind).await?;
    eprintln!("tracker listening on {bind} (cap {MAX_NODES})");

    loop {
        let (stream, _) = listener.accept().await?;
        let reg = reg.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, reg).await {
                eprintln!("connection error: {e:?}");
            }
        });
    }
}

async fn handle(stream: TcpStream, reg: Registry) -> std::io::Result<()> {
    let (mut r, mut w) = stream.into_split();

    let sender_ip = match r.peer_addr()? {
        SocketAddr::V4(v4) => *v4.ip(),
        SocketAddr::V6(_) => return Ok(()),
    };

    let mut opcode = [0u8; 1];
    if r.read_exact(&mut opcode).await.is_err() {
        return Ok(());
    }

    match opcode[0] {
        OP_GET => {
            let mut payload = [0u8; 2];
            if r.read_exact(&mut payload).await.is_err() {
                return Ok(());
            }

            // The requester's own DHT port. We don't store it (the source IP is
            // the key), but parse for symmetry and future use.
            let _requester_udp = u16::from_be_bytes(payload);

            // Exclude the requester by IP so it never gets itself back.
            let peers: Vec<(Ipv4Addr, Entry)> = reg
                .snapshot()
                .into_iter()
                .filter(|(ip, _)| *ip != sender_ip)
                .collect();

            let count = peers.len().min(255) as u8;
            let mut buf = Vec::with_capacity(1 + (count as usize * 6));
            buf.push(count);

            for (ip, entry) in peers.into_iter().take(count as usize) {
                buf.extend_from_slice(&ip.octets());
                buf.extend_from_slice(&entry.udp.to_be_bytes());
            }

            w.write_all(&buf).await?;
        }

        // PUT / PING: [0xFF, udp_hi, udp_lo, tcp_hi, tcp_lo]
        //    -> [status]
        OP_PING => {
            let mut payload = [0u8; 4];
            if r.read_exact(&mut payload).await.is_err() {
                // Silent drop — nothing we can do about a half-written request.
                return Ok(());
            }

            let udp = u16::from_be_bytes([payload[0], payload[1]]);
            let tcp = u16::from_be_bytes([payload[2], payload[3]]);

            // Reject obviously-broken registrations. Port 0 is never valid.
            if udp == 0 || tcp == 0 {
                w.write_all(&[STATUS_FULL]).await?;
                w.flush().await?;
                return Ok(());
            }

            let status = if reg.insert(sender_ip, Entry { udp, tcp }) {
                STATUS_OK
            } else {
                STATUS_FULL
            };
            let ip = sender_ip.octets();
            w.write_all(&[status, ip[0], ip[1], ip[2], ip[3]]).await?;        
        }

        _ => w.write_all(&[STATUS_BAD_OPCODE]).await?,
    }

    w.flush().await?;
    Ok(())
}

async fn registry_refresh_loop(registry: Registry) {
    let mut tick = tokio::time::interval(PING_INTERVAL);
    // Consume the immediate first tick so we don't sweep at t=0.
    tick.tick().await;

    loop {
        tick.tick().await;

        let active = registry.snapshot();

        let mut tasks = Vec::with_capacity(active.len());
        for (ip, entry) in active {
            tasks.push(tokio::spawn(async move {
                // Probe the *TCP* listener — that's the socket a live pod
                // actually accepts connections on.
                let addr = SocketAddr::V4(SocketAddrV4::new(ip, entry.tcp));
                let is_alive = tokio::time::timeout(PING_TIMEOUT, TcpStream::connect(addr))
                    .await
                    .map(|r| r.is_ok())
                    .unwrap_or(false);

                (ip, is_alive)
            }));
        }

        let mut evicted = 0;
        for task in tasks {
            if let Ok((ip, is_alive)) = task.await {
                if !is_alive {
                    registry.remove(ip);
                    evicted += 1;
                }
            }
        }

        if evicted > 0 {
            eprintln!(
                "ping sweep: evicted {evicted}, {} online",
                registry.len()
            );
        } else {
            eprintln!("ping sweep: {} online", registry.len());
        }
    }
}