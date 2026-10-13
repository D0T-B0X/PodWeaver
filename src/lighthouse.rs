use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::collections::HashSet;

use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

const MAX_NODES: usize = 20;
const PING_INTERVAL: Duration = Duration::from_secs(120);
const PING_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct Registry {
    nodes: Arc<Mutex<HashSet<SocketAddr>>>,
}

impl Registry {
    fn new() -> Self {
        Self { nodes: Arc::new(Mutex::new(HashSet::new())) }
    }

    /// add a new pod to the registry.
    fn insert(&self, addr: SocketAddr) -> bool {
        let mut n = self.nodes.lock().unwrap();
        if n.contains(&addr) {
            return true;
        }
        if n.len() >= MAX_NODES {
            return false;
        }
        n.insert(addr);
        true
    }

    fn snapshot(&self) -> Vec<SocketAddr> {
        self.nodes.lock().unwrap().iter().copied().collect()
    }

    fn remove(&self, addr: SocketAddr) {
        self.nodes.lock().unwrap().remove(&addr);
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


    // Every PIN_INTERVAL, dead nodes are kicked from the registry to 
    // make space for new insertions which is possible only through
    // pings from any online node
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
        return Ok(())
    }

    match opcode[0] {
        // get request
        0xAA => {
            let mut payload = [0u8; 2];
            if r.read_exact(&mut payload).await.is_err() {
                return Ok(());
            }

            let port = u16::from_be_bytes(payload);
            let requester_addr = SocketAddr::V4(SocketAddrV4::new(sender_ip, port));

            let mut peers = reg.snapshot();
            peers.retain(|a| *a != requester_addr);

            let count = peers.len().min(255) as u8;
            let mut buf = Vec::with_capacity(1 + (count as usize * 6));
            buf.push(count);

            for addr in peers {
                if let SocketAddr::V4(v4) = addr {
                    buf.extend_from_slice(&v4.ip().octets());
                    buf.extend_from_slice(&v4.port().to_be_bytes());
                }
            }
            w.write_all(&buf).await?;
        },

        // put request
        0xFF => {
            let mut payload = [0u8; 2];
            if r.read_exact(&mut payload).await.is_err() {
                // silent drop, cant be helped
                return Ok(())
            }

            let port = u16::from_be_bytes([payload[0], payload[1]]);
            let addr = SocketAddr::V4(SocketAddrV4::new(sender_ip, port));

            let status = if reg.insert(addr) { 0x01 } else { 0x00 };
            w.write_all(&[status]).await?;
        },

        _ => w.write_all(&[0x02]).await?,
    }

    w.flush().await?;
    Ok(())
}

async fn registry_refresh_loop(registry: Registry) {
    let mut tick = tokio::time::interval(PING_INTERVAL);
    // consume first useless tick
    tick.tick().await;

    loop {
        tick.tick().await;

        let active_nodes = registry.snapshot();

        let mut tasks = Vec::with_capacity(active_nodes.len());
        for addr in active_nodes {
            // 3 second timeout for ping return
            tasks.push(tokio::spawn(async move {
                let is_alive = tokio::time::timeout(PING_TIMEOUT, TcpStream::connect(addr))
                    .await
                    .map(|r| r.is_ok())
                    .unwrap_or(false);

                (addr, is_alive)
            }));
        }        

        let mut evicted = 0;
        for task in tasks {
            if let Ok((addr, is_alive)) = task.await {
                if !is_alive {
                    registry.remove(addr);
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