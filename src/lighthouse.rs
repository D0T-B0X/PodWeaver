use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashSet;

use tokio::sync::Mutex;
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncBufReadExt;

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
    async fn insert(&self, addr: SocketAddr) -> bool {
        let mut n = self.nodes.lock().await;
        if n.contains(&addr) {
            return true;
        }
        if n.len() >= MAX_NODES {
            return false;
        }
        n.insert(addr);
        true
    }

    async fn snapshot(&self) -> Vec<SocketAddr> {
        self.nodes.lock().await.iter().copied().collect()
    }

    async fn remove(&self, addr: SocketAddr) {
        self.nodes.lock().await.remove(&addr);
    }

    async fn len(&self) -> usize {
        self.nodes.lock().await.len()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:9000".to_string());

    let reg = Registry::new();

    // Active health check: TCP-connect to every node every PING_INTERVAL.
    // Anything that doesn't accept the connection within PING_TIMEOUT is
    // evicted, freeing its slot for the next PUT.
    {
        let reg = reg.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PING_INTERVAL);
            tick.tick().await; // consume immediate first tick
            loop {
                tick.tick().await;

                let peers = reg.snapshot().await;
                let mut dead = Vec::new();

                for addr in peers {
                    let ok = tokio::time::timeout(
                        PING_TIMEOUT,
                        TcpStream::connect(addr),
                    )
                    .await
                    .map(|r| r.is_ok())
                    .unwrap_or(false);

                    if !ok {
                        dead.push(addr);
                    }
                }

                for addr in &dead {
                    reg.remove(*addr).await;
                }

                if !dead.is_empty() {
                    eprintln!("ping sweep: evicted {}, {} online",
                        dead.len(), reg.len().await);
                } else {
                    eprintln!("ping sweep: {} online", reg.len().await);
                }
            }
        });
    }

    let listener = TcpListener::bind(&bind).await?;
    eprintln!("tracker listening on {bind} (cap {MAX_NODES})");

    loop {
        let (stream, _) = listener.accept().await?;
        let reg = reg.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, reg).await {
                eprintln!("conn error: {e:?}");
            }
        });
    }
}

async fn handle(stream: TcpStream, reg: Registry) -> std::io::Result<()> {
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let line = line.trim_end();

    if let Some(addr_str) = line.strip_prefix("PUT ") {
        match addr_str.parse::<SocketAddr>() {
            Ok(addr) => {
                if reg.insert(addr).await {
                    w.write_all(b"OK\n").await?;
                } else {
                    w.write_all(b"FULL\n").await?;
                }
            }
            Err(_) => w.write_all(b"ERR bad addr\n").await?,
        }
    } else if line == "GET" {
        for a in reg.snapshot().await {
            w.write_all(format!("{a}\n").as_bytes()).await?;
        }
        w.write_all(b".\n").await?;
    } else {
        w.write_all(b"ERR\n").await?;
    }

    w.flush().await?;
    Ok(())
}