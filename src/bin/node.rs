use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

// Replace `pod_weaver` with your crate name (whatever is in Cargo.toml).
use pod_weaver::dht::DHT;
use pod_weaver::types::NodeID;
use pod_weaver::transport::Transport;
use pod_weaver::storage::disk::set_healthy;
use pod_weaver::transport::contact::Contact;
use pod_weaver::transport::pending_map::PendingMap;
use pod_weaver::storage::peer_cache::PeerCache;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let bootstrap_addr: Option<SocketAddr> = match &args.bootstrap {
        Some(s) => {
            use tokio::net::lookup_host;
            let mut addrs = lookup_host(s).await
                .map_err(|e| format!("cannot resolve {s}: {e}"))?;
            Some(addrs.next().ok_or(format!("no addresses for {s}"))?)
        }
        None => None,
    };

    // --- Set up the node ---
    let contact = Contact::with_node(args.id, &args.bind).await?;
    let local_addr = contact.local_addr()?;

    set_healthy()?;

    let transport = Transport::new(contact, PendingMap::new());
    let dht = DHT::new(*transport.id(), transport.clone())?;

    // --- Channels ---
    let (qtx, qrx) = mpsc::channel(64);
    let (stop_tx, stop_rx) = watch::channel(false);

    // --- Spawn the two run loops ---
    tokio::spawn(transport.run(qtx, stop_rx.clone()));
    tokio::spawn({
        let dht = dht.clone();
        async move {
            if let Err(e) = dht.run(qrx, stop_rx).await {
                eprintln!("dht.run exited with error: {e:?}");
            }
        }
    });

    // --- DHT Bootstrap peer cache ---
    let cache = match PeerCache::extract_peers() {
        Ok(Some(cached)) => cached,
        Ok(None) => Vec::new(),
        Err(e) => {
            eprintln!("Error, Could not read peer cache file {e:?}");
            Vec::new()
        }
    };

    if !cache.is_empty() {
        let online_nodes = dht.bootstrap(cache).await;
        println!("Bootstrap success, found {online_nodes}");
    } else {
        println!("Bootstrap needs central peer server, peer cache is empty");
    }

    println!("node id   : {}", hex(&args.id.0));
    println!("listening : {}", local_addr);

    // --- Ctrl-C handler: flip stop, then exit the process ---
    tokio::spawn({
        let dht_clone = dht.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            println!("\nSaving peer cache...");

            let peers: Vec<SocketAddr> = dht_clone.routing_table.lock().unwrap()
                .buckets.iter()
                .flat_map(|b| b.nodes.iter())
                .map(|n| n.address)
                .collect();
            match PeerCache::save_peers(&peers) {
                Ok(()) => println!("saved {} peers", peers.len()),
                Err(e) => eprintln!("Peer save failed {e:?}")
            };

            let _ = stop_tx.send(true);
            tokio::time::sleep(Duration::from_millis(150)).await;
            std::process::exit(0);
        }
    });

    // --- One-shot bootstrap ---
    if let Some(boot) = bootstrap_addr {
        println!("bootstrap : {}", boot);
        match dht.find_node(args.id, boot).await {
            Ok(nodes) => {
                println!("find_node : ok ({} nodes returned)", nodes.len());
                for (id, addr) in &nodes {
                    println!("   peer {} @ {}", hex(&id.0), addr);
                }
                for (_, addr) in nodes.iter().take(3) {
                    match dht.ping(*addr).await {
                        Ok(()) => println!("ping {}: ok", addr),
                        Err(e) => println!("ping {}: {e:?}", addr),
                    }
                }
            }
            Err(e) => {
                eprintln!("find_node : {e:?}");
                eprintln!("(bootstrap node unreachable or not listening)");
            }
        }
    } else {
        println!("bootstrap : (none — start another node pointed at this one)");
    }

    // --- Periodic liveness loop: ping the bootstrap every 10s ---
    let start = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    tick.tick().await; // consume the immediate first tick

    loop {
        tick.tick().await;
        let secs = start.elapsed().as_secs();

        if secs > 0 && secs % 20 == 0 {
            let nodes = dht.routing_table.lock().unwrap().shake();

            let mut handles = Vec::new();
            for node in nodes.iter().cloned() {
                let dht = dht.clone();
                handles.push(tokio::spawn(async move {
                    dht.ping(node.address).await.is_ok()
                }));
            }

            let mut alive = Vec::new();
            for (handle, node) in handles.into_iter().zip(nodes.iter()) {
                if handle.await.unwrap_or(false) {
                    alive.push(node.address);
                } else {
                    dht.routing_table.lock().unwrap().pluck(&node.id);
                }
            }

            if let Err(e) = PeerCache::save_peers(&alive) {
                eprintln!("Error, Could not save peers in cache: {e:?}");
            } else {
                println!("{} online nodes saved in peer cache", alive.len())
            }

            println!("Table refreshed");
        } else {
            println!("[{secs:>3}s] alive")
        }
    }
}

// -------------------- CLI parsing --------------------

struct Args {
    id: NodeID,
    bind: String,
    bootstrap: Option<String>,
}
fn parse_args() -> Result<Args, String> {
    let mut iter = std::env::args().skip(1);
    let mut id: Option<NodeID> = None;
    let mut bind: Option<String> = None;
    let mut bootstrap: Option<String> = None;

    while let Some(key) = iter.next() {
        match key.as_str() {
            "--id" => {
                let v = iter.next().ok_or("--id needs a value")?;
                id = Some(parse_id(&v)?);
            }
            "--bind" => {
                bind = Some(iter.next().ok_or("--bind needs a value")?);
            }
            "--bootstrap" => {
                let v = iter.next().ok_or("--bootstrap needs a value")?;
                bootstrap = Some(v);
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    Ok(Args {
        id: id.ok_or("--id is required")?,
        bind: bind.ok_or("--bind is required")?,
        bootstrap,
    })
}

/// Accepts "7" -> [7u8; 32] (great for tests) or 64 hex chars -> raw bytes.
fn parse_id(s: &str) -> Result<NodeID, String> {
    if !s.is_empty() && s.len() <= 3 && s.chars().all(|c| c.is_ascii_digit()) {
        let n: u8 = s.parse().map_err(|_| format!("bad id: {s}"))?;
        return Ok(NodeID([n; 32]));
    }
    if s.len() == 64 {
        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| format!("bad hex at byte {i}"))?;
        }
        return Ok(NodeID(out));
    }
    Err(format!("id must be 0-255 or 64 hex chars, got: {s}"))
}

fn print_help() {
    println!(
        "usage: node --id <n|hex> --bind <addr> [--bootstrap <addr>]\n\
         \n\
         examples:\n\
           node --id 1 --bind 127.0.0.1:8001\n\
           node --id 2 --bind 127.0.0.1:8002 --bootstrap 127.0.0.1:8001"
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}