use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pod_weaver::peer::YarnFabric;
use pod_weaver::types::{InfoHash, NodeID};
use pod_weaver::yarn::Yarn;
use pod_weaver::Engine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage();
        std::process::exit(1);
    }
    match args[1].as_str() {
        "seed" => run_seed(&args).await?,
        "download" => run_download(&args).await?,
        "-h" | "--help" => usage(),
        other => {
            eprintln!("unknown subcommand: {other}");
            usage();
            std::process::exit(1);
        }
    }
    Ok(())
}

fn usage() {
    eprintln!(
        "\
podweaver — usage:

  seed <file> [--tcp ADDR] [--udp ADDR] [--bootstrap ADDR]... [--id HEX]

  download <yarn-file> <output-file>
           (--peer ADDR | --bootstrap ADDR...)
           [--tcp ADDR] [--udp ADDR] [--id HEX]

  --tcp ADDR        TCP bind address for the peer mesh (default 127.0.0.1:0)
  --udp ADDR        UDP bind address for DHT (omit for mesh-only)
  --peer ADDR       Skip discovery, dial this peer directly
  --bootstrap ADDR  DHT bootstrap node (repeatable)
  --id HEX          64 hex chars; overrides the persisted node ID
"
    );
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn flags(args: &[String], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == name {
            if let Some(v) = args.get(i + 1) {
                out.push(v.clone());
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    out
}

fn parse_id(s: &str) -> Result<NodeID, Box<dyn std::error::Error>> {
    let s = s.trim_start_matches("0x");
    if s.len() != 64 {
        return Err(format!("--id must be 64 hex chars, got {}", s.len()).into());
    }
    let mut b = [0u8; 32];
    for i in 0..32 {
        b[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?;
    }
    Ok(NodeID(b))
}

fn tcp_bind(args: &[String]) -> String {
    flag(args, "--tcp").unwrap_or_else(|| "127.0.0.1:0".to_string())
}

async fn resolve_addrs(v: &[String]) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for s in v {
        let mut addrs = tokio::net::lookup_host(s).await?;
        match addrs.next() {
            Some(a) => out.push(a),
            None => return Err(format!("no addresses for {s}").into()),
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------

async fn run_seed(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let Some(file) = args.get(2) else {
        return Err("seed: missing <file>".into());
    };
    let source = PathBuf::from(file);
    if !source.exists() {
        return Err(format!("seed: {file} does not exist").into());
    }

    let id = match flag(args, "--id") {
        Some(s) => Some(parse_id(&s)?),
        None => None,
    };
    let bootstrap = resolve_addrs(&flags(args, "--bootstrap")).await?;
    let udp = flag(args, "--udp");
    let tcp = tcp_bind(args);

    let engine = Engine::new(udp.as_deref(), &tcp, id, bootstrap).await?;

    // Build the yarn and persist it so the leecher can copy it over.
    let yarn = Arc::new(Yarn::from_file(None, None, None, file.clone()).await?);
    let info_hash = yarn.info_hash()?;

    let yarn_path = PathBuf::from(format!("{file}.yarn"));
    std::fs::write(&yarn_path, yarn.encode()?)?;

    let my_tcp: SocketAddr = format!("127.0.0.1:{}", engine.listen_port())
        .parse()
        .unwrap();
    let fabric = YarnFabric::from_source_file(
        None,
        None,
        None,
        source.clone(),
        engine.spawner(),
        my_tcp,
        engine.cancel_token(),
    )
    .await?;

    engine.add_yarn(fabric).await?;

    println!("node-id     : {}", hex(&engine.node_id().0));
    println!("tcp listen  : {my_tcp}");
    if let Some(d) = engine.dht_addr() {
        println!("dht listen  : {d}");
    }
    println!("info-hash   : {}", hex(&info_hash.0));
    println!("yarn written: {}", yarn_path.display());
    println!("seeding {file} — Ctrl-C to stop");

    tokio::signal::ctrl_c().await?;
    engine.shutdown().await;
    Ok(())
}

async fn run_download(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() < 4 {
        return Err("download: usage: download <yarn-file> <output-file> --peer ADDR".into());
    }
    let yarn_path = PathBuf::from(&args[2]);
    let out_path = PathBuf::from(&args[3]);

    let id = match flag(args, "--id") {
        Some(s) => Some(parse_id(&s)?),
        None => None,
    };
    let bootstrap = resolve_addrs(&flags(args, "--bootstrap")).await?;
    let peer = match flag(args, "--peer") {
        Some(s) => Some(resolve_addrs(&[s]).await?.into_iter().next().unwrap()),
        None => None,
    };
    if peer.is_none() && bootstrap.is_empty() {
        return Err("download: need --peer or --bootstrap".into());
    }
    let udp = flag(args, "--udp");
    let tcp = tcp_bind(args);

    // Load the yarn.
    let bytes = std::fs::read(&yarn_path)?;
    let yarn = Arc::new(Yarn::decode(&bytes)?);
    let info_hash: InfoHash = yarn.info_hash()?;

    // Pre-allocate the output file — Spool::write_block does not create it.
    let total = yarn.info.total_length;
    {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&out_path)?;
        f.set_len(total)?;
    }

    let engine = Engine::new(udp.as_deref(), &tcp, id, bootstrap).await?;
    let my_tcp: SocketAddr = format!("127.0.0.1:{}", engine.listen_port())
        .parse()
        .unwrap();

    let fabric = YarnFabric::new(
        yarn.clone(),
        out_path.clone(),
        engine.spawner(),
        my_tcp,
        false,
        engine.cancel_token(),
    )?;
    let piece_manager = fabric.piece_manager.clone();
    let total_pieces = fabric.yarn.info.pieces.len() as u32;

    engine.add_yarn(fabric).await?;

    println!("node-id     : {}", hex(&engine.node_id().0));
    println!("tcp listen  : {my_tcp}");
    println!("info-hash   : {}", hex(&info_hash.0));
    println!("pieces      : {total_pieces}");
    println!("output      : {}", out_path.display());

    if let Some(p) = peer {
        println!("dialing     : {p}");
        engine.dial(p, info_hash).await?;
    } else {
        let found = engine.find_and_dial(info_hash).await?;
        println!("dht found   : {} peer(s)", found.len());
        for p in found {
            println!("  -> {p}");
        }
    }

    // Poll for completion.
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let (have, total) = {
            let pm = piece_manager.lock().unwrap();
            pm.progress()
        };
        if have == total {
            println!("done: {have}/{total} pieces");
            break;
        }
        if Instant::now() > deadline {
            eprintln!("timed out at {have}/{total} pieces");
            engine.shutdown().await;
            std::process::exit(2);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Byte-for-byte check against the seeder's file (if it's on this host).
    let src = yarn_path
        .parent()
        .map(|p| p.join(&yarn.info.filename))
        .unwrap_or_else(|| PathBuf::from(&yarn.info.filename));
    if src.exists() {
        let a = std::fs::read(&src)?;
        let b = std::fs::read(&out_path)?;
        if a == b {
            println!("verify      : OK ({} bytes match)", a.len());
        } else {
            eprintln!("verify      : MISMATCH ({} vs {} bytes)", a.len(), b.len());
        }
    } else {
        println!("verify      : skipped (source {} not present)", src.display());
    }

    engine.shutdown().await;
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}