use std::path::PathBuf;
use std::time::{Duration, Instant};

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
        "idle" => run_idle().await?,
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
podweaver — zero-config peer engine

usage:
  podweaver seed <file>
  podweaver download <yarn-file> [output-dir]
  podweaver idle
"
    );
}

async fn run_seed(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let Some(file_str) = args.get(2) else {
        return Err("seed: missing <file>".into());
    };

    let source = PathBuf::from(file_str);
    if !source.exists() {
        return Err(format!("seed: {file_str} does not exist").into());
    }

    let engine = Engine::new().await?;

    let info_hash = engine
        .add_yarn(None, None, None, file_str.clone())
        .await?;

    let yarn = Yarn::from_file(None, None, None, file_str.clone()).await?;
    let yarn_path = PathBuf::from(format!("{file_str}.yarn"));
    std::fs::write(&yarn_path, yarn.encode()?)?;

    println!("node-id     : {}", hex(&engine.node_id().0));
    println!("tcp listen  : {}", engine.listen_port());
    println!("dht listen  : {}", engine.dht_addr());
    println!("info-hash   : {}", hex(&info_hash.0));
    println!("yarn written: {}", yarn_path.display());
    println!("seeding {file_str} — press Ctrl-C to stop");

    tokio::signal::ctrl_c().await?;
    engine.shutdown().await;
    Ok(())
}

async fn run_download(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let Some(yarn_file) = args.get(2) else {
        return Err("download: usage: download <yarn-file> [output-dir]".into());
    };

    let yarn_path = PathBuf::from(yarn_file);
    let yarn_bytes = std::fs::read(&yarn_path)?;
    let decoded_yarn = Yarn::decode(&yarn_bytes)?;
    let filename = decoded_yarn.info.filename.clone();

    let output_dir = args.get(3).map(PathBuf::from);

    let engine = Engine::new().await?;

    let fabric = engine
        .download_yarn(yarn_bytes, output_dir, &filename)
        .await?;
    let piece_manager = fabric.piece_manager.clone();
    let total_pieces = fabric.yarn.info.pieces.len() as u32;

    println!("node-id     : {}", hex(&engine.node_id().0));
    println!("tcp listen  : {}", engine.listen_port());
    println!("info-hash   : {}", hex(&fabric.info_hash.0));
    println!("pieces      : {total_pieces}");

    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let (have, total) = {
            let pm = piece_manager.lock().unwrap();
            pm.progress()
        };

        if have == total {
            println!("done: {have}/{total} pieces downloaded");
            break;
        }

        if Instant::now() > deadline {
            eprintln!("timed out at {have}/{total} pieces");
            engine.shutdown().await;
            std::process::exit(2);
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    engine.shutdown().await;
    Ok(())
}

async fn run_idle() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::new().await?;
    std::fs::write("/tmp/healthy", b"ok")?;

    println!("node-id     : {}", hex(&engine.node_id().0));
    println!("tcp listen  : {}", engine.listen_port());
    println!("dht listen  : {}", engine.dht_addr());
    println!("idle node running — press Ctrl-C to stop");

    tokio::signal::ctrl_c().await?;
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