use std::path::PathBuf;
use std::sync::Arc;
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
  podweaver seed <file> [<file> ...]
  podweaver download [--out <dir>] <yarn-file> [<yarn-file> ...]
  podweaver idle
"
    );
}

async fn run_seed(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let files: Vec<String> = args.iter().skip(2).cloned().collect();
    if files.is_empty() {
        return Err("seed: missing <file>...".into());
    }

    // Validate everything up front so we don't half-seed.
    for f in &files {
        if !PathBuf::from(f).exists() {
            return Err(format!("seed: {f} does not exist").into());
        }
    }

    let engine = Engine::new().await?;

    for file_str in &files {
        let info_hash = engine
            .add_yarn(None, None, None, file_str.clone())
            .await?;

        // Write the .yarn metadata next to the source file.
        let yarn = Yarn::from_file(None, None, None, file_str.clone()).await?;
        let yarn_path = PathBuf::from(format!("{file_str}.yarn"));
        std::fs::write(&yarn_path, yarn.encode()?)?;

        println!(
            "seeding {:<24} info-hash {}",
            file_str,
            hex(&info_hash.0)
        );
        println!("yarn written: {}", yarn_path.display());
    }

    println!("node-id     : {}", hex(&engine.node_id().0));
    println!("tcp listen  : {}", engine.listen_port());
    println!("dht listen  : {}", engine.dht_addr());
    println!("seeding {} file(s) — press Ctrl-C to stop", files.len());

    tokio::signal::ctrl_c().await?;
    engine.shutdown().await;
    Ok(())
}

async fn run_download(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    // parse: download [--out <dir>] <yarn>...
    let mut yarn_files: Vec<PathBuf> = Vec::new();
    let mut output_dir: Option<PathBuf> = None;

    let mut iter = args.iter().skip(2);
    while let Some(a) = iter.next() {
        if a == "--out" {
            let Some(d) = iter.next() else {
                return Err("download: --out requires a directory".into());
            };
            output_dir = Some(PathBuf::from(d));
        } else {
            yarn_files.push(PathBuf::from(a));
        }
    }

    if yarn_files.is_empty() {
        return Err("download: missing <yarn-file>...".into());
    }

    let resolve_output = |filename: &str| -> PathBuf {
        match &output_dir {
            Some(dir) => dir.join(filename),
            None => PathBuf::from(filename),
        }
    };

    let engine = Engine::new().await?;
    let cancel = engine.cancel_token();
    
    let mut fabrics = Vec::new();
    for path in &yarn_files {
        let bytes = std::fs::read(path)?;
        let yarn = Arc::new(Yarn::decode(&bytes)?);
        let out_path = resolve_output(&yarn.info.filename);

        let fabric = engine.download_yarn(yarn, out_path).await?;

        println!(
            "downloading {:<24} info-hash {}",
            path.display(),
            hex(&fabric.info_hash.0)
        );
        fabrics.push(fabric);
    }

    println!("node-id     : {}", hex(&engine.node_id().0));
    println!("tcp listen  : {}", engine.listen_port());
    println!("yarns       : {}", fabrics.len());

    let deadline = Instant::now() + Duration::from_secs(600);
    let mut interrupted = false;

    loop {
        let mut all_done = true;
        for f in &fabrics {
            let (have, total) = f.piece_manager.lock().unwrap().progress();
            if have < total {
                all_done = false;
                break;
            }
        }
        if all_done {
            for f in &fabrics {
                let (have, total) = f.piece_manager.lock().unwrap().progress();
                println!("done: {} {have}/{total}", hex(&f.info_hash.0));
            }
            break;
        }

        if cancel.is_cancelled() {
            for f in &fabrics {
                let (have, total) = f.piece_manager.lock().unwrap().progress();
                eprintln!("interrupted: {} {have}/{total}", hex(&f.info_hash.0));
            }
            interrupted = true;
            break;
        }

        if Instant::now() > deadline {
            for f in &fabrics {
                let (have, total) = f.piece_manager.lock().unwrap().progress();
                eprintln!("timed out:   {} {have}/{total}", hex(&f.info_hash.0));
            }
            engine.shutdown().await;
            std::process::exit(2);
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    engine.shutdown().await;

    if interrupted {
        std::process::exit(130);
    }
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