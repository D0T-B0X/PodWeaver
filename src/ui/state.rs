use std::sync::Arc;
use std::sync::Mutex;
use std::path::PathBuf;
use std::time::SystemTime;
use std::collections::HashMap;

use crate::Engine;
use crate::yarn::Yarn;
use crate::types::InfoHash;
use crate::peer::YarnFabric;
use crate::peer::piece::{Bitfield, PieceManager, PieceStatus};
use crate::scan_existing_pieces;

const PIECE_MAP_BUCKETS: usize = 512;

#[derive(Clone, Debug, serde::Serialize)]
pub struct TorrentSnapshot {
    pub info_hash: String,
    pub filename: String,
    pub total_length: u64,
    pub piece_length: u32,
    pub piece_count: u32,
    pub pieces_have: u32,
    pub is_seeding: bool,
    pub is_paused: bool,
    pub peers: usize,
    pub piece_map: Vec<u8>,
}

#[derive(Clone)]
pub struct NodeInfo {
    pub node_id: String,
    pub listen_port: u16,
    pub peer_addr: String,
    pub dht_addr: String,
}

pub struct TorrentView {
    pub info_hash: InfoHash,
    pub fabric: Arc<YarnFabric>,
    pub is_seeding: bool,
    pub output_path: PathBuf,
    pub added_at: SystemTime,
}

pub struct PausedEntry {
    pub yarn: Arc<Yarn>,
    pub output_path: PathBuf,
    pub is_seeding: bool,
    pub cached_bitfield: Option<Bitfield>,
}

pub struct AppState {
    pub engine: Arc<Engine>,
    pub active: Mutex<HashMap<InfoHash, TorrentView>>,
    pub paused: Mutex<HashMap<InfoHash, PausedEntry>>,
}

impl AppState {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            active: Mutex::new(HashMap::new()),
            paused: Mutex::new(HashMap::new()),
        }
    }

    pub fn snapshot(&self) -> Vec<TorrentSnapshot> {
        let mut out = Vec::new();

        for v in self.active.lock().unwrap().values() {
            let (have, total, piece_map) = {
                let pm = v.fabric.piece_manager.lock().unwrap();
                let (h, t) = pm.progress();
                (h, t, build_piece_map(&pm))
            };
            out.push(TorrentSnapshot {
                info_hash: hex(&v.info_hash.0),
                filename: v.fabric.yarn.info.filename.clone(),
                total_length: v.fabric.yarn.info.total_length,
                piece_length: v.fabric.yarn.info.piece_length,
                piece_count: total,
                pieces_have: have,
                is_seeding: v.is_seeding,
                is_paused: false,
                peers: v.fabric.peer_count(),
                piece_map,
            });
        }

        for (hash, p) in self.paused.lock().unwrap().iter() {
            let total = p.yarn.info.pieces.len() as u32;
            let (have, piece_map) = match &p.cached_bitfield {
                Some(bf) => (bf.count_ones(), bitfield_to_map(bf)),
                None => (0, vec![0u8; total.min(PIECE_MAP_BUCKETS as u32) as usize]),
            };
            out.push(TorrentSnapshot {
                info_hash: hex(&hash.0),
                filename: p.yarn.info.filename.clone(),
                total_length: p.yarn.info.total_length,
                piece_length: p.yarn.info.piece_length,
                piece_count: total,
                pieces_have: have,
                is_seeding: p.is_seeding,
                is_paused: true,
                peers: 0,
                piece_map,
            });
        }

        out.sort_by(|a, b| b.info_hash.cmp(&a.info_hash));
        out
    }

    pub fn node_info(&self) -> NodeInfo {
        NodeInfo {
            node_id: hex(&self.engine.node_id().0),
            listen_port: self.engine.listen_port(),
            peer_addr: self.engine.peer_addr().to_string(),
            dht_addr: self.engine.dht_addr().to_string(),
        }
    }

    pub async fn add_seed(&self, path: String) -> Result<InfoHash, String> {
        let info_hash = self
            .engine
            .add_yarn(None, None, None, path.clone())
            .await
            .map_err(|e| format!("{e:?}"))?;

        let fabric = self
            .engine
            .pod_manager
            .get_yarn(&info_hash)
            .ok_or_else(|| "fabric missing after add".to_string())?;

        // Write the .yarn metadata next to the source file so other pods can
        // discover it via the shared volume.
        let yarn_path = format!("{path}.yarn");
        let encoded = fabric
            .yarn
            .encode()
            .map_err(|e| format!("encode yarn: {e:?}"))?;
        std::fs::write(&yarn_path, encoded).map_err(|e| format!("write yarn: {e}"))?;

        self.active.lock().unwrap().insert(
            info_hash,
            TorrentView {
                info_hash,
                fabric,
                is_seeding: true,
                output_path: PathBuf::from(path),
                added_at: SystemTime::now(),
            },
        );

        Ok(info_hash)
    }

    pub async fn add_download(
        &self,
        yarn_path: String,
        output_dir: String,
    ) -> Result<InfoHash, String> {
        let bytes = std::fs::read(&yarn_path).map_err(|e| format!("read: {e}"))?;
        let yarn = Arc::new(Yarn::decode(&bytes).map_err(|e| format!("decode: {e:?}"))?);
        let out_file = PathBuf::from(&output_dir).join(&yarn.info.filename);

        let fabric = self
            .engine
            .download_yarn(yarn, out_file.clone())
            .await
            .map_err(|e| format!("{e:?}"))?;

        let info_hash = fabric.info_hash;
        self.active.lock().unwrap().insert(
            info_hash,
            TorrentView {
                info_hash,
                fabric,
                is_seeding: false,
                output_path: out_file,
                added_at: SystemTime::now(),
            },
        );

        Ok(info_hash)
    }

    pub fn pause_torrent(&self, hash: InfoHash) -> Result<(), String> {
        let view = self
            .active
            .lock()
            .unwrap()
            .remove(&hash)
            .ok_or_else(|| "torrent not active".to_string())?;

        let bitfield = view.fabric.piece_manager.lock().unwrap().bitfield.clone();

        view.fabric.stop.cancel();
        self.engine.pod_manager.remove_yarn(&hash);

        self.paused.lock().unwrap().insert(
            hash,
            PausedEntry {
                yarn: view.fabric.yarn.clone(),
                output_path: view.output_path,
                is_seeding: view.is_seeding,
                cached_bitfield: Some(bitfield),
            },
        );

        Ok(())
    }

    pub async fn resume_torrent(&self, hash: InfoHash) -> Result<(), String> {
        let entry = self
            .paused
            .lock()
            .unwrap()
            .remove(&hash)
            .ok_or_else(|| "torrent not paused".to_string())?;

        let bitfield = match entry.cached_bitfield {
            Some(bf) => bf,
            None => {
                let path = entry.output_path.clone();
                let yarn = entry.yarn.clone();
                tokio::task::spawn_blocking(move || scan_existing_pieces(&path, &yarn))
                    .await
                    .map_err(|_| "rescan task failed".to_string())?
                    .map_err(|e| format!("rescan: {e:?}"))?
            }
        };

        let out = entry.output_path.to_string_lossy().into_owned();

        let fabric = YarnFabric::new(
            entry.yarn,
            out,
            self.engine.spawner(),
            self.engine.peer_addr(),
            Some(bitfield),
            self.engine.child_token(),
        )
        .map_err(|e| format!("{e:?}"))?;

        self.engine.pod_manager.add_yarn(fabric.clone());
        self.engine.spawn_announce_loop(fabric.clone());

        self.active.lock().unwrap().insert(
            hash,
            TorrentView {
                info_hash: hash,
                fabric,
                is_seeding: entry.is_seeding,
                output_path: entry.output_path,
                added_at: SystemTime::now(),
            },
        );

        Ok(())
    }

    pub fn remove_torrent(&self, hash: InfoHash) -> Result<(), String> {
        if let Some(view) = self.active.lock().unwrap().remove(&hash) {
            view.fabric.stop.cancel();
            self.engine.pod_manager.remove_yarn(&hash);
        }
        self.paused.lock().unwrap().remove(&hash);
        Ok(())
    }

    pub fn output_path_for(&self, hash: &InfoHash) -> Option<PathBuf> {
        if let Some(v) = self.active.lock().unwrap().get(hash) {
            return Some(v.output_path.clone());
        }
        self.paused.lock().unwrap().get(hash).map(|p| p.output_path.clone())
    }
}

fn build_piece_map(pm: &PieceManager) -> Vec<u8> {
    let n = pm.piece_count as usize;
    let buckets = n.min(PIECE_MAP_BUCKETS);

    if n <= PIECE_MAP_BUCKETS {
        pm.piece_state
            .iter()
            .map(|s| match s {
                PieceStatus::Complete => 2u8,
                PieceStatus::InFlight { .. } => 1u8,
                PieceStatus::Missing => 0u8,
            })
            .collect()
    } else {
        let per = n.div_ceil(buckets);
        (0..buckets)
            .map(|b| {
                let start = b * per;
                let end = (start + per).min(n);
                let mut any_inflight = false;
                let mut all_complete = true;
                for i in start..end {
                    match &pm.piece_state[i] {
                        PieceStatus::Complete => {}
                        PieceStatus::InFlight { .. } => {
                            any_inflight = true;
                            all_complete = false;
                        }
                        PieceStatus::Missing => all_complete = false,
                    }
                }
                if all_complete { 2 } else if any_inflight { 1 } else { 0 }
            })
            .collect()
    }
}

fn bitfield_to_map(bf: &Bitfield) -> Vec<u8> {
    let n = bf.piece_count as usize;
    let buckets = n.min(PIECE_MAP_BUCKETS);

    if n <= PIECE_MAP_BUCKETS {
        (0..n as u32).map(|i| if bf.has(i) { 2u8 } else { 0u8 }).collect()
    } else {
        let per = n.div_ceil(buckets);
        (0..buckets)
            .map(|b| {
                let start = b * per;
                let end = (start + per).min(n);
                if (start..end).all(|i| bf.has(i as u32)) { 2 } else { 0 }
            })
            .collect()
    }
}

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub fn parse_hash(s: &str) -> Result<InfoHash, String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let bytes = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .map_err(|_| "invalid hex".to_string())?;
    let arr: [u8; 32] = bytes.try_into().map_err(|_| "hash must be 32 bytes".to_string())?;
    Ok(InfoHash(arr))
}