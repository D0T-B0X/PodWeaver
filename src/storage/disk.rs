use std::fs;
use std::env;
use std::io;
use std::io::Write;
use std::path::PathBuf;

use crate::error::Error;
use crate::types::Result;

const DEFAULT_DOWNLOAD_PATH: &str = "downloads";

pub fn save_to_disk(filename: &str, value: &[u8]) -> Result<()> {
    let filepath = find_path(filename)?;

    let tmp_file = filepath.with_extension("tmp");
    fs::write(&tmp_file, value)?;
    fs::rename(&tmp_file, filepath)?;

    Ok(())
}

pub fn save_to_disk_atomic(filename: &str, value: &[u8]) -> Result<()> {
    let filepath = find_path(filename)?;

    // store the value
    let temp_path = filepath.with_extension("tmp");
    let mut temp_file = std::fs::File::create(&temp_path)?;
    temp_file.write_all(value)?;
    temp_file.sync_all()?;

    fs::rename(&temp_path, &filepath)?;

    Ok(())
}

pub fn read_or_create_file(filename: &str) -> Result<Vec<u8>> {
    let filepath = find_path(filename)?;

    match fs::read(&filepath) {
        Ok(bytes) => {
            Ok(bytes)
        }

        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            fs::File::create(filepath)?;
            Ok(Vec::new())
        }

        _ => {
            Err(Error::FileReadFailed)
        }
    }
}

pub fn allocate_file_space(filepath: Option<PathBuf>, filename: &str, size: u64) -> Result<PathBuf> {

    let path = if let Some(path) = filepath { 
        path 
    } else {
        let home = find_or_set_home()?;
        home.join(DEFAULT_DOWNLOAD_PATH).join(filename)
    };

    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)?;

    file.set_len(size)?;
    Ok(path)
}

pub fn read_file(filename: &str) -> Result<Vec<u8>> {
    let filepath = find_path(filename)?;
    fs::read(filepath).map_err(|_| Error::FileReadFailed)
}

pub fn set_healthy() -> Result<()> {
    fs::File::create("/tmp/healthy")?;
    Ok(())
}

fn find_or_set_home() -> Result<PathBuf> {
    let path = if let Ok(home) = env::var("PODWEAVER_HOME") {
        PathBuf::from(home)
    } else {
        let home_dir = env::var("HOME").map_err(|_| Error::HomeNotFound)?; // The fuck?????
        let deafult_path = PathBuf::from(home_dir).join(".config").join("PodWeaver");

        deafult_path
    };

    fs::create_dir_all(&path)?;

    Ok(path)
}

fn find_path(filename: &str) -> Result<PathBuf> {
    let podweaver_home = find_or_set_home()?;
    let filepath = podweaver_home.join(filename);

    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(&parent)?;
    }

    Ok(filepath)
}
