use std::fs;
use std::env;
use std::path::PathBuf;

use crate::error::*;

pub fn find_path(filename: &str) -> Result<PathBuf> {
    let podweaver_home = find_or_set_home()?;
    let filepath = podweaver_home.join(filename);

    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(&parent)?;
    }

    Ok(filepath)
}

pub fn save_to_disk(filename: &str, value: &[u8]) -> Result<()> {
    let filepath = find_path(filename)?;

    // store the value
    let temp_path = filepath.with_extension("tmp");
    fs::write(&temp_path, value)?;
    fs::rename(&temp_path, &filepath)?;

    Ok(())
}

pub fn read_file(filename: &str) -> Result<Vec<u8>> {
    let filepath = find_path(filename)?;

    match fs::read(filepath) {
        Ok(bytes) => {
            Ok(bytes)
        }

        _ => {
            Err(Error::FileReadFailed)
        }
    }
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