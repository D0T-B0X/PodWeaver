pub mod dht;
pub mod wire;
pub mod weft;
pub mod error;
pub mod types;
pub mod runtime;
pub mod storage;
pub mod transport;

use std::path::Path;

use weft::Weft;
use types::Result;
use runtime::RuntimeManager;
use runtime::shutdown::ShutdownController;

pub struct Engine {
    runtime: RuntimeManager,
    shutdown: ShutdownController
}

impl Engine {
    pub fn new() -> Self {
        let (runtime, shutdown) = RuntimeManager::new();
        Self { runtime, shutdown }
    }

    pub async fn init<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path: &Path = path.as_ref();
        let weft = Weft::new(None, None, None, path)?;
        let info_hash = weft.info_hash()?;

        // let (candidate_tx, candidate_rx) = mpsc::channel(100);
        // let (block_tx, block_rx) = mpsc::channel(256);
        Ok(())        
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        // self.shutdown.wait_for_completion();
    }
}