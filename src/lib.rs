pub mod dht;
pub mod peer;
pub mod yarn;
pub mod error;
pub mod types;
pub mod runtime;
pub mod storage;
pub mod transport;

use yarn::Yarn;
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

    pub async fn init(&self, path: String) -> Result<()> {
        let yarn = Yarn::from_file(None, None, None, path).await?;
        let info_hash = yarn.info_hash()?;

        // let (candidate_tx, candidate_rx) = mpsc::channel(100);
        // let (block_tx, block_rx) = mpsc::channel(256);
        Ok(())        
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        // self.shutdown.wait_for_completion();
    }
}