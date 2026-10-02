pub mod dht;
pub mod wire;
pub mod weft;
pub mod error;
pub mod types;
pub mod runtime;
pub mod storage;

use std::path::Path;

use error::*;
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

    pub async fn start_weft<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        
        Ok(())        
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        // self.shutdown.wait_for_completion();
    }
}