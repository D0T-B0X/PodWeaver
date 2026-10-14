pub mod tracker;
pub mod shutdown;
pub mod blocking;

use tokio_util::sync::CancellationToken;

use tracker::*;
use shutdown::*;

#[derive(Clone)]
pub struct RuntimeManager {
    spawner: TaskSpawner,
    shutdown: ShutdownController,
}

impl RuntimeManager {
    pub fn new() -> (Self, ShutdownController) {
        let shutdown = ShutdownController::new();
        let spawner = TaskSpawner::new(shutdown.token());

        let handle = Self {
            spawner,
            shutdown: shutdown.clone()
        };

        (handle, shutdown)
    }

    pub async fn wait_for_tasks(&self) {
        self.spawner.wait_for_completion().await;
    }

    pub fn spawner(&self) -> &TaskSpawner {
        &self.spawner
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.shutdown.token()
    }
}