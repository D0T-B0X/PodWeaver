use std::future::Future;
use tokio_util::task::TaskTracker;
use tokio_util::sync:: CancellationToken;

#[derive(Clone)]
pub struct TaskSpawner {
    tracker: TaskTracker,
    cancel_token: CancellationToken,
}

impl TaskSpawner {
    pub fn new(cancel_token: CancellationToken) -> Self {
        Self {
            tracker: TaskTracker::new(),
            cancel_token
        }
    }

    pub fn spawn<F, Fut>(&self, task_builder: F) 
    where 
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static
    {
        let token = self.cancel_token.child_token();
        let fut = task_builder(token);
        self.tracker.spawn(fut);
    }

    pub async fn wait_for_completion(&self) {
        self.tracker.close();
        self.tracker.wait().await;
    }
}