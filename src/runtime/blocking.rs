use tokio::task::JoinHandle;

pub struct BlockingPool;

impl BlockingPool {
    pub fn spawn<F, R>(f: F) -> JoinHandle<R> 
    where 
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static
    {
        tokio::task::spawn_blocking(f)
    }
}