use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct ShutdownController {
    token: CancellationToken,
}

impl ShutdownController {
    pub fn new() -> Self {
        Self { token: CancellationToken::new() }
    }

    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub async fn listen_for_signals(&self) {
        let ctrl_c = tokio::signal::ctrl_c();

        #[cfg(unix)]
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate()
        ).expect("Failed to register SIGTERM handler");

        #[cfg(unix)]
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }

        #[cfg(not(unix))]
        let _ = ctrl_c.await;

        self.cancel();
    }
}