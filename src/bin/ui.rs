use std::sync::Arc;
use std::net::SocketAddr;

use pod_weaver::Engine;
use pod_weaver::ui::{AppState, server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Arc::new(Engine::new().await?);
    let state = Arc::new(AppState::new(engine));

    let port: u16 = std::env::var("UI_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let app = server::build_router(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("UI listening on http://{}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}