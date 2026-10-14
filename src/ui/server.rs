use std::sync::Arc;
use std::time::Duration;
use std::convert::Infallible;

use axum::{
    Router,
    routing::{get, post},
    extract::State,
    response::{Html, IntoResponse, sse::{Event, Sse, KeepAlive}},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;

use super::state::{AppState, parse_hash};

const INDEX_HTML: &str = include_str!("../../ui/index.html");
const STYLE_CSS: &str  = include_str!("../../ui/style.css");
const MAIN_JS: &str    = include_str!("../../ui/main.js");

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/",              get(serve_index))
        .route("/style.css",     get(serve_css))
        .route("/main.js",       get(serve_js))
        .route("/api/torrents",  get(list_torrents))
        .route("/api/node",      get(node_info))
        .route("/api/events",    get(events))
        .route("/api/add-seed",     post(add_seed))
        .route("/api/add-download", post(add_download))
        .route("/api/pause",        post(pause))
        .route("/api/resume",       post(resume))
        .route("/api/remove",       post(remove))
        .with_state(state)
}

async fn serve_index() -> Html<&'static str> { Html(INDEX_HTML) }
async fn serve_css() -> impl IntoResponse {
    ([(axum::http::header::CONTENT_TYPE, "text/css")], STYLE_CSS)
}
async fn serve_js() -> impl IntoResponse {
    ([(axum::http::header::CONTENT_TYPE, "application/javascript")], MAIN_JS)
}

async fn list_torrents(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(serde_json::to_value(state.snapshot()).unwrap_or(serde_json::Value::Array(vec![])))
}

async fn node_info(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let info = state.node_info();
    Json(serde_json::json!({
        "node_id": info.node_id,
        "listen_port": info.listen_port,
        "peer_addr": info.peer_addr,
        "dht_addr": info.dht_addr,
    }))
}

async fn events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let interval = tokio::time::interval(Duration::from_millis(500));
    let stream = IntervalStream::new(interval).map(move |_| {
        let snapshot = state.snapshot();
        let json = serde_json::to_string(&snapshot).unwrap_or_else(|_| "[]".into());
        Ok(Event::default().event("progress").data(json))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Deserialize)]
struct AddSeedReq { path: String }

async fn add_seed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddSeedReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    state.add_seed(req.path).await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))
}

#[derive(Deserialize)]
struct AddDownloadReq { yarn_path: String, output_dir: String }

async fn add_download(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddDownloadReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    state.add_download(req.yarn_path, req.output_dir).await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))
}

#[derive(Deserialize)]
struct HashReq { info_hash: String }

async fn pause(
    State(state): State<Arc<AppState>>,
    Json(req): Json<HashReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    let hash = parse_hash(&req.info_hash).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    state.pause_torrent(hash)
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))
}

async fn resume(
    State(state): State<Arc<AppState>>,
    Json(req): Json<HashReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    let hash = parse_hash(&req.info_hash).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    state.resume_torrent(hash).await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))
}

async fn remove(
    State(state): State<Arc<AppState>>,
    Json(req): Json<HashReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    let hash = parse_hash(&req.info_hash).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    state.remove_torrent(hash)
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))
}