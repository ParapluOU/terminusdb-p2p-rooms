//! HTTP layer: JSON API for the frontends, websocket per room (live state
//! push + WebRTC signalling relay), and static file serving for the example
//! apps.
//!
//! The websocket doubles as the WebRTC signalling server: browser clients in
//! the same room discover each other through `peer-joined` events and relay
//! SDP offers/answers/ICE candidates via `signal` frames, then exchange their
//! ops directly over WebRTC data channels (with the node as the anchoring
//! writer for the hypercore log).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{any, get, post};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use room_protocol::{encode_envelope, valid_branch_name, OpEnvelope, RoomOp};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::{debug, info};

use crate::engine::EngineHandle;
use crate::materialise::{self, SharedMatState};
use crate::presence::{parse_key, Directory, HostInfo};

#[derive(Clone)]
pub struct NodeMeta {
    pub name: String,
    pub roomnet_id: String,
    pub gossip_id: String,
    pub http_url: Option<String>,
    pub tdb_url: String,
    pub tdb_user: String,
    pub tdb_pass: String,
    pub tdb_org: String,
}

/// Per-room hub of connected websocket clients, for state push + signalling.
#[derive(Default)]
pub struct SignalHub {
    next_id: AtomicU64,
    rooms: Mutex<HashMap<String, HashMap<u64, mpsc::UnboundedSender<Message>>>>,
}

impl SignalHub {
    async fn join(&self, room: &str, tx: mpsc::UnboundedSender<Message>) -> (u64, Vec<u64>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut rooms = self.rooms.lock().await;
        let clients = rooms.entry(room.to_string()).or_default();
        let peers: Vec<u64> = clients.keys().copied().collect();
        clients.insert(id, tx);
        for (peer, sender) in clients.iter() {
            if *peer != id {
                let _ = sender.send(text(&json!({"t": "peer-joined", "id": id})));
            }
        }
        (id, peers)
    }

    async fn leave(&self, room: &str, id: u64) {
        let mut rooms = self.rooms.lock().await;
        if let Some(clients) = rooms.get_mut(room) {
            clients.remove(&id);
            for sender in clients.values() {
                let _ = sender.send(text(&json!({"t": "peer-left", "id": id})));
            }
            if clients.is_empty() {
                rooms.remove(room);
            }
        }
    }

    async fn relay(&self, room: &str, from: u64, to: u64, data: Value) {
        let rooms = self.rooms.lock().await;
        if let Some(sender) = rooms.get(room).and_then(|c| c.get(&to)) {
            let _ = sender.send(text(&json!({"t": "signal", "from": from, "data": data})));
        }
    }
}

fn text(v: &Value) -> Message {
    Message::Text(v.to_string().into())
}

#[derive(Clone)]
pub struct AppState {
    pub engine: EngineHandle,
    pub directory: Arc<Directory>,
    pub mat: SharedMatState,
    pub hub: Arc<SignalHub>,
    pub meta: NodeMeta,
}

pub fn router(state: AppState, frontend_dir: &str) -> Router {
    Router::new()
        .route("/api/info", get(info))
        .route("/api/hosts", get(hosts))
        .route("/api/rooms", get(list_rooms).post(create_room))
        .route("/api/rooms/{id}", get(room_snapshot))
        .route("/api/rooms/{id}/join", post(join_room))
        .route("/api/rooms/{id}/state", get(room_state))
        .route("/api/rooms/{id}/log", get(room_log))
        .route("/api/rooms/{id}/ops", post(append_op))
        .route("/api/rooms/{id}/tdb", get(room_tdb))
        .route("/api/rooms/{id}/ws", any(ws_handler))
        .fallback_service(ServeDir::new(frontend_dir).append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn info(State(s): State<AppState>) -> Json<Value> {
    Json(json!({
        "name": s.meta.name,
        "roomnet_id": s.meta.roomnet_id,
        "gossip_id": s.meta.gossip_id,
        "http_url": s.meta.http_url,
        // Browser-side writers (the wasm client) open their Room with the
        // same indexer set so their finality matches the node's.
        "indexers": s.engine.indexers.iter().map(hex::encode).collect::<Vec<_>>(),
        "tdb": {
            "endpoint": *s.mat.endpoint.read().await,
            "connected": *s.mat.connected.read().await,
        },
    }))
}

/// The room's complete autobase hypercores: every writer's log, with causal
/// heads and the raw payload bytes exactly as stored in the ledger.
async fn room_log(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let room = parse_key(&id).map_err(ApiError::bad_request)?;
    let logs = s.engine.logs(room).await.ok_or_else(ApiError::not_found)?;
    Ok(Json(json!({ "id": id, "writers": logs })))
}

/// The host directory: this node plus every node heard on the presence topic.
async fn hosts(State(s): State<AppState>) -> Json<Value> {
    let rooms = s.engine.list().await;
    let me = HostInfo {
        name: s.meta.name.clone(),
        gossip_id: s.meta.gossip_id.clone(),
        roomnet_id: s.meta.roomnet_id.clone(),
        http_url: s.meta.http_url.clone(),
        rooms: rooms
            .into_iter()
            .map(|r| crate::presence::RoomAd { id: r.id, title: r.title, app: r.app })
            .collect(),
        seq: 0,
    };
    let peers = s.directory.hosts().await;
    Json(json!({ "self": me, "peers": peers }))
}

async fn list_rooms(State(s): State<AppState>) -> Json<Value> {
    Json(json!({ "rooms": s.engine.list().await }))
}

#[derive(Deserialize)]
struct CreateRoom {
    title: Option<String>,
    app: Option<String>,
}

async fn create_room(
    State(s): State<AppState>,
    Json(req): Json<CreateRoom>,
) -> Result<Json<Value>, ApiError> {
    let room: [u8; 32] = rand::random();
    if !s.engine.host(room).await {
        return Err(ApiError::internal("failed to host room"));
    }
    if let Some(title) = req.title.filter(|t| !t.is_empty()) {
        let env = OpEnvelope::main(RoomOp::SetTitle { title });
        s.engine.append(room, encode_envelope(&env)).await;
    }
    if let Some(app) = req.app.filter(|a| !a.is_empty()) {
        let env = OpEnvelope::main(RoomOp::SetApp { app });
        s.engine.append(room, encode_envelope(&env)).await;
    }
    info!(room = %hex::encode(room), "hosted new room");
    Ok(Json(json!({ "id": hex::encode(room) })))
}

#[derive(Deserialize)]
struct JoinRoom {
    /// Roomnet endpoint id (hex) of a node known to serve the room.
    origin: Option<String>,
}

async fn join_room(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<JoinRoom>,
) -> Result<Json<Value>, ApiError> {
    let room = parse_key(&id).map_err(ApiError::bad_request)?;
    let origin = match req.origin {
        Some(o) => Some(parse_key(&o).map_err(ApiError::bad_request)?),
        None => None,
    };
    if !s.engine.join(room, origin).await {
        return Err(ApiError::internal("failed to join room"));
    }
    Ok(Json(json!({ "id": id, "joined": true })))
}

async fn room_snapshot(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let room = parse_key(&id).map_err(ApiError::bad_request)?;
    let snap = s.engine.snapshot(room).await.ok_or_else(ApiError::not_found)?;
    Ok(Json(json!({
        "id": id,
        "live": snap.live,
        "finalized": snap.finalized,
        "finalized_len": snap.finalized_len,
    })))
}

#[derive(Deserialize)]
struct StateQuery {
    branch: Option<String>,
    /// "live" (default) or "finalized".
    view: Option<String>,
}

async fn room_state(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<StateQuery>,
) -> Result<Json<Value>, ApiError> {
    let room = parse_key(&id).map_err(ApiError::bad_request)?;
    let snap = s.engine.snapshot(room).await.ok_or_else(ApiError::not_found)?;
    let state = match q.view.as_deref() {
        Some("finalized") => &snap.finalized,
        _ => &snap.live,
    };
    let branch = q.branch.unwrap_or_else(|| room_protocol::MAIN_BRANCH.to_string());
    let branch_state = state.branches.get(&branch).ok_or_else(ApiError::not_found)?;
    Ok(Json(json!({
        "id": id,
        "title": state.title,
        "app": state.app,
        "nicks": state.nicks,
        "branches": state.branches.keys().collect::<Vec<_>>(),
        "branch": branch,
        "state": branch_state,
        "ops": state.ops_applied,
        "finalized_len": snap.finalized_len,
    })))
}

#[derive(Deserialize)]
struct AppendOp {
    #[serde(default = "default_branch")]
    branch: String,
    op: RoomOp,
}

fn default_branch() -> String {
    room_protocol::MAIN_BRANCH.to_string()
}

async fn append_op(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<AppendOp>,
) -> Result<Json<Value>, ApiError> {
    let room = parse_key(&id).map_err(ApiError::bad_request)?;
    if let RoomOp::BranchCreate { name } = &req.op {
        if !valid_branch_name(name) {
            return Err(ApiError::bad_request(anyhow::anyhow!(
                "branch names must be 1-64 chars of [a-zA-Z0-9_-]"
            )));
        }
    }
    let env = OpEnvelope::new(req.branch, req.op);
    if !s.engine.append(room, encode_envelope(&env)).await {
        return Err(ApiError::not_found());
    }
    Ok(Json(json!({ "ok": true })))
}

/// Materialisation status + live doc counts straight from TerminusDB, as
/// proof that the room really landed in a database.
async fn room_tdb(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let status = s.mat.rooms.read().await.get(&id).cloned();
    let Some(status) = status else {
        return Ok(Json(json!({ "materialised": false })));
    };
    let mut branches = serde_json::Map::new();
    if *s.mat.connected.read().await {
        for branch in &status.branches {
            if let Ok(counts) = materialise::branch_doc_counts(
                &s.meta.tdb_url,
                &s.meta.tdb_user,
                &s.meta.tdb_pass,
                &s.meta.tdb_org,
                &status.db,
                branch,
            )
            .await
            {
                branches.insert(branch.clone(), counts);
            }
        }
    }
    Ok(Json(json!({
        "materialised": true,
        "db": status.db,
        "last_version": status.last_version,
        "last_error": status.last_error,
        "branch_doc_counts": branches,
    })))
}

// --- websocket: live state + WebRTC signalling ------------------------------

async fn ws_handler(
    State(s): State<AppState>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, ApiError> {
    let room = parse_key(&id).map_err(ApiError::bad_request)?;
    Ok(ws.on_upgrade(move |socket| client_session(s, id, room, socket)))
}

async fn client_session(s: AppState, room_hex: String, room: [u8; 32], socket: WebSocket) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let (client_id, peers) = s.hub.join(&room_hex, tx.clone()).await;
    debug!(room = %room_hex, client_id, "ws client joined");

    // Binary lane: raw roomnet wire frames for browser-side hypercore writers
    // (the wasm client). `bound_writer` is set once the client identifies its
    // writer key, so Fanout::Peer replies can find this socket.
    let (sync_tx, mut sync_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let bus_id = s.engine.bus.join(room, sync_tx.clone());
    let mut bound_writer: Option<[u8; 32]> = None;

    let _ = tx.send(text(&json!({"t": "welcome", "you": client_id, "peers": peers})));
    if let Some(snap) = s.engine.snapshot(room).await {
        let _ = tx.send(text(&json!({
            "t": "state", "live": snap.live, "finalized_len": snap.finalized_len,
        })));
    }

    let mut events = s.engine.subscribe();
    loop {
        tokio::select! {
            // Outbound: hub relays + our own pushes.
            maybe = rx.recv() => match maybe {
                Some(msg) => {
                    if ws_tx.send(msg).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            // Outbound: engine live-state events for this room.
            event = events.recv() => {
                if let Ok(ev) = event {
                    if ev.room == room {
                        let msg = text(&json!({
                            "t": "state", "live": &*ev.live, "finalized_len": ev.finalized_len,
                        }));
                        if ws_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                }
            }
            // Outbound: sync frames for browser-side writers.
            maybe = sync_rx.recv() => match maybe {
                Some(bytes) => {
                    if ws_tx.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            // Inbound: signalling + ops from the browser.
            frame = ws_rx.next() => match frame {
                Some(Ok(Message::Text(raw))) => {
                    let Ok(v) = serde_json::from_str::<Value>(&raw) else { continue };
                    match v["t"].as_str() {
                        Some("signal") => {
                            if let Some(to) = v["to"].as_u64() {
                                s.hub.relay(&room_hex, client_id, to, v["data"].clone()).await;
                            }
                        }
                        // A wasm writer announcing its writer key: binds this
                        // socket for Fanout::Peer replies (Want -> Block).
                        Some("writer") => {
                            if let Some(key) = v["key"].as_str().and_then(|k| parse_key(k).ok()) {
                                s.engine.bus.bind_writer(key, sync_tx.clone());
                                bound_writer = Some(key);
                            }
                        }
                        Some("append") => {
                            let branch = v["branch"].as_str().unwrap_or(room_protocol::MAIN_BRANCH);
                            if let Ok(op) = serde_json::from_value::<RoomOp>(v["op"].clone()) {
                                let valid_branch_op = match &op {
                                    RoomOp::BranchCreate { name } => valid_branch_name(name),
                                    _ => true,
                                };
                                if valid_branch_op {
                                    let env = OpEnvelope::new(branch, op);
                                    s.engine.append(room, encode_envelope(&env)).await;
                                }
                            }
                        }
                        _ => {}
                    }
                }
                // Binary frames: roomnet wire SyncMessages from a wasm writer.
                Some(Ok(Message::Binary(bytes))) => {
                    if let (Some(writer), Ok(msg)) = (bound_writer, roomnet::wire::decode(&bytes)) {
                        s.engine.client_sync(room, writer, msg).await;
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(_)) => break,
            }
        }
    }

    s.engine.bus.leave(room, bus_id, bound_writer);
    s.hub.leave(&room_hex, client_id).await;
    debug!(room = %room_hex, client_id, "ws client left");
}

// --- error plumbing ---------------------------------------------------------

pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(e: anyhow::Error) -> Self {
        Self { status: StatusCode::BAD_REQUEST, message: format!("{e:#}") }
    }
    fn not_found() -> Self {
        Self { status: StatusCode::NOT_FOUND, message: "room not found on this node".into() }
    }
    fn internal(msg: &str) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: msg.into() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}
