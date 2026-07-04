//! The room engine: one tokio task owning the [`RoomServer`] and the roomnet
//! [`IrohTransport`], driven by commands from the HTTP layer and the presence
//! layer.
//!
//! This mirrors `roomnet::run_server` but adds what a serving node needs on
//! top of a headless replica: snapshot queries, live-state push events for
//! websocket subscribers, auto-replication of rooms it hears about on the
//! wire, raw log access, a channel of finalized states for the TerminusDB
//! materialiser — and first-class **browser writers**: websocket clients that
//! run their own hypercore (the wasm client) exchange the same roomnet wire
//! frames as iroh peers, routed through the [`SyncBus`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use room_protocol::{RoomProjection, RoomState};
use roomnet::{
    wire, Fanout, Inbound, IrohTransport, MemStoreFactory, Origin, Outbound, PeerId, RoomId,
    RoomServer, SyncMessage,
};
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, info, warn};

pub type NodeKey = [u8; 32];

const TICK: Duration = Duration::from_millis(50);
const ANNOUNCE_EVERY_TICKS: u64 = 40;

/// Compact room listing for the HTTP API and presence announcements.
#[derive(Clone, Debug, Serialize)]
pub struct RoomSummary {
    pub id: String,
    pub title: Option<String>,
    pub app: Option<String>,
    pub branches: Vec<String>,
    pub origin: &'static str,
    pub ops: u64,
    pub finalized_ops: usize,
}

/// Full state answer for one room.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub live: RoomState,
    pub finalized: RoomState,
    pub finalized_len: usize,
}

/// One entry of one writer's hypercore, as served over HTTP.
#[derive(Clone, Debug, Serialize)]
pub struct LogEntry {
    pub seq: u64,
    /// Causal references (autobase heads) recorded in the entry.
    pub heads: Vec<HeadRef>,
    /// Raw payload bytes (hex) — exactly what is stored in the ledger.
    pub payload_hex: String,
    /// Decoded L2 envelope, when the payload parses as one.
    pub envelope: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HeadRef {
    pub writer: String,
    pub seq: u64,
}

/// One writer's complete log within a room.
#[derive(Clone, Debug, Serialize)]
pub struct WriterLog {
    pub writer: String,
    pub len: u64,
    pub entries: Vec<LogEntry>,
}

/// Live-state push event, broadcast to websocket subscribers.
#[derive(Clone, Debug)]
pub struct RoomEvent {
    pub room: RoomId,
    pub live: Arc<RoomState>,
    pub finalized_len: usize,
}

/// A finalized room state headed for the TerminusDB materialiser.
#[derive(Clone, Debug)]
pub struct MatJob {
    pub room: RoomId,
    pub version: u64,
    pub state: RoomState,
}

/// Registry of websocket-connected sync clients (the wasm hypercore writers).
///
/// Frames pushed here are raw roomnet wire bytes; the per-room websocket
/// carries them as binary messages. `writers` routes `Fanout::Peer` replies
/// (e.g. `Block`s answering a client's `Want`) to the right socket.
#[derive(Clone, Default)]
pub struct SyncBus {
    inner: Arc<Mutex<SyncBusInner>>,
}

#[derive(Default)]
struct SyncBusInner {
    next_id: u64,
    rooms: HashMap<RoomId, HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>,
    writers: HashMap<PeerId, mpsc::UnboundedSender<Vec<u8>>>,
}

impl SyncBus {
    /// Register a websocket client for `room`; returns its subscription id.
    pub fn join(&self, room: RoomId, tx: mpsc::UnboundedSender<Vec<u8>>) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner.next_id += 1;
        let id = inner.next_id;
        inner.rooms.entry(room).or_default().insert(id, tx);
        id
    }

    /// Bind a client's writer key to its socket so peer-directed replies reach it.
    pub fn bind_writer(&self, writer: PeerId, tx: mpsc::UnboundedSender<Vec<u8>>) {
        self.inner.lock().unwrap().writers.insert(writer, tx);
    }

    pub fn leave(&self, room: RoomId, id: u64, writer: Option<PeerId>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(subs) = inner.rooms.get_mut(&room) {
            subs.remove(&id);
            if subs.is_empty() {
                inner.rooms.remove(&room);
            }
        }
        if let Some(w) = writer {
            inner.writers.remove(&w);
        }
    }

    fn push_room(&self, room: RoomId, bytes: &[u8]) {
        let inner = self.inner.lock().unwrap();
        if let Some(subs) = inner.rooms.get(&room) {
            for tx in subs.values() {
                let _ = tx.send(bytes.to_vec());
            }
        }
    }

    fn send_writer(&self, writer: &PeerId, bytes: Vec<u8>) -> bool {
        let inner = self.inner.lock().unwrap();
        match inner.writers.get(writer) {
            Some(tx) => tx.send(bytes).is_ok(),
            None => false,
        }
    }
}

pub enum EngineCmd {
    Host { room: RoomId, reply: oneshot::Sender<bool> },
    Join { room: RoomId, origin: Option<NodeKey>, reply: oneshot::Sender<bool> },
    Append { room: RoomId, payload: Vec<u8>, reply: oneshot::Sender<bool> },
    /// A roomnet wire frame from a websocket client (a browser-side writer).
    ClientSync { room: RoomId, from: PeerId, msg: SyncMessage },
    Snapshot { room: RoomId, reply: oneshot::Sender<Option<Snapshot>> },
    Logs { room: RoomId, reply: oneshot::Sender<Option<Vec<WriterLog>>> },
    List { reply: oneshot::Sender<Vec<RoomSummary>> },
    AddPeer { peer: NodeKey },
}

/// Cheap-to-clone handle used by the HTTP and presence layers.
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<EngineCmd>,
    events: broadcast::Sender<RoomEvent>,
    pub bus: SyncBus,
    pub node_key: NodeKey,
    /// The indexer set every room on this node is opened with (browser
    /// writers need it to compute the same finality).
    pub indexers: Arc<Vec<NodeKey>>,
}

impl EngineHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<RoomEvent> {
        self.events.subscribe()
    }

    async fn ask<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> EngineCmd) -> Option<T> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(make(tx)).await.ok()?;
        rx.await.ok()
    }

    pub async fn host(&self, room: RoomId) -> bool {
        self.ask(|reply| EngineCmd::Host { room, reply }).await.unwrap_or(false)
    }

    pub async fn join(&self, room: RoomId, origin: Option<NodeKey>) -> bool {
        self.ask(|reply| EngineCmd::Join { room, origin, reply }).await.unwrap_or(false)
    }

    pub async fn append(&self, room: RoomId, payload: Vec<u8>) -> bool {
        self.ask(|reply| EngineCmd::Append { room, payload, reply }).await.unwrap_or(false)
    }

    pub async fn client_sync(&self, room: RoomId, from: PeerId, msg: SyncMessage) {
        let _ = self.tx.send(EngineCmd::ClientSync { room, from, msg }).await;
    }

    pub async fn snapshot(&self, room: RoomId) -> Option<Snapshot> {
        self.ask(|reply| EngineCmd::Snapshot { room, reply }).await.flatten()
    }

    pub async fn logs(&self, room: RoomId) -> Option<Vec<WriterLog>> {
        self.ask(|reply| EngineCmd::Logs { room, reply }).await.flatten()
    }

    pub async fn list(&self) -> Vec<RoomSummary> {
        self.ask(|reply| EngineCmd::List { reply }).await.unwrap_or_default()
    }

    pub async fn add_peer(&self, peer: NodeKey) {
        let _ = self.tx.send(EngineCmd::AddPeer { peer }).await;
    }
}

type Server = RoomServer<MemStoreFactory, RoomProjection>;

pub struct Engine {
    server: Server,
    transport: IrohTransport,
    cmd_rx: mpsc::Receiver<EngineCmd>,
    events: broadcast::Sender<RoomEvent>,
    bus: SyncBus,
    mat_tx: mpsc::Sender<MatJob>,
    /// Replicate any room we see frames or announcements for.
    auto_replicate: bool,
    /// Materialise the live projection instead of the finalized one. Useful
    /// in multi-indexer deployments where finality lags (see README).
    mat_live: bool,
    last_activity: HashMap<RoomId, u64>,
    /// Rooms that ingested a browser writer's block since the last anchor.
    /// The next tick appends a `meta.checkpoint` so this node's (indexer)
    /// entry causally references — and thereby finalizes — those entries.
    needs_anchor: std::collections::HashSet<RoomId>,
}

impl Engine {
    pub fn new(
        server: Server,
        transport: IrohTransport,
        mat_tx: mpsc::Sender<MatJob>,
        auto_replicate: bool,
        mat_live: bool,
        indexers: Vec<NodeKey>,
    ) -> (Self, EngineHandle) {
        let node_key = transport.endpoint_id();
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (events, _) = broadcast::channel(256);
        let bus = SyncBus::default();
        let engine = Self {
            server,
            transport,
            cmd_rx,
            events: events.clone(),
            bus: bus.clone(),
            mat_tx,
            auto_replicate,
            mat_live,
            last_activity: HashMap::new(),
            needs_anchor: std::collections::HashSet::new(),
        };
        (engine, EngineHandle { tx: cmd_tx, events, bus, node_key, indexers: Arc::new(indexers) })
    }

    pub async fn run(mut self) {
        let mut tick = 0u64;
        loop {
            tokio::select! {
                maybe = self.cmd_rx.recv() => match maybe {
                    Some(cmd) => self.on_command(cmd),
                    None => break,
                },
                _ = tokio::time::sleep(TICK) => {
                    self.on_tick(tick);
                    tick = tick.wrapping_add(1);
                }
            }
        }
        info!("engine loop stopped");
    }

    /// Ship one room Outbound to wherever its fanout says — iroh peers,
    /// websocket clients, or both.
    fn route(&self, room: RoomId, out: Outbound) {
        match out.to {
            Fanout::Clients => {
                // Lane 1: push to this room's websocket clients.
                self.bus.push_room(room, &wire::encode(&out.msg));
                // Blocks are self-verifying (SignedHead + Merkle proof), so a
                // browser writer's block can be push-relayed to iroh peers —
                // roomnet nodes only serve their *local* writer on pull, so
                // without this relay client-authored entries would never
                // leave their home node.
                if matches!(out.msg, SyncMessage::Block { .. }) {
                    self.transport.dispatch(room, Outbound { msg: out.msg, to: Fanout::Gossip });
                }
            }
            Fanout::Peer(p) => {
                // Websocket-bound writers first; fall back to dialing iroh.
                let bytes = wire::encode(&out.msg);
                if !self.bus.send_writer(&p, bytes) {
                    self.transport.dispatch(room, out);
                }
            }
            Fanout::Gossip => {
                // Head adverts reach both iroh peers and websocket clients
                // (the wasm client answers them with Want, like any replica).
                self.bus.push_room(room, &wire::encode(&out.msg));
                self.transport.dispatch(room, out);
            }
        }
    }

    fn on_command(&mut self, cmd: EngineCmd) {
        match cmd {
            EngineCmd::Host { room, reply } => {
                let ok = self.server.host(room).is_ok();
                if ok {
                    self.announce(room);
                }
                let _ = reply.send(ok);
            }
            EngineCmd::Join { room, origin, reply } => {
                if let Some(origin) = origin {
                    self.transport.add_peer(origin);
                }
                let ok = self.server.join_remote(room).is_ok();
                if ok {
                    self.announce(room);
                }
                let _ = reply.send(ok);
            }
            EngineCmd::Append { room, payload, reply } => {
                let ok = match self.server.get_mut(room) {
                    Some(r) => match r.local_append(&payload) {
                        Ok(outs) => {
                            for o in outs {
                                self.route(room, o);
                            }
                            true
                        }
                        Err(e) => {
                            warn!(room = %hex::encode(room), "append failed: {e:?}");
                            false
                        }
                    },
                    None => false,
                };
                let _ = reply.send(ok);
            }
            EngineCmd::ClientSync { room, from, msg } => {
                let is_block = matches!(msg, SyncMessage::Block { .. });
                self.ingest(room, from, msg);
                if is_block {
                    self.needs_anchor.insert(room);
                }
            }
            EngineCmd::Snapshot { room, reply } => {
                let snap = self.server.get(room).map(|r| Snapshot {
                    live: r.snapshot_live().clone(),
                    finalized: r.snapshot_finalized().clone(),
                    finalized_len: r.finalized_len(),
                });
                let _ = reply.send(snap);
            }
            EngineCmd::Logs { room, reply } => {
                let _ = reply.send(self.collect_logs(room));
            }
            EngineCmd::List { reply } => {
                let rooms = self
                    .server
                    .rooms()
                    .map(|(id, r)| {
                        let live = r.snapshot_live();
                        RoomSummary {
                            id: hex::encode(id),
                            title: live.title.clone(),
                            app: live.app.clone(),
                            branches: live.branches.keys().cloned().collect(),
                            origin: match r.origin() {
                                Origin::Original => "original",
                                Origin::Replicated { .. } => "replica",
                            },
                            ops: live.ops_applied,
                            finalized_ops: r.finalized_len(),
                        }
                    })
                    .collect();
                let _ = reply.send(rooms);
            }
            EngineCmd::AddPeer { peer } => self.transport.add_peer(peer),
        }
    }

    /// Feed one inbound sync message (from an iroh peer or a ws client) into
    /// its room, joining the room first when in replicate mode.
    fn ingest(&mut self, room: RoomId, from: PeerId, msg: SyncMessage) {
        if self.server.get(room).is_none() {
            if !self.auto_replicate {
                return;
            }
            if self.server.join_remote(room).is_ok() {
                debug!(room = %hex::encode(room), "auto-replicating room heard on the wire");
            }
        }
        if let Some(r) = self.server.get_mut(room) {
            match r.on_inbound(from, msg) {
                Ok(outs) => {
                    for o in outs {
                        self.route(room, o);
                    }
                }
                Err(e) => warn!(room = %hex::encode(room), "inbound failed: {e:?}"),
            }
        }
    }

    /// The room's complete autobase hypercores: every writer's log with
    /// causal heads and raw payloads.
    fn collect_logs(&self, room: RoomId) -> Option<Vec<WriterLog>> {
        let r = self.server.get(room)?;
        // order() covers every node the linearizer has ingested; group by writer.
        let mut lens: HashMap<[u8; 32], u64> = HashMap::new();
        for node in r.order() {
            let len = lens.entry(node.key).or_default();
            *len = (*len).max(node.seq + 1);
        }
        let mut logs: Vec<WriterLog> = lens
            .into_iter()
            .map(|(writer, len)| {
                let entries = r
                    .logs(writer, 0, len)
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                    .map(|(seq, e)| LogEntry {
                        seq: seq as u64,
                        heads: e
                            .heads
                            .iter()
                            .map(|h| HeadRef { writer: hex::encode(h.key), seq: h.seq })
                            .collect(),
                        envelope: room_protocol::decode_envelope(&e.payload)
                            .and_then(|env| serde_json::to_value(env).ok()),
                        payload_hex: hex::encode(&e.payload),
                    })
                    .collect();
                WriterLog { writer: hex::encode(writer), len, entries }
            })
            .collect();
        logs.sort_by(|a, b| a.writer.cmp(&b.writer));
        Some(logs)
    }

    fn on_tick(&mut self, tick: u64) {
        // Pump inbound sync frames from iroh peers into their rooms.
        for Inbound { room, from, msg } in self.transport.drain_inbound() {
            self.transport.add_peer(from);
            self.ingest(room, from, msg);
        }

        // Anchor browser writers' entries: our own append links the DAG
        // frontier, so it causally sees their blocks and votes them toward
        // quorum finality.
        for room in std::mem::take(&mut self.needs_anchor) {
            if let Some(r) = self.server.get_mut(room) {
                let anchor = room_protocol::encode_envelope(&room_protocol::OpEnvelope::main(
                    room_protocol::RoomOp::Checkpoint,
                ));
                match r.local_append(&anchor) {
                    Ok(outs) => {
                        for o in outs {
                            self.route(room, o);
                        }
                    }
                    Err(e) => warn!(room = %hex::encode(room), "anchor append failed: {e:?}"),
                }
            }
        }

        let ids: Vec<RoomId> = self.server.rooms().map(|(id, _)| *id).collect();
        for id in ids {
            if let Some(r) = self.server.get_mut(id) {
                // Lane 3: hand newly finalized state to the materialiser.
                let deltas = r.poll_finalized();
                if !self.mat_live {
                    if let Some(last) = deltas.last() {
                        let job = MatJob { room: id, version: last.version, state: r.snapshot_finalized().clone() };
                        if let Err(e) = self.mat_tx.try_send(job) {
                            debug!("materialiser busy, dropping update (coalesced later): {e}");
                        }
                    }
                }
                // Lane 1: push the live view to local subscribers on any change.
                let activity = r.last_activity();
                if self.last_activity.insert(id, activity) != Some(activity) {
                    let live = Arc::new(r.snapshot_live().clone());
                    if self.mat_live {
                        let job =
                            MatJob { room: id, version: live.ops_applied, state: (*live).clone() };
                        if let Err(e) = self.mat_tx.try_send(job) {
                            debug!("materialiser busy, dropping update (coalesced later): {e}");
                        }
                    }
                    let _ = self.events.send(RoomEvent {
                        room: id,
                        live,
                        finalized_len: r.finalized_len(),
                    });
                }
            }
            if tick % ANNOUNCE_EVERY_TICKS == 0 {
                self.announce(id);
            }
        }
    }

    fn announce(&self, room: RoomId) {
        if let Some(r) = self.server.get(room) {
            for o in r.announce() {
                self.route(room, o);
            }
        }
    }
}
