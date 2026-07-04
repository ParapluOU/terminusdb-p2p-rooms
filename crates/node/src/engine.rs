//! The room engine: one tokio task owning the [`RoomServer`] and the roomnet
//! [`IrohTransport`], driven by commands from the HTTP layer and the presence
//! layer.
//!
//! This mirrors `roomnet::run_server` but adds what a serving node needs on
//! top of a headless replica: snapshot queries, live-state push events for
//! websocket subscribers, auto-replication of rooms it hears about on the
//! wire, and a channel of finalized states for the TerminusDB materialiser.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use room_protocol::{RoomProjection, RoomState};
use roomnet::{Inbound, IrohTransport, MemStoreFactory, Origin, RoomId, RoomServer};
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

pub enum EngineCmd {
    Host { room: RoomId, reply: oneshot::Sender<bool> },
    Join { room: RoomId, origin: Option<NodeKey>, reply: oneshot::Sender<bool> },
    Append { room: RoomId, payload: Vec<u8>, reply: oneshot::Sender<bool> },
    Snapshot { room: RoomId, reply: oneshot::Sender<Option<Snapshot>> },
    List { reply: oneshot::Sender<Vec<RoomSummary>> },
    AddPeer { peer: NodeKey },
}

/// Cheap-to-clone handle used by the HTTP and presence layers.
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<EngineCmd>,
    events: broadcast::Sender<RoomEvent>,
    pub node_key: NodeKey,
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

    pub async fn snapshot(&self, room: RoomId) -> Option<Snapshot> {
        self.ask(|reply| EngineCmd::Snapshot { room, reply }).await.flatten()
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
    mat_tx: mpsc::Sender<MatJob>,
    /// Replicate any room we see frames or announcements for.
    auto_replicate: bool,
    last_activity: HashMap<RoomId, u64>,
}

impl Engine {
    pub fn new(
        server: Server,
        transport: IrohTransport,
        mat_tx: mpsc::Sender<MatJob>,
        auto_replicate: bool,
    ) -> (Self, EngineHandle) {
        let node_key = transport.endpoint_id();
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (events, _) = broadcast::channel(256);
        let engine = Self {
            server,
            transport,
            cmd_rx,
            events: events.clone(),
            mat_tx,
            auto_replicate,
            last_activity: HashMap::new(),
        };
        (engine, EngineHandle { tx: cmd_tx, events, node_key })
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
                                self.transport.dispatch(room, o);
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
            EngineCmd::Snapshot { room, reply } => {
                let snap = self.server.get(room).map(|r| Snapshot {
                    live: r.snapshot_live().clone(),
                    finalized: r.snapshot_finalized().clone(),
                    finalized_len: r.finalized_len(),
                });
                let _ = reply.send(snap);
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

    fn on_tick(&mut self, tick: u64) {
        // Pump inbound sync frames into their rooms; in trustless mode a frame
        // for an unknown room is an invitation to replicate it.
        for Inbound { room, from, msg } in self.transport.drain_inbound() {
            self.transport.add_peer(from);
            if self.server.get(room).is_none() {
                if !self.auto_replicate {
                    continue;
                }
                if self.server.join_remote(room).is_ok() {
                    debug!(room = %hex::encode(room), "auto-replicating room heard on the wire");
                }
            }
            if let Some(r) = self.server.get_mut(room) {
                match r.on_inbound(from, msg) {
                    Ok(outs) => {
                        for o in outs {
                            self.transport.dispatch(room, o);
                        }
                    }
                    Err(e) => warn!(room = %hex::encode(room), "inbound failed: {e:?}"),
                }
            }
        }

        let ids: Vec<RoomId> = self.server.rooms().map(|(id, _)| *id).collect();
        for id in ids {
            if let Some(r) = self.server.get_mut(id) {
                // Lane 3: hand newly finalized state to the materialiser.
                let deltas = r.poll_finalized();
                if let Some(last) = deltas.last() {
                    let job = MatJob { room: id, version: last.version, state: r.snapshot_finalized().clone() };
                    if let Err(e) = self.mat_tx.try_send(job) {
                        debug!("materialiser busy, dropping update (coalesced later): {e}");
                    }
                }
                // Lane 1: push the live view to local subscribers on any change.
                let activity = r.last_activity();
                if self.last_activity.insert(id, activity) != Some(activity) {
                    let _ = self.events.send(RoomEvent {
                        room: id,
                        live: Arc::new(r.snapshot_live().clone()),
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
                self.transport.dispatch(room, o);
            }
        }
    }
}
