//! Node presence over iroh-gossip.
//!
//! Every node subscribes to one well-known gossip topic and periodically
//! announces itself: who it is (roomnet + gossip endpoint ids), where its
//! HTTP API lives (so browsers can pick it as a host), and which rooms it
//! serves. Nodes that hear an announcement add the sender as a roomnet peer
//! and — in replicate mode — join every advertised room, which is what makes
//! the same rooms reachable through every host in the network.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use iroh::endpoint::presets::N0;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointId, SecretKey};
use iroh_gossip::api::Event;
use iroh_gossip::{Gossip, TopicId};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::engine::EngineHandle;

/// All nodes of the demo network meet on this topic.
const PRESENCE_TOPIC: &str = "terminusdb-p2p-rooms/presence/v1";
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5);
const PEER_EXPIRY: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoomAd {
    pub id: String,
    pub title: Option<String>,
    pub app: Option<String>,
}

/// One node's presence announcement (also the /api/hosts entry format).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostInfo {
    pub name: String,
    /// Gossip endpoint id (hex) — what `--bootstrap` takes.
    pub gossip_id: String,
    /// Roomnet endpoint id == autobase writer key (hex).
    pub roomnet_id: String,
    /// URL where browsers can reach this node's HTTP API + frontends.
    pub http_url: Option<String>,
    pub rooms: Vec<RoomAd>,
    pub seq: u64,
}

#[derive(Default)]
pub struct Directory {
    /// gossip_id → (last announcement, last heard).
    peers: RwLock<HashMap<String, (HostInfo, Instant)>>,
}

impl Directory {
    pub async fn hosts(&self) -> Vec<HostInfo> {
        let peers = self.peers.read().await;
        let mut hosts: Vec<HostInfo> = peers
            .values()
            .filter(|(_, seen)| seen.elapsed() < PEER_EXPIRY)
            .map(|(info, _)| info.clone())
            .collect();
        hosts.sort_by(|a, b| a.gossip_id.cmp(&b.gossip_id));
        hosts
    }
}

pub struct PresenceConfig {
    pub node_name: String,
    pub gossip_seed: [u8; 32],
    pub bootstrap: Vec<EndpointId>,
    pub http_url: Option<String>,
    pub replicate: bool,
}

pub struct Presence {
    pub directory: Arc<Directory>,
    pub gossip_id: EndpointId,
    _router: Router,
    gossip: Gossip,
    cfg: PresenceConfig,
}

impl Presence {
    /// Bind the presence endpoint (separate from the roomnet endpoint: the
    /// roomnet transport owns its own iroh Router, so gossip gets its own).
    pub async fn bind(cfg: PresenceConfig) -> Result<Self> {
        let secret = SecretKey::from_bytes(&cfg.gossip_seed);
        let endpoint = Endpoint::builder(N0)
            .secret_key(secret)
            .bind()
            .await
            .map_err(|e| anyhow::anyhow!("bind gossip endpoint: {e}"))?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let gossip_id = endpoint.id();
        Ok(Self { directory: Arc::new(Directory::default()), gossip_id, _router: router, gossip, cfg })
    }

    /// Run the announce/listen loop until the process exits.
    pub async fn run(self, engine: EngineHandle) -> Result<()> {
        let topic = TopicId::from_bytes(*blake3::hash(PRESENCE_TOPIC.as_bytes()).as_bytes());
        let (sender, mut receiver) = self
            .gossip
            .subscribe(topic, self.cfg.bootstrap.clone())
            .await
            .map_err(|e| anyhow::anyhow!("subscribe presence topic: {e}"))?
            .split();
        info!(
            gossip_id = %self.gossip_id,
            bootstrap = self.cfg.bootstrap.len(),
            "presence: subscribed to {PRESENCE_TOPIC}"
        );

        let roomnet_id = hex::encode(engine.node_key);
        let mut seq = 0u64;
        let mut announce = tokio::time::interval(ANNOUNCE_INTERVAL);
        loop {
            tokio::select! {
                _ = announce.tick() => {
                    seq += 1;
                    let rooms = engine
                        .list()
                        .await
                        .into_iter()
                        .map(|r| RoomAd { id: r.id, title: r.title, app: r.app })
                        .collect();
                    let info = HostInfo {
                        name: self.cfg.node_name.clone(),
                        gossip_id: self.gossip_id.to_string(),
                        roomnet_id: roomnet_id.clone(),
                        http_url: self.cfg.http_url.clone(),
                        rooms,
                        seq,
                    };
                    let bytes = serde_json::to_vec(&info).expect("host info serializes");
                    if let Err(e) = sender.broadcast(bytes.into()).await {
                        debug!("presence broadcast failed (no neighbors yet?): {e}");
                    }
                    self.directory.peers.write().await.retain(|_, (_, seen)| seen.elapsed() < PEER_EXPIRY);
                }
                event = receiver.next() => match event {
                    Some(Ok(Event::Received(msg))) => {
                        match serde_json::from_slice::<HostInfo>(&msg.content) {
                            Ok(info) => self.on_announcement(&engine, info).await,
                            Err(e) => debug!("presence: ignoring undecodable announcement: {e}"),
                        }
                    }
                    Some(Ok(other)) => debug!("presence event: {other:?}"),
                    Some(Err(e)) => warn!("presence receiver error: {e}"),
                    None => break,
                }
            }
        }
        Ok(())
    }

    async fn on_announcement(&self, engine: &EngineHandle, info: HostInfo) {
        if info.gossip_id == self.gossip_id.to_string() {
            return;
        }
        let Ok(roomnet_key) = parse_key(&info.roomnet_id) else {
            return;
        };
        let known_rooms: Vec<String> =
            engine.list().await.into_iter().map(|r| r.id).collect();

        engine.add_peer(roomnet_key).await;
        if self.cfg.replicate {
            for room in &info.rooms {
                if known_rooms.contains(&room.id) {
                    continue;
                }
                if let Ok(id) = parse_key(&room.id) {
                    info!(room = %room.id, from = %info.name, "replicating room from presence announcement");
                    engine.join(id, Some(roomnet_key)).await;
                }
            }
        }

        let fresh = {
            let mut peers = self.directory.peers.write().await;
            let prev = peers.insert(info.gossip_id.clone(), (info.clone(), Instant::now()));
            prev.is_none()
        };
        if fresh {
            info!(name = %info.name, gossip_id = %info.gossip_id, "presence: new node in the network");
        }
    }
}

pub fn parse_key(hex_str: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(hex_str).context("invalid hex")?;
    let arr: [u8; 32] = bytes.as_slice().try_into().context("expected 32 bytes")?;
    Ok(arr)
}

pub fn parse_endpoint_id(hex_str: &str) -> Result<EndpointId> {
    let key = parse_key(hex_str)?;
    EndpointId::from_bytes(&key).context("invalid endpoint id")
}
