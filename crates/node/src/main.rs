//! tdb-room-node — one node of the trustless TerminusDB p2p rooms network.
//!
//! Responsibilities:
//!   1. host/replicate branchable rooms (autobase multiwriter logs) over iroh
//!   2. materialise finalized room state into TerminusDB (db per room,
//!      TerminusDB branch per room branch)
//!   3. announce itself on an iroh-gossip presence topic and replicate rooms
//!      announced by other nodes, so every room is reachable via every host
//!   4. serve the example frontends + JSON API, and act as the WebRTC
//!      signalling server so browser clients can exchange edits p2p

// The terminusdb-client trait tower is deep; async fn layout blows the
// default query depth limit.
#![recursion_limit = "256"]

mod engine;
mod http;
mod materialise;
mod presence;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use identity::SecretKey;
use room_protocol::RoomProjection;
use roomnet::{IrohConfig, IrohTransport, RoomServer, ServerConfig};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use crate::engine::Engine;
use crate::materialise::{MatState, MaterialiserConfig};
use crate::presence::{parse_endpoint_id, parse_key, Presence, PresenceConfig};

const SYNC_ALPN: &[u8] = b"terminusdb-p2p-rooms/sync/1";

#[derive(Parser, Debug)]
#[command(name = "tdb-room-node", about = "TerminusDB p2p rooms node")]
struct Args {
    /// Human-readable node name, shown in host dropdowns.
    #[arg(long, default_value = "node")]
    name: String,

    /// 32-byte hex identity seed. Drives the roomnet writer key AND the
    /// gossip endpoint id (derived). Random when omitted (fresh identity per run).
    #[arg(long)]
    seed: Option<String>,

    /// Fixed UDP port for the roomnet iroh endpoint (ephemeral when omitted).
    #[arg(long)]
    iroh_port: Option<u16>,

    /// HTTP listen address (API, frontends, websocket signalling).
    #[arg(long, default_value = "127.0.0.1:8080")]
    http: SocketAddr,

    /// Public base URL browsers should use for this node (announced on the
    /// presence topic). Defaults to http://<http-listen-addr>.
    #[arg(long)]
    public_url: Option<String>,

    /// Gossip endpoint ids (hex, comma-separated) of nodes to bootstrap the
    /// presence swarm from. Printed in every node's startup banner.
    #[arg(long, value_delimiter = ',')]
    bootstrap: Vec<String>,

    /// Extra indexer writer keys (hex, comma-separated). All nodes of a
    /// deployment should agree on the same indexer set; this node's own key
    /// is always included.
    #[arg(long, value_delimiter = ',')]
    indexers: Vec<String>,

    /// Replicate every room announced by other nodes (default). Disable to
    /// only serve rooms created or joined explicitly through the API.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    replicate: bool,

    /// Directory with the example frontends.
    #[arg(long, default_value = "frontend")]
    frontend_dir: String,

    /// External TerminusDB endpoint to materialise into.
    #[arg(long, default_value = "http://localhost:6363")]
    tdb_url: String,
    #[arg(long, default_value = "admin")]
    tdb_user: String,
    #[arg(long, default_value = "root")]
    tdb_pass: String,
    #[arg(long, default_value = "admin")]
    tdb_org: String,

    /// Disable TerminusDB materialisation entirely (rooms stay in memory).
    #[arg(long)]
    no_tdb: bool,

    /// Which projection to materialise into TerminusDB. "finalized" waits
    /// for indexer quorum; "live" writes the optimistic view (recommended
    /// while browser wasm writers can't reach quorum — see README).
    #[arg(long, default_value = "finalized", value_parser = ["finalized", "live"])]
    materialise: String,

    /// Start an embedded TerminusDB server instead of connecting to
    /// --tdb-url. Requires building with `--features embedded-tdb`.
    #[arg(long)]
    embedded_tdb: bool,

    /// Data directory (embedded TerminusDB store).
    #[arg(long, default_value = ".data")]
    data_dir: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,iroh=warn,iroh_gossip=warn")),
        )
        .init();
    #[allow(unused_mut)] // mutated only when the embedded-tdb feature is on
    let mut args = Args::parse();

    // --- identity -----------------------------------------------------------
    let seed: [u8; 32] = match &args.seed {
        Some(s) => parse_key(s).context("--seed must be 32 bytes of hex")?,
        None => rand::random(),
    };
    let secret = SecretKey::from_seed(&seed);
    let writer_key = secret.public().to_bytes();
    // The gossip endpoint needs its own key pair (the roomnet transport owns
    // the identity endpoint), derived so one --seed pins both.
    let gossip_seed = blake3::derive_key("terminusdb-p2p-rooms/gossip/v1", &seed);

    // --- embedded TerminusDB (optional feature) ------------------------------
    #[allow(unused_mut)]
    let mut _embedded_guard: Option<Box<dyn std::any::Any>> = None;
    if args.embedded_tdb {
        #[cfg(feature = "embedded-tdb")]
        {
            let server = terminusdb_bin::start_server(terminusdb_bin::ServerOptions {
                memory: false,
                db_path: Some(args.data_dir.join("terminusdb")),
                quiet: true,
                ..Default::default()
            })
            .await
            .context("start embedded TerminusDB")?;
            args.tdb_url = format!("http://127.0.0.1:{}", server.port());
            tracing::info!(url = %args.tdb_url, "embedded TerminusDB started");
            // Keep the handle alive for the process lifetime (Drop kills it).
            _embedded_guard = Some(Box::new(server));
        }
        #[cfg(not(feature = "embedded-tdb"))]
        anyhow::bail!(
            "--embedded-tdb requires a build with `cargo build --features embedded-tdb` \
             (compiles TerminusDB from source; needs swipl, make, protoc, gmp)"
        );
    }

    // --- roomnet transport + engine ------------------------------------------
    let iroh_cfg = IrohConfig {
        alpn: SYNC_ALPN.to_vec(),
        seed,
        bind_port: args.iroh_port,
        ..Default::default()
    };
    let transport = IrohTransport::bind(&iroh_cfg)
        .await
        .map_err(|e| anyhow::anyhow!("bind roomnet endpoint: {e}"))?;

    let mut indexers = vec![writer_key];
    for extra in &args.indexers {
        let key = parse_key(extra).context("--indexers entries must be 32-byte hex keys")?;
        if !indexers.contains(&key) {
            indexers.push(key);
        }
    }
    let server = RoomServer::open(
        ServerConfig { identity_seed: seed, indexers: indexers.clone(), replica_stale_after: None },
        RoomProjection::default(),
    );

    let (mat_tx, mat_rx) = mpsc::channel(256);
    let (engine, handle) = Engine::new(
        server,
        transport,
        mat_tx,
        args.replicate,
        args.materialise == "live",
        indexers,
    );
    tokio::spawn(engine.run());

    // --- TerminusDB materialiser ---------------------------------------------
    let mat_state = Arc::new(MatState::default());
    tokio::spawn(materialise::run(
        MaterialiserConfig {
            url: args.tdb_url.clone(),
            user: args.tdb_user.clone(),
            pass: args.tdb_pass.clone(),
            org: args.tdb_org.clone(),
            enabled: !args.no_tdb,
        },
        mat_rx,
        mat_state.clone(),
    ));

    // --- presence gossip -------------------------------------------------------
    let public_url = args.public_url.clone().or_else(|| Some(format!("http://{}", args.http)));
    let bootstrap = args
        .bootstrap
        .iter()
        .map(|s| parse_endpoint_id(s))
        .collect::<Result<Vec<_>>>()
        .context("--bootstrap entries must be 32-byte hex gossip ids")?;
    let presence = Presence::bind(PresenceConfig {
        node_name: args.name.clone(),
        gossip_seed,
        bootstrap,
        http_url: public_url.clone(),
        replicate: args.replicate,
    })
    .await?;
    let directory = presence.directory.clone();
    let gossip_id = presence.gossip_id;
    {
        let handle = handle.clone();
        tokio::spawn(async move {
            if let Err(e) = presence.run(handle).await {
                tracing::error!("presence task failed: {e:#}");
            }
        });
    }

    // --- HTTP -----------------------------------------------------------------
    let app_state = http::AppState {
        engine: handle,
        directory,
        mat: mat_state,
        hub: Arc::new(http::SignalHub::default()),
        meta: http::NodeMeta {
            name: args.name.clone(),
            roomnet_id: hex::encode(writer_key),
            gossip_id: gossip_id.to_string(),
            http_url: public_url,
            tdb_url: args.tdb_url.clone(),
            tdb_user: args.tdb_user.clone(),
            tdb_pass: args.tdb_pass.clone(),
            tdb_org: args.tdb_org.clone(),
        },
    };
    let router = http::router(app_state, &args.frontend_dir);
    let listener = tokio::net::TcpListener::bind(args.http).await.context("bind http")?;

    println!("┌─ tdb-room-node ─────────────────────────────────────────────");
    println!("│ name        {}", args.name);
    println!("│ http        http://{}", args.http);
    println!("│ roomnet id  {}", hex::encode(writer_key));
    println!("│ gossip id   {gossip_id}");
    println!("│ terminusdb  {}", if args.no_tdb { "disabled".into() } else { args.tdb_url.clone() });
    println!("│");
    println!("│ join this network from another machine:");
    println!("│   tdb-room-node --name other --bootstrap {gossip_id}");
    println!("└─────────────────────────────────────────────────────────────");

    axum::serve(listener, router).await.context("http server")?;
    Ok(())
}
