//! TerminusDB materialisation: the ProjectionSink side of the demo.
//!
//! The engine hands this task finalized room states (Lane 3). Each room maps
//! to its own TerminusDB database (`room_<idprefix>`), and each room branch
//! maps to a TerminusDB branch of that database, forked from the same parent
//! the room branch was forked from. Materialisation is idempotent and
//! diff-based: on each finalized version we upsert changed documents and
//! delete removed ones, so the TerminusDB commit history on every branch
//! tracks the room's finalized history.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use room_protocol::{BranchState, FsNode, MAIN_BRANCH};
use roomnet::RoomId;
use serde::Serialize;
use terminusdb_client::{BranchSpec, DocumentInsertArgs, TerminusDBHttpClient};
use terminusdb_schema::{EntityIDFor, ToTDBInstance};
use terminusdb_schema_derive::{FromTDBInstance, TerminusDBModel};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use crate::engine::MatJob;

// --- the TerminusDB shape of a materialised room ---------------------------

/// Singleton per branch: the room header at this branch's tip.
#[derive(Debug, Clone, Default, PartialEq, TerminusDBModel, FromTDBInstance)]
#[tdb(id_field = "id")]
pub struct RoomMeta {
    pub id: EntityIDFor<Self>,
    pub room: String,
    pub title: String,
    pub app: String,
    pub branch: String,
    pub forked_from: String,
    pub version: i32,
}

#[derive(Debug, Clone, Default, PartialEq, TerminusDBModel, FromTDBInstance)]
#[tdb(id_field = "id")]
pub struct ChatMessageDoc {
    pub id: EntityIDFor<Self>,
    pub author: String,
    pub nick: String,
    pub text: String,
    pub at_op: i32,
}

#[derive(Debug, Clone, Default, PartialEq, TerminusDBModel, FromTDBInstance)]
#[tdb(id_field = "id")]
pub struct TodoItemDoc {
    pub id: EntityIDFor<Self>,
    pub key: String,
    pub title: String,
    pub done: bool,
    pub author: String,
    pub created_at_op: i32,
}

#[derive(Debug, Clone, Default, PartialEq, TerminusDBModel, FromTDBInstance)]
#[tdb(id_field = "id")]
pub struct FsNodeDoc {
    pub id: EntityIDFor<Self>,
    pub path: String,
    pub is_dir: bool,
    pub content: String,
    pub author: String,
    pub modified_at_op: i32,
}

// --- status shared with the HTTP layer -------------------------------------

#[derive(Clone, Debug, Default, Serialize)]
pub struct MatRoomStatus {
    pub db: String,
    pub last_version: u64,
    pub branches: Vec<String>,
    pub last_error: Option<String>,
}

#[derive(Default)]
pub struct MatState {
    pub connected: RwLock<bool>,
    pub endpoint: RwLock<String>,
    pub rooms: RwLock<HashMap<String, MatRoomStatus>>,
}

pub type SharedMatState = Arc<MatState>;

pub struct MaterialiserConfig {
    pub url: String,
    pub user: String,
    pub pass: String,
    pub org: String,
    pub enabled: bool,
}

/// TerminusDB database name for a room id.
pub fn room_db_name(room: &RoomId) -> String {
    format!("room_{}", &hex::encode(room)[..16])
}

pub async fn run(
    cfg: MaterialiserConfig,
    mut rx: mpsc::Receiver<MatJob>,
    state: SharedMatState,
) {
    *state.endpoint.write().await = cfg.url.clone();
    if !cfg.enabled {
        info!("materialiser disabled (--no-tdb): finalized states stay in memory only");
        while rx.recv().await.is_some() {}
        return;
    }

    let mut mat = Materialiser {
        cfg,
        client: None,
        state,
        ensured_dbs: HashSet::new(),
        known_branches: HashMap::new(),
        written: HashMap::new(),
    };

    while let Some(first) = rx.recv().await {
        // Coalesce: under load only the newest finalized state per room matters.
        let mut latest: HashMap<RoomId, MatJob> = HashMap::new();
        latest.insert(first.room, first);
        while let Ok(job) = rx.try_recv() {
            latest.insert(job.room, job);
        }
        for (_, job) in latest {
            mat.process(job).await;
        }
    }
}

struct Materialiser {
    cfg: MaterialiserConfig,
    client: Option<TerminusDBHttpClient>,
    state: SharedMatState,
    ensured_dbs: HashSet<String>,
    /// db → branches known to exist in TerminusDB.
    known_branches: HashMap<String, HashSet<String>>,
    /// (db, branch) → branch state as last written, for diffing.
    written: HashMap<(String, String), BranchState>,
}

impl Materialiser {
    async fn process(&mut self, job: MatJob) {
        let room_hex = hex::encode(job.room);
        let db = room_db_name(&job.room);
        let result = self.sync_room(&db, &job).await;
        let failed = result.is_err();
        {
            let mut rooms = self.state.rooms.write().await;
            let entry = rooms.entry(room_hex).or_default();
            entry.db = db;
            match result {
                Ok(()) => {
                    entry.last_version = job.version;
                    entry.branches = job.state.branches.keys().cloned().collect();
                    entry.last_error = None;
                }
                Err(e) => {
                    warn!("materialisation failed: {e:#}");
                    entry.last_error = Some(format!("{e:#}"));
                    // Force a reconnect check on the next job.
                    self.client = None;
                }
            }
        }
        if failed {
            *self.state.connected.write().await = false;
        }
    }

    async fn connect(&mut self) -> Result<TerminusDBHttpClient> {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }
        let url = url::Url::parse(&self.cfg.url).context("parse --tdb-url")?;
        let client =
            TerminusDBHttpClient::new(url, &self.cfg.user, &self.cfg.pass, &self.cfg.org)
                .await
                .context("connect to TerminusDB")?;
        client.info().await.context("TerminusDB not reachable")?;
        info!(url = %self.cfg.url, "materialiser: connected to TerminusDB");
        *self.state.connected.write().await = true;
        self.client = Some(client.clone());
        Ok(client)
    }

    async fn sync_room(&mut self, db: &str, job: &MatJob) -> Result<()> {
        let client = self.connect().await?;

        if !self.ensured_dbs.contains(db) {
            client.ensure_database(db).await.context("ensure database")?;
            let args = DocumentInsertArgs::from(BranchSpec::with_branch(db, MAIN_BRANCH));
            client
                .insert_schemas::<(RoomMeta, ChatMessageDoc, TodoItemDoc, FsNodeDoc)>(args)
                .await
                .context("insert schemas")?;
            self.ensured_dbs.insert(db.to_string());
            self.known_branches
                .entry(db.to_string())
                .or_default()
                .insert(MAIN_BRANCH.to_string());
        }

        // Create TerminusDB branches for room branches we haven't seen yet,
        // forking from the same parent branch the room forked from.
        let known = self.known_branches.entry(db.to_string()).or_default();
        for (name, branch) in &job.state.branches {
            if known.contains(name) {
                continue;
            }
            let parent = branch.forked_from.as_deref().unwrap_or(MAIN_BRANCH);
            let path = format!("{}/{}/local/branch/{}", self.cfg.org, db, name);
            let origin = format!("{}/{}/local/branch/{}", self.cfg.org, db, parent);
            match client.create_branch(&path, &origin).await {
                Ok(_) => info!(db, branch = name, parent, "created TerminusDB branch"),
                // Tolerate "already exists" (e.g. node restarted with a fresh cache).
                Err(e) => debug!(db, branch = name, "create_branch: {e:#} (may already exist)"),
            }
            known.insert(name.clone());
        }

        for (name, branch) in &job.state.branches {
            self.sync_branch(&client, db, name, branch, job).await?;
            self.written.insert((db.to_string(), name.clone()), branch.clone());
        }
        Ok(())
    }

    async fn sync_branch(
        &self,
        client: &TerminusDBHttpClient,
        db: &str,
        name: &str,
        branch: &BranchState,
        job: &MatJob,
    ) -> Result<()> {
        let empty = BranchState::default();
        let prev = self.written.get(&(db.to_string(), name.to_string())).unwrap_or(&empty);
        if prev == branch && job.version > 1 {
            return Ok(());
        }

        let spec = BranchSpec::with_branch(db, name);
        let mut args = DocumentInsertArgs::from(spec);
        args.author = "tdb-room-node".to_string();
        args.message = format!("materialise room {} v{}", hex::encode(job.room), job.version);

        // Room header (always upserted so `version` tracks finality).
        let meta = RoomMeta {
            id: EntityIDFor::new("meta").context("meta id")?,
            room: hex::encode(job.room),
            title: job.state.title.clone().unwrap_or_default(),
            app: job.state.app.clone().unwrap_or_default(),
            branch: name.to_string(),
            forked_from: branch.forked_from.clone().unwrap_or_default(),
            version: job.version as i32,
        };
        client.save_instance(&meta, args.clone()).await.context("save room meta")?;

        // Chat is append-only: write only the tail beyond what we last wrote.
        for msg in branch.chat.iter().skip(prev.chat.len()) {
            let doc = ChatMessageDoc {
                id: EntityIDFor::new(&format!("chat_{}", msg.at_op)).context("chat id")?,
                author: msg.author.clone(),
                nick: msg.nick.clone().unwrap_or_default(),
                text: msg.text.clone(),
                at_op: msg.at_op as i32,
            };
            client.save_instance(&doc, args.clone()).await.context("save chat message")?;
        }

        // Todos: upsert new/changed, delete removed.
        for (key, item) in &branch.todos {
            if prev.todos.get(key) == Some(item) {
                continue;
            }
            let doc = TodoItemDoc {
                id: EntityIDFor::new(&format!("todo_{}", hex::encode(key))).context("todo id")?,
                key: key.clone(),
                title: item.title.clone(),
                done: item.done,
                author: item.author.clone(),
                created_at_op: item.created_at_op as i32,
            };
            client.save_instance(&doc, args.clone()).await.context("save todo")?;
        }
        for key in prev.todos.keys() {
            if !branch.todos.contains_key(key) {
                let id = format!("todo_{}", hex::encode(key));
                client
                    .delete_instance_by_id::<TodoItemDoc>(&id, args.clone(), Default::default())
                    .await
                    .context("delete todo")?;
            }
        }

        // Filesystem: upsert new/changed nodes, delete removed paths.
        for (path, node) in &branch.files {
            if prev.files.get(path) == Some(node) {
                continue;
            }
            let (is_dir, content, author, modified) = match node {
                FsNode::Dir => (true, String::new(), String::new(), 0),
                FsNode::File { content, author, modified_at_op } => {
                    (false, content.clone(), author.clone(), *modified_at_op as i32)
                }
            };
            let doc = FsNodeDoc {
                id: EntityIDFor::new(&format!("fs_{}", hex::encode(path))).context("fs id")?,
                path: path.clone(),
                is_dir,
                content,
                author,
                modified_at_op: modified,
            };
            client.save_instance(&doc, args.clone()).await.context("save fs node")?;
        }
        for path in prev.files.keys() {
            if !branch.files.contains_key(path) {
                let id = format!("fs_{}", hex::encode(path));
                client
                    .delete_instance_by_id::<FsNodeDoc>(&id, args.clone(), Default::default())
                    .await
                    .context("delete fs node")?;
            }
        }

        debug!(db, branch = name, version = job.version, "branch materialised");
        Ok(())
    }
}

/// Verify materialisation from the HTTP layer: live doc counts per branch.
pub async fn branch_doc_counts(
    cfg_url: &str,
    user: &str,
    pass: &str,
    org: &str,
    db: &str,
    branch: &str,
) -> Result<serde_json::Value> {
    let client = TerminusDBHttpClient::new(url::Url::parse(cfg_url)?, user, pass, org).await?;
    let spec = BranchSpec::with_branch(db, branch);
    let chat = client.count_instances::<ChatMessageDoc>(&spec).await.unwrap_or(0);
    let todos = client.count_instances::<TodoItemDoc>(&spec).await.unwrap_or(0);
    let files = client.count_instances::<FsNodeDoc>(&spec).await.unwrap_or(0);
    Ok(serde_json::json!({ "chat": chat, "todos": todos, "files": files }))
}
