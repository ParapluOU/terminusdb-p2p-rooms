//! The deterministic fold: room state as a pure function of the linearized
//! op stream.
//!
//! Everything here must be a pure function of prior state and `(node, payload)`
//! — no clocks, no randomness, no iteration over unordered maps — so that every
//! replica folding the same autobase order lands on the same state. That is
//! also what makes the state safe to hand to a `ProjectionSink` for
//! materialisation into TerminusDB.

use std::collections::BTreeMap;

use autobase::NodeId;
use serde::{Deserialize, Serialize};

use crate::op::{decode_envelope, RoomOp, MAIN_BRANCH};

/// A chat message inside one branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Hex-encoded writer key of the author (the signed hypercore identity).
    pub author: String,
    /// Self-reported display name carried on the message (browser clients of
    /// one node share its writer key, so this is the per-client attribution).
    pub nick: Option<String>,
    pub text: String,
    /// Position in the room's linearized op stream when this was applied;
    /// gives a stable, replica-independent ordering handle.
    pub at_op: u64,
}

/// A todo item inside one branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TodoItem {
    pub id: String,
    pub title: String,
    pub done: bool,
    pub author: String,
    pub created_at_op: u64,
}

/// A node in the virtual filesystem of one branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FsNode {
    Dir,
    File { content: String, author: String, modified_at_op: u64 },
}

/// State of one branch: all three example-app surfaces folded side by side.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchState {
    /// Branch this one was forked from (`None` for `main`).
    pub forked_from: Option<String>,
    /// Value of `RoomState::ops_applied` at the fork point.
    pub forked_at_op: u64,
    pub chat: Vec<ChatMessage>,
    /// Keyed by client-chosen item id.
    pub todos: BTreeMap<String, TodoItem>,
    /// Keyed by normalized absolute path ("/a/b").
    pub files: BTreeMap<String, FsNode>,
}

/// The full folded state of a room across all of its branches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomState {
    pub title: Option<String>,
    /// UI hint set at room creation: "chat" | "todo" | "fs".
    pub app: Option<String>,
    /// Writer key (hex) → display name; room-global across branches.
    pub nicks: BTreeMap<String, String>,
    pub branches: BTreeMap<String, BranchState>,
    /// Total ops applied (including skipped ones), i.e. the version of this fold.
    pub ops_applied: u64,
}

impl Default for RoomState {
    fn default() -> Self {
        let mut branches = BTreeMap::new();
        branches.insert(MAIN_BRANCH.to_string(), BranchState::default());
        Self { title: None, app: None, nicks: BTreeMap::new(), branches, ops_applied: 0 }
    }
}

impl RoomState {
    /// Fold one linearized log entry into the state.
    ///
    /// Invalid payloads (undecodable, unknown branch, duplicate branch name,
    /// bad path, …) are skipped, but still count towards `ops_applied` so op
    /// positions stay stable.
    pub fn apply_payload(&mut self, node: NodeId, payload: &[u8]) {
        let at_op = self.ops_applied;
        self.ops_applied += 1;
        let Some(env) = decode_envelope(payload) else { return };
        let author = hex::encode(node.key);

        // Branch ops address the room's branch set; everything else addresses
        // the branch named by the envelope's branch pointer.
        match env.op {
            RoomOp::SetTitle { title } => self.title = Some(title),
            RoomOp::SetApp { app } => self.app = Some(app),
            RoomOp::Checkpoint => {}
            RoomOp::ChatSetNick { nick } => {
                self.nicks.insert(author, nick);
            }
            RoomOp::BranchCreate { name } => {
                if !valid_branch_name(&name) || self.branches.contains_key(&name) {
                    return;
                }
                let Some(source) = self.branches.get(&env.branch) else { return };
                let mut forked = source.clone();
                forked.forked_from = Some(env.branch);
                forked.forked_at_op = at_op;
                self.branches.insert(name, forked);
            }
            op => {
                let Some(branch) = self.branches.get_mut(&env.branch) else { return };
                branch.apply(op, author, at_op);
            }
        }
    }

    pub fn branch_names(&self) -> Vec<&str> {
        self.branches.keys().map(String::as_str).collect()
    }
}

impl BranchState {
    fn apply(&mut self, op: RoomOp, author: String, at_op: u64) {
        match op {
            RoomOp::ChatPost { text, nick } => {
                self.chat.push(ChatMessage { author, nick, text, at_op });
            }
            RoomOp::TodoAdd { id, title } => {
                // First writer wins: a concurrent add with the same id does
                // not clobber the earlier one in the linearized order.
                self.todos.entry(id.clone()).or_insert(TodoItem {
                    id,
                    title,
                    done: false,
                    author,
                    created_at_op: at_op,
                });
            }
            RoomOp::TodoSetDone { id, done } => {
                if let Some(item) = self.todos.get_mut(&id) {
                    item.done = done;
                }
            }
            RoomOp::TodoRetitle { id, title } => {
                if let Some(item) = self.todos.get_mut(&id) {
                    item.title = title;
                }
            }
            RoomOp::TodoRemove { id } => {
                self.todos.remove(&id);
            }
            RoomOp::FsWrite { path, content } => {
                if let Some(path) = normalize_path(&path) {
                    self.ensure_parent_dirs(&path);
                    self.files.insert(path, FsNode::File { content, author, modified_at_op: at_op });
                }
            }
            RoomOp::FsMkdir { path } => {
                if let Some(path) = normalize_path(&path) {
                    self.ensure_parent_dirs(&path);
                    self.files.entry(path).or_insert(FsNode::Dir);
                }
            }
            RoomOp::FsRemove { path } => {
                if let Some(path) = normalize_path(&path) {
                    // Remove the node and everything beneath it.
                    let prefix = format!("{path}/");
                    self.files.retain(|p, _| p != &path && !p.starts_with(&prefix));
                }
            }
            RoomOp::FsMove { from, to } => {
                let (Some(from), Some(to)) = (normalize_path(&from), normalize_path(&to)) else {
                    return;
                };
                if from == to || to.starts_with(&format!("{from}/")) || self.files.contains_key(&to)
                {
                    return;
                }
                let prefix = format!("{from}/");
                let moved: Vec<(String, FsNode)> = self
                    .files
                    .iter()
                    .filter(|(p, _)| *p == &from || p.starts_with(&prefix))
                    .map(|(p, n)| (format!("{to}{}", &p[from.len()..]), n.clone()))
                    .collect();
                if moved.is_empty() {
                    return;
                }
                self.files.retain(|p, _| p != &from && !p.starts_with(&prefix));
                self.ensure_parent_dirs(&to);
                self.files.extend(moved);
            }
            // Room-level ops are handled by RoomState::apply_payload.
            RoomOp::SetTitle { .. }
            | RoomOp::SetApp { .. }
            | RoomOp::Checkpoint
            | RoomOp::ChatSetNick { .. }
            | RoomOp::BranchCreate { .. } => {}
        }
    }

    fn ensure_parent_dirs(&mut self, path: &str) {
        let mut cur = String::new();
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        for seg in &segments[..segments.len().saturating_sub(1)] {
            cur.push('/');
            cur.push_str(seg);
            self.files.entry(cur.clone()).or_insert(FsNode::Dir);
        }
    }
}

/// Branch names become TerminusDB branch names and URL path segments, so keep
/// them to a safe alphabet.
pub fn valid_branch_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Normalize to "/a/b" form; reject traversal and empty segments.
fn normalize_path(path: &str) -> Option<String> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() || segments.iter().any(|s| *s == "." || *s == "..") {
        return None;
    }
    Some(format!("/{}", segments.join("/")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::{encode_envelope, OpEnvelope};

    fn node(key_byte: u8, seq: u64) -> NodeId {
        NodeId::new([key_byte; 32], seq)
    }

    fn apply(state: &mut RoomState, key: u8, seq: u64, branch: &str, op: RoomOp) {
        state.apply_payload(node(key, seq), &encode_envelope(&OpEnvelope::new(branch, op)));
    }

    #[test]
    fn chat_folds_in_order() {
        let mut s = RoomState::default();
        apply(&mut s, 1, 0, "main", RoomOp::ChatSetNick { nick: "alice".into() });
        apply(&mut s, 1, 1, "main", RoomOp::ChatPost { text: "hello".into(), nick: None });
        apply(&mut s, 2, 0, "main", RoomOp::ChatPost { text: "hi".into(), nick: None });
        assert_eq!(s.nicks.get(&hex::encode([1u8; 32])).unwrap(), "alice");
        let chat = &s.branches["main"].chat;
        assert_eq!(chat.len(), 2);
        assert_eq!(chat[0].text, "hello");
        assert_eq!(chat[1].at_op, 2);
    }

    #[test]
    fn branch_create_forks_state_and_diverges() {
        let mut s = RoomState::default();
        apply(&mut s, 1, 0, "main", RoomOp::TodoAdd { id: "t1".into(), title: "shared".into() });
        apply(&mut s, 1, 1, "main", RoomOp::BranchCreate { name: "dev".into() });
        apply(&mut s, 1, 2, "dev", RoomOp::TodoAdd { id: "t2".into(), title: "dev only".into() });
        apply(&mut s, 1, 3, "main", RoomOp::TodoSetDone { id: "t1".into(), done: true });

        let main = &s.branches["main"];
        let dev = &s.branches["dev"];
        assert_eq!(dev.forked_from.as_deref(), Some("main"));
        assert!(dev.todos.contains_key("t1"), "fork carries state over");
        assert!(dev.todos.contains_key("t2"));
        assert!(!main.todos.contains_key("t2"), "main unaffected by dev op");
        assert!(main.todos["t1"].done);
        assert!(!dev.todos["t1"].done, "post-fork main op does not leak into dev");
    }

    #[test]
    fn branch_create_ignores_duplicates_and_bad_sources() {
        let mut s = RoomState::default();
        apply(&mut s, 1, 0, "nope", RoomOp::BranchCreate { name: "dev".into() });
        assert!(!s.branches.contains_key("dev"), "unknown source branch ignored");
        apply(&mut s, 1, 1, "main", RoomOp::BranchCreate { name: "dev".into() });
        apply(&mut s, 1, 2, "dev", RoomOp::ChatPost { text: "x".into(), nick: None });
        apply(&mut s, 2, 0, "main", RoomOp::BranchCreate { name: "dev".into() });
        assert_eq!(s.branches["dev"].chat.len(), 1, "duplicate create does not reset the branch");
        apply(&mut s, 1, 3, "main", RoomOp::BranchCreate { name: "bad name!".into() });
        assert_eq!(s.branches.len(), 2);
    }

    #[test]
    fn ops_on_unknown_branch_are_skipped_but_counted() {
        let mut s = RoomState::default();
        apply(&mut s, 1, 0, "ghost", RoomOp::ChatPost { text: "x".into(), nick: None });
        assert_eq!(s.branches["main"].chat.len(), 0);
        assert_eq!(s.ops_applied, 1);
    }

    #[test]
    fn fs_write_creates_parents_and_move_is_recursive() {
        let mut s = RoomState::default();
        apply(&mut s, 1, 0, "main", RoomOp::FsWrite { path: "src/lib.rs".into(), content: "x".into() });
        apply(&mut s, 1, 1, "main", RoomOp::FsMove { from: "/src".into(), to: "/lib".into() });
        let files = &s.branches["main"].files;
        assert!(matches!(files.get("/lib"), Some(FsNode::Dir)));
        assert!(matches!(files.get("/lib/lib.rs"), Some(FsNode::File { .. })));
        assert!(files.get("/src").is_none());

        let mut s2 = RoomState::default();
        apply(&mut s2, 1, 0, "main", RoomOp::FsWrite { path: "../evil".into(), content: "x".into() });
        assert!(s2.branches["main"].files.is_empty(), "traversal rejected");
    }

    #[test]
    fn fs_remove_is_recursive() {
        let mut s = RoomState::default();
        apply(&mut s, 1, 0, "main", RoomOp::FsWrite { path: "/a/b/c.txt".into(), content: "x".into() });
        apply(&mut s, 1, 1, "main", RoomOp::FsRemove { path: "/a".into() });
        assert!(s.branches["main"].files.is_empty());
    }
}
