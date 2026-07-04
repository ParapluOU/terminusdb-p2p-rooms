//! Op vocabulary and wire encoding of the room payload.
//!
//! Encoding is versioned JSON so browser clients can produce and consume the
//! exact bytes that end up in the hypercore ledger. A log is immutable history
//! forever, so decoding is tolerant: unknown versions and unknown op kinds are
//! skipped, never fatal.

use serde::{Deserialize, Serialize};

/// Current envelope version. Bump when the layout changes; decoders keep
/// accepting old versions.
pub const ENVELOPE_VERSION: u32 = 1;

/// The branch every room starts with.
pub const MAIN_BRANCH: &str = "main";

/// What actually rides inside a hypercore `Entry.payload`.
///
/// `branch` is the branch pointer that the raw hypercore ledger lacks: app ops
/// apply to that branch, and `branch.create` forks *from* it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpEnvelope {
    /// Envelope version, for permanent-history tolerance.
    pub v: u32,
    /// Branch pointer: the branch this op targets.
    #[serde(default = "default_branch")]
    pub branch: String,
    pub op: RoomOp,
}

fn default_branch() -> String {
    MAIN_BRANCH.to_string()
}

impl OpEnvelope {
    pub fn new(branch: impl Into<String>, op: RoomOp) -> Self {
        Self { v: ENVELOPE_VERSION, branch: branch.into(), op }
    }

    pub fn main(op: RoomOp) -> Self {
        Self::new(MAIN_BRANCH, op)
    }
}

/// The flat op vocabulary shared by all example apps.
///
/// A room is not typed to one app: every branch folds all three app states
/// (chat / todo / fs), and `meta.set_app` is only a UI hint. Kinds are dotted
/// strings so browser clients can emit them as plain JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum RoomOp {
    // -- room metadata ------------------------------------------------------
    #[serde(rename = "meta.set_title")]
    SetTitle { title: String },
    /// UI hint: which example front-end this room was created for
    /// ("chat" | "todo" | "fs").
    #[serde(rename = "meta.set_app")]
    SetApp { app: String },
    /// A no-op anchor. An indexer node appends one after ingesting entries
    /// from non-indexer writers (e.g. browser wasm clients): the anchor
    /// causally references those entries, which is what lets them reach
    /// quorum finality. Folds to nothing.
    #[serde(rename = "meta.checkpoint")]
    Checkpoint,

    // -- branching ----------------------------------------------------------
    /// Fork a new branch named `name` from the envelope's `branch` pointer,
    /// carrying over its full state at this point in the linearized history.
    #[serde(rename = "branch.create")]
    BranchCreate { name: String },

    // -- chat ---------------------------------------------------------------
    /// Author display name; room-global (not per-branch), keyed by writer key.
    #[serde(rename = "chat.set_nick")]
    ChatSetNick { nick: String },
    /// `nick` rides on the message because browser clients of the same node
    /// share that node's writer key — per-client attribution can't come from
    /// the signature alone.
    #[serde(rename = "chat.post")]
    ChatPost {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nick: Option<String>,
    },

    // -- collaborative todo list --------------------------------------------
    /// `id` is chosen by the client (e.g. a random hex string) so concurrent
    /// adds never collide and later ops can address the item.
    #[serde(rename = "todo.add")]
    TodoAdd { id: String, title: String },
    #[serde(rename = "todo.set_done")]
    TodoSetDone { id: String, done: bool },
    #[serde(rename = "todo.retitle")]
    TodoRetitle { id: String, title: String },
    #[serde(rename = "todo.remove")]
    TodoRemove { id: String },

    // -- virtual filesystem ---------------------------------------------------
    #[serde(rename = "fs.write")]
    FsWrite { path: String, content: String },
    #[serde(rename = "fs.mkdir")]
    FsMkdir { path: String },
    #[serde(rename = "fs.remove")]
    FsRemove { path: String },
    #[serde(rename = "fs.move")]
    FsMove { from: String, to: String },
}

/// Encode an envelope to the bytes appended to the hypercore.
pub fn encode_envelope(env: &OpEnvelope) -> Vec<u8> {
    serde_json::to_vec(env).expect("op envelope serializes")
}

/// Decode payload bytes. `None` means "not a payload this version
/// understands" — callers must skip it deterministically.
pub fn decode_envelope(payload: &[u8]) -> Option<OpEnvelope> {
    let env: OpEnvelope = serde_json::from_slice(payload).ok()?;
    (env.v <= ENVELOPE_VERSION).then_some(env)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let env = OpEnvelope::new("dev", RoomOp::ChatPost { text: "hi".into(), nick: None });
        let bytes = encode_envelope(&env);
        assert_eq!(decode_envelope(&bytes), Some(env));
    }

    #[test]
    fn json_shape_is_js_friendly() {
        let env = OpEnvelope::main(RoomOp::TodoAdd { id: "a1".into(), title: "milk".into() });
        let v: serde_json::Value = serde_json::from_slice(&encode_envelope(&env)).unwrap();
        assert_eq!(v["branch"], "main");
        assert_eq!(v["op"]["kind"], "todo.add");
        assert_eq!(v["op"]["title"], "milk");
    }

    #[test]
    fn garbage_and_future_versions_are_skipped() {
        assert_eq!(decode_envelope(b"not json"), None);
        let future = serde_json::json!({"v": 999, "branch": "main", "op": {"kind": "chat.post", "text": "x"}});
        assert_eq!(decode_envelope(future.to_string().as_bytes()), None);
        let unknown_kind = serde_json::json!({"v": 1, "branch": "main", "op": {"kind": "nope"}});
        assert_eq!(decode_envelope(unknown_kind.to_string().as_bytes()), None);
    }

    #[test]
    fn missing_branch_defaults_to_main() {
        let raw = serde_json::json!({"v": 1, "op": {"kind": "chat.post", "text": "x"}});
        let env = decode_envelope(raw.to_string().as_bytes()).unwrap();
        assert_eq!(env.branch, MAIN_BRANCH);
    }
}
