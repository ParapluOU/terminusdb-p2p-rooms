//! Browser-side room client.
//!
//! This is a full roomnet replica compiled to `wasm32-unknown-unknown`: the
//! browser holds its own ed25519 writer identity, appends signed entries to
//! its own hypercore, linearizes every writer it hears about, and folds the
//! room state with the exact same `room-protocol` code the nodes run.
//!
//! JavaScript never sees hypercore internals. The API surface is:
//!   frames in  → [`WasmRoom::on_frame`]   (opaque bytes from ws / WebRTC)
//!   frames out ← returned as `[{to, frame}]` for JS to ship verbatim
//!   ops in     → [`WasmRoom::append`]     (the app's JSON op + branch pointer)
//!   state out  ← [`WasmRoom::live_json`] / [`WasmRoom::finalized_json`]

use identity::SecretKey;
use room_protocol::{encode_envelope, OpEnvelope, RoomOp, RoomProjection};
use roomnet::{wire, Fanout, MemStoreFactory, Outbound, Room, RoomConfig};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmRoom {
    room: Room<MemStoreFactory, RoomProjection>,
    writer_key: [u8; 32],
}

/// Serialize outbound sync messages for JS to ship. `to` is `"gossip"`,
/// `"clients"`, or the hex writer key of a specific peer; `frame` is the
/// hex-encoded roomnet wire frame (opaque to JS).
fn frames_json(outs: Vec<Outbound>) -> String {
    let arr: Vec<serde_json::Value> = outs
        .into_iter()
        .map(|o| {
            let to = match o.to {
                Fanout::Gossip => "gossip".to_string(),
                Fanout::Clients => "clients".to_string(),
                Fanout::Peer(p) => hex::encode(p),
            };
            serde_json::json!({ "to": to, "frame": hex::encode(wire::encode(&o.msg)) })
        })
        .collect();
    serde_json::to_string(&arr).expect("frames serialize")
}

#[wasm_bindgen]
impl WasmRoom {
    /// Open a replica of a room.
    ///
    /// `seed` — 32 random bytes (e.g. `crypto.getRandomValues`); derives the
    /// client's writer identity. `indexers_json` — JSON array of hex writer
    /// keys (from the node's `/api/info`) so finality matches the network's.
    #[wasm_bindgen(constructor)]
    pub fn new(seed: &[u8], indexers_json: &str) -> Result<WasmRoom, JsError> {
        let seed: [u8; 32] =
            seed.try_into().map_err(|_| JsError::new("seed must be 32 bytes"))?;
        let indexer_hexes: Vec<String> = serde_json::from_str(indexers_json)
            .map_err(|e| JsError::new(&format!("indexers_json: {e}")))?;
        let mut indexers = Vec::new();
        for h in indexer_hexes {
            let bytes = hex::decode(&h).map_err(|e| JsError::new(&format!("indexer key: {e}")))?;
            let key: [u8; 32] =
                bytes.as_slice().try_into().map_err(|_| JsError::new("indexer key must be 32 bytes"))?;
            indexers.push(key);
        }
        let secret = SecretKey::from_seed(&seed);
        let writer_key = secret.public().to_bytes();
        let room = Room::open(
            RoomConfig::original(secret, indexers),
            MemStoreFactory,
            RoomProjection::default(),
        )
        .map_err(|e| JsError::new(&format!("open room: {e:?}")))?;
        Ok(WasmRoom { room, writer_key })
    }

    /// This client's writer key (hex) — also sent to the node as the ws
    /// `writer` binding, and shared with WebRTC peers for frame routing.
    pub fn writer_key(&self) -> String {
        hex::encode(self.writer_key)
    }

    /// Append an op to this client's own hypercore. `op_json` is the same op
    /// shape the HTTP API takes (`{"kind": "chat.post", ...}`); `branch` is
    /// the branch pointer. Returns frames for JS to ship.
    pub fn append(&mut self, branch: &str, op_json: &str) -> Result<String, JsError> {
        let op: RoomOp =
            serde_json::from_str(op_json).map_err(|e| JsError::new(&format!("op: {e}")))?;
        let payload = encode_envelope(&OpEnvelope::new(branch, op));
        let outs = self
            .room
            .local_append(&payload)
            .map_err(|e| JsError::new(&format!("append: {e:?}")))?;
        Ok(frames_json(outs))
    }

    /// Feed one inbound wire frame (from the node websocket or a WebRTC
    /// peer). `from_hex` is the sender's writer key. Returns reply frames.
    pub fn on_frame(&mut self, from_hex: &str, frame: &[u8]) -> Result<String, JsError> {
        let from = hex::decode(from_hex)
            .ok()
            .and_then(|b| <[u8; 32]>::try_from(b.as_slice()).ok())
            .ok_or_else(|| JsError::new("from must be a 32-byte hex key"))?;
        let msg = wire::decode(frame).map_err(|e| JsError::new(&format!("bad frame: {e:?}")))?;
        let outs = self
            .room
            .on_inbound(from, msg)
            .map_err(|e| JsError::new(&format!("inbound: {e:?}")))?;
        Ok(frames_json(outs))
    }

    /// Advertise our head (send on connect / to newly joined WebRTC peers).
    pub fn announce(&self) -> String {
        frames_json(self.room.announce())
    }

    /// The optimistic live projection (finalized + unconfirmed tail), as JSON.
    pub fn live_json(&self) -> String {
        serde_json::to_string(self.room.snapshot_live()).expect("state serializes")
    }

    /// The quorum-finalized projection, as JSON.
    pub fn finalized_json(&self) -> String {
        serde_json::to_string(self.room.snapshot_finalized()).expect("state serializes")
    }

    pub fn finalized_len(&self) -> usize {
        self.room.finalized_len()
    }

    /// A monotone counter that changes whenever the replica ingests anything —
    /// cheap change detection for the UI.
    pub fn activity(&self) -> u64 {
        self.room.last_activity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The wasm surface is also a plain Rust API; exercise the full loop the
    // browser drives: two client replicas syncing through returned frames.
    #[test]
    fn two_wasm_rooms_converge_via_frames() {
        let indexers = format!(
            "[\"{}\"]",
            hex::encode(SecretKey::from_seed(&[9; 32]).public().to_bytes())
        );
        let mut a = WasmRoom::new(&[9u8; 32], &indexers).unwrap();
        let mut b = WasmRoom::new(&[8u8; 32], &indexers).unwrap();

        let outs = a.append("main", r#"{"kind":"chat.post","text":"from a"}"#).unwrap();
        let frames: Vec<serde_json::Value> = serde_json::from_str(&outs).unwrap();
        // Ship every network-bound frame to b, and b's replies back to a.
        let mut queue: Vec<(bool, Vec<serde_json::Value>)> = vec![(true, frames)];
        while let Some((from_a, frames)) = queue.pop() {
            for f in frames {
                if f["to"] == "clients" {
                    continue;
                }
                let bytes = hex::decode(f["frame"].as_str().unwrap()).unwrap();
                let (target, source) =
                    if from_a { (&mut b, a.writer_key()) } else { (&mut a, b.writer_key()) };
                let replies = target.on_frame(&source, &bytes).unwrap();
                queue.push((!from_a, serde_json::from_str(&replies).unwrap()));
            }
        }

        assert_eq!(a.live_json(), b.live_json());
        assert!(b.live_json().contains("from a"));
    }
}
