//! # room-protocol — the L2 payload of a branchable room
//!
//! roomnet's L1 log is domain-agnostic: a hypercore `Entry` carries only its
//! causal `heads` and an opaque `payload`. The hypercore ledger has **no
//! notion of a branch**, so this crate adds one at the payload layer: every
//! op rides inside an [`OpEnvelope`] whose `branch` field is the branch
//! pointer the op applies to (and, for `branch.create`, forks from).
//!
//! The deterministic fold over the autobase-linearized op stream lives in
//! [`state::RoomState`]; [`RoomProjection`] adapts it to roomnet's
//! [`Projection`](roomnet::Projection) trait so a node can materialise the
//! finalized state — e.g. into a TerminusDB database per room, with a
//! TerminusDB branch per room branch.

pub mod op;
pub mod state;

pub use op::{decode_envelope, encode_envelope, OpEnvelope, RoomOp, ENVELOPE_VERSION, MAIN_BRANCH};
pub use state::{valid_branch_name, BranchState, ChatMessage, FsNode, RoomState, TodoItem};

use autobase::NodeId;

/// Adapter: [`RoomState`]'s fold as a roomnet [`Projection`](roomnet::Projection).
///
/// `apply` never fails: an undecodable or invalid payload is deterministically
/// ignored. In a trustless room any writer can append garbage; poisoning the
/// projection must not wedge replication, and every honest replica must skip
/// the same ops to converge on the same state.
#[derive(Clone, Debug, Default)]
pub struct RoomProjection {
    state: RoomState,
}

impl roomnet::Projection for RoomProjection {
    type State = RoomState;
    type Error = core::convert::Infallible;

    fn apply(&mut self, node: NodeId, payload: &[u8]) -> Result<(), Self::Error> {
        self.state.apply_payload(node, payload);
        Ok(())
    }

    fn snapshot(&self) -> &Self::State {
        &self.state
    }

    fn reset_to(&mut self, checkpoint: &Self::State) {
        self.state = checkpoint.clone();
    }
}
