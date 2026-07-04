//! Two Room replicas exchanging SyncMessages by hand (roomnet's sans-IO seam)
//! must converge on the same folded RoomState, including branch forks.
//!
//! Substrate note: at the current hypercore-rs revision, `Room::local_append`
//! passes `Linearizer::tails()` (the DAG *roots*) as an entry's causal heads,
//! so cross-writer ordering degenerates to the writer-key tiebreak: all of the
//! smaller key's entries linearize before the larger key's. Convergence and
//! determinism still hold — but "I replied after seeing your message" is not
//! reflected in the order. To keep assertions deterministic, the test gives
//! the "early" role (title, first post, the branch fork) to whichever writer
//! key sorts first.

use identity::SecretKey;
use room_protocol::{encode_envelope, OpEnvelope, RoomOp, RoomProjection, RoomState};
use roomnet::{Fanout, MemStoreFactory, Outbound, PeerId, Room, RoomConfig};

type TestRoom = Room<MemStoreFactory, RoomProjection>;

fn open_room(seed: u8, indexers: Vec<[u8; 32]>) -> (TestRoom, PeerId) {
    let secret = SecretKey::from_seed(&[seed; 32]);
    let key = secret.public().to_bytes();
    let room = Room::open(RoomConfig::original(secret, indexers), MemStoreFactory, RoomProjection::default())
        .expect("room opens");
    (room, key)
}

/// Deliver every network-bound Outbound to the other replica until both queues
/// drain. `Fanout::Clients` outbounds are local push notifications, not network
/// traffic, so they are dropped here. `from_lo` marks which side produced the
/// messages.
fn pump(
    lo: &mut TestRoom,
    lo_id: PeerId,
    hi: &mut TestRoom,
    hi_id: PeerId,
    mut pending: Vec<(bool, Vec<Outbound>)>,
) {
    while let Some((from_lo, outs)) = pending.pop() {
        for out in outs {
            match out.to {
                Fanout::Clients => continue,
                Fanout::Gossip | Fanout::Peer(_) => {
                    let (target, source) = if from_lo { (&mut *hi, lo_id) } else { (&mut *lo, hi_id) };
                    let more = target.on_inbound(source, out.msg).expect("inbound ok");
                    pending.push((!from_lo, more));
                }
            }
        }
    }
}

fn append(room: &mut TestRoom, branch: &str, op: RoomOp) -> Vec<Outbound> {
    room.local_append(&encode_envelope(&OpEnvelope::new(branch, op))).expect("append ok")
}

#[test]
fn two_writers_converge_across_branches() {
    let k1 = SecretKey::from_seed(&[1; 32]).public().to_bytes();
    let k2 = SecretKey::from_seed(&[2; 32]).public().to_bytes();
    // "lo" linearizes first (smaller writer key), "hi" second.
    let (lo_seed, hi_seed) = if k1 <= k2 { (1u8, 2u8) } else { (2u8, 1u8) };
    let indexers = vec![k1, k2];
    let (mut lo, lo_id) = open_room(lo_seed, indexers.clone());
    let (mut hi, hi_id) = open_room(hi_seed, indexers);

    // Round 1: lo sets up the room and forks a branch off main.
    let outs = vec![
        (true, append(&mut lo, "main", RoomOp::SetTitle { title: "demo".into() })),
        (true, append(&mut lo, "main", RoomOp::ChatPost { text: "first post".into(), nick: None })),
        (true, append(&mut lo, "main", RoomOp::BranchCreate { name: "experiment".into() })),
        (true, append(&mut lo, "experiment", RoomOp::ChatPost { text: "lo on branch".into(), nick: None })),
    ];
    pump(&mut lo, lo_id, &mut hi, hi_id, outs);

    // Round 2: hi, having replicated the fork, writes to both branches.
    let outs = vec![
        (false, append(&mut hi, "main", RoomOp::ChatPost { text: "hi on main".into(), nick: None })),
        (false, append(&mut hi, "experiment", RoomOp::ChatPost { text: "hi on branch".into(), nick: None })),
    ];
    pump(&mut lo, lo_id, &mut hi, hi_id, outs);

    let a: &RoomState = lo.snapshot_live();
    let b: &RoomState = hi.snapshot_live();
    assert_eq!(a, b, "replicas converge on identical folded state");

    assert_eq!(a.title.as_deref(), Some("demo"));
    assert_eq!(a.branch_names(), vec!["experiment", "main"]);
    let main = &a.branches["main"];
    let exp = &a.branches["experiment"];
    assert_eq!(exp.forked_from.as_deref(), Some("main"));

    let main_texts: Vec<&str> = main.chat.iter().map(|m| m.text.as_str()).collect();
    let exp_texts: Vec<&str> = exp.chat.iter().map(|m| m.text.as_str()).collect();
    assert_eq!(main_texts, vec!["first post", "hi on main"]);
    // The fork carried main's pre-fork chat, then collected both branch posts.
    assert_eq!(exp_texts, vec!["first post", "lo on branch", "hi on branch"]);

    // Both writers are indexers; after another full exchange a prefix reaches
    // quorum and the finalized views converge too.
    let outs = vec![(true, append(&mut lo, "main", RoomOp::ChatPost { text: "seal".into(), nick: None }))];
    pump(&mut lo, lo_id, &mut hi, hi_id, outs);
    let outs = vec![(false, append(&mut hi, "main", RoomOp::ChatPost { text: "seal2".into(), nick: None }))];
    pump(&mut lo, lo_id, &mut hi, hi_id, outs);

    assert!(lo.finalized_len() > 0, "quorum finalizes a prefix");
    assert_eq!(lo.snapshot_finalized(), hi.snapshot_finalized());
}
