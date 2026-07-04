//! Two Room replicas exchanging SyncMessages by hand (roomnet's sans-IO seam)
//! must converge on the same folded RoomState, including branch forks — and,
//! since hypercore-rs links appends against the DAG frontier, an op appended
//! after syncing linearizes after everything its writer had seen, regardless
//! of writer-key order.

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
/// traffic, so they are dropped here. `true` marks messages produced by alice.
fn pump(
    alice: &mut TestRoom,
    alice_id: PeerId,
    bob: &mut TestRoom,
    bob_id: PeerId,
    mut pending: Vec<(bool, Vec<Outbound>)>,
) {
    while let Some((from_alice, outs)) = pending.pop() {
        for out in outs {
            match out.to {
                Fanout::Clients => continue,
                Fanout::Gossip | Fanout::Peer(_) => {
                    let (target, source) =
                        if from_alice { (&mut *bob, alice_id) } else { (&mut *alice, bob_id) };
                    let more = target.on_inbound(source, out.msg).expect("inbound ok");
                    pending.push((!from_alice, more));
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
    let alice_key = SecretKey::from_seed(&[1; 32]).public().to_bytes();
    let bob_key = SecretKey::from_seed(&[2; 32]).public().to_bytes();
    let indexers = vec![alice_key, bob_key];
    let (mut alice, a_id) = open_room(1, indexers.clone());
    let (mut bob, b_id) = open_room(2, indexers);

    // Round 1: alice sets up the room.
    let outs = vec![
        (true, append(&mut alice, "main", RoomOp::SetTitle { title: "demo".into() })),
        (true, append(&mut alice, "main", RoomOp::ChatPost { text: "first post".into(), nick: None })),
    ];
    pump(&mut alice, a_id, &mut bob, b_id, outs);

    // Round 2: bob — having seen alice's history — forks a branch and posts
    // to it. Causal linking guarantees his ops linearize after what he saw,
    // so the fork must carry alice's chat even though bob is another writer.
    let outs = vec![
        (false, append(&mut bob, "main", RoomOp::BranchCreate { name: "experiment".into() })),
        (false, append(&mut bob, "experiment", RoomOp::ChatPost { text: "bob on branch".into(), nick: None })),
    ];
    pump(&mut alice, a_id, &mut bob, b_id, outs);

    // Round 3: alice keeps talking on main only.
    let outs = vec![
        (true, append(&mut alice, "main", RoomOp::ChatPost { text: "alice still on main".into(), nick: None })),
    ];
    pump(&mut alice, a_id, &mut bob, b_id, outs);

    let a: &RoomState = alice.snapshot_live();
    let b: &RoomState = bob.snapshot_live();
    assert_eq!(a, b, "replicas converge on identical folded state");

    assert_eq!(a.title.as_deref(), Some("demo"));
    assert_eq!(a.branch_names(), vec!["experiment", "main"]);
    let main = &a.branches["main"];
    let exp = &a.branches["experiment"];
    assert_eq!(exp.forked_from.as_deref(), Some("main"));

    let main_texts: Vec<&str> = main.chat.iter().map(|m| m.text.as_str()).collect();
    let exp_texts: Vec<&str> = exp.chat.iter().map(|m| m.text.as_str()).collect();
    assert_eq!(main_texts, vec!["first post", "alice still on main"]);
    // Bob's fork carried the chat he had seen; his branch post follows it.
    assert_eq!(exp_texts, vec!["first post", "bob on branch"]);

    // Both writers are indexers and have referenced each other's entries, so
    // a prefix reaches quorum and the finalized views converge too.
    let outs = vec![(true, append(&mut alice, "main", RoomOp::ChatPost { text: "seal".into(), nick: None }))];
    pump(&mut alice, a_id, &mut bob, b_id, outs);
    let outs = vec![(false, append(&mut bob, "main", RoomOp::ChatPost { text: "seal2".into(), nick: None }))];
    pump(&mut alice, a_id, &mut bob, b_id, outs);

    assert!(alice.finalized_len() > 0, "quorum finalizes a prefix");
    assert_eq!(alice.snapshot_finalized(), bob.snapshot_finalized());
    let fin = alice.snapshot_finalized();
    assert!(
        fin.branches["main"].chat.iter().any(|m| m.text == "first post"),
        "finalized view includes the settled history"
    );
}
