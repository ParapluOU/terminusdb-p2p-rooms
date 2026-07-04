# terminusdb-p2p-rooms

Demo of a **trustless network of TerminusDB nodes serving branchable rooms**:
hypercore/autobase multiwriter logs replicated between nodes over
[iroh](https://iroh.computer) 1.x, materialised into TerminusDB (one database
per room, one TerminusDB branch per room branch), discoverable through an
iroh-gossip presence channel, and edited live from the browser — where the
nodes double as WebRTC signalling servers so clients also exchange their edits
peer-to-peer.

Built on two ParapluOU substrates, pulled in as git dependencies:

| repo | crates used | role |
|------|-------------|------|
| [ParapluOU/hypercore-rs](https://github.com/ParapluOU/hypercore-rs) | `roomnet`, `autobase`, `identity` | L1: signed append-only logs, causal linearization, room replication over iroh QUIC |
| [ParapluOU/terminusdb-rs](https://github.com/ParapluOU/terminusdb-rs) | `terminusdb-client`, `terminusdb-schema(-derive)`, `terminusdb-bin` (opt-in) | L3: the materialised, queryable, *natively branchable* database |

---

## The idea

A **room** is an autobase-ordered multiwriter hypercore log. Every
participating node appends signed entries to its own writer log; autobase
linearizes the causal DAG deterministically, so every replica folds the same
op stream into the same state — no coordinator, no trusted server. roomnet
(hypercore-rs) provides exactly this L1 substrate, plus rolling projections
and an iroh transport.

This demo adds the three layers above it:

```
┌──────────────────────────────────────────────────────────────────────────┐
│  browser clients        chat.html · todo.html · fs.html                  │
│    · pick ANY host (host dropdown ← presence directory)                  │
│    · pick/fork a branch (branch dropdown ← branch pointers in the log)   │
│    · ops go p2p over WebRTC data channels (node = signalling server)     │
│      AND to the node, which anchors them into the hypercore              │
└───────────────▲───────────────────────────────▲──────────────────────────┘
                │ http + websocket + webrtc sig │
┌───────────────┴───────────┐     ┌─────────────┴─────────────┐
│  tdb-room-node "alpha"    │     │  tdb-room-node "beta"     │   … more
│                           │     │                           │
│  rooms: autobase logs ◄───┼─────┼──► same rooms, replicated │  iroh QUIC
│  presence: iroh-gossip ◄──┼─────┼──► node discovery topic   │  (pubsub)
│                           │     │                           │
│  ProjectionSink           │     │  ProjectionSink           │
│      ▼                    │     │      ▼                    │
│  TerminusDB               │     │  TerminusDB               │
│    db  = room_<id>        │     │    db  = room_<id>        │
│    branch per room branch │     │    branch per room branch │
└───────────────────────────┘     └───────────────────────────┘
```

- **L1 — transport/causality** (`roomnet`/`autobase`): signed logs, causal
  refs, deterministic linearization, quorum finality. Domain-blind.
- **L2 — room protocol** (`crates/room-protocol`, this repo): what the opaque
  payload bytes *mean*. Op vocabulary for the three example apps **plus the
  branch pointer** — see below. A pure, deterministic fold implements
  roomnet's `Projection`.
- **L3 — materialisation** (`crates/node`): each *finalized* room version is
  written into TerminusDB through the `ProjectionSink` seam, giving you a
  queryable, versioned, branch-aware view of every room.

### Trust model

Nodes don't trust each other. Every log entry is ed25519-signed by its writer
and verified against Merkle proofs on ingest (L1). Ordering is causal +
deterministic tiebreak — never timestamps, so nodes can't reorder history by
lying about time. Finality is quorum-based: entries are *finalized* (and only
then materialised into TerminusDB) once a majority of the configured indexer
set has confirmed them. A malicious writer can only append garbage under its
own key; the L2 fold deterministically skips invalid ops on every replica, so
it cannot wedge replication or fork the state.

---

## The branch pointer

The hypercore ledger itself has **no notion of a branch** — an `Entry` carries
only causal `heads` and an opaque `payload`. So the branch pointer lives in
the payload envelope that every op rides in (L2):

```json
{ "v": 1, "branch": "experiment", "op": { "kind": "chat.post", "text": "hi" } }
```

- `branch` — the branch this op applies to (defaults to `main`).
- `{"kind": "branch.create", "name": "experiment"}` — forks a new branch
  *from* the envelope's `branch` pointer, carrying over its full state at that
  point of the linearized history. From then on the two branches diverge.

Because branch ops are ordinary log entries, branches replicate with the room,
are signed by their author, converge on every replica, and are visible to the
materialiser — which mirrors them as **real TerminusDB branches**
(`terminusdb-client`'s `create_branch`, forked from the same parent). One log,
many branches; every branch a first-class TerminusDB branch.

The fold is intentionally simple (fork = copy-on-create, no merge op yet);
TerminusDB's `rebase`/`squash` machinery is the natural target for a future
`branch.merge` op.

---

## What's in the workspace

```
crates/
├── room-protocol/      # L2: op vocabulary, branch pointer envelope,
│   │                   #     deterministic fold (RoomState), RoomProjection
│   ├── src/op.rs       #     versioned JSON payload codec (browser-friendly)
│   ├── src/state.rs    #     branches × {chat, todos, files} fold
│   └── tests/          #     two-replica convergence via roomnet's sans-IO seam
└── node/               # the daemon: tdb-room-node
    ├── src/engine.rs   # room engine loop (RoomServer + IrohTransport):
    │                   #   commands, live-state events, auto-replication,
    │                   #   finalized states → materialiser
    ├── src/presence.rs # iroh-gossip presence topic: announce/discover nodes,
    │                   #   auto-join announced rooms
    ├── src/materialise.rs # TerminusDB sink: db per room, branch per branch,
    │                   #   diff-based upserts/deletes, typed schema models
    ├── src/http.rs     # JSON API + websocket (state push + WebRTC signalling)
    └── src/main.rs     # CLI wiring
frontend/               # static, no build step
├── index.html          # room browser: host dropdown, create/join rooms
├── chat.html           # example app 1: chat
├── todo.html           # example app 2: collaborative todo list
├── fs.html             # example app 3: virtual filesystem (git-repo-like)
└── js/client.js        # shared client: ws state sync, WebRTC mesh,
                        #   optimistic overlay (JS mirror of the fold)
```

Every app page has the two dropdowns the network is about:

- **host** — any node from the presence directory; the same room is served by
  all of them, so switching hosts keeps the room (and its branches) intact.
- **branch** — switch between branches or fork a new one from the current one.

---

## Running it

Requires nightly Rust (pinned via `rust-toolchain.toml`; the terminusdb-rs
crates use nightly features) and, for cross-node traffic, ordinary outbound
internet access (iroh's default n0 discovery + relay infrastructure).

### One node, no TerminusDB (quickest look)

```sh
cargo run -p tdb-room-node -- --name solo --no-tdb
# open http://127.0.0.1:8080
```

### One node + TerminusDB materialisation

```sh
# any running TerminusDB works; the stock docker image is easiest:
docker run -d -p 6363:6363 terminusdb/terminusdb-server

cargo run -p tdb-room-node -- --name solo
# open http://127.0.0.1:8080 — the footer badge shows db + version once
# finalized ops start landing in TerminusDB
```

Materialisation is verifiable per room at `/api/rooms/<id>/tdb` (database
name, last materialised version, live per-branch document counts), or directly
in TerminusDB: database `room_<first-16-hex-of-room-id>`, branches mirroring
the room's branches, documents `RoomMeta`, `ChatMessageDoc`, `TodoItemDoc`,
`FsNodeDoc`.

### A network of nodes

```sh
# node 1
cargo run -p tdb-room-node -- --name alpha --http 127.0.0.1:8080
# ── prints its gossip id, e.g. dfcf6c15…

# node 2 (any machine)
cargo run -p tdb-room-node -- --name beta --http 127.0.0.1:8081 \
    --bootstrap <alpha-gossip-id>
```

Nodes announce themselves every 5s on the shared iroh-gossip topic
(`terminusdb-p2p-rooms/presence/v1`): name, HTTP URL, endpoint ids, and the
rooms they serve. Every node (with the default `--replicate true`) joins every
room it hears about, so **any room is reachable through any host** — that's
what the frontend's host dropdown switches between. Each node materialises
into *its own* TerminusDB, so the network also gives you N independent,
consistent TerminusDB mirrors of every room.

For stable identities across restarts pass `--seed <64-hex>`; for shared
finality across the network give all nodes the same `--indexers` set (each
node always includes itself).

### Embedded TerminusDB (optional)

```sh
cargo run -p tdb-room-node --features embedded-tdb -- --name solo --embedded-tdb
```

Uses `terminusdb-bin`'s `TerminusDBServer` to compile (at build time, from
source — needs `swipl`, `make`, `protoc`, GMP) and supervise a private
TerminusDB under `--data-dir`. The default build avoids this cost, which is
why it's a feature.

### Tests

```sh
cargo test -p room-protocol   # fold unit tests + two-replica convergence

# browser end-to-end (needs playwright + a node running on 127.0.0.1:8091):
cargo run -p tdb-room-node -- --name alpha --http 127.0.0.1:8091 --no-tdb &
node scripts/e2e-browser.js   # 2 tabs: chat, webrtc mesh, branch fork/switch
```

---

## HTTP API (per node)

| method/path | purpose |
|---|---|
| `GET /api/info` | node identity + TerminusDB connectivity |
| `GET /api/hosts` | presence directory (self + peers) → host dropdown |
| `GET /api/rooms` | rooms this node serves (id, title, app, branches, origin) |
| `POST /api/rooms` `{title?, app?}` | create + host a new room |
| `POST /api/rooms/:id/join` `{origin?}` | replicate an existing room |
| `GET /api/rooms/:id` | full snapshot (live + finalized state) |
| `GET /api/rooms/:id/state?branch=&view=live\|finalized` | one branch's folded state |
| `POST /api/rooms/:id/ops` `{branch, op}` | append an op (see L2 vocabulary) |
| `GET /api/rooms/:id/tdb` | materialisation status + live TerminusDB doc counts |
| `GET /api/rooms/:id/ws` | websocket: state push + WebRTC signalling |

Websocket frames (JSON, `t`-tagged): server → `welcome`, `peer-joined`,
`peer-left`, `state`, `signal`; client → `append` (op), `signal` (SDP/ICE
relay to another client of the same room).

### Client p2p (WebRTC)

Browser clients of a room discover each other via `welcome`/`peer-joined`,
negotiate RTCPeerConnections through the node's `signal` relay, and open an
`ops` data channel mesh. Every edit is (1) applied locally as an optimistic
overlay, (2) broadcast to WebRTC peers, who overlay it too, and (3) appended
via the node, which anchors it into the signed hypercore log. The
authoritative folded state then comes back down the websocket and replaces
the overlays. So clients see each other's edits at data-channel latency even
while the log round-trip is in flight.

---

## Design notes & current limitations

- **Browser clients are not yet hypercore writers.** Ops from all clients of a
  node are signed by that node's writer key; per-client attribution rides in
  the ops themselves (e.g. chat nicks). hypercore-rs is deliberately
  wasm-clean, so the natural next step is compiling the L1 core to wasm and
  having browsers keep real writer logs, with WebRTC as a roomnet transport
  and nodes as (optional) always-on replicas + TerminusDB materialisers.
- **Causal heads**: at the pinned hypercore-rs revision, `Room::local_append`
  records `Linearizer::tails()` — the DAG *roots*, not the frontier — as an
  entry's causal references. Replicas still converge deterministically, but
  cross-writer ordering degenerates to the writer-key tiebreak ("I replied
  after seeing your message" is not reflected in the final order). A
  frontier-based fix belongs upstream in hypercore-rs.
- **Finality across nodes**: each node's room uses its configured indexer set;
  deployments should share one `--indexers` list so all replicas finalize the
  same prefix. Rooms with a single indexer finalize immediately (the demo
  default: each node indexes the rooms it hosts).
- **Persistence**: rooms currently use roomnet's `MemStoreFactory` (the
  `RoomServer` path requires a `Default` store factory). Room state rebuilds
  from peers on restart; TerminusDB keeps the durable, versioned record.
  Wiring `DiskStoreFactory`/`CachedFactory` is a follow-up.
- **Merges**: branches fork; a `branch.merge` op mapped onto TerminusDB
  `rebase` is future work.
- **Network requirements**: iroh endpoints use the default n0 preset (DNS +
  pkarr discovery, public relays). In fully offline/airgapped environments —
  including TLS-intercepting proxies that break the relay handshake — nodes
  won't find each other; single-node mode still works.
- **Trustless ≠ access-controlled**: any node that learns a room id can
  replicate it and any client can append ops. Capability tokens / writer
  allowlists are out of scope for this demo.
