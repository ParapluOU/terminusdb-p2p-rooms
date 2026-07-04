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

With the wasm client this extends into the browser: every user edit is signed
by the user's own writer key and travels as self-verifying blocks — nodes are
replicas, relays and materialisers, never authorities over content. (In
fallback mode, without the wasm build, clients do trust their chosen node to
anchor ops faithfully.) Anyone can audit any node by fetching a room's raw
hypercores from `/api/rooms/:id/log` and re-running the fold.

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
├── room-client-wasm/   # the browser replica: a real roomnet Room (hypercore
│                       #   writer + autobase + fold) compiled to wasm — no
│                       #   hypercore logic in JavaScript (scripts/build-wasm.sh)
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

### Browser-side hypercores (recommended)

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.126
./scripts/build-wasm.sh        # emits frontend/wasm/
```

With `frontend/wasm/` present, every browser tab runs its **own roomnet
replica** (writer identity, hypercore, linearizer, fold — all Rust/wasm) and
the footer shows “connected (wasm replica)”. Without it, the frontends fall
back to node-anchored ops. Since browser writers can't reach quorum finality
yet (see limitations), run nodes with `--materialise live` to land their
entries in TerminusDB.

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
| `GET /api/rooms/:id/log` | the room's complete autobase hypercores: every writer's log with causal heads + raw payloads |
| `POST /api/rooms/:id/ops` `{branch, op}` | append an op (see L2 vocabulary) |
| `GET /api/rooms/:id/tdb` | materialisation status + live TerminusDB doc counts |
| `GET /api/rooms/:id/ws` | websocket: state push + WebRTC signalling |

Websocket frames: JSON text frames (`t`-tagged) — server → `welcome`,
`peer-joined`, `peer-left`, `state`, `signal`; client → `append` (fallback-mode
op), `signal` (SDP/ICE relay), `writer` (bind a wasm writer key). **Binary
frames** are raw roomnet wire `SyncMessage`s — the same protocol iroh peers
speak — for browser-side hypercore writers.

### Client p2p (WebRTC + wasm hypercores)

Browser clients of a room discover each other via `welcome`/`peer-joined`,
negotiate RTCPeerConnections through the node's `signal` relay, and open an
`ops` data channel mesh. In wasm mode each tab is a **real hypercore writer**:
an edit is signed into the tab's own log, the resulting sync frames go to
WebRTC peers (true browser↔browser hypercore replication: Head → Want → Block
with Merkle proofs) and to the node's websocket, where the node ingests the
self-verifying blocks, folds them, materialises them, and push-relays them to
its iroh peers. JavaScript never parses a frame — it routes opaque bytes by
the `to` tag the wasm module attaches. In fallback mode (no wasm build) ops
are JSON, overlays are optimistic, and the node anchors everything under its
own writer key.

---

## Design notes & current limitations

- **Causal heads (upstream)**: at the pinned hypercore-rs revision,
  `Room::local_append` records `Linearizer::tails()` — the DAG *roots*, not
  the frontier — as an entry's causal references. Replicas still converge
  deterministically, but cross-writer ordering degenerates to the writer-key
  tiebreak ("I replied after seeing your message" is not reflected in the
  final order), and — more importantly — indexer entries never causally *see*
  other writers' entries, so **non-indexer writers (e.g. browser wasm
  clients) rarely reach quorum finality**. Until a frontier-based fix lands
  upstream in hypercore-rs, run nodes with `--materialise live` when using
  wasm writers.
- **Block relay (upstream)**: roomnet nodes serve only their *local* writer's
  blocks on pull (`Want`); this demo compensates by push-relaying
  self-verifying client blocks to iroh peers, but late joiners can't backfill
  another writer's history from a third-party node. Serving replicated
  writers' blocks (with stored proofs) is the upstream follow-on roomnet's
  own comments call out.
- **Client identity is ephemeral** in the demo: each tab derives a fresh
  writer key. Persisting the seed + log (roomnet's storage layer has an
  OPFS/IndexedDB backend) would give durable browser identities.
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
