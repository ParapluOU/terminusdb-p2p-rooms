// Shared browser client for the p2p rooms demo.
//
// Talks to a chosen node over HTTP + websocket, and to other browser clients
// of the same room directly over WebRTC data channels (the node's websocket
// doubles as the signalling server). Ops are:
//   1. applied locally as an optimistic overlay,
//   2. broadcast to WebRTC peers (they overlay them too),
//   3. appended to the room's hypercore via the node, which is what makes
//      them durable and totally ordered — the authoritative state then comes
//      back down the websocket and replaces all overlays.

export function currentHost() {
  const p = new URLSearchParams(location.search);
  return (p.get('host') || location.origin).replace(/\/$/, '');
}

export function currentRoom() {
  return new URLSearchParams(location.search).get('room');
}

export function currentBranch() {
  return new URLSearchParams(location.search).get('branch') || 'main';
}

export async function api(host, path, opts) {
  const res = await fetch(host + path, {
    headers: { 'content-type': 'application/json' },
    ...opts,
  });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
  return res.json();
}

const RTC_CONFIG = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

export class RoomClient extends EventTarget {
  constructor(host, roomId) {
    super();
    this.host = host;
    this.roomId = roomId;
    this.you = null;
    this.state = null;          // authoritative live state from the node
    this.finalizedLen = 0;
    this.pendingOps = [];       // [{branch, op}] optimistic (ours + rtc peers')
    this.peers = new Map();     // peerId -> { pc, channel }
    this.ws = null;
  }

  connect() {
    const wsUrl = this.host.replace(/^http/, 'ws') + `/api/rooms/${this.roomId}/ws`;
    this.ws = new WebSocket(wsUrl);
    this.ws.onmessage = (e) => this.onServer(JSON.parse(e.data));
    this.ws.onclose = () => {
      this.emit('status', 'disconnected — retrying…');
      setTimeout(() => this.connect(), 2000);
    };
    this.ws.onopen = () => this.emit('status', 'connected');
  }

  emit(type, detail) {
    this.dispatchEvent(new CustomEvent(type, { detail }));
  }

  onServer(msg) {
    switch (msg.t) {
      case 'welcome':
        this.you = msg.you;
        // Newcomer initiates a WebRTC connection to every existing client.
        for (const peer of msg.peers) this.dial(peer);
        this.emit('peers', this.peerCount());
        break;
      case 'peer-joined':
        // The joiner dials us; nothing to do until their offer arrives.
        break;
      case 'peer-left':
        this.dropPeer(msg.id);
        break;
      case 'signal':
        this.onSignal(msg.from, msg.data);
        break;
      case 'state':
        this.state = msg.live;
        this.finalizedLen = msg.finalized_len;
        // Authoritative state supersedes every overlay we have accumulated.
        this.pendingOps = [];
        this.emit('state', this.view());
        break;
    }
  }

  // --- WebRTC mesh -----------------------------------------------------------

  async dial(peerId) {
    const pc = new RTCPeerConnection(RTC_CONFIG);
    const channel = pc.createDataChannel('ops');
    this.setupPeer(peerId, pc, channel);
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    this.signal(peerId, { sdp: pc.localDescription });
  }

  async onSignal(from, data) {
    let entry = this.peers.get(from);
    if (!entry) {
      const pc = new RTCPeerConnection(RTC_CONFIG);
      entry = this.setupPeer(from, pc, null);
      pc.ondatachannel = (e) => {
        entry.channel = e.channel;
        this.setupChannel(from, e.channel);
      };
    }
    const pc = entry.pc;
    if (data.sdp) {
      await pc.setRemoteDescription(data.sdp);
      if (data.sdp.type === 'offer') {
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        this.signal(from, { sdp: pc.localDescription });
      }
    } else if (data.ice) {
      try { await pc.addIceCandidate(data.ice); } catch {}
    }
  }

  setupPeer(peerId, pc, channel) {
    const entry = { pc, channel };
    this.peers.set(peerId, entry);
    pc.onicecandidate = (e) => {
      if (e.candidate) this.signal(peerId, { ice: e.candidate });
    };
    pc.onconnectionstatechange = () => {
      if (['failed', 'closed', 'disconnected'].includes(pc.connectionState)) this.dropPeer(peerId);
    };
    if (channel) this.setupChannel(peerId, channel);
    return entry;
  }

  setupChannel(peerId, channel) {
    channel.onopen = () => this.emit('peers', this.peerCount());
    channel.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.t === 'op') {
        // A peer's edit, delivered p2p before the node's ordered state: show
        // it optimistically.
        this.pendingOps.push({ branch: msg.branch, op: msg.op });
        this.emit('state', this.view());
      }
    };
  }

  dropPeer(peerId) {
    const entry = this.peers.get(peerId);
    if (entry) {
      try { entry.pc.close(); } catch {}
      this.peers.delete(peerId);
      this.emit('peers', this.peerCount());
    }
  }

  peerCount() {
    let n = 0;
    for (const { channel } of this.peers.values()) {
      if (channel && channel.readyState === 'open') n++;
    }
    return n;
  }

  signal(to, data) {
    this.ws.send(JSON.stringify({ t: 'signal', to, data }));
  }

  // --- ops --------------------------------------------------------------------

  sendOp(branch, op) {
    // 1. optimistic overlay
    this.pendingOps.push({ branch, op });
    // 2. p2p to other clients
    const wire = JSON.stringify({ t: 'op', branch, op });
    for (const { channel } of this.peers.values()) {
      if (channel && channel.readyState === 'open') channel.send(wire);
    }
    // 3. anchor into the hypercore via the node
    this.ws.send(JSON.stringify({ t: 'append', branch, op }));
    this.emit('state', this.view());
  }

  // Authoritative state + optimistic overlay, folded with the same rules as
  // the Rust projection (additive approximation — good enough for UI).
  view() {
    if (!this.state) return null;
    const state = structuredClone(this.state);
    for (const { branch, op } of this.pendingOps) applyOp(state, branch, op);
    return state;
  }
}

// Mirror of room-protocol's fold, for optimistic client-side rendering only —
// the node's Rust projection remains the source of truth.
export function applyOp(state, branchName, op) {
  if (op.kind === 'meta.set_title') { state.title = op.title; return; }
  if (op.kind === 'meta.set_app') { state.app = op.app; return; }
  if (op.kind === 'branch.create') {
    if (!state.branches[op.name] && state.branches[branchName]) {
      state.branches[op.name] = structuredClone(state.branches[branchName]);
      state.branches[op.name].forked_from = branchName;
    }
    return;
  }
  const b = state.branches[branchName];
  if (!b) return;
  switch (op.kind) {
    case 'chat.post':
      b.chat.push({ author: '', nick: op.nick || null, text: op.text, at_op: -1, pending: true });
      break;
    case 'todo.add':
      if (!b.todos[op.id]) b.todos[op.id] = { id: op.id, title: op.title, done: false, author: 'you', created_at_op: -1, pending: true };
      break;
    case 'todo.set_done':
      if (b.todos[op.id]) b.todos[op.id].done = op.done;
      break;
    case 'todo.retitle':
      if (b.todos[op.id]) b.todos[op.id].title = op.title;
      break;
    case 'todo.remove':
      delete b.todos[op.id];
      break;
    case 'fs.write': {
      const p = norm(op.path);
      if (p) { mkparents(b, p); b.files[p] = { type: 'file', content: op.content, author: 'you', modified_at_op: -1 }; }
      break;
    }
    case 'fs.mkdir': {
      const p = norm(op.path);
      if (p) { mkparents(b, p); if (!b.files[p]) b.files[p] = { type: 'dir' }; }
      break;
    }
    case 'fs.remove': {
      const p = norm(op.path);
      if (p) for (const k of Object.keys(b.files)) if (k === p || k.startsWith(p + '/')) delete b.files[k];
      break;
    }
    case 'fs.move': {
      const from = norm(op.from), to = norm(op.to);
      if (!from || !to || b.files[to]) break;
      for (const k of Object.keys(b.files)) {
        if (k === from || k.startsWith(from + '/')) {
          b.files[to + k.slice(from.length)] = b.files[k];
          delete b.files[k];
        }
      }
      break;
    }
  }
}

function norm(path) {
  const segs = path.split('/').filter(Boolean);
  if (!segs.length || segs.some((s) => s === '.' || s === '..')) return null;
  return '/' + segs.join('/');
}

function mkparents(b, path) {
  const segs = path.slice(1).split('/');
  let cur = '';
  for (const seg of segs.slice(0, -1)) {
    cur += '/' + seg;
    if (!b.files[cur]) b.files[cur] = { type: 'dir' };
  }
}

export function randomId() {
  return Array.from(crypto.getRandomValues(new Uint8Array(8)), (b) => b.toString(16).padStart(2, '0')).join('');
}

export function shortKey(hex) {
  return hex ? hex.slice(0, 8) : '?';
}
