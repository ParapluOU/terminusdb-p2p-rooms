// Shared browser client for the p2p rooms demo.
//
// Two modes, one interface:
//
// WASM mode (preferred): a real roomnet replica — hypercore writer, autobase
// linearizer, state fold — runs in the browser via crates/room-client-wasm.
// JavaScript is a dumb byte pipe: it ships the opaque wire frames the wasm
// room emits over the node websocket (binary) and WebRTC data channels, and
// feeds inbound frames back in. No hypercore logic lives in JS.
//
// Fallback mode (when frontend/wasm hasn't been built): ops are JSON-posted
// to the node, which anchors them under its own writer key; a JS overlay
// provides optimistic rendering. Same UI, weaker trust story.

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

const hexToBytes = (hex) => Uint8Array.from(hex.match(/.{2}/g).map((b) => parseInt(b, 16)));

export class RoomClient extends EventTarget {
  /// Use this instead of the constructor: it loads the wasm replica when the
  /// frontend ships one, and falls back to the JSON path otherwise.
  static async create(host, roomId) {
    const client = new RoomClient(host, roomId);
    try {
      client.info = await api(host, '/api/info');
    } catch {
      client.info = null;
    }
    try {
      const wasm = await import('../wasm/room_client_wasm.js');
      await wasm.default(); // init the module
      const seed = crypto.getRandomValues(new Uint8Array(32));
      client.wasm = new wasm.WasmRoom(seed, JSON.stringify(client.info?.indexers || []));
      console.info('room client: wasm replica active, writer', client.wasm.writer_key());
    } catch (e) {
      console.info('room client: wasm module not available, using node-anchored ops', e?.message || '');
    }
    return client;
  }

  constructor(host, roomId) {
    super();
    this.host = host;
    this.roomId = roomId;
    this.info = null;
    this.wasm = null;           // WasmRoom instance (wasm mode)
    this.you = null;            // signalling id assigned by the node
    this.state = null;          // authoritative live state (fallback mode)
    this.finalizedLen = 0;
    this.pendingOps = [];       // fallback-mode optimistic overlay
    this.peers = new Map();     // signalling peerId -> { pc, channel, writer }
    this.ws = null;
  }

  connect() {
    const wsUrl = this.host.replace(/^http/, 'ws') + `/api/rooms/${this.roomId}/ws`;
    this.ws = new WebSocket(wsUrl);
    this.ws.binaryType = 'arraybuffer';
    this.ws.onmessage = (e) => {
      if (typeof e.data === 'string') this.onServer(JSON.parse(e.data));
      else this.onSyncFrame(this.info?.roomnet_id, new Uint8Array(e.data));
    };
    this.ws.onclose = () => {
      this.emit('status', 'disconnected — retrying…');
      setTimeout(() => this.connect(), 2000);
    };
    this.ws.onopen = () => {
      this.emit('status', this.wasm ? 'connected (wasm replica)' : 'connected');
      if (this.wasm) {
        // Bind our writer key so the node can route replies to this socket,
        // then advertise our head.
        this.ws.send(JSON.stringify({ t: 'writer', key: this.wasm.writer_key() }));
        this.shipFrames(JSON.parse(this.wasm.announce()));
      }
    };
  }

  emit(type, detail) {
    this.dispatchEvent(new CustomEvent(type, { detail }));
  }

  onServer(msg) {
    switch (msg.t) {
      case 'welcome':
        this.you = msg.you;
        for (const peer of msg.peers) this.dial(peer);
        this.emit('peers', this.peerCount());
        break;
      case 'peer-joined':
        break; // the joiner dials us
      case 'peer-left':
        this.dropPeer(msg.id);
        break;
      case 'signal':
        this.onSignal(msg.from, msg.data);
        break;
      case 'state':
        // Authoritative fold from the node. In wasm mode our own replica is
        // the view; still track finalized_len as a fallback indicator.
        if (this.wasm) {
          this.finalizedLen = this.wasm.finalized_len();
          this.emit('state', this.view());
        } else {
          this.state = msg.live;
          this.finalizedLen = msg.finalized_len;
          this.pendingOps = [];
          this.emit('state', this.view());
        }
        break;
    }
  }

  // --- sync frames (wasm mode) ------------------------------------------------

  /// Feed one inbound wire frame into the wasm replica and ship its replies.
  onSyncFrame(fromWriterHex, bytes) {
    if (!this.wasm || !fromWriterHex) return;
    try {
      const replies = JSON.parse(this.wasm.on_frame(fromWriterHex, bytes));
      this.shipFrames(replies);
    } catch (e) {
      console.debug('dropped frame:', e?.message);
    }
    this.finalizedLen = this.wasm.finalized_len();
    this.emit('state', this.view());
  }

  /// Ship outbound frames. JS does not interpret them: routing uses only the
  /// `to` tag (gossip / clients / a writer key).
  shipFrames(frames) {
    for (const { to, frame } of frames) {
      if (to === 'clients') continue; // local echo — our own state covers it
      const bytes = hexToBytes(frame);
      if (to !== 'gossip') {
        // Directed: to a WebRTC peer we know, or the node.
        const peer = [...this.peers.values()].find((p) => p.writer === to);
        if (peer?.channel?.readyState === 'open') {
          peer.channel.send(bytes);
          continue;
        }
        if (this.ws?.readyState === 1) this.ws.send(bytes);
        continue;
      }
      // Gossip: node + every open data channel.
      if (this.ws?.readyState === 1) this.ws.send(bytes);
      for (const { channel } of this.peers.values()) {
        if (channel?.readyState === 'open') channel.send(bytes);
      }
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
    const entry = { pc, channel, writer: null };
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
    channel.binaryType = 'arraybuffer';
    channel.onopen = () => {
      this.emit('peers', this.peerCount());
      if (this.wasm) {
        // Introduce our writer key, then advertise our head p2p.
        channel.send(JSON.stringify({ t: 'hello', writer: this.wasm.writer_key() }));
        this.shipFrames(JSON.parse(this.wasm.announce()));
      }
    };
    channel.onmessage = (e) => {
      const entry = this.peers.get(peerId);
      if (typeof e.data === 'string') {
        const msg = JSON.parse(e.data);
        if (msg.t === 'hello' && entry) entry.writer = msg.writer;
        if (msg.t === 'op' && !this.wasm) {
          // Fallback mode: a peer's edit as an optimistic overlay.
          this.pendingOps.push({ branch: msg.branch, op: msg.op });
          this.emit('state', this.view());
        }
        return;
      }
      // Binary: a wire frame from the peer's own hypercore.
      if (entry?.writer) this.onSyncFrame(entry.writer, new Uint8Array(e.data));
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
    if (this.wasm) {
      // Sign into our own hypercore; ship the resulting frames everywhere.
      const frames = JSON.parse(this.wasm.append(branch, JSON.stringify(op)));
      this.shipFrames(frames);
      this.emit('state', this.view());
      return;
    }
    // Fallback: optimistic overlay + p2p hint + node-anchored append.
    this.pendingOps.push({ branch, op });
    const wire = JSON.stringify({ t: 'op', branch, op });
    for (const { channel } of this.peers.values()) {
      if (channel && channel.readyState === 'open') channel.send(wire);
    }
    this.ws.send(JSON.stringify({ t: 'append', branch, op }));
    this.emit('state', this.view());
  }

  /// The state to render: the wasm replica's own live fold, or (fallback)
  /// the node's last push + optimistic overlay.
  view() {
    if (this.wasm) return JSON.parse(this.wasm.live_json());
    if (!this.state) return null;
    const state = structuredClone(this.state);
    for (const { branch, op } of this.pendingOps) applyOp(state, branch, op);
    return state;
  }
}

// Fallback-mode-only mirror of room-protocol's fold, for optimistic rendering —
// in wasm mode the real Rust fold runs client-side and this is unused.
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
