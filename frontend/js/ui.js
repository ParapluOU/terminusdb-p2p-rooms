// Shared UI components: the host dropdown (pick any node in the network) and
// the branch dropdown (switch / fork room branches). Every app page gets both.

import { api, currentHost, currentRoom, currentBranch } from './client.js';

// Populate a <select> with all hosts known to the current host's presence
// directory. Changing it reloads the page pointed at the chosen node — the
// same room is served by every replicating node.
export async function mountHostSelect(el) {
  const host = currentHost();
  el.innerHTML = '';
  try {
    const { self, peers } = await api(host, '/api/hosts');
    const entries = [{ ...self, self: true }, ...peers.filter((p) => p.gossip_id !== self.gossip_id)];
    for (const h of entries) {
      if (!h.http_url) continue;
      const opt = document.createElement('option');
      opt.value = h.http_url;
      opt.textContent = `${h.name}${h.self ? ' (this node)' : ''} — ${h.http_url}`;
      if (h.http_url.replace(/\/$/, '') === host) opt.selected = true;
      el.appendChild(opt);
    }
  } catch (e) {
    const opt = document.createElement('option');
    opt.textContent = `${host} (directory unavailable)`;
    el.appendChild(opt);
  }
  el.onchange = () => {
    const p = new URLSearchParams(location.search);
    p.set('host', el.value);
    location.search = p.toString();
  };
}

// Populate a <select> with the room's branches plus a "fork…" action.
// `client` is a connected RoomClient; the dropdown re-renders on state pushes.
export function mountBranchSelect(el, client, onSwitch) {
  const render = (state) => {
    if (!state) return;
    const current = currentBranch();
    el.innerHTML = '';
    for (const name of Object.keys(state.branches)) {
      const opt = document.createElement('option');
      opt.value = name;
      const from = state.branches[name].forked_from;
      opt.textContent = name + (from ? ` (from ${from})` : '');
      if (name === current) opt.selected = true;
      el.appendChild(opt);
    }
    const fork = document.createElement('option');
    fork.value = '__fork__';
    fork.textContent = '＋ fork new branch…';
    el.appendChild(fork);
  };
  client.addEventListener('state', (e) => render(e.detail));
  render(client.view());

  el.onchange = () => {
    if (el.value === '__fork__') {
      const name = prompt('New branch name (forked from ' + currentBranch() + '):');
      el.value = currentBranch();
      if (!name) return;
      if (!/^[a-zA-Z0-9_-]{1,64}$/.test(name)) {
        alert('Branch names: 1-64 chars of letters, digits, _ or -');
        return;
      }
      client.sendOp(currentBranch(), { kind: 'branch.create', name });
      switchBranch(name);
      if (onSwitch) onSwitch(name);
      return;
    }
    switchBranch(el.value);
    if (onSwitch) onSwitch(el.value);
  };
}

export function switchBranch(name) {
  const p = new URLSearchParams(location.search);
  p.set('branch', name);
  history.replaceState(null, '', '?' + p.toString());
}

// Standard header for app pages: room title, host + branch dropdowns, status.
export function mountHeader(client, appName) {
  const room = currentRoom();
  document.querySelector('#room-label').textContent = `${appName} · room ${room.slice(0, 8)}…`;
  mountHostSelect(document.querySelector('#host-select'));
  mountBranchSelect(document.querySelector('#branch-select'), client, () => {
    client.emit('state', client.view());
  });
  client.addEventListener('state', (e) => {
    const s = e.detail;
    if (s && s.title) document.querySelector('#room-label').textContent = `${appName} · ${s.title}`;
    document.querySelector('#finality').textContent = `${s ? s.ops : 0} ops · ${client.finalizedLen} finalized`;
  });
  client.addEventListener('peers', (e) => {
    document.querySelector('#rtc-peers').textContent = `${e.detail} webrtc peer${e.detail === 1 ? '' : 's'}`;
  });
  client.addEventListener('status', (e) => {
    document.querySelector('#conn-status').textContent = e.detail;
  });
  mountTdbBadge(room);
}

async function mountTdbBadge(room) {
  const el = document.querySelector('#tdb-badge');
  if (!el) return;
  const refresh = async () => {
    try {
      const info = await api(currentHost(), `/api/rooms/${room}/tdb`);
      if (info.materialised && !info.last_error) {
        el.textContent = `TerminusDB ✓ ${info.db} @ v${info.last_version}`;
        el.className = 'badge ok';
      } else if (info.materialised) {
        el.textContent = 'TerminusDB: ' + info.last_error.slice(0, 60);
        el.className = 'badge warn';
      } else {
        el.textContent = 'TerminusDB: not materialised yet';
        el.className = 'badge';
      }
    } catch {
      el.textContent = 'TerminusDB: unknown';
      el.className = 'badge';
    }
  };
  refresh();
  setInterval(refresh, 5000);
}

export function esc(s) {
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}
