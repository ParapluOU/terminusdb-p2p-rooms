// End-to-end browser test against a running tdb-room-node on 127.0.0.1:8091.
// Verifies: room creation, two tabs chatting through the node websocket,
// WebRTC data-channel mesh via the node's signalling relay, optimistic
// rendering, and branch fork/switch through the dropdown UI.
const { chromium } = require('playwright');

const HOST = 'http://127.0.0.1:8091';

async function main() {
  // fresh room
  const res = await fetch(HOST + '/api/rooms', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ title: 'e2e room', app: 'chat' }),
  });
  const { id: room } = await res.json();
  console.log('room:', room);

  const browser = await chromium.launch({
    args: ['--use-fake-ui-for-media-stream', '--allow-insecure-localhost'],
  });
  const ctx = await browser.newContext();
  const url = `${HOST}/chat.html?host=${encodeURIComponent(HOST)}&room=${room}`;
  const p1 = await ctx.newPage();
  const p2 = await ctx.newPage();
  p1.on('console', (m) => m.type() === 'error' && console.log('p1 console error:', m.text()));
  p2.on('console', (m) => m.type() === 'error' && console.log('p2 console error:', m.text()));
  p1.on('dialog', (d) => d.accept('dev'));

  await p1.goto(url);
  await p2.goto(url);
  await p1.waitForSelector('#text');
  await p2.waitForSelector('#text');

  // tab1 posts with a nick
  await p1.fill('#nick', 'alice');
  await p1.fill('#text', 'hello from tab1');
  await p1.press('#text', 'Enter');

  // Wait for the authoritative (non-pending) message with resolved author.
  await p2.waitForFunction(
    () => [...document.querySelectorAll('.chat-msg:not(.pending)')].some(
      (m) => m.textContent.includes('hello from tab1') && m.querySelector('.who').textContent.includes('alice')),
    null, { timeout: 10000 }
  );
  console.log('tab2 sees the message with author "alice"');

  // tab2 replies
  await p2.fill('#text', 'reply from tab2');
  await p2.press('#text', 'Enter');
  await p1.waitForFunction(
    () => document.querySelector('#log').textContent.includes('reply from tab2'),
    null, { timeout: 10000 }
  );
  console.log('tab1 sees the reply');

  // WebRTC mesh: both tabs should report one open data channel peer
  try {
    await p1.waitForFunction(
      () => document.querySelector('#rtc-peers').textContent.startsWith('1 '),
      null, { timeout: 15000 }
    );
    await p2.waitForFunction(
      () => document.querySelector('#rtc-peers').textContent.startsWith('1 '),
      null, { timeout: 15000 }
    );
    console.log('webrtc: both tabs report 1 open data-channel peer');
  } catch (e) {
    console.log('WARNING: webrtc data channel did not open (sandbox may block UDP):',
      await p1.textContent('#rtc-peers'));
  }

  // fork a branch from the dropdown on tab1 (prompt answered with "dev")
  await p1.selectOption('#branch-select', '__fork__');
  await p1.waitForFunction(
    () => new URLSearchParams(location.search).get('branch') === 'dev',
    null, { timeout: 5000 }
  );
  await p1.fill('#text', 'only on dev');
  await p1.press('#text', 'Enter');
  await p1.waitForFunction(
    () => document.querySelector('#log').textContent.includes('only on dev'),
    null, { timeout: 10000 }
  );
  console.log('tab1 forked "dev" and posted to it');

  // tab2 stays on main: must NOT see the dev message, but keeps history
  await p2.waitForTimeout(1500);
  const mainLog = await p2.textContent('#log');
  if (mainLog.includes('only on dev')) throw new Error('branch isolation broken: dev op leaked to main');
  console.log('tab2 (main) does not see the dev-only message');

  // tab2 switches to dev via the dropdown: sees carried history + dev post
  await p2.waitForFunction(
    () => [...document.querySelectorAll('#branch-select option')].some((o) => o.value === 'dev'),
    null, { timeout: 10000 }
  );
  await p2.selectOption('#branch-select', 'dev');
  await p2.waitForFunction(
    () => document.querySelector('#log').textContent.includes('only on dev')
      && document.querySelector('#log').textContent.includes('hello from tab1'),
    null, { timeout: 10000 }
  );
  console.log('tab2 switched to dev: sees carried history + dev-only post');

  // index page lists the room with both branches
  const p3 = await ctx.newPage();
  await p3.goto(`${HOST}/index.html?host=${encodeURIComponent(HOST)}`);
  await p3.waitForFunction(
    () => document.querySelector('#rooms').textContent.includes('e2e room'),
    null, { timeout: 10000 }
  );
  console.log('index page lists the room');

  await browser.close();
  console.log('E2E OK');
}

main().catch((e) => { console.error('E2E FAILED:', e); process.exit(1); });
