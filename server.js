const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 }); // 정상 메시지는 수백 바이트

// ---- 한도 (public/index.html 의 같은 이름 상수와 값이 같아야 한다) ----
const PEN_MAX = 200;          // 펜 굵기
const ERASER_R_MAX = 500;     // 지우개 반지름
const MAX_STROKES_PER_ERASE = 300; // 획 지우개 한 번에 지울 수 있는 획 수
const MAX_HISTORY = 100000;
const PING_MS = +process.env.PING_MS || 30000;
const SID_RE = /^[0-9a-z]{1,12}$/;
const SID_CHARS = '0123456789abcdefghijklmnopqrstuvwxyz';
const SAVE_MS = +process.env.SAVE_MS || 5000;
const ROOM_RE = /^[A-Za-z0-9_-]{1,32}$/;
const ID_ALPHABET = 'abcdefghjkmnpqrstuvwxyz23456789';
const NAME_MAX = 12;
const rooms = new Map(); // roomId -> { clients, history, names, dirty, noSave, ready }

const r1 = (n) => Math.round(+n * 10) / 10;
const okNum = (v) => Number.isFinite(v) && Math.abs(v) <= 1e7;

// ---- 지우개: 서버와 화면이 똑같은 함수를 써야 모두의 그림이 같아진다 (server.js와 index.html에 같은 내용) ----
// 선(s)에서 지우개 경로(캡슐: 선분 + 반지름)와 겹치는 구간 [시작, 끝](0~1)을 구한다. 겹치지 않으면 null.
function erInterval(s, ex0, ey0, edx, edy, eL, rho) {
  const dx = s.x1 - s.x0, dy = s.y1 - s.y0;
  const a = dx * dx + dy * dy;
  let lo = Infinity, hi = -Infinity;
  const add = (l, h) => { if (l <= h) { if (l < lo) lo = l; if (h > hi) hi = h; } };
  const circle = (cx, cy) => {
    const px = s.x0 - cx, py = s.y0 - cy;
    const c = px * px + py * py - rho * rho;
    if (a === 0) { if (c <= 0) add(0, 1); return; }
    const b = 2 * (dx * px + dy * py);
    const disc = b * b - 4 * a * c;
    if (disc < 0) return;
    const q = Math.sqrt(disc);
    add(Math.max((-b - q) / (2 * a), 0), Math.min((-b + q) / (2 * a), 1));
  };
  circle(ex0, ey0);
  if (eL > 0) {
    circle(ex0 + edx, ey0 + edy);
    const ux = edx / eL, uy = edy / eL, nx = -uy, ny = ux;
    const qx = s.x0 - ex0, qy = s.y0 - ey0;
    let l = 0, h = 1;
    const clip = (f0, f1, m, M) => {
      if (Math.abs(f1) < 1e-12) { if (f0 < m || f0 > M) { l = 1; h = 0; } return; }
      let t1 = (m - f0) / f1, t2 = (M - f0) / f1;
      if (t1 > t2) { const t = t1; t1 = t2; t2 = t; }
      if (t1 > l) l = t1;
      if (t2 < h) h = t2;
    };
    clip(qx * ux + qy * uy, dx * ux + dy * uy, 0, eL);
    clip(qx * nx + qy * ny, dx * nx + dy * ny, -rho, rho);
    add(l, h);
  }
  return lo <= hi ? [lo, hi] : null;
}
// owner 가 null 이 아니면 그 사람의 선만 지운다. 지워진 곳이 없으면 null, 있으면 새 배열(잘린 조각 포함)을 돌려준다.
// 잘린 조각은 원래 획의 sid 를 그대로 가진다.
function eraseCapsule(list, owner, x0, y0, x1, y1, r) {
  r = Math.min(Math.max(r, 1), ERASER_R_MAX);
  const edx = x1 - x0, edy = y1 - y0;
  const eL = Math.sqrt(edx * edx + edy * edy);
  const minX = Math.min(x0, x1) - r, maxX = Math.max(x0, x1) + r;
  const minY = Math.min(y0, y1) - r, maxY = Math.max(y0, y1) + r;
  let hits = null;
  for (let i = 0; i < list.length; i++) {
    const s = list[i];
    if (owner !== null && s.o !== owner) continue;
    const m = s.size / 2;
    if (Math.max(s.x0, s.x1) + m < minX || Math.min(s.x0, s.x1) - m > maxX ||
        Math.max(s.y0, s.y1) + m < minY || Math.min(s.y0, s.y1) - m > maxY) continue;
    const iv = erInterval(s, x0, y0, edx, edy, eL, r + m);
    if (!iv) continue;
    if (!hits) hits = new Map();
    hits.set(i, iv);
  }
  if (!hits) return null;
  const R1 = (n) => Math.round(n * 10) / 10;
  const out = [];
  for (let i = 0; i < list.length; i++) {
    const s = list[i];
    const iv = hits.get(i);
    if (!iv) { out.push(s); continue; }
    const dx = s.x1 - s.x0, dy = s.y1 - s.y0;
    const len = Math.sqrt(dx * dx + dy * dy);
    const piece = (a, b) => ({ x0: R1(s.x0 + dx * a), y0: R1(s.y0 + dy * a), x1: R1(s.x0 + dx * b), y1: R1(s.y0 + dy * b), color: s.color, size: s.size, o: s.o, sid: s.sid });
    if (iv[0] * len >= 0.25) out.push(piece(0, iv[0]));
    if ((1 - iv[1]) * len >= 0.25) out.push(piece(iv[1], 1));
  }
  return out;
}
// 지우개 경로에 한 번이라도 닿은 획의 [주인, sid] 목록 (최대 max개). owner 가 null 이 아니면 그 사람의 획만.
function hitStrokes(list, owner, x0, y0, x1, y1, r, max) {
  r = Math.min(Math.max(r, 1), ERASER_R_MAX);
  const edx = x1 - x0, edy = y1 - y0;
  const eL = Math.sqrt(edx * edx + edy * edy);
  const minX = Math.min(x0, x1) - r, maxX = Math.max(x0, x1) + r;
  const minY = Math.min(y0, y1) - r, maxY = Math.max(y0, y1) + r;
  const keys = new Map();
  for (let i = 0; i < list.length; i++) {
    const s = list[i];
    if (!s.sid || !s.o) continue;
    if (owner !== null && s.o !== owner) continue;
    const k = s.o + '|' + s.sid;
    if (keys.has(k)) continue;
    const m = s.size / 2;
    if (Math.max(s.x0, s.x1) + m < minX || Math.min(s.x0, s.x1) - m > maxX ||
        Math.max(s.y0, s.y1) + m < minY || Math.min(s.y0, s.y1) - m > maxY) continue;
    if (!erInterval(s, x0, y0, edx, edy, eL, r + m)) continue;
    if (keys.size >= max) break;
    keys.set(k, [s.o, s.sid]);
  }
  return [...keys.values()];
}
// 키 목록([[주인, sid], ...])에 해당하는 선분을 모두 뺀 새 배열. 지운 게 없으면 null.
function removeStrokes(list, keys) {
  const set = new Set(keys.map((k) => k[0] + '|' + k[1]));
  const out = [];
  for (const s of list) { if (s.sid && s.o && set.has(s.o + '|' + s.sid)) continue; out.push(s); }
  return out.length === list.length ? null : out;
}

function newRoomId() {
  let s = '';
  for (let i = 0; i < 10; i++) s += ID_ALPHABET[crypto.randomInt(ID_ALPHABET.length)];
  return s;
}

// ---- 저장소: DATABASE_URL 이 있으면 DB, 없으면 메모리만 ----
const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 3 })
  : null;

const dbReady = pool
  ? (async () => {
      try {
        await pool.query("CREATE TABLE IF NOT EXISTS rooms (id TEXT PRIMARY KEY, history TEXT NOT NULL, names TEXT NOT NULL DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT now())");
        await pool.query("ALTER TABLE rooms ADD COLUMN IF NOT EXISTS names TEXT NOT NULL DEFAULT '{}'");
        console.log('DB 연결됨: 그림이 영구 저장돼요');
      } catch (e) {
        console.error('DB 초기화 실패:', e.message);
      }
    })()
  : Promise.resolve(console.log('DATABASE_URL 없음: 서버가 재시작되면 그림이 사라져요'));

async function loadRoom(id) {
  if (!pool) return { history: [], names: {} };
  try {
    await dbReady;
    const r = await pool.query('SELECT history, names FROM rooms WHERE id = $1', [id]);
    if (!r.rows.length) return { history: [], names: {} };
    const row = r.rows[0];
    return { history: JSON.parse(row.history), names: JSON.parse(row.names || '{}') };
  } catch (e) {
    // 읽기에 실패한 채로 저장하면 기존 그림을 덮어쓰게 되므로, 이 방은 저장하지 않는다
    console.error('불러오기 실패 (이 방은 저장하지 않아요):', e.message);
    return { history: [], names: {}, failed: true };
  }
}

async function saveRoom(id, room) {
  if (!pool || !room.dirty || room.noSave) return;
  room.dirty = false;
  try {
    await pool.query(
      'INSERT INTO rooms (id, history, names, updated_at) VALUES ($1, $2, $3, now()) ON CONFLICT (id) DO UPDATE SET history = $2, names = $3, updated_at = now()',
      [id, JSON.stringify(room.history), JSON.stringify(room.names)]
    );
  } catch (e) {
    room.dirty = true;
    console.error('저장 실패:', e.message);
  }
}

// ---- 닉네임 ----
function cleanNick(s) {
  const t = String(s || '')
    .normalize('NFC')
    .replace(/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/gu, '')
    .replace(/\s+/g, ' ')
    .trim();
  return Array.from(t).slice(0, NAME_MAX).join('');
}

function uniqueNick(room, owner, want) {
  const taken = new Set();
  for (const o of Object.keys(room.names)) if (o !== owner) taken.add(String(room.names[o]).toLowerCase());
  let name = want;
  for (let i = 2; taken.has(name.toLowerCase()); i++) {
    const suf = ' ' + i;
    name = Array.from(want).slice(0, NAME_MAX - suf.length).join('') + suf;
  }
  return name;
}

function setName(room, owner, want) {
  const has = Object.prototype.hasOwnProperty.call(room.names, owner);
  const clean = cleanNick(want) || (has ? String(room.names[owner]) : '익명');
  const name = uniqueNick(room, owner, clean);
  if (room.names[owner] !== name && (has || Object.keys(room.names).length < 300)) {
    room.names[owner] = name;
    room.dirty = true;
  }
  return name;
}

// 접속 중이 아니고 그림도 없는 사람의 이름은 정리한다
function pruneNames(room) {
  const alive = new Set();
  for (const s of room.history) alive.add(s.o);
  for (const c of room.clients) alive.add(c.owner);
  for (const o of Object.keys(room.names)) {
    if (!alive.has(o)) { delete room.names[o]; room.dirty = true; }
  }
}

async function saveAll() {
  for (const [id, room] of rooms) {
    pruneNames(room);
    await saveRoom(id, room);
  }
}
setInterval(saveAll, SAVE_MS);
process.on('SIGTERM', async () => { await saveAll(); process.exit(0); });
process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e));

// ---- 획 ID(sid) ----
// sid 없는 옛 선: 같은 주인·같은 색·같은 굵기이고 앞 선분의 끝점에서 이어지는 선분들을 한 획으로 보고 임시 sid 를 붙인다.
let legacyN = 0;
function assignLegacySids(history) {
  const st = new Map();
  let changed = false;
  for (const s of history) {
    if (s.sid) continue;
    const key = s.o || '?';
    const cur = st.get(key);
    if (cur && cur.color === s.color && cur.size === s.size && Math.abs(s.x0 - cur.x) < 0.15 && Math.abs(s.y0 - cur.y) < 0.15) s.sid = cur.sid;
    else s.sid = 'l' + (legacyN++).toString(36).padStart(6, '0');
    st.set(key, { sid: s.sid, color: s.color, size: s.size, x: s.x1, y: s.y1 });
    changed = true;
  }
  return changed;
}
function randSid() {
  let s = '';
  for (const b of crypto.randomBytes(8)) s += SID_CHARS[b % 36];
  return s;
}
// 옛 클라이언트가 sid 없이 보낸 선: 접속별로 같은 방식으로 이어 붙여 sid 를 정한다.
function autoSid(ws, s) {
  const a = ws.auto;
  if (a && a.color === s.color && a.size === s.size && Math.abs(s.x0 - a.x) < 0.15 && Math.abs(s.y0 - a.y) < 0.15) { a.x = s.x1; a.y = s.y1; return a.sid; }
  ws.auto = { sid: randSid(), color: s.color, size: s.size, x: s.x1, y: s.y1 };
  return ws.auto.sid;
}

function getRoom(id) {
  if (!rooms.has(id)) {
    const room = { clients: new Set(), history: [], names: {}, dirty: false, noSave: false };
    room.ready = loadRoom(id).then((d) => {
      room.history = Array.isArray(d.history) ? d.history : [];
      room.names = d.names && typeof d.names === 'object' && !Array.isArray(d.names) ? d.names : {};
      room.noSave = !!d.failed;
      if (assignLegacySids(room.history)) room.dirty = true;
    });
    rooms.set(id, room);
  }
  return rooms.get(id);
}

function broadcast(room, sender, obj) {
  const data = JSON.stringify(obj);
  for (const c of room.clients) if (c !== sender && c.readyState === 1) c.send(data);
}
const broadcastAll = (room, obj) => broadcast(room, null, obj);
// 접속자 수와 목록은 소켓 수가 아니라 주인 ID 기준 고유 인원이다 (탭을 여러 개 열어도 1명)
function announce(room) {
  const owners = [...new Set([...room.clients].map((c) => c.owner))];
  broadcastAll(room, { type: 'presence', owners });
  broadcastAll(room, { type: 'count', n: owners.length });
}
function notice(ws, code, text) {
  const now = Date.now();
  if (now - (ws.lastNotice || 0) < 3000 || ws.readyState !== 1) return;
  ws.lastNotice = now;
  ws.send(JSON.stringify({ type: 'notice', code, text }));
}

// 접속마다 초당 250개까지 선을 받는다 (악의적인 도배 방지)
function allowSeg(ws) {
  const now = Date.now();
  ws.tokens = Math.min(400, ws.tokens + (now - ws.tokAt) * 0.25);
  ws.tokAt = now;
  if (ws.tokens < 1) return false;
  ws.tokens -= 1;
  return true;
}

// ---- 웹 서버 ----
app.get('/', (req, res, next) => {
  if (!req.query.room) return res.redirect('/?room=' + newRoomId());
  next();
});
app.use(express.static(path.join(__dirname, 'public')));

wss.on('error', (e) => console.error('wss error:', e.message));

// 약 30초마다 ping: 응답이 없는(반쯤 끊긴) 소켓은 정리한다
const pingTimer = setInterval(() => {
  for (const c of wss.clients) {
    if (c.isAlive === false) { c.terminate(); continue; }
    c.isAlive = false;
    try { c.ping(); } catch {}
  }
}, PING_MS);
if (pingTimer.unref) pingTimer.unref();
wss.on('close', () => clearInterval(pingTimer));

wss.on('connection', async (ws, req) => {
  ws.on('error', (e) => console.error('ws error:', e.message)); // 없으면 잘못된 프레임 하나에 서버가 죽는다

  const url = new URL(req.url, 'http://localhost');
  const rawRoom = url.searchParams.get('room') || 'lobby';
  const roomId = ROOM_RE.test(rawRoom) ? rawRoom : 'lobby';
  const room = getRoom(roomId);
  room.clients.add(ws);

  // 브라우저가 보낸 비밀 키를 해시해 '주인 ID'로 쓴다. 키가 없으면 이번 접속에서만 주인.
  const key = url.searchParams.get('k') || '';
  const owner = /^[A-Za-z0-9_-]{16,64}$/.test(key)
    ? crypto.createHash('sha256').update(key).digest('base64url').slice(0, 10)
    : 'g' + crypto.randomBytes(5).toString('hex');
  ws.owner = owner;
  ws.tokens = 400; ws.tokAt = Date.now(); ws.lastCur = 0; ws.lastNick = 0; ws.lastSync = 0;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.send(JSON.stringify({ type: 'hello', me: owner }));

  ws.on('message', async (raw) => {
    try {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }
      if (!msg || typeof msg !== 'object') return;
      await room.ready;

      if (msg.type === 'seg') {
        if (!allowSeg(ws)) return;
        const s = {
          x0: r1(msg.x0), y0: r1(msg.y0), x1: r1(msg.x1), y1: r1(msg.y1),
          color: String(msg.color).slice(0, 9),
          size: Math.min(Math.max(+msg.size || 4, 1), PEN_MAX),
          o: owner, // 클라이언트가 보낸 값은 무시하고 서버가 정한다
        };
        if (![s.x0, s.y0, s.x1, s.y1].every(okNum)) return;
        s.sid = typeof msg.sid === 'string' && SID_RE.test(msg.sid) ? msg.sid : autoSid(ws, s);
        if (room.history.length >= MAX_HISTORY) { notice(ws, 'full', '그림이 가득 찼어요. 일부를 지우고 다시 그려 주세요.'); return; }
        room.history.push(s);
        room.dirty = true;
        broadcast(room, ws, { type: 'seg', ...s });
      } else if (msg.type === 'clear') {
        // 내가 그린 선만 지운다 (다른 사람 그림은 그대로)
        room.history = room.history.filter((s) => s.o !== owner);
        room.dirty = true;
        broadcast(room, ws, { type: 'clear', o: owner });
        saveRoom(roomId, room);
      } else if (msg.type === 'erase') {
        // 지우개: 내가 그린 선만 지운다. 잘린 결과를 다른 사람 화면도 같은 함수로 계산한다.
        if (!allowSeg(ws)) return;
        const x0 = r1(msg.x0), y0 = r1(msg.y0), x1 = r1(msg.x1), y1 = r1(msg.y1), r = r1(msg.r);
        if (![x0, y0, x1, y1, r].every(okNum)) return;
        const res = eraseCapsule(room.history, owner, x0, y0, x1, y1, r);
        if (!res || res.length > MAX_HISTORY * 1.5) return;
        room.history = res;
        room.dirty = true;
        broadcast(room, ws, { type: 'erase', o: owner, x0, y0, x1, y1, r });
      } else if (msg.type === 'erasestroke') {
        // 획 지우개: 경로에 닿은 획 전체를 지우고, 지워진 획 목록을 모두에게 알린다 (받는 쪽은 재계산하지 않는다)
        if (!allowSeg(ws)) return;
        const x0 = r1(msg.x0), y0 = r1(msg.y0), x1 = r1(msg.x1), y1 = r1(msg.y1), r = r1(msg.r);
        if (![x0, y0, x1, y1, r].every(okNum)) return;
        const q = Number.isInteger(msg.q) && msg.q >= 0 && msg.q < 1e9 ? msg.q : 0;
        const keys = hitStrokes(room.history, owner, x0, y0, x1, y1, r, MAX_STROKES_PER_ERASE);
        if (keys.length) {
          const res = removeStrokes(room.history, keys);
          if (res) { room.history = res; room.dirty = true; }
          broadcastAll(room, { type: 'strokesgone', keys, by: owner, q });
        } else if (ws.readyState === 1) {
          ws.send(JSON.stringify({ type: 'strokesgone', keys: [], by: owner, q })); // 보낸 사람이 예상과 다른지 확인할 수 있게
        }
      } else if (msg.type === 'cancelstroke') {
        // 잘못 시작된 내 획 취소 (모두의 화면과 저장본에서 지움)
        if (!allowSeg(ws)) return;
        if (typeof msg.sid !== 'string' || !SID_RE.test(msg.sid)) return;
        const keys = [[owner, msg.sid]];
        const res = removeStrokes(room.history, keys);
        if (res) { room.history = res; room.dirty = true; broadcastAll(room, { type: 'strokesgone', keys, by: owner, cancel: true }); }
      } else if (msg.type === 'resync') {
        const now = Date.now();
        if (now - ws.lastSync < 2000) return;
        ws.lastSync = now;
        ws.send(JSON.stringify({ type: 'history', data: room.history }));
      } else if (msg.type === 'cur') {
        const now = Date.now();
        if (now - ws.lastCur < 25) return;
        ws.lastCur = now;
        const x = r1(msg.x), y = r1(msg.y);
        if (!okNum(x) || !okNum(y)) return;
        broadcast(room, ws, { type: 'cur', o: owner, x, y });
      } else if (msg.type === 'curhide') {
        broadcast(room, ws, { type: 'curhide', o: owner });
      } else if (msg.type === 'nick') {
        const now = Date.now();
        if (now - ws.lastNick < 300) return;
        ws.lastNick = now;
        broadcastAll(room, { type: 'name', o: owner, name: setName(room, owner, msg.name) });
      }
    } catch (e) {
      console.error('메시지 처리 오류:', e.message);
    }
  });

  ws.on('close', () => {
    room.clients.delete(ws);
    broadcastAll(room, { type: 'curhide', o: owner });
    announce(room);
    if (room.clients.size === 0) {
      pruneNames(room);
      saveRoom(roomId, room);
      setTimeout(() => {
        const r = rooms.get(roomId);
        if (r && r.clients.size === 0) rooms.delete(roomId);
      }, 10 * 60 * 1000);
    }
  });

  await room.ready;
  if (ws.readyState !== 1) return;
  const name = setName(room, owner, url.searchParams.get('n'));
  ws.send(JSON.stringify({ type: 'history', data: room.history }));
  ws.send(JSON.stringify({ type: 'names', names: room.names }));
  broadcastAll(room, { type: 'name', o: owner, name });
  announce(room);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('http://localhost:' + PORT));
