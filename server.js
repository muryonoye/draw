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
const MAX_HISTORY = +process.env.MAX_HISTORY || 100000;   // 방 하나에 둘 수 있는 선분 수 (테스트에서만 줄인다)
const PING_MS = +process.env.PING_MS || 30000;
const SID_RE = /^[0-9a-z]{1,12}$/;
const SID_CHARS = '0123456789abcdefghijklmnopqrstuvwxyz';
// ---- 방장 / 채팅 (2단계) ----
const MIN_MS = +process.env.MIN_MS || 60000;                   // 밴·채팅 금지의 '1분' (테스트에서만 줄인다)
const LEAVE_GRACE_MS = +process.env.LEAVE_GRACE_MS || 10000;   // 이 시간 안에 다시 들어오면 입장/퇴장 메시지를 내지 않는다
const LOCK_GRACE_MS = +process.env.LOCK_GRACE_MS || 120000;    // 방이 잠겨 있어도 방금 나간 사람은 다시 들어올 수 있다
const MAX_CHAT = 100, CHAT_MAX_CHARS = 200, REASON_MAX = 50, MAX_LIST = 200, BAN_MAX_MIN = 43200;
const BYE_CODE = { kicked: 4001, banned: 4002, locked: 4003 };
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
// 잘린 조각은 원래 획의 sid 를 그대로 가진다. diff({removed:[], added:[]})를 넘기면 지워진 원본/새 조각을 담아 준다(이전·되돌리기용).
function eraseCapsule(list, owner, x0, y0, x1, y1, r, diff) {
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
    if (diff) diff.removed.push(s);
    const dx = s.x1 - s.x0, dy = s.y1 - s.y0;
    const len = Math.sqrt(dx * dx + dy * dy);
    const piece = (a, b) => ({ x0: R1(s.x0 + dx * a), y0: R1(s.y0 + dy * a), x1: R1(s.x0 + dx * b), y1: R1(s.y0 + dy * b), color: s.color, size: s.size, o: s.o, sid: s.sid });
    if (iv[0] * len >= 0.25) { const p = piece(0, iv[0]); out.push(p); if (diff) diff.added.push(p); }
    if ((1 - iv[1]) * len >= 0.25) { const p = piece(iv[1], 1); out.push(p); if (diff) diff.added.push(p); }
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
function removeStrokes(list, keys, diff) {
  const set = new Set(keys.map((k) => k[0] + '|' + k[1]));
  const out = [];
  for (const s of list) { if (s.sid && s.o && set.has(s.o + '|' + s.sid)) { if (diff) diff.removed.push(s); continue; } out.push(s); }
  return out.length === list.length ? null : out;
}
// 같은 값(주인·sid·좌표)의 선분을 하나씩 지운 새 배열. 지운 게 없으면 null. (이전·되돌리기가 잘린 조각을 정확히 되돌릴 때 쓴다)
function removeExactSegs(list, items) {
  const need = new Map();
  const key = (s) => s.o + '|' + s.sid + '|' + s.x0 + '|' + s.y0 + '|' + s.x1 + '|' + s.y1;
  for (const s of items) need.set(key(s), (need.get(key(s)) || 0) + 1);
  const out = [];
  let n = 0;
  for (const s of list) { const k = key(s), c = need.get(k); if (c) { need.set(k, c - 1); n++; continue; } out.push(s); }
  return n ? out : null;
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
        await pool.query("CREATE TABLE IF NOT EXISTS rooms (id TEXT PRIMARY KEY, history TEXT NOT NULL, names TEXT NOT NULL DEFAULT '{}', meta TEXT NOT NULL DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT now())");
      } catch (e) {
        console.error('DB 초기화 실패:', e.message);
      }
      for (const col of ['names', 'meta']) {          // 이미 있던 표에는 새 컬럼만 덧붙인다 (기존 데이터는 그대로)
        try { await pool.query(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS ${col} TEXT NOT NULL DEFAULT '{}'`); }
        catch (e) { console.error(`컬럼 추가 실패(${col}):`, e.message); }
      }
      console.log('DB 연결됨: 그림이 영구 저장돼요');
    })()
  : Promise.resolve(console.log('DATABASE_URL 없음: 서버가 재시작되면 그림이 사라져요'));

async function loadRoom(id) {
  if (!pool) return { history: [], names: {}, meta: null };
  try {
    await dbReady;
    const r = await pool.query('SELECT history, names, meta FROM rooms WHERE id = $1', [id]);
    if (!r.rows.length) return { history: [], names: {}, meta: null };
    const row = r.rows[0];
    return { history: JSON.parse(row.history), names: JSON.parse(row.names || '{}'), meta: JSON.parse(row.meta || '{}') };
  } catch (e) {
    // 읽기에 실패한 채로 저장하면 기존 그림을 덮어쓰게 되므로, 이 방은 저장하지 않는다
    console.error('불러오기 실패 (이 방은 저장하지 않아요):', e.message);
    return { history: [], names: {}, meta: null, failed: true };
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
    purgeMeta(room);
    await saveRoom(id, room);
    await saveMeta(id, room);
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
    const room = { id, clients: new Set(), history: [], names: {}, meta: normalizeMeta(null), dirty: false, metaDirty: false, noSave: false,
      chat: [], chatSeq: 0, leaveTimers: new Map(), recent: new Map(), rate: new Map() };
    room.ready = loadRoom(id).then((d) => {
      room.history = Array.isArray(d.history) ? d.history : [];
      room.names = d.names && typeof d.names === 'object' && !Array.isArray(d.names) ? d.names : {};
      room.noSave = !!d.failed;
      room.meta = normalizeMeta(d.meta);
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

// ---- 방 설정(meta): 방장, 밴, 채팅 금지, 관전, 잠금 + 채팅 (2단계) ----
const OWNER_RE = /^[A-Za-z0-9_-]{1,32}$/;
const send = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const isGuest = (o) => String(o).startsWith('g');                       // 키 없는 접속은 방장이 될 수 없다
const isRoomOwner = (room, o) => !room.noSave && !!room.meta.owner && room.meta.owner === o;
const hasOwner = (room, o) => { for (const c of room.clients) if (c.owner === o) return true; return false; };

function cleanText(s, max) {
  const t = String(s == null ? '' : s).normalize('NFC').replace(/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/gu, '').replace(/\s+/g, ' ').trim();
  return Array.from(t).slice(0, max).join('');
}
const cleanReason = (s) => cleanText(s, REASON_MAX);
// 채팅: 제어문자·방향 제어문자 제거 (이모지용 ZWJ U+200D 는 허용), 최대 200자
function cleanChat(s) {
  if (typeof s !== 'string') return '';
  const t = s.normalize('NFC')
    .replace(/[\p{Cc}\p{Zl}\p{Zp}\u00AD\u061C\u200B\u200C\u200E\u200F\u202A-\u202E\u2060-\u2064\u2066-\u206F\uFEFF]/gu, '')
    .trim();
  return Array.from(t).slice(0, CHAT_MAX_CHARS).join('');
}
const fmtDur = (min) => (min === 0 ? '영구' : min % 60 === 0 ? `${min / 60}시간` : min > 60 ? `${Math.floor(min / 60)}시간 ${min % 60}분` : `${min}분`);
function fmtRemain(ms) {
  const sec = Math.max(0, Math.ceil(ms / 1000));
  if (sec < 60) return `${sec}초`;
  if (sec < 3600) return `${Math.ceil(sec / 60)}분`;
  return `${Math.floor(sec / 3600)}시간 ${Math.floor((sec % 3600) / 60)}분`;
}

function normalizeMeta(j) {
  const m = { owner: null, locked: false, bans: {}, mutes: {}, spectators: {} };
  if (!j || typeof j !== 'object') return m;
  if (typeof j.owner === 'string' && OWNER_RE.test(j.owner)) m.owner = j.owner;
  m.locked = j.locked === true;
  for (const key of ['bans', 'mutes', 'spectators']) {
    const src = j[key];
    if (!src || typeof src !== 'object') continue;
    for (const o of Object.keys(src).slice(0, MAX_LIST)) {
      const v = src[o];
      if (!v || typeof v !== 'object' || !OWNER_RE.test(o)) continue;
      m[key][o] = { until: Number.isFinite(+v.until) ? +v.until : 0, name: cleanNick(v.name) || '알 수 없음', reason: cleanReason(v.reason) };
    }
  }
  return m;
}
// 만료된 밴·채팅 금지를 정리한다
function purgeMeta(room, now = Date.now()) {
  let changed = false;
  const unmuted = [];
  for (const key of ['bans', 'mutes']) {
    for (const o of Object.keys(room.meta[key])) {
      const u = room.meta[key][o].until;
      if (u > 0 && u <= now) { delete room.meta[key][o]; changed = true; if (key === 'mutes') unmuted.push(o); }
    }
  }
  if (changed) { room.metaDirty = true; for (const o of unmuted) sendMe(room, o); sendAdmin(room); }
  return changed;
}
// 방 설정은 작아서 바뀌는 즉시 저장한다 (행이 아직 없는 방도 저장되도록 UPSERT, history 는 건드리지 않는다)
async function saveMeta(id, room) {
  if (!pool || room.noSave || !room.metaDirty) return;
  room.metaDirty = false;
  try {
    await pool.query(
      'INSERT INTO rooms (id, history, names, meta, updated_at) VALUES ($1, $2, $3, $4, now()) ON CONFLICT (id) DO UPDATE SET meta = $4, updated_at = now()',
      [id, '[]', '{}', JSON.stringify(room.meta)]
    );
  } catch (e) {
    room.metaDirty = true;
    console.error('방 설정 저장 실패:', e.message);
  }
}
function persistMeta(room) { room.metaDirty = true; saveMeta(room.id, room); }

const roomStateMsg = (room) => ({ type: 'roomstate', owner: room.noSave ? null : room.meta.owner, locked: room.meta.locked });
function meMsg(room, o, now = Date.now()) {
  const mu = room.meta.mutes[o];
  return { type: 'me', muted: mu && (mu.until === 0 || mu.until > now) ? { until: mu.until, now, reason: mu.reason || '' } : null, spectator: !!room.meta.spectators[o] };
}
function adminMsg(room) {
  const L = (src) => Object.keys(src).map((o) => ({ o, name: src[o].name, until: src[o].until, reason: src[o].reason || '' }));
  return { type: 'admin', now: Date.now(), locked: room.meta.locked, bans: L(room.meta.bans), mutes: L(room.meta.mutes), spectators: L(room.meta.spectators) };
}
function sendAdmin(room) { for (const c of room.clients) if (isRoomOwner(room, c.owner)) send(c, adminMsg(room)); }
function sendMe(room, o) { for (const c of room.clients) if (c.owner === o) send(c, meMsg(room, o)); }

function pushChat(room, m) {
  m.id = ++room.chatSeq; m.t = Date.now();
  room.chat.push(m);
  if (room.chat.length > MAX_CHAT) room.chat.shift();
  broadcastAll(room, { type: 'chat', m });
}
const sysChat = (room, text) => pushChat(room, { sys: true, text });
function chatAllow(room, o, now) {                      // 2초에 3개 (사람 기준, 탭을 여러 개 열어도 같은 한도)
  const a = room.rate.get(o) || [];
  while (a.length && a[0] <= now - 2000) a.shift();
  if (a.length >= 3) { room.rate.set(o, a); return false; }
  a.push(now); room.rate.set(o, a);
  return true;
}
function byeAndClose(ws, info) {                        // 서버가 의도적으로 끊을 때: 안내 메시지 + 4000번대 코드 (클라이언트는 자동 재접속하지 않는다)
  send(ws, { type: 'bye', ...info, now: Date.now() });
  ws.noLeaveMsg = true;
  try { ws.close(BYE_CODE[info.code], info.code); } catch {}
}

// 접속 승인: 방장 지정 → 밴 → 잠금 (거부하면 안내 정보를 돌려준다)
function admit(room, roomId, owner) {
  const now = Date.now();
  purgeMeta(room, now);
  if (roomId !== 'lobby' && !room.noSave && !room.meta.owner && !isGuest(owner)) {
    room.meta.owner = owner;                            // 방장 정보가 없는 방에 처음 도착한 사람이 방장이 된다
    persistMeta(room);
  }
  if (isRoomOwner(room, owner)) return null;            // 방장은 밴·잠금과 무관하게 항상 입장
  const b = room.meta.bans[owner];
  if (b && (b.until === 0 || b.until > now)) return { code: 'banned', text: '방장에 의해 밴되었습니다' + (b.reason ? `(${b.reason})` : ''), until: b.until, reason: b.reason || '' };
  if (room.meta.locked && !hasOwner(room, owner) && !((room.recent.get(owner) || 0) > now - LOCK_GRACE_MS)) return { code: 'locked', text: '방장이 새 접속을 막아 두었어요. 잠시 뒤 다시 시도해 주세요.' };
  return null;
}
function approve(ws, room, wantName) {
  const owner = ws.owner;
  ws.approved = true;
  const wasPresent = hasOwner(room, owner);
  room.clients.add(ws);
  room.recent.set(owner, Date.now());
  const name = setName(room, owner, wantName);
  send(ws, { type: 'hello', me: owner });
  send(ws, { type: 'history', data: room.history });
  send(ws, { type: 'names', names: room.names });
  send(ws, roomStateMsg(room));
  send(ws, meMsg(room, owner));
  if (isRoomOwner(room, owner)) send(ws, adminMsg(room));
  send(ws, { type: 'chats', list: room.chat });
  broadcastAll(room, { type: 'name', o: owner, name });
  announce(room);
  const t = room.leaveTimers.get(owner);                // 10초 안에 다시 들어오면(앱 전환 등) 입장/퇴장 메시지를 내지 않는다
  if (t) { clearTimeout(t); room.leaveTimers.delete(owner); }
  else if (!wasPresent) sysChat(room, `${name}님이 들어왔어요`);
}

// 방장 명령: 모든 명령은 서버가 방장 여부를 검사한다
function cmdErr(ws, text) { send(ws, { type: 'cmderr', text }); }
function checkMinutes(v) { return Number.isInteger(v) && (v === 0 || (v >= 1 && v <= BAN_MAX_MIN)); }
function handleCmd(ws, room, roomId, msg) {
  const now = Date.now();
  ws.cmdTok = Math.min(5, ws.cmdTok + (now - ws.cmdAt) * 0.005); ws.cmdAt = now;
  if (ws.cmdTok < 1) return cmdErr(ws, '명령을 너무 빠르게 보내고 있어요');
  ws.cmdTok -= 1;
  if (room.noSave) return cmdErr(ws, '이 방은 저장소를 읽지 못해 방장 기능을 쓸 수 없어요');
  if (!isRoomOwner(room, ws.owner)) return cmdErr(ws, '권한이 없습니다');
  const cmd = msg.cmd;
  const TARGET_CMDS = ['kick', 'ban', 'unban', 'mute', 'unmute', 'spectate', 'transfer', 'clearuser'];
  let target = null;
  if (TARGET_CMDS.includes(cmd)) {
    target = typeof msg.target === 'string' && OWNER_RE.test(msg.target) ? msg.target : null;
    if (!target) return cmdErr(ws, '대상이 올바르지 않아요');
    if (target === room.meta.owner) return cmdErr(ws, '방장에게는 쓸 수 없는 명령이에요');
  }
  const online = target ? [...room.clients].filter((c) => c.owner === target) : [];
  const nm = target ? (room.names[target] || (room.meta.bans[target] || room.meta.mutes[target] || room.meta.spectators[target] || {}).name || '알 수 없음') : '';
  const reason = cleanReason(msg.reason);
  const minutes = msg.minutes;
  if (cmd === 'kick') {
    if (!online.length) return cmdErr(ws, '접속 중인 사용자가 아니에요');
    for (const c of online) byeAndClose(c, { code: 'kicked', text: '방장에 의해 강퇴되었습니다' + (reason ? `(${reason})` : ''), reason });
    sysChat(room, `${nm}님이 강퇴되었습니다`);
  } else if (cmd === 'ban') {
    if (!online.length) return cmdErr(ws, '접속 중인 사용자가 아니에요');
    if (!checkMinutes(minutes)) return cmdErr(ws, '밴 시간이 올바르지 않아요 (1~43200분 또는 영구)');
    if (Object.keys(room.meta.bans).length >= MAX_LIST && !room.meta.bans[target]) return cmdErr(ws, '밴 목록이 가득 찼어요');
    const until = minutes === 0 ? 0 : now + minutes * MIN_MS;
    room.meta.bans[target] = { until, name: nm, reason };
    persistMeta(room);
    for (const c of online) byeAndClose(c, { code: 'banned', text: '방장에 의해 밴되었습니다' + (reason ? `(${reason})` : ''), until, reason });
    sysChat(room, minutes === 0 ? `${nm}님이 영구 밴되었습니다` : `${nm}님이 ${fmtDur(minutes)} 동안 밴되었습니다`);
    sendAdmin(room);
  } else if (cmd === 'unban') {
    if (!room.meta.bans[target]) return cmdErr(ws, '밴 목록에 없는 사용자예요');
    delete room.meta.bans[target]; persistMeta(room);
    sysChat(room, `${nm}님의 밴이 해제되었습니다`); sendAdmin(room);
  } else if (cmd === 'mute') {
    if (!online.length) return cmdErr(ws, '접속 중인 사용자가 아니에요');
    if (!checkMinutes(minutes)) return cmdErr(ws, '시간이 올바르지 않아요 (1~43200분 또는 해제할 때까지)');
    if (Object.keys(room.meta.mutes).length >= MAX_LIST && !room.meta.mutes[target]) return cmdErr(ws, '채팅 금지 목록이 가득 찼어요');
    room.meta.mutes[target] = { until: minutes === 0 ? 0 : now + minutes * MIN_MS, name: nm, reason };
    persistMeta(room); sendMe(room, target);
    sysChat(room, minutes === 0 ? `${nm}님의 채팅이 금지되었습니다` : `${nm}님의 채팅이 ${fmtDur(minutes)} 동안 금지되었습니다`);
    sendAdmin(room);
  } else if (cmd === 'unmute') {
    if (!room.meta.mutes[target]) return cmdErr(ws, '채팅 금지 목록에 없는 사용자예요');
    delete room.meta.mutes[target]; persistMeta(room); sendMe(room, target);
    sysChat(room, `${nm}님의 채팅 금지가 해제되었습니다`); sendAdmin(room);
  } else if (cmd === 'spectate') {
    if (msg.on === true) {
      if (!online.length) return cmdErr(ws, '접속 중인 사용자가 아니에요');
      if (Object.keys(room.meta.spectators).length >= MAX_LIST && !room.meta.spectators[target]) return cmdErr(ws, '관전 목록이 가득 찼어요');
      room.meta.spectators[target] = { until: 0, name: nm, reason: '' };
    } else {
      if (!room.meta.spectators[target]) return cmdErr(ws, '관전 목록에 없는 사용자예요');
      delete room.meta.spectators[target];
    }
    persistMeta(room); sendMe(room, target);
    sysChat(room, msg.on === true ? `${nm}님이 관전(보기만 가능)으로 바뀌었습니다` : `${nm}님의 그리기 제한이 해제되었습니다`);
    sendAdmin(room);
  } else if (cmd === 'lock') {
    room.meta.locked = msg.on === true; persistMeta(room);
    broadcastAll(room, roomStateMsg(room));
    sysChat(room, room.meta.locked ? '방이 잠겼습니다 (새 접속을 막아요)' : '방 잠금이 해제되었습니다');
    sendAdmin(room);
  } else if (cmd === 'transfer') {
    if (!online.length || isGuest(target)) return cmdErr(ws, '접속 중인 다른 사용자에게만 넘길 수 있어요');
    const old = ws.owner, oldName = room.names[old] || '방장';
    room.meta.owner = target; persistMeta(room);
    broadcastAll(room, roomStateMsg(room));
    sendMe(room, old); sendMe(room, target); sendAdmin(room);
    sysChat(room, `${oldName}님이 ${nm}님에게 방장을 넘겼습니다`);
  } else if (cmd === 'clearall') {
    room.history = []; room.dirty = true;
    broadcastAll(room, { type: 'clearall' });
    saveRoom(roomId, room);
    sysChat(room, '방장이 모든 그림을 지웠습니다');
  } else if (cmd === 'clearuser') {
    const before = room.history.length;
    room.history = room.history.filter((s) => s.o !== target);
    if (room.history.length !== before) { room.dirty = true; broadcastAll(room, { type: 'clear', o: target }); saveRoom(roomId, room); }
    sysChat(room, `${nm}님의 그림이 모두 지워졌습니다`);
  } else {
    cmdErr(ws, '알 수 없는 명령이에요');
  }
}
function undoAllow(ws) {                              // 이전/되돌리기 일괄 메시지: 초당 15개
  const now = Date.now();
  ws.undoTok = Math.min(30, ws.undoTok + (now - ws.undoAt) * 0.015); ws.undoAt = now;
  if (ws.undoTok < 1) return false;
  ws.undoTok -= 1;
  return true;
}
// 접속 단위 전체 메시지 상한 (도배로 서버 CPU 를 점유하는 것 방지). 초과분은 조용히 버린다.
function globalAllow(ws) {
  const now = Date.now();
  ws.gTok = Math.min(1000, ws.gTok + (now - ws.gAt) * 0.8);
  ws.gAt = now;
  if (ws.gTok < 1) return false;
  ws.gTok -= 1;
  return true;
}

// ---- 웹 서버 ----
// 어느 파일이 올라갔는지 바로 확인하는 용도 (화면의 설정 맨 아래에 '서버 버전'으로 보인다)
app.get('/version', (req, res) => res.json({ stage: 2, date: '2026-10-03' }));
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

const DRAW_TYPES = new Set(['seg', 'clear', 'erase', 'erasestroke', 'cancelstroke', 'addsegs', 'removesegs']);
function onMessage(ws, room, roomId, owner, raw) {
  try {
    if (!globalAllow(ws)) return;
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg !== 'object') return;
    const now = Date.now();
    if (DRAW_TYPES.has(msg.type) && room.meta.spectators[owner]) {   // 관전(보기만 가능)으로 바뀐 사람은 그리기를 할 수 없다
      notice(ws, 'spectator', '방장이 그리기를 제한했어요 (보기만 가능)');
      return;
    }
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
      // 지우개: 기본은 내가 그린 선만. 방장은 all 로 남의 선도 지울 수 있다. 잘린 결과를 다른 사람 화면도 같은 함수로 계산한다.
      if (!allowSeg(ws)) return;
      const x0 = r1(msg.x0), y0 = r1(msg.y0), x1 = r1(msg.x1), y1 = r1(msg.y1), r = r1(msg.r);
      if (![x0, y0, x1, y1, r].every(okNum)) return;
      const all = msg.all === true && isRoomOwner(room, owner);
      const res = eraseCapsule(room.history, all ? null : owner, x0, y0, x1, y1, r);
      if (!res || res.length > MAX_HISTORY * 1.5) return;
      room.history = res;
      room.dirty = true;
      broadcast(room, ws, { type: 'erase', o: all ? null : owner, x0, y0, x1, y1, r });
    } else if (msg.type === 'erasestroke') {
      // 획 지우개: 경로에 닿은 획 전체를 지우고, 지워진 획 목록을 모두에게 알린다 (받는 쪽은 재계산하지 않는다)
      if (!allowSeg(ws)) return;
      const x0 = r1(msg.x0), y0 = r1(msg.y0), x1 = r1(msg.x1), y1 = r1(msg.y1), r = r1(msg.r);
      if (![x0, y0, x1, y1, r].every(okNum)) return;
      const all = msg.all === true && isRoomOwner(room, owner);
      const q = Number.isInteger(msg.q) && msg.q >= 0 && msg.q < 1e9 ? msg.q : 0;
      const keys = hitStrokes(room.history, all ? null : owner, x0, y0, x1, y1, r, MAX_STROKES_PER_ERASE);
      if (keys.length) {
        const res = removeStrokes(room.history, keys);
        if (res) { room.history = res; room.dirty = true; }
        broadcastAll(room, { type: 'strokesgone', keys, by: owner, q });
      } else send(ws, { type: 'strokesgone', keys: [], by: owner, q }); // 보낸 사람이 예상과 다른지 확인할 수 있게
    } else if (msg.type === 'cancelstroke') {
      // 잘못 시작된 내 획 취소 (모두의 화면과 저장본에서 지움)
      if (!allowSeg(ws)) return;
      if (typeof msg.sid !== 'string' || !SID_RE.test(msg.sid)) return;
      const keys = [[owner, msg.sid]];
      const res = removeStrokes(room.history, keys);
      if (res) { room.history = res; room.dirty = true; broadcastAll(room, { type: 'strokesgone', keys, by: owner, cancel: true }); }
    } else if (msg.type === 'addsegs') {
      // 되돌리기(redo)·지우개 이전(undo)으로 내 선분을 다시 넣는다 (최대 100개, 주인은 항상 나)
      if (!undoAllow(ws) || !Array.isArray(msg.segs)) return;
      const list = [];
      for (const m of msg.segs.slice(0, 100)) {
        if (!m || typeof m !== 'object') continue;
        const s = { x0: r1(m.x0), y0: r1(m.y0), x1: r1(m.x1), y1: r1(m.y1), color: String(m.color).slice(0, 9), size: Math.min(Math.max(+m.size || 4, 1), PEN_MAX), o: owner, sid: m.sid };
        if (![s.x0, s.y0, s.x1, s.y1].every(okNum) || typeof s.sid !== 'string' || !SID_RE.test(s.sid)) continue;
        if (room.history.length + list.length >= MAX_HISTORY) { notice(ws, 'full', '그림이 가득 찼어요. 일부를 지우고 다시 그려 주세요.'); break; }
        list.push(s);
      }
      if (!list.length) return;
      for (const s of list) room.history.push(s);
      room.dirty = true;
      broadcast(room, ws, { type: 'segs', list });
    } else if (msg.type === 'removesegs') {
      // 이전(undo): 내 선분 중 값이 같은 것을 정확히 지운다 (잘린 조각 되돌리기)
      if (!undoAllow(ws) || !Array.isArray(msg.segs)) return;
      const items = [];
      for (const m of msg.segs.slice(0, 100)) {
        if (!m || typeof m !== 'object' || typeof m.sid !== 'string' || !SID_RE.test(m.sid)) continue;
        const s = { x0: r1(m.x0), y0: r1(m.y0), x1: r1(m.x1), y1: r1(m.y1), o: owner, sid: m.sid };
        if ([s.x0, s.y0, s.x1, s.y1].every(okNum)) items.push(s);
      }
      if (!items.length) return;
      const res = removeExactSegs(room.history, items);
      if (!res) return;
      room.history = res;
      room.dirty = true;
      broadcast(room, ws, { type: 'unsegs', list: items });
    } else if (msg.type === 'resync') {
      if (now - ws.lastSync < 2000) return;
      ws.lastSync = now;
      send(ws, { type: 'history', data: room.history });
    } else if (msg.type === 'cur') {
      if (now - ws.lastCur < 25) return;
      ws.lastCur = now;
      const x = r1(msg.x), y = r1(msg.y);
      if (!okNum(x) || !okNum(y)) return;
      broadcast(room, ws, { type: 'cur', o: owner, x, y });
    } else if (msg.type === 'curhide') {
      broadcast(room, ws, { type: 'curhide', o: owner });
    } else if (msg.type === 'nick') {
      if (now - ws.lastNick < 300) return;
      ws.lastNick = now;
      broadcastAll(room, { type: 'name', o: owner, name: setName(room, owner, msg.name) });
    } else if (msg.type === 'chat') {
      const mu = room.meta.mutes[owner];
      if (mu && (mu.until === 0 || mu.until > now)) {
        send(ws, { type: 'notice', code: 'muted', text: mu.until === 0 ? '채팅이 금지되었어요 (해제될 때까지)' : `채팅이 금지되었어요 (남은 시간 ${fmtRemain(mu.until - now)})` });
        return;
      }
      if (!chatAllow(room, owner, now)) { send(ws, { type: 'notice', code: 'chatrate', text: '너무 빠르게 보내고 있어요' }); return; }
      const text = cleanChat(msg.text);
      if (!text) return;
      pushChat(room, { o: owner, name: room.names[owner] || '익명', text });   // 시각과 닉네임은 서버가 붙인다 (그 시점의 이름으로 저장)
    } else if (msg.type === 'cmd') {
      handleCmd(ws, room, roomId, msg);
    }
  } catch (e) {
    console.error('메시지 처리 오류:', e.message);
  }
}

function onClose(ws, room, roomId, owner) {
  if (!ws.approved) return;
  room.clients.delete(ws);
  broadcastAll(room, { type: 'curhide', o: owner });
  announce(room);
  if (!hasOwner(room, owner)) {
    room.recent.set(owner, Date.now());
    if (!ws.noLeaveMsg) {                                              // 강퇴·밴은 이미 시스템 메시지가 있으니 퇴장 메시지는 생략
      const nm = room.names[owner] || '누군가';
      room.leaveTimers.set(owner, setTimeout(() => {
        room.leaveTimers.delete(owner);
        if (!hasOwner(room, owner)) sysChat(room, `${nm}님이 나갔어요`);
      }, LEAVE_GRACE_MS));
    }
  }
  if (room.clients.size === 0) {
    pruneNames(room);
    saveRoom(roomId, room);
    saveMeta(roomId, room);
    setTimeout(() => {
      const r = rooms.get(roomId);
      if (r && r.clients.size === 0) rooms.delete(roomId);
    }, 10 * 60 * 1000);
  }
}

wss.on('connection', async (ws, req) => {
  ws.on('error', (e) => console.error('ws error:', e.message)); // 없으면 잘못된 프레임 하나에 서버가 죽는다

  const url = new URL(req.url, 'http://localhost');
  const rawRoom = url.searchParams.get('room') || 'lobby';
  const roomId = ROOM_RE.test(rawRoom) ? rawRoom : 'lobby';
  const room = getRoom(roomId);

  // 브라우저가 보낸 비밀 키를 해시해 '주인 ID'로 쓴다. 키가 없으면 이번 접속에서만 주인.
  const key = url.searchParams.get('k') || '';
  const owner = /^[A-Za-z0-9_-]{16,64}$/.test(key)
    ? crypto.createHash('sha256').update(key).digest('base64url').slice(0, 10)
    : 'g' + crypto.randomBytes(5).toString('hex');
  ws.owner = owner;
  const t0 = Date.now();
  ws.approved = false;
  ws.tokens = 400; ws.tokAt = t0; ws.gTok = 1000; ws.gAt = t0; ws.cmdTok = 5; ws.cmdAt = t0; ws.undoTok = 30; ws.undoAt = t0;
  ws.lastCur = 0; ws.lastNick = 0; ws.lastSync = 0;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => { if (ws.approved) onMessage(ws, room, roomId, owner, raw); });   // 승인 전에 도착한 메시지는 무시한다
  ws.on('close', () => onClose(ws, room, roomId, owner));

  await room.ready;
  if (ws.readyState !== 1) return;
  const verdict = admit(room, roomId, owner);          // 1) 방장 지정  2) 밴  3) 잠금
  if (verdict) { byeAndClose(ws, verdict); return; }   // 거부: clients 에 넣지 않고 입장·접속자 수 알림도 하지 않는다
  approve(ws, room, url.searchParams.get('n'));        // 승인: 초기 동기화
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('http://localhost:' + PORT));
