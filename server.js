const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const MAX_HISTORY = 100000;
const SAVE_MS = +process.env.SAVE_MS || 5000;
const rooms = new Map(); // roomId -> { clients, history, dirty, ready }

// ---- 저장소: DATABASE_URL 이 있으면 DB, 없으면 메모리만 ----
const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 3 })
  : null;

const dbReady = pool
  ? pool.query('CREATE TABLE IF NOT EXISTS rooms (id TEXT PRIMARY KEY, history TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT now())')
      .then(() => console.log('DB 연결됨: 그림이 영구 저장돼요'))
      .catch((e) => console.error('DB 초기화 실패 (메모리로만 동작):', e.message))
  : Promise.resolve(console.log('DATABASE_URL 없음: 서버가 재시작되면 그림이 사라져요'));

async function loadHistory(id) {
  if (!pool) return [];
  try {
    await dbReady;
    const r = await pool.query('SELECT history FROM rooms WHERE id = $1', [id]);
    return r.rows.length ? JSON.parse(r.rows[0].history) : [];
  } catch (e) {
    console.error('불러오기 실패:', e.message);
    return [];
  }
}

async function saveRoom(id, room) {
  if (!pool || !room.dirty) return;
  room.dirty = false;
  try {
    await pool.query(
      'INSERT INTO rooms (id, history, updated_at) VALUES ($1, $2, now()) ON CONFLICT (id) DO UPDATE SET history = $2, updated_at = now()',
      [id, JSON.stringify(room.history)]
    );
  } catch (e) {
    room.dirty = true;
    console.error('저장 실패:', e.message);
  }
}

async function saveAll() {
  for (const [id, room] of rooms) await saveRoom(id, room);
}
setInterval(saveAll, SAVE_MS);
process.on('SIGTERM', async () => { await saveAll(); process.exit(0); });

function getRoom(id) {
  if (!rooms.has(id)) {
    const room = { clients: new Set(), history: [], dirty: false };
    room.ready = loadHistory(id).then((h) => { room.history = h; });
    rooms.set(id, room);
  }
  return rooms.get(id);
}

// ---- 웹 서버 ----
app.get('/', (req, res, next) => {
  if (!req.query.room) return res.redirect('/?room=' + Math.random().toString(36).slice(2, 8));
  next();
});
app.use(express.static(path.join(__dirname, 'public')));

wss.on('connection', async (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const roomId = (url.searchParams.get('room') || 'lobby').slice(0, 32);
  const room = getRoom(roomId);
  room.clients.add(ws);

  // 브라우저가 보낸 비밀 키를 해시해 '주인 ID'로 쓴다. 키가 없으면 이번 접속에서만 주인.
  const key = url.searchParams.get('k') || '';
  const owner = /^[A-Za-z0-9_-]{16,64}$/.test(key)
    ? crypto.createHash('sha256').update(key).digest('base64url').slice(0, 10)
    : 'g' + crypto.randomBytes(5).toString('hex');
  ws.send(JSON.stringify({ type: 'hello', me: owner }));

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    await room.ready;
    if (msg.type === 'seg') {
      const r1 = (n) => Math.round(+n * 10) / 10;
      const s = {
        x0: r1(msg.x0), y0: r1(msg.y0), x1: r1(msg.x1), y1: r1(msg.y1),
        color: String(msg.color).slice(0, 9),
        size: Math.min(Math.max(+msg.size || 4, 1), 60),
        o: owner, // 클라이언트가 보낸 값은 무시하고 서버가 정한다
      };
      if ([s.x0, s.y0, s.x1, s.y1].some((v) => !isFinite(v) || Math.abs(v) > 1e7)) return;
      if (room.history.length < MAX_HISTORY) { room.history.push(s); room.dirty = true; }
      broadcast(room, ws, { type: 'seg', ...s });
    } else if (msg.type === 'clear') {
      // 내가 그린 선만 지운다 (다른 사람 그림은 그대로)
      room.history = room.history.filter((s) => s.o !== owner);
      room.dirty = true;
      broadcast(room, ws, { type: 'clear', o: owner });
      saveRoom(roomId, room);
    }
  });

  ws.on('close', () => {
    room.clients.delete(ws);
    if (room.clients.size === 0) {
      saveRoom(roomId, room);
      setTimeout(() => {
        const r = rooms.get(roomId);
        if (r && r.clients.size === 0) rooms.delete(roomId);
      }, 10 * 60 * 1000);
    }
  });

  await room.ready;
  if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'history', data: room.history }));
});

function broadcast(room, sender, obj) {
  const data = JSON.stringify(obj);
  for (const c of room.clients) if (c !== sender && c.readyState === 1) c.send(data);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('http://localhost:' + PORT));
