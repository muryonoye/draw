// Cloudflare 판 서버: Worker(입구) + Durable Object(방 하나 = 객체 하나, 저장은 그 객체 안의 SQLite).
// 방·그림·방장 규칙은 core.js 를 그대로 쓴다 (Node 판 server.js 와 같은 코드).
// 무료 플랜: 요청 하루 10만 번(받은 WebSocket 메시지는 20개 = 1번), 객체가 깨어 있는 시간 하루 13,000 GB-s, 넘으면 그날은 오류 (돈은 안 나간다). 전송량 요금 없음.
// 방 객체는 할 일이 없으면 잠들고(접속은 그대로), 오래 쉬면 메모리에서 내려간다 → 다시 깨어날 때 SQLite 에서 방을, 소켓에 적어 둔 정보에서 접속을 되살린다.
import { DurableObject } from 'cloudflare:workers';
import { Buffer } from 'node:buffer';
import { createHmac, createHash, timingSafeEqual } from 'node:crypto';
import core from '../core.js';

// 방 하나의 저장소 (그 방 객체의 SQLite). 표: kv(history 'p1:…'·names·meta·extra·roomId), parts(덩어리 번호 → 'z2:'+base64). Node 판 Postgres 와 같은 내용·형식.
// importOld(id): 이 방 객체가 비어 있을 때 한 번, 예전 저장소에서 옮겨 올 방 { head, names, meta, parts } 을 돌려준다 (없으면 null, 못 읽으면 throw)
function sqlStore(storage, importOld) {
  const sql = storage.sql;
  sql.exec('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)');
  sql.exec('CREATE TABLE IF NOT EXISTS parts (pid INTEGER PRIMARY KEY, data TEXT NOT NULL)');
  // 되감기 사본 (✦ Plus 방): snaps = 때·조각 수·이름·나눈 수, snapparts = 900KB 씩 나눈 내용 (값 하나 2MB 한도)
  sql.exec("CREATE TABLE IF NOT EXISTS snaps (at INTEGER PRIMARY KEY, n INTEGER NOT NULL, label TEXT NOT NULL, k INTEGER NOT NULL, h TEXT NOT NULL DEFAULT '')");
  sql.exec('CREATE TABLE IF NOT EXISTS snapparts (at INTEGER NOT NULL, i INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (at, i))');
  const SNAP_CHUNK = 900000;
  const get = (k) => { const r = sql.exec('SELECT v FROM kv WHERE k = ?', k).toArray()[0]; return r ? r.v : null; };
  const put = (k, v) => { sql.exec('INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v', k, v); };
  let savedNames = null;                                          // 마지막으로 쓴 이름 목록
  const row = () => {
    const h = get('history'), m = get('meta');
    if (h === null && m === null) return null;
    return { history: h === null ? '[]' : h, names: get('names') || '{}', meta: m || '{}', extra: get('extra') };
  };
  // 옮겨 온 방 쓰기 (덩어리 형식). 이미 그림·설정이 있으면 하지 않는다
  const writeImported = (p, how) => storage.transactionSync(() => {
    if (get('history') !== null || get('meta') !== null) return false;
    sql.exec('DELETE FROM parts');
    for (const x of p.parts) sql.exec('INSERT INTO parts (pid, data) VALUES (?, ?)', x.pid, x.data);
    put('history', p.head); put('names', p.names); put('meta', p.meta); put('migrated', how);
    return true;
  });
  return {
    get, put, writeImported,
    async loadRow(id) {
      if (!row() && importOld && get('migrated') === null) {          // 처음 열리는 방: 예전 저장소에 있으면 옮겨 온다 (실패하면 throw → 다음에 다시)
        const p = await importOld(id);
        if (p) { writeImported(p, 'yes ' + new Date().toISOString()); console.log(`방 ${id}: 예전 저장소에서 옮겨 옴 (${p.head.split(':')[1]} 조각)`); }
        else put('migrated', 'none');
      }
      return row();
    },
    async loadParts() { return sql.exec('SELECT pid, data FROM parts').toArray(); },
    async saveRoom(id, { clear, put: ps, del, head, names }) {      // 한 트랜잭션: 중간에 실패하면 아무것도 바뀌지 않는다
      const nm = head !== null && names !== savedNames;              // (이름 목록은 바뀌었을 때만 쓴다: 무료 플랜은 쓴 줄 수를 하루 10만까지 센다)
      storage.transactionSync(() => {
        if (clear) sql.exec('DELETE FROM parts');
        for (const p of ps) sql.exec('INSERT INTO parts (pid, data) VALUES (?, ?) ON CONFLICT(pid) DO UPDATE SET data = excluded.data', p.pid, p.data);
        for (const pid of del) sql.exec('DELETE FROM parts WHERE pid = ?', pid);
        if (head !== null) { put('history', head); if (nm) put('names', names); }
      });
      if (nm) savedNames = names;
    },
    async saveMeta(id, json) { put('meta', json); },
    snapSave(at, n, label, text, h) {
      const k = Math.max(1, Math.ceil(text.length / SNAP_CHUNK));
      storage.transactionSync(() => {
        sql.exec('DELETE FROM snapparts WHERE at = ?', at);
        sql.exec('INSERT OR REPLACE INTO snaps (at, n, label, k, h) VALUES (?, ?, ?, ?, ?)', at, n, label, k, h || '');
        for (let i = 0; i < k; i++) sql.exec('INSERT INTO snapparts (at, i, data) VALUES (?, ?, ?)', at, i, text.slice(i * SNAP_CHUNK, (i + 1) * SNAP_CHUNK));
      });
    },
    snapList() { return sql.exec('SELECT at, n, label, h FROM snaps ORDER BY at DESC').toArray(); },
    snapLoad(at) {
      const r = sql.exec('SELECT k FROM snaps WHERE at = ?', at).toArray()[0];
      if (!r) return null;
      const ps = sql.exec('SELECT data FROM snapparts WHERE at = ? ORDER BY i', at).toArray();
      return ps.length === r.k ? ps.map((x) => x.data).join('') : null;
    },
    snapDel(at) { storage.transactionSync(() => { sql.exec('DELETE FROM snaps WHERE at = ?', at); sql.exec('DELETE FROM snapparts WHERE at = ?', at); }); },
    async saveExtra(id, json) { put('extra', json); },
  };
}

// 예전 저장소(Render 때의 Postgres, Neon)에서 방 하나 읽기 → 덩어리 형식으로. DATABASE_URL 비밀값이 있을 때만 (옮겨 오는 기간에만 넣어 둔다)
async function readOld(dbUrl, id) {
  const { Client } = await import('pg');
  const info = core.parseDbUrl(dbUrl);
  const c = new Client({ connectionString: info.url, ssl: info.local ? false : { rejectUnauthorized: false }, connectionTimeoutMillis: 12000, query_timeout: 20000 });   // (Neon 무료는 쉬다가 깨어나는 데 몇 초 걸린다)
  await c.connect();
  try {
    const r = (await c.query('SELECT history, names, meta FROM rooms WHERE id = $1', [id])).rows[0];
    if (!r) return null;
    const parts = typeof r.history === 'string' && r.history.startsWith('p1:') ? (await c.query('SELECT pid, data FROM room_parts WHERE room_id = $1', [id])).rows : [];
    return await core.packRoom(r, parts);
  } finally { try { await c.end(); } catch {} }
}

// 같은 프로그램(isolate) 안에 방 객체가 여럿 있을 수 있어서 저장소는 방 id 로 찾는다
const stores = new Map();
// 방 id → 그 방 객체(WeakRef). 방 객체가 메모리에서 내려가 사라지면 그 방의 그림·저장소도 공용 목록에서 지운다 (같은 프로그램에 방이 쌓여 메모리 128MB 를 넘지 않게)
const live = new Map();
function pruneGone() { for (const [id, w] of live) if (!w.deref()) { live.delete(id); stores.delete(id); core.dropRoom(id); } }
// 방 객체가 깨어날 때 Plus 를 다시 묻지 않게: 아는 Plus 기간·꾸미기를 소켓에
const plusAtt = (ws) => { const pp = ws.room && ws.room.plus && ws.room.plus.get(ws.owner); return pp && !pp.asking && pp.until > Date.now() ? { pu: pp.until, pa: pp.at, d: pp.deco || {} } : {}; };
core.setPlatform({
  hasStore: true,
  storeFor: (id) => stores.get(id) || null,
  term: (ws) => core.closed(ws),
  clientCloses: true,
  // 강퇴·밴: 다른 사람의 메시지를 처리하는 중에 닫으면 닫기 인사가 상대에게 안 가는 경우가 있다 (쉬는 소켓 방식). 화면은 'bye' 를 받으면 스스로 끊으므로 방에서만 바로 뺀다
  afterBye: (ws) => core.closed(ws),
  // 접속 정보를 소켓에 적어 둔다 (방 객체가 메모리에서 내려갔다 깨어나도 누가 어떤 접속인지 알 수 있게, 최대 2KB)
  attach: (ws) => { try { ws.serializeAttachment({ o: ws.owner, ipt: ws.ipt || '', z: ws.zOk || 0, ap: ws.approved ? 1 : 0, e: ws.echoOk ? 1 : 0, ...(ws.knock ? { k: 1, t: ws.knock.tries | 0, n: String(ws.knock.n || '').slice(0, 40) } : {}), ...plusAtt(ws) }); } catch {} },
  // 되감기 사본: 그 방 객체의 SQLite
  snaps: {
    save: async (id, at, n, label, text, h) => { const st = stores.get(id); if (st) st.snapSave(at, n, label, text, h); },
    list: async (id) => { const st = stores.get(id); return st ? st.snapList() : []; },
    load: async (id, at) => { const st = stores.get(id); return st ? st.snapLoad(at) : null; },
    del: async (id, at) => { const st = stores.get(id); if (st) st.snapDel(at); },
  },
  snapLater: (id, ms) => { const w = live.get(id), obj = w && w.deref(); if (obj) obj.snapLater(ms); },   // (그 방 객체의 알람)
  // 화면이 버벅였다는 알림: Stats 객체에 모아 /stats 에서 본다
  onPerf: (roomId, r) => { if (ENV && ENV.STATS) statsObj(ENV).fetch('https://stats/perf', { method: 'POST', body: JSON.stringify({ room: roomId, ...r, at: Date.now() }) }).catch(() => {}); },
  // Plus: 그 사람(주인 ID)의 Plus 기간·꾸미기를 계정 저장소에서
  plusOf: async (o) => {
    if (!ENV || !ENV.ACCT) return null;
    const r = await acctObj(ENV).fetch('https://acct/plus/get', { method: 'POST', body: JSON.stringify({ o }) });
    return r.ok ? r.json() : null;
  },
});
let ENV = null;                                                  // (방 객체가 만들어질 때 적어 둔다: 위 onPerf 가 Stats 객체를 찾으려고)

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ENV = env;
    this.store = sqlStore(ctx.storage, env.DATABASE_URL ? (id) => readOld(env.DATABASE_URL, id) : null);
    this.id = this.store.get('roomId');               // 이 객체가 맡은 방 (처음 접속 때 적어 둔다)
    this.saveT = null;
    pruneGone();
    if (this.id) live.set(this.id, new WeakRef(this));
    // 화면이 다시 보일 때 보내는 확인 메시지는 방 객체를 깨우지 않고 바로 답한다
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"type":"ping"}', '{"type":"pong"}'));
    if (this.id) {
      stores.set(this.id, this.store);
      core.dropRoom(this.id);                          // (같은 프로그램에 남아 있던 예전 메모리는 버리고 저장소에서 다시 읽는다)
      const socks = ctx.getWebSockets();
      if (socks.length) {
        console.log(`방 ${this.id}: 다시 깨어남 (열린 접속 ${socks.length}개를 되살림)`);
        ctx.blockConcurrencyWhile(() => core.restore(this.id, socks.map((ws) => [ws, ws.deserializeAttachment() || {}])));
      }
    }
  }

  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === '/title') {                    // 링크 미리보기용 방 이름 (저장소에서 읽기만: 방을 불러오거나 아무것도 쓰지 않는다)
      let t = '';
      try { const m = JSON.parse(this.store.get('meta') || '{}'); t = m.secret ? '' : m.title || ''; } catch {}   // (비밀 방은 이름도 안 알려 준다)
      return new Response(String(t).slice(0, 60), { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
    }
    const raw = url.searchParams.get('room') || 'lobby', roomId = core.ROOM_RE.test(raw) ? raw : 'lobby';
    if (!this.id) { this.id = roomId; this.store.put('roomId', roomId); stores.set(roomId, this.store); live.set(roomId, new WeakRef(this)); }
    if (roomId !== this.id) return new Response('wrong room', { status: 400 });
    if (url.pathname === '/__test/seed') {             // 시험 전용 (LOCAL_DEV): 예전 저장소에 있던 방처럼 미리 넣어 두기
      if (this.env.LOCAL_DEV !== '1') return new Response('no', { status: 404 });
      const { row, parts } = await req.json();
      return Response.json({ ok: this.store.writeImported(await core.packRoom(row, parts || []), 'seed') });
    }
    if (url.pathname === '/__test/dump') {             // 시험 전용: 이 방 객체의 저장소 들여다보기
      if (this.env.LOCAL_DEV !== '1') return new Response('no', { status: 404 });
      const kv = {};
      for (const r of this.ctx.storage.sql.exec('SELECT k, v FROM kv').toArray()) kv[r.k] = r.v;
      const parts = this.ctx.storage.sql.exec('SELECT pid, data FROM parts').toArray();
      return Response.json({ kv, parts: Object.fromEntries(parts.map((x) => [x.pid, x.data])) });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    // 접속한 곳: Cloudflare 가 넣는 CF-Connecting-IP (브라우저가 바꿀 수 없다).
    // 시험(wrangler dev, LOCAL_DEV=1)에서는 모든 접속이 127.0.0.1 이라: 시험이 보낸 X-Test-IP 를 쓰고, 없으면 local (IP 기능·제한 없음)
    const dev = this.env.LOCAL_DEV === '1';
    const ip = (dev ? req.headers.get('X-Test-IP') : req.headers.get('CF-Connecting-IP')) || '';
    const local = !ip;
    core.connect(server, { url, cip: { ip, local } }).finally(() => { this.later(); this.report(); });   // (들어올 때도 크기를 알림: 10분에 한 번까지)
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    if (msg === '{"type":"__evict"}' && this.env.LOCAL_DEV === '1') return this.testEvict();
    if (msg === '{"type":"__crash"}' && this.env.LOCAL_DEV === '1') return this.testCrash();
    const bin = typeof msg !== 'string';
    core.message(ws, bin ? Buffer.from(msg) : Buffer.from(msg, 'utf8'), bin);
    this.later();
  }
  async webSocketClose(ws, code) {
    core.closed(ws);
    try { ws.close(code >= 1000 && code < 5000 && code !== 1005 && code !== 1006 ? code : 1000, 'bye'); } catch {}
    this.later();
  }
  async webSocketError(ws) { core.closed(ws); this.later(); }

  // 시험 전용: 방 객체가 메모리에서 내려갔다가 다시 깨어난 것처럼 (저장 → 메모리 버림 → 생성자와 같은 방법으로 되살림)
  // 시험 전용: 저장하지 않고 메모리를 버림 (배포로 방 객체가 다시 시작될 때처럼 — 마지막 저장 뒤의 변화가 사라진다)
  testCrash() {
    const room = core.rooms.get(this.id);
    if (room) for (const t of room.leaveTimers.values()) clearTimeout(t);
    if (this.saveT) { clearTimeout(this.saveT); this.saveT = null; }
    core.dropRoom(this.id);
    console.log(`방 ${this.id}: (시험) 저장 없이 다시 시작`);
  }
  async testEvict() {
    const room = core.rooms.get(this.id);
    if (room) { for (const t of room.leaveTimers.values()) clearTimeout(t); await core.maintainRoom(this.id, room, true); }
    if (this.saveT) { clearTimeout(this.saveT); this.saveT = null; }
    core.dropRoom(this.id);
    const socks = this.ctx.getWebSockets();
    console.log(`방 ${this.id}: (시험) 다시 깨어남 (열린 접속 ${socks.length}개를 되살림)`);
    await core.restore(this.id, socks.map((ws) => [ws, ws.deserializeAttachment() || {}]));
  }

  // 되감기 사본 알람: 이미 더 이른 알람이 있으면 그대로
  async snapLater(ms) {
    try { const at = Date.now() + ms, cur = await this.ctx.storage.getAlarm(); if (cur && cur <= at) return; await this.ctx.storage.setAlarm(at); } catch (e) { console.error('알람 설정 실패:', e.message); }
  }
  async alarm() {                                    // (방 객체가 잠들었으면 방을 불러와서 점검: 저장·되감기 사본)
    if (!this.id) return;
    try { const room = core.getRoom(this.id); await room.ready; await core.maintainRoom(this.id, room, true); this.report(); }
    catch (e) { console.error('알람 점검 오류:', e.message); }
  }

  // 바뀐 게 있으면 SAVE_MS 뒤 저장 (타이머가 도는 동안은 잠들지 않고, 다 저장하면 다시 잠들 수 있다)
  later() {
    if (this.saveT || !this.id) return;
    const room = core.rooms.get(this.id);
    if (!room || !(room.dirty || room.metaDirty || room.extraDirty)) return;
    this.saveT = setTimeout(async () => {
      this.saveT = null;
      try { await core.maintainRoom(this.id, room, false); } catch (e) { console.error('저장 점검 오류:', e.message); }
      this.report();
      this.later();
    }, core.SAVE_MS);
  }

  // 저장 크기 알리기 (들어올 때·저장한 뒤, 10분에 한 번까지. 그림 양이 크게 바뀌었으면 그 안이라도): 계정 전체 저장 공간(무료 5GB)을 얼마나 쓰는지 /stats 에서 보려고
  report() {
    const t = Date.now();
    if (!this.env.STATS) return;
    let segs = 0;
    const h = /^p1:(\d+):/.exec(this.store.get('history') || '');
    if (h) segs = +h[1];
    const last = this.reportedSegs ?? -1, big = Math.abs(segs - last) >= Math.max(500, last * 0.1) || (last <= 0 && segs > 0);
    if (this.reportedAt && t - this.reportedAt < STATS_EVERY_MS && !big) return;
    this.reportedAt = t; this.reportedSegs = segs;
    let bytes = 0;
    try { bytes = this.ctx.storage.sql.databaseSize; } catch {}
    let title = '';
    try { title = String(JSON.parse(this.store.get('meta') || '{}').title || '').slice(0, 30); } catch {}
    this.ctx.waitUntil(statsObj(this.env).fetch('https://stats/report', { method: 'POST', body: JSON.stringify({ id: this.id, bytes, segs, title, at: t }) }).catch(() => {}));
  }
}

// ── 저장 공간 사용량 (Stats 객체 하나: 방마다 마지막으로 알린 저장 크기·조각 수·때) ──
const STATS_EVERY_MS = 10 * 60 * 1000, FREE_STORAGE = 5 * 1024 ** 3;   // (Cloudflare 무료: Durable Objects 저장 계정 전체 5GB)
const statsObj = (env) => env.STATS.get(env.STATS.idFromName('stats'));
export class Stats extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec('CREATE TABLE IF NOT EXISTS rooms (id TEXT PRIMARY KEY, bytes INTEGER NOT NULL, segs INTEGER NOT NULL, title TEXT NOT NULL, at INTEGER NOT NULL)');
    this.sql.exec('CREATE TABLE IF NOT EXISTS perf (at INTEGER NOT NULL, data TEXT NOT NULL)');
    this.sql.exec('CREATE TABLE IF NOT EXISTS fails (k TEXT NOT NULL, at INTEGER NOT NULL)');
  }
  async fetch(req) {
    const path = new URL(req.url).pathname, s = this.sql;
    let b = {}; try { b = await req.json(); } catch {}
    if (path === '/report') {
      if (typeof b.id !== 'string' || !core.ROOM_RE.test(b.id)) return new Response('bad', { status: 400 });
      s.exec('INSERT INTO rooms (id, bytes, segs, title, at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET bytes = excluded.bytes, segs = excluded.segs, title = excluded.title, at = excluded.at',
        b.id, Math.max(0, +b.bytes || 0), Math.max(0, +b.segs || 0), String(b.title || '').slice(0, 30), +b.at || Date.now());
      return new Response('ok');
    }
    if (path === '/fail') {                                    // 관리 코드를 틀림: { k, add } → 그 곳 10분 · 모두 1시간 횟수
      const now = Date.now(), k = String(b.k || '').slice(0, 64);
      s.exec('DELETE FROM fails WHERE at < ?', now - 3600000);
      if (b.add) s.exec('INSERT INTO fails (k, at) VALUES (?, ?)', k, now);
      const one = (q, ...a) => (s.exec(q, ...a).toArray()[0] || {}).n || 0;
      return Response.json({ mine: one('SELECT COUNT(*) AS n FROM fails WHERE k = ? AND at >= ?', k, now - 600000), all: one('SELECT COUNT(*) AS n FROM fails') });
    }
    if (path === '/perf') {                                    // 버벅임 알림 (최근 300개만)
      s.exec('INSERT INTO perf (at, data) VALUES (?, ?)', Date.now(), JSON.stringify(b).slice(0, 1000));
      s.exec('DELETE FROM perf WHERE at NOT IN (SELECT at FROM perf ORDER BY at DESC LIMIT 300)');
      return new Response('ok');
    }
    if (path === '/summary') {
      const now = Date.now(), day = 86400000;
      const one = (q, ...a) => s.exec(q, ...a).toArray()[0] || {};
      const all = one('SELECT COUNT(*) AS n, COALESCE(SUM(bytes), 0) AS bytes, COALESCE(SUM(segs), 0) AS segs FROM rooms');
      const old = one('SELECT COUNT(*) AS n, COALESCE(SUM(bytes), 0) AS bytes FROM rooms WHERE at < ?', now - 30 * day);
      const empty = one('SELECT COUNT(*) AS n, COALESCE(SUM(bytes), 0) AS bytes FROM rooms WHERE segs = 0');
      const top = s.exec('SELECT id, bytes, segs, title, at FROM rooms ORDER BY bytes DESC LIMIT 20').toArray();
      const perf = s.exec('SELECT data FROM perf ORDER BY at DESC LIMIT 40').toArray().map((r) => { try { return JSON.parse(r.data); } catch { return null; } }).filter(Boolean);
      return Response.json({ rooms: all.n, bytes: all.bytes, segs: all.segs, free: FREE_STORAGE, old30: old, empty, top, perf, at: now });
    }
    return new Response('not found', { status: 404 });
  }
}
const STATS_PAGE = `<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>저장 공간 · 같이 그리기</title>
<style>body{margin:0;background:#f4f2ec;color:#1d1d1b;font:16px/1.5 system-ui,"Malgun Gothic","Apple SD Gothic Neo",sans-serif}main{max-width:760px;margin:0 auto;padding:28px 16px}
h1{font-size:24px;margin:0 0 12px}.card{background:#fdfdfa;border:2px solid #1d1d1b;border-radius:14px;padding:16px;margin:12px 0}input,button{font:inherit;padding:8px 12px;border:2px solid #1d1d1b;border-radius:10px;background:#fdfdfa}
button{background:#1d1d1b;color:#fdfdfa;cursor:pointer}.bar{height:16px;border:2px solid #1d1d1b;border-radius:9px;overflow:hidden;background:#fdfdfa}.bar i{display:block;height:100%;background:#1d1d1b}
table{width:100%;border-collapse:collapse;font-size:14px}td,th{text-align:left;padding:6px 4px;border-bottom:1px dashed #b8b2a4}td.n{text-align:right;font-variant-numeric:tabular-nums}.mute{color:#6a6457;font-size:14px}.err{color:#c8372d}</style></head>
<body><main><h1>저장 공간</h1><p class="mute">방장 되찾기 코드(OWNER_CLAIM)를 넣으면 보여요. 방은 그림을 저장할 때 10분에 한 번까지 크기를 알려요 (그 뒤로 안 연 방은 마지막 크기).</p>
<form id="f" class="card"><label>코드 <input id="k" type="password" autocomplete="current-password" required></label> <button>보기</button> <span id="e" class="err"></span></form><div id="out"></div></main>
<script>
const $ = (id) => document.getElementById(id), mb = (b) => (b / 1048576).toFixed(b < 10485760 ? 2 : 0) + 'MB', esc = (t) => String(t).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
$('f').onsubmit = async (ev) => { ev.preventDefault(); $('e').textContent = '';
  const r = await fetch('/__stats', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: $('k').value }) });
  if (!r.ok) { $('e').textContent = r.status === 403 ? '코드가 맞지 않아요' : '못 불러왔어요 (' + r.status + ')'; return; }
  const j = await r.json(), pct = Math.min(100, j.bytes / j.free * 100);
  $('out').innerHTML = '<div class="card"><b>' + mb(j.bytes) + '</b> / 5GB (' + pct.toFixed(2) + '%) · 방 ' + j.rooms + '개 · 그림 ' + j.segs.toLocaleString() + '조각<div class="bar" style="margin-top:8px"><i style="width:' + pct + '%"></i></div>'
    + '<p class="mute">30일 넘게 안 그린 방: ' + j.old30.n + '개 (' + mb(j.old30.bytes) + ') · 그림 없는 방: ' + j.empty.n + '개 (' + mb(j.empty.bytes) + ')</p></div>'
    + '<div class="card"><b>큰 방 20개</b><table><tr><th>방</th><th>이름</th><th class="n">크기</th><th class="n">조각</th><th>마지막</th></tr>'
    + j.top.map((x) => '<tr><td><a href="/?room=' + esc(x.id) + '">' + esc(x.id) + '</a></td><td>' + esc(x.title) + '</td><td class="n">' + mb(x.bytes) + '</td><td class="n">' + x.segs.toLocaleString() + '</td><td>' + new Date(x.at).toLocaleDateString('ko-KR') + '</td></tr>').join('') + '</table></div>'
    + '<div class="card"><b>버벅임 알림 (최근 40개)</b><p class="mute">그리는 동안 화면이 초당 30장 아래로 느려졌던 기기. 90% = 프레임 10장 중 9장이 이 시간 안.</p><table><tr><th>때</th><th>방</th><th class="n">90%</th><th class="n">조각</th><th>화면</th><th>기기</th></tr>'
    + (j.perf || []).map((x) => '<tr><td>' + new Date(x.at).toLocaleString('ko-KR') + '</td><td>' + esc(x.room) + '</td><td class="n">' + x.p90 + 'ms</td><td class="n">' + (x.segs || 0).toLocaleString() + '</td><td>' + x.w + 'x' + x.h + '@' + x.dpr + ' · ' + esc(x.tool) + '</td><td class="mute">' + esc(x.ua) + '</td></tr>').join('') + '</table></div>'
    + '<div class="card"><b>Plus</b> <span class="mute">결제(Polar): ' + (j.polar ? '켜짐' : '꺼짐 (비밀값 POLAR_TOKEN·POLAR_WEBHOOK_SECRET, 상품 POLAR_PRODUCT 필요)') + '</span>'
    + '<form id="g" style="margin:10px 0"><label>계정 번호 <input id="go" size="12" required placeholder="설정 → 계정에 보임"></label> <label>며칠 <input id="gd" type="number" min="0" max="3660" value="30" style="width:6em"></label> <button>선물</button> <span class="mute">(0일 = 선물 취소)</span> <span id="ge" class="err"></span></form>'
    + '<table><tr><th>계정 번호</th><th>결제</th><th>선물</th><th>끝나는 날</th></tr>' + (j.plus || []).map((x) => '<tr><td>' + esc(x.o) + '</td><td>' + (x.pu > Date.now() ? '구독 중' : '') + '</td><td>' + (x.gu > Date.now() ? '선물' : '') + '</td><td>' + new Date(Math.max(x.pu, x.gu)).toLocaleDateString('ko-KR') + '</td></tr>').join('') + '</table></div>';
  if ($('g')) $('g').onsubmit = async (e2) => { e2.preventDefault();
    const r2 = await fetch('/__stats', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: $('k').value, act: 'gift', o: $('go').value.trim(), days: +$('gd').value }) });
    if (!r2.ok) { $('ge').textContent = '못 했어요 (' + r2.status + ')'; return; }
    $('f').requestSubmit(); };
};
</script></body></html>`;
// 같은 길이로 바꿔 비교 (맞는 글자 수에 따라 걸리는 시간이 달라지지 않게)
const sameSecret = (a, b) => { const x = createHash('sha256').update(String(a)).digest(), y = createHash('sha256').update(String(b)).digest(); return timingSafeEqual(x, y) && String(a) === String(b); };
// 틀린 시도 세기 (이 프로그램 안, IP 마다): 10분에 10번 넘게 틀리면 잠시 막는다
const fails = new Map();
function tooManyFails(ip, add) {
  const now = Date.now(), a = (fails.get(ip) || []).filter((t) => t > now - 600000);
  if (add) a.push(now);
  if (a.length) fails.set(ip, a); else fails.delete(ip);
  if (fails.size > 5000) for (const x of [...fails.keys()].slice(0, 1000)) fails.delete(x);
  return a.length >= 10;
}
// 그 밖의 횟수 제한 (이 프로그램 안): k 마다 ms 동안 n 번까지
const hits = new Map();
function rateOk(k, n, ms) { const now = Date.now(), a = (hits.get(k) || []).filter((t) => t > now - ms); if (a.length >= n) { hits.set(k, a); return false; } a.push(now); hits.delete(k); hits.set(k, a); if (hits.size > 5000) for (const x of [...hits.keys()].slice(0, 1000)) hits.delete(x); return true; }
const clientIp = (req, env) => (env.LOCAL_DEV === '1' ? req.headers.get('X-Test-IP') : req.headers.get('CF-Connecting-IP')) || 'local';
async function stats(req, env, url) {
  if (url.pathname === '/stats') return new Response(STATS_PAGE, { headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' } });
  if (req.method !== 'POST' || !env.STATS) return new Response('not found', { status: 404 });
  const ip = clientIp(req, env), k = ip.includes(':') ? ip.split(':').slice(0, 4).join(':') : ip;   // (IPv6 는 앞 64비트: 한 집·한 회사)
  const guard = async (add) => { try { return await (await statsObj(env).fetch('https://stats/fail', { method: 'POST', body: JSON.stringify({ k, add }) })).json(); } catch { return { mine: 0, all: 0 }; } };
  const g0 = await guard(false);
  if (tooManyFails(k, false) || g0.mine >= 10 || g0.all >= 50) return new Response('wait', { status: 429 });
  let b = {}; try { b = await req.json(); } catch {}
  const code = String(env.OWNER_CLAIM || '');
  if (code.length < 8 || typeof b.key !== 'string' || !sameSecret(b.key, code)) { tooManyFails(k, true); await guard(true); return new Response('no', { status: 403 }); }
  const J = (r) => new Response(r.body, { status: r.status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' } });
  if (b.act === 'gift' && env.ACCT) {                     // Plus 선물 (친구·시험용): 주인 ID 에 며칠 (0 이면 선물 취소)
    return J(await acctObj(env).fetch('https://acct/plus/gift', { method: 'POST', body: JSON.stringify({ o: b.o, days: b.days }) }));
  }
  const r = await statsObj(env).fetch('https://stats/summary');
  const j = await r.json().catch(() => ({}));
  if (env.ACCT) { try { j.plus = (await (await acctObj(env).fetch('https://acct/plus/list', { method: 'POST', body: '{}' })).json()).list; } catch {} }
  j.polar = plusReady(env);
  return Response.json(j, { headers: { 'Cache-Control': 'no-store' } });
}

// 무료 하루 한도를 넘었다는 오류인지 (정확한 문구는 Cloudflare 가 정하므로 넓게 본다. 메모리·CPU 초과는 아니다)
const isQuota = (m) => !/memory|cpu/i.test(m) && /free tier|daily|quota|exceeded allowed|limit exceeded/i.test(m);
// 방 객체에 못 갈 때 화면에 이유를 알려 주는 접속: 'bye' 하나 보내고 닫는다 (화면은 끝 화면을 띄우고 스스로 다시 시도한다)
function byeSocket(code, text) {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  server.send(JSON.stringify({ type: 'bye', code, text }));
  server.close(4006, code);
  return new Response(null, { status: 101, webSocket: client });
}

// ── 계정 (구글·카카오 로그인) ──
// 계정 = 로그인한 곳(구글·카카오의 사용자 번호)마다 비밀 키 하나. 화면은 로그인하면 그 키를 이 기기의 '나'(draw:key)로 쓴다
// → 어느 기기에서든 같은 사람(방장 자격·그림 주인). 처음 로그인하면 그 기기의 지금 키가 계정의 키가 된다 (지금까지의 '나'가 그대로 계정이 됨).
// 순서: /auth/start → 구글·카카오 → /auth/cb/<곳> (사용자 번호 확인 → 표 하나) → 원래 방 주소#login=표 → 화면이 /auth/finish 로 표를 내고 키를 받음.
// 표는 한 번만·10분 안에, 로그인을 시작한 브라우저(쿠키)에서만 쓸 수 있다. 비밀값: GOOGLE_ID·GOOGLE_SECRET, KAKAO_ID(REST API 키)·KAKAO_SECRET(쓸 때만)
const AUTH_COOKIE = 'dg_st', TICKET_MS = 10 * 60 * 1000, KEY_RE = /^[A-Za-z0-9_-]{16,64}$/;
const randId = (n) => Buffer.from(crypto.getRandomValues(new Uint8Array(n))).toString('base64url');
const form = (o) => ({ method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8' }, body: new URLSearchParams(o) });
const PROVIDERS = {
  google: {
    on: (env) => !!(env.GOOGLE_ID && env.GOOGLE_SECRET),
    authUrl: (env, redirect, state) => 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({ client_id: env.GOOGLE_ID, redirect_uri: redirect, response_type: 'code', scope: 'openid', state, prompt: 'select_account' }),
    async user(env, code, redirect) {
      const r = await fetch('https://oauth2.googleapis.com/token', form({ code, client_id: env.GOOGLE_ID, client_secret: env.GOOGLE_SECRET, redirect_uri: redirect, grant_type: 'authorization_code' }));
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.id_token) throw new Error(`구글 토큰 ${r.status} ${j.error || ''}`);
      // (구글에서 비밀값으로 직접 받은 토큰이라 서명 확인은 하지 않아도 된다: OpenID Connect Core 3.1.3.7)
      const p = JSON.parse(Buffer.from(String(j.id_token).split('.')[1] || '', 'base64url').toString('utf8'));
      if (p.aud !== env.GOOGLE_ID || !/^(https:\/\/)?accounts\.google\.com$/.test(p.iss) || !p.sub) throw new Error('구글 id_token 이 올바르지 않아요');
      return String(p.sub);
    },
  },
  kakao: {
    on: (env) => !!env.KAKAO_ID,
    authUrl: (env, redirect, state) => 'https://kauth.kakao.com/oauth/authorize?' + new URLSearchParams({ client_id: env.KAKAO_ID, redirect_uri: redirect, response_type: 'code', state }),
    async user(env, code, redirect) {
      const o = { grant_type: 'authorization_code', client_id: env.KAKAO_ID, redirect_uri: redirect, code };
      if (env.KAKAO_SECRET) o.client_secret = env.KAKAO_SECRET;
      const r = await fetch('https://kauth.kakao.com/oauth/token', form(o));
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.access_token) throw new Error(`카카오 토큰 ${r.status} ${j.error_code || j.error || ''}`);
      const u = await fetch('https://kapi.kakao.com/v2/user/me', { headers: { Authorization: 'Bearer ' + j.access_token } });
      const uj = await u.json().catch(() => ({}));
      if (!u.ok || !uj.id) throw new Error(`카카오 사용자 ${u.status}`);
      return String(uj.id);
    },
  },
  test: {                                                // 시험 전용 (LOCAL_DEV): 진짜 로그인 없이 ?as=이름 → 그 이름이 사용자 번호
    on: (env) => env.LOCAL_DEV === '1',
    authUrl: (env, redirect, state, url) => redirect + '?' + new URLSearchParams(url.searchParams.get('as') === '-' ? { error: 'access_denied', state } : { code: url.searchParams.get('as') || 'tester', state }),
    async user(env, code) { return code.slice(0, 64); },
  },
};
const provOf = (name, env) => (Object.prototype.hasOwnProperty.call(PROVIDERS, name) && PROVIDERS[name].on(env) ? PROVIDERS[name] : null);
const safeRet = (r) => (typeof r === 'string' && /^\/(\?[^#]*)?$/.test(r) && r.length <= 300 ? r : '/');   // (돌아갈 곳: 이 사이트의 첫 화면 주소만)
function readCookie(req, name) {
  for (const part of (req.headers.get('Cookie') || '').split(/;\s*/)) { const i = part.indexOf('='); if (i > 0 && part.slice(0, i) === name) return part.slice(i + 1); }
  return null;
}
const stCookie = (url, v, age) => `${AUTH_COOKIE}=${v}; Path=/auth; HttpOnly; SameSite=Lax; Max-Age=${age}` + (url.protocol === 'https:' ? '; Secure' : '');
const go = (to, cookie) => new Response(null, { status: 302, headers: { Location: to, 'Cache-Control': 'no-store', ...(cookie ? { 'Set-Cookie': cookie } : {}) } });
const acctObj = (env) => env.ACCT.get(env.ACCT.idFromName('accounts'));

async function auth(req, env, url) {
  const path = url.pathname;
  if (path === '/auth/config') {                        // 화면: 어떤 로그인 버튼을 보일지
    const on = {}; for (const k of Object.keys(PROVIDERS)) on[k] = PROVIDERS[k].on(env);
    return Response.json(on, { headers: { 'Cache-Control': 'no-store' } });
  }
  if (path === '/auth/start') {
    const name = url.searchParams.get('p') || '', prov = provOf(name, env);
    if (!prov) return new Response('이 로그인은 아직 준비되지 않았어요', { status: 404, headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
    const state = randId(18), ret = safeRet(url.searchParams.get('ret'));
    return go(prov.authUrl(env, url.origin + '/auth/cb/' + name, state, url), stCookie(url, `${name}.${state}.${Buffer.from(ret).toString('base64url')}`, 600));
  }
  const cb = /^\/auth\/cb\/([a-z]+)$/.exec(path);
  if (cb) {
    const name = cb[1], prov = provOf(name, env);
    const [cn, st, r64] = (readCookie(req, AUTH_COOKIE) || '').split('.');
    let ret = '/'; try { ret = safeRet(Buffer.from(r64 || '', 'base64url').toString('utf8')); } catch {}
    if (!prov || cn !== name || !st || st !== url.searchParams.get('state')) return go(ret + '#login=fail', stCookie(url, '', 0));   // (다른 브라우저에서 왔거나 너무 오래됨)
    const code = url.searchParams.get('code');
    if (!code || url.searchParams.get('error')) return go(ret, stCookie(url, '', 0));   // 사용자가 취소함
    let sub;
    try { sub = await prov.user(env, code, url.origin + '/auth/cb/' + name); }
    catch (e) { console.error('로그인 확인 실패:', e.message); return go(ret + '#login=fail', stCookie(url, '', 0)); }
    const r = await acctObj(env).fetch('https://acct/ticket', { method: 'POST', body: JSON.stringify({ pid: name + ':' + sub, st }) });
    const { ticket } = await r.json();
    return go(ret + '#login=' + ticket);                // (쿠키는 표를 낼 때까지 그대로)
  }
  if (path === '/auth/linked' && req.method === 'POST') {    // 이 키에 이어진 로그인 (설정의 계정 칸 표시용. 키를 아는 본인만 물을 수 있다)
    const r = await acctObj(env).fetch('https://acct/linked', { method: 'POST', body: await req.text() });
    return new Response(r.body, { status: r.status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' } });
  }
  if (path === '/auth/finish' && req.method === 'POST') {
    const [, st] = (readCookie(req, AUTH_COOKIE) || '').split('.');
    let b = {}; try { b = await req.json(); } catch {}
    const r = await acctObj(env).fetch('https://acct/finish', { method: 'POST', body: JSON.stringify({ ticket: b.ticket, key: b.key, nick: b.nick, st: st || '' }) });
    return new Response(r.body, { status: r.status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Set-Cookie': stCookie(url, '', 0) } });
  }
  return new Response('not found', { status: 404 });
}

// 계정 저장소 (객체 하나): acct(로그인한 곳:사용자 번호 → 비밀 키·처음 닉네임), ticket(로그인 표, 10분)
export class Accounts extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec('CREATE TABLE IF NOT EXISTS acct (pid TEXT PRIMARY KEY, key TEXT NOT NULL, nick TEXT NOT NULL, at INTEGER NOT NULL)');
    this.sql.exec('CREATE TABLE IF NOT EXISTS ticket (id TEXT PRIMARY KEY, pid TEXT NOT NULL, st TEXT NOT NULL, at INTEGER NOT NULL)');
    this.sql.exec('CREATE INDEX IF NOT EXISTS acct_key ON acct(key)');
    // Plus: 주인 ID 마다 결제(Polar)로 받은 기간 pu · 선물받은 기간 gu (끝나는 시각, ms) · 꾸미기
    this.sql.exec("CREATE TABLE IF NOT EXISTS plus (o TEXT PRIMARY KEY, pu INTEGER NOT NULL DEFAULT 0, gu INTEGER NOT NULL DEFAULT 0, deco TEXT NOT NULL DEFAULT '{}', at INTEGER NOT NULL)");
  }
  plusRow(o) { return this.sql.exec('SELECT pu, gu, deco FROM plus WHERE o = ?', o).toArray()[0] || { pu: 0, gu: 0, deco: '{}' }; }
  plusSet(o, f) {                                              // f: 바꿀 칸들 { pu?, gu?, deco? }
    const r = { ...this.plusRow(o), ...f };
    this.sql.exec('INSERT INTO plus (o, pu, gu, deco, at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(o) DO UPDATE SET pu = excluded.pu, gu = excluded.gu, deco = excluded.deco, at = excluded.at', o, r.pu, r.gu, r.deco, Date.now());
  }
  async fetch(req) {
    const path = new URL(req.url).pathname, s = this.sql, now = Date.now();
    let b = {}; try { b = await req.json(); } catch {}
    const bad = (error) => Response.json({ error }, { status: 400 });
    if (path === '/ticket') {
      s.exec('DELETE FROM ticket WHERE at < ?', now - TICKET_MS);
      const id = randId(18);
      s.exec('INSERT INTO ticket (id, pid, st, at) VALUES (?, ?, ?, ?)', id, String(b.pid), String(b.st), now);
      return Response.json({ ticket: id });
    }
    if (path === '/finish') {
      const id = typeof b.ticket === 'string' ? b.ticket : '';
      const t = s.exec('SELECT pid, st, at FROM ticket WHERE id = ?', id).toArray()[0];
      if (!t || t.at < now - TICKET_MS) return bad('ticket');
      if (!b.st || t.st !== b.st) return bad('browser');                 // (로그인을 시작한 브라우저가 아님)
      s.exec('DELETE FROM ticket WHERE id = ?', id);                     // (한 번만)
      let a = s.exec('SELECT key, nick FROM acct WHERE pid = ?', t.pid).toArray()[0], created = false;
      if (!a) {
        if (typeof b.key !== 'string' || !KEY_RE.test(b.key)) return bad('key');
        a = { key: b.key, nick: Array.from(String(b.nick || '')).slice(0, 12).join('') };
        s.exec('INSERT INTO acct (pid, key, nick, at) VALUES (?, ?, ?, ?)', t.pid, a.key, a.nick, now);
        created = true;
        console.log('새 계정: ' + t.pid.split(':')[0]);
      }
      return Response.json({ key: a.key, nick: a.nick, p: t.pid.split(':')[0], created, linked: this.linked(a.key) });
    }
    if (path === '/linked') return Response.json({ linked: typeof b.key === 'string' && KEY_RE.test(b.key) ? this.linked(b.key) : [] });
    // ── Plus ──
    const O_RE = /^[A-Za-z0-9_-]{10}$/;
    const deco = (r) => { try { return core.cleanDeco(JSON.parse(r.deco)); } catch { return {}; } };
    if (path === '/plus/get') {                                // 방이 묻는다: 이 사람의 Plus
      if (!O_RE.test(b.o)) return Response.json({ until: 0, deco: {} });
      const r = this.plusRow(b.o);
      return Response.json({ until: Math.max(r.pu, r.gu), deco: deco(r) });
    }
    if (path === '/plus/me') {                                 // 본인: 키 → 계정인지·주인 ID·Plus
      const key = typeof b.key === 'string' && KEY_RE.test(b.key) ? b.key : null, o = key && core.ownerIdOf(key);
      if (!o) return Response.json({ acct: false });
      const acct = !!this.sql.exec('SELECT 1 AS x FROM acct WHERE key = ? LIMIT 1', key).toArray()[0], r = this.plusRow(o);
      return Response.json({ acct, o, until: Math.max(r.pu, r.gu), pu: r.pu, gu: r.gu, deco: deco(r) });
    }
    if (path === '/plus/polar') {                              // 결제 소식(웹훅)에서 확인한 구독 기간
      if (!O_RE.test(b.o) || !(b.until >= 0)) return bad('o');
      if (b.until === 0 && !this.sql.exec('SELECT 1 AS x FROM plus WHERE o = ?', b.o).toArray()[0]) return Response.json({ ok: true });
      this.plusSet(b.o, { pu: Math.floor(b.until) });
      return Response.json({ ok: true });
    }
    if (path === '/plus/gift') {                               // 관리: 선물 (며칠, 0 = 취소)
      const days = Number.isInteger(b.days) ? b.days : -1;
      if (!O_RE.test(b.o) || days < 0 || days > 3660) return bad('o');
      const r = this.plusRow(b.o);
      this.plusSet(b.o, { gu: days ? Math.max(now, r.gu) + days * 86400000 : 0 });
      console.log(`Plus 선물: ${b.o} ${days}일`);
      return Response.json({ ok: true });
    }
    if (path === '/plus/deco') {                               // 본인: 꾸미기 바꾸기 (Plus 일 때만)
      const key = typeof b.key === 'string' && KEY_RE.test(b.key) ? b.key : null, o = key && core.ownerIdOf(key);
      if (!o) return bad('key');
      const r = this.plusRow(o);
      if (!core.PLUS_FREE && Math.max(r.pu, r.gu) <= now) return Response.json({ error: 'plus' }, { status: 403 });   // (무료인 동안은 누구나 이름 색)
      const d = core.cleanDeco(b.deco);
      this.plusSet(o, { deco: JSON.stringify(d) });
      return Response.json({ ok: true, deco: d });
    }
    if (path === '/plus/list') {                               // 관리: 최근 30일 안에 Plus 였던 사람
      return Response.json({ list: this.sql.exec('SELECT o, pu, gu FROM plus WHERE pu > ? OR gu > ? ORDER BY MAX(pu, gu) DESC LIMIT 200', now - 30 * 86400000, now - 30 * 86400000).toArray() });
    }
    return new Response('not found', { status: 404 });
  }
  linked(key) { return [...new Set(this.sql.exec('SELECT pid FROM acct WHERE key = ?', key).toArray().map((r) => r.pid.split(':')[0]))]; }   // 이 키(같은 사람)에 이어진 로그인들
}

// ── Plus 구독 (Polar) ──
// 결제·구독 관리는 Polar(판매 대행: 카드 결제·나라별 세금). Plus 는 계정(로그인)이 있어야 살 수 있다 (기기를 바꿔도 따라오게).
// Polar 의 '외부 고객 ID' = 계정 키로 만든 주인 ID (방에서 쓰는 10글자, 남에게 보여도 되는 값).
// 비밀값: POLAR_TOKEN (조직 액세스 토큰: checkouts:write · customers:read · customer_sessions:write), POLAR_WEBHOOK_SECRET (whsec_…)
// 설정(vars): POLAR_PRODUCT (Plus 상품 ID), POLAR_API (기본 https://api.polar.sh · 시험판 https://sandbox-api.polar.sh), PLUS_PRICE (화면에 보일 가격, 예: 월 $3.99)
// 기간은 웹훅 내용을 믿지 않고 Polar 에 그 사람의 '지금 상태'를 다시 물어서 정한다 (순서가 바뀌어 와도·여러 번 와도 결과가 같다)
const GRACE_MS = 2 * 86400000;                               // 다음 결제가 조금 늦게 확인돼도 Plus 가 끊기지 않게 (해지 예약이면 그날까지만)
const plusReady = (env) => !!(env.POLAR_TOKEN && env.POLAR_WEBHOOK_SECRET && env.POLAR_PRODUCT);
async function polar(env, method, path, body) {
  const base = String(env.POLAR_API || 'https://api.polar.sh').replace(/\/+$/, '');
  const r = await fetch(base + path, { method, headers: { Authorization: 'Bearer ' + env.POLAR_TOKEN, 'Content-Type': 'application/json', Accept: 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  const j = await r.json().catch(() => ({}));
  return { ok: r.ok, status: r.status, j };
}
function untilOf(env, st, now = Date.now()) {               // Polar 고객 상태 → Plus 가 끝나는 때 (구독이 없으면 0)
  let u = 0;
  for (const x of (st && Array.isArray(st.active_subscriptions) ? st.active_subscriptions : [])) {
    if (x.product_id !== env.POLAR_PRODUCT || (x.status !== 'active' && x.status !== 'trialing')) continue;
    const end = Date.parse(x.ends_at || x.current_period_end || '') || now + 31 * 86400000;
    u = Math.max(u, x.cancel_at_period_end || x.ends_at ? end : end + GRACE_MS);
  }
  return u;
}
async function refreshPlus(env, o, sub) {                    // Polar 에 이 사람의 상태를 물어 계정 저장소에 적는다 (sub: 이 사람이 결제한 구독 소식 — 다른 고객에 붙었을 때)
  const st = await polar(env, 'GET', '/v1/customers/external/' + encodeURIComponent(o) + '/state');
  if (!st.ok && st.status !== 404) throw new Error('Polar 고객 상태 ' + st.status);
  let until = st.ok ? untilOf(env, st.j) : 0;
  if (sub && sub.id && /^[0-9a-f-]{36}$/.test(sub.id) && (sub.product_id === env.POLAR_PRODUCT || (sub.product && sub.product.id === env.POLAR_PRODUCT))) {   // (그 구독을 Polar 에 다시 물어 확인)
    const r = await polar(env, 'GET', '/v1/subscriptions/' + sub.id);
    if (r.ok && r.j && r.j.metadata && r.j.metadata.o === o) until = Math.max(until, untilOf(env, { active_subscriptions: [r.j] }));
  }
  await acctObj(env).fetch('https://acct/plus/polar', { method: 'POST', body: JSON.stringify({ o, until }) });
  return until;
}
// 웹훅 서명 확인 (Standard Webhooks: HMAC-SHA256(아이디.시각.본문), 키 = whsec_ 뒤를 base64 로 푼 것 / 예전 Polar 방식: whsec_ 글자 그대로). 5분 넘은 것은 거절
function hookOk(env, req, body) {
  const id = req.headers.get('webhook-id') || '', ts = req.headers.get('webhook-timestamp') || '', sig = req.headers.get('webhook-signature') || '';
  const secret = String(env.POLAR_WEBHOOK_SECRET || '');
  if (!id || !secret || !/^\d{1,12}$/.test(ts) || Math.abs(Date.now() / 1000 - +ts) > 300) return false;
  const msg = id + '.' + ts + '.' + body, keys = [Buffer.from(secret.replace(/^whsec_/, ''), 'base64'), Buffer.from(secret, 'utf8')];
  const given = sig.split(' ').map((x) => x.split(',')).filter((x) => x[0] === 'v1' && x[1]).map((x) => Buffer.from(x[1], 'base64'));
  for (const k of keys) { const mac = createHmac('sha256', k).update(msg).digest(); if (given.some((g) => g.length === mac.length && timingSafeEqual(g, mac))) return true; }
  return false;
}
const extOf = (d) => { const x = d && (d.external_id ?? (d.customer && d.customer.external_id) ?? d.external_customer_id); return typeof x === 'string' && /^[A-Za-z0-9_-]{10}$/.test(x) ? x : null; };
async function plusRoute(req, env, url) {
  const path = url.pathname, J = (o, status = 200) => Response.json(o, { status, headers: { 'Cache-Control': 'no-store' } });
  if (path === '/plus/webhook' && req.method === 'POST') {    // Polar → 결제·구독이 바뀜
    const body = await req.text();
    if (body.length > 1000000) return new Response('too big', { status: 413 });
    if (!plusReady(env) || !hookOk(env, req, body)) return new Response('no', { status: 403 });
    let ev = null; try { ev = JSON.parse(body); } catch { return new Response('bad', { status: 400 }); }
    const o = extOf(ev && ev.data), md = ev && ev.data && ev.data.metadata, mo = md && typeof md.o === 'string' && /^[A-Za-z0-9_-]{10}$/.test(md.o) ? md.o : null;
    const os = [...new Set([o, mo].filter(Boolean))];
    if (!os.length || !/^(subscription|order|customer)\./.test(String(ev.type))) return new Response('', { status: 202 });   // (상관없는 소식·우리 계정이 아닌 고객)
    if (o && mo && o !== mo) console.log(`Plus 소식: 고객 ${o} 와 결제한 계정 ${mo} 가 달라요 (같은 이메일?)`);
    try { for (const x of os) { const u = await refreshPlus(env, x, x === mo ? ev.data : null); console.log(`Plus 소식 ${ev.type}: ${x} → ${u ? new Date(u).toISOString() : '없음'}`); } }
    catch (e) { console.error('Plus 소식 처리 실패:', e.message); return new Response('retry', { status: 500 }); }   // (Polar 가 다시 보낸다)
    return new Response('', { status: 202 });
  }
  if (req.method !== 'POST') return new Response('not found', { status: 404 });
  let b = {}; try { b = await req.json(); } catch {}
  const me = await (await acctObj(env).fetch('https://acct/plus/me', { method: 'POST', body: JSON.stringify({ key: b.key }) })).json();
  if (path === '/plus/status') {
    const now = Date.now();
    if (plusReady(env) && me.acct && me.pu > 0 && me.pu < now + GRACE_MS && me.pu > now - 45 * 86400000 && rateOk('st:' + me.o, 1, 3600000)) {
      try { me.pu = await refreshPlus(env, me.o); me.until = Math.max(me.pu, me.gu || 0); } catch {}
    }
    return J({ ...me, on: !core.PLUS_FREE && plusReady(env), free: core.PLUS_FREE, price: String(env.PLUS_PRICE || '').slice(0, 40) });
  }
  if (!me.o) return J({ error: 'key' }, 400);
  if (path === '/plus/deco') {
    const r = await acctObj(env).fetch('https://acct/plus/deco', { method: 'POST', body: JSON.stringify({ key: b.key, deco: b.deco }) });
    return new Response(r.body, { status: r.status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' } });
  }
  if (!plusReady(env) || (core.PLUS_FREE && path === '/plus/checkout')) return J({ error: 'off' }, 503);   // (무료인 동안은 새 결제를 받지 않는다)
  if (path === '/plus/refresh') {                             // 결제하고 돌아옴: 웹훅을 기다리지 않고 바로 확인 (계정만, 1분에 6번까지)
    if (!me.acct) return J({ error: 'login' }, 403);
    if (!rateOk('re:' + me.o, 6, 60000) || !rateOk('reip:' + clientIp(req, env), 20, 60000)) return J({ error: 'wait' }, 429);
    try { return J({ until: Math.max(await refreshPlus(env, me.o), me.gu || 0) }); } catch (e) { console.error(e.message); return J({ error: 'polar' }, 502); }
  }
  if (!me.acct) return J({ error: 'login' }, 403);
  if (!rateOk('pay:' + me.o, 10, 60000)) return J({ error: 'wait' }, 429);            // 사려면 로그인 (기기를 바꿔도 Plus 가 따라오게)
  const ret = safeRet(b.ret);
  if (path === '/plus/checkout') {
    const r = await polar(env, 'POST', '/v1/checkouts/', { products: [env.POLAR_PRODUCT], external_customer_id: me.o, metadata: { o: me.o }, success_url: url.origin + ret + '#plus=ok', return_url: url.origin + ret });
    if (!r.ok || !/^https:\/\//.test(String(r.j.url || ''))) { console.error('Polar 결제 만들기 실패:', r.status, JSON.stringify(r.j).slice(0, 300)); return J({ error: 'polar' }, 502); }
    return J({ url: r.j.url });
  }
  if (path === '/plus/portal') {                              // 구독 관리 (해지·카드 바꾸기·영수증): Polar 의 고객 화면
    const r = await polar(env, 'POST', '/v1/customer-sessions/', { external_customer_id: me.o, return_url: url.origin + ret });
    if (!r.ok || !/^https:\/\//.test(String(r.j.customer_portal_url || ''))) return J({ error: r.status === 404 || r.status === 422 ? 'none' : 'polar' }, r.status === 404 || r.status === 422 ? 404 : 502);
    return J({ url: r.j.customer_portal_url });
  }
  return new Response('not found', { status: 404 });
}

// ── 보안 머리글 ──
// 모든 응답: 내용 종류 추측 금지 · 다른 사이트 안에 넣기(iframe) 금지 · 다른 사이트로 갈 때 주소(방 코드) 대신 사이트 이름만 · 카메라·마이크·위치 안 씀 · https 만
const SEC_HEADERS = { 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'Referrer-Policy': 'strict-origin-when-cross-origin',
  'Permissions-Policy': 'camera=(), microphone=(), geolocation=(), payment=(), usb=()', 'Cross-Origin-Opener-Policy': 'same-origin' };
function secure(res, url, csp) {
  if (res.status === 101 || res.webSocket) return res;
  const h = new Headers(res.headers);
  for (const [k, v] of Object.entries(SEC_HEADERS)) h.set(k, v);
  if (url.protocol === 'https:') h.set('Strict-Transport-Security', 'max-age=31536000');
  if (csp) h.set('Content-Security-Policy', csp);
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers: h });
}
// 화면(HTML)의 내용 보안 정책: 스크립트는 이 사이트 파일과 페이지 안의 그 스크립트(해시가 같은 것)만 → 누가 글자 칸에 스크립트를 몰래 넣어도 실행되지 않는다 (비밀 키를 훔치지 못하게)
const b64sha = async (t) => Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(t))).toString('base64');
async function scriptHashes(html) { const out = []; for (const m of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) out.push(await b64sha(m[1])); return out; }
const cspOf = (url, hashes) => ["default-src 'self'", "script-src 'self' " + hashes.map((h) => `'sha256-${h}'`).join(' '),
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com", "font-src 'self' https://fonts.gstatic.com data:", "img-src 'self' data: blob:",
  `connect-src 'self' ${url.protocol === 'https:' ? 'wss' : 'ws'}://${url.host} blob: data:`, "media-src 'self' blob:", "worker-src 'self' blob:", "frame-src 'none'", "object-src 'none'",
  "base-uri 'none'", "form-action 'self'", "frame-ancestors 'none'"].join('; ');
let indexCache = null, statsHash = null;                     // { etag, hashes } (배포 때만 바뀐다)

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const res = await route(req, env, url);
    if (url.pathname === '/stats') return secure(res, url, cspOf(url, [statsHash || (statsHash = (await scriptHashes(STATS_PAGE))[0])]));
    return secure(res, url, res.csp || null);
  },
};
async function route(req, env, url) {
  {
    if (url.pathname.startsWith('/auth/')) return auth(req, env, url);
    if (url.pathname.startsWith('/plus/')) return plusRoute(req, env, url);
    if (url.pathname === '/stats' || url.pathname === '/__stats') return stats(req, env, url);
    if (url.pathname === '/ws') {                        // 실시간 접속 → 그 방의 객체로
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('websocket only', { status: 426 });
      const origin = req.headers.get('Origin');           // 브라우저는 다른 사이트에서 연 접속이면 그 사이트를 Origin 에 적는다: 이 사이트에서 연 것만 받는다
      if (origin && origin !== url.origin) return new Response('forbidden origin', { status: 403 });
      const raw = url.searchParams.get('room') || 'lobby', id = core.ROOM_RE.test(raw) ? raw : 'lobby';
      try {
        const f = env.LOCAL_DEV === '1' && url.searchParams.get('__fail');   // (시험 전용: 방 객체가 거절한 것처럼)
        if (f) throw new Error(f === 'quota' ? 'Exceeded allowed volume of requests in Durable Objects free tier.' : 'Durable Object is overloaded.');
        return await env.ROOM.get(env.ROOM.idFromName(id)).fetch(req);
      } catch (e) {
        const msg = String((e && e.message) || e);
        console.error('방 객체에 연결하지 못했어요:', msg);
        if (isQuota(msg)) return byeSocket('quota', '오늘 쓸 수 있는 무료 사용량을 다 썼어요 (Cloudflare 무료 플랜의 하루 한도). 한국 시간 오전 9시에 다시 열려요. 저장된 그림은 그대로 있어요.');
        return new Response('room unavailable', { status: 503 });   // (잠깐 문제: 화면이 알아서 다시 접속한다)
      }
    }
    if ((url.pathname === '/__test/seed' || url.pathname === '/__test/dump') && env.LOCAL_DEV === '1') {
      const raw = url.searchParams.get('room') || '', id = core.ROOM_RE.test(raw) ? raw : null;
      return id ? env.ROOM.get(env.ROOM.idFromName(id)).fetch(req) : new Response('bad room', { status: 400 });
    }
    if (url.pathname === '/version') return Response.json({ ...core.versionInfo(), host: 'cloudflare' }, { headers: { 'Cache-Control': 'no-store' } });
    if (url.pathname === '/' && !url.searchParams.get('room')) {   // 방 없이 오면 새 방 (new=1: 화면이 '이 방 링크를 보내 주세요' 안내를 띄운다)
      return new Response(null, { status: 302, headers: { Location: '/?room=' + core.newRoomId() + '&new=1', 'Cache-Control': 'no-store' } });
    }
    let res = await env.ASSETS.fetch(req);               // 화면(index.html·og.png): Cloudflare 가 압축·캐시
    // 첫 화면: 링크 미리보기 그림 주소(og:image)를 이 주소로 (미리보기는 절대 주소여야 한다. index.html 에는 Render 주소가 적혀 있다)
    if (url.pathname === '/' && (res.headers.get('Content-Type') || '').includes('text/html')) {
      let csp = null;
      if (res.status === 200) {                         // (304 등은 본문이 없다: 정책은 처음 받은 것 그대로)
        const html = await res.text(), et = res.headers.get('ETag') || html.length + ':' + html.slice(-200);
        // (HEAD 요청은 본문이 비어 있다: 빈 본문으로 해시를 만들어 두면 그 뒤 화면 스크립트가 모두 막힌다 → GET 의 진짜 본문으로만)
        if (req.method === 'GET' && html.includes('<script>') && (!indexCache || indexCache.etag !== et)) indexCache = { etag: et, hashes: await scriptHashes(html) };
        csp = indexCache && indexCache.etag === et ? cspOf(url, indexCache.hashes) : null;
        const h = new Headers(res.headers); h.delete('Content-Length');
        res = new Response(html, { status: res.status, statusText: res.statusText, headers: h });
      }
      const rw = new HTMLRewriter().on('meta[property="og:image"]', { element(e) { e.setAttribute('content', url.origin + '/og.png'); } });
      const raw = url.searchParams.get('room') || '', id = core.ROOM_RE.test(raw) && !url.searchParams.get('new') ? raw : null;
      let title = '';
      if (id) {                                         // 방 이름이 있으면 미리보기 제목·탭 제목에 (늦으면 기다리지 않는다)
        const ask = env.ROOM.get(env.ROOM.idFromName(id)).fetch(new Request(url.origin + '/title?room=' + id)).then((r) => r.text()).catch(() => '');
        title = await Promise.race([ask, new Promise((r) => setTimeout(() => r(''), 1500))]);
      }
      if (title) {
        const full = title + ' · 같이 그리기';
        rw.on('meta[property="og:title"]', { element(e) { e.setAttribute('content', full); } }).on('title', { element(e) { e.setInnerContent(full); } });
      }
      const out = rw.transform(res);
      if (csp) out.csp = csp;
      return out;
    }
    return res;
  }
}
