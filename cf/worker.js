// Cloudflare 판 서버: Worker(입구) + Durable Object(방 하나 = 객체 하나, 저장은 그 객체 안의 SQLite).
// 방·그림·방장 규칙은 core.js 를 그대로 쓴다 (Node 판 server.js 와 같은 코드).
// 무료 플랜: 요청 하루 10만 번(받은 WebSocket 메시지는 20개 = 1번), 객체가 깨어 있는 시간 하루 13,000 GB-s, 넘으면 그날은 오류 (돈은 안 나간다). 전송량 요금 없음.
// 방 객체는 할 일이 없으면 잠들고(접속은 그대로), 오래 쉬면 메모리에서 내려간다 → 다시 깨어날 때 SQLite 에서 방을, 소켓에 적어 둔 정보에서 접속을 되살린다.
import { DurableObject } from 'cloudflare:workers';
import { Buffer } from 'node:buffer';
import core from '../core.js';

// 방 하나의 저장소 (그 방 객체의 SQLite). 표: kv(history 'p1:…'·names·meta·extra·roomId), parts(덩어리 번호 → 'z2:'+base64). Node 판 Postgres 와 같은 내용·형식.
// importOld(id): 이 방 객체가 비어 있을 때 한 번, 예전 저장소에서 옮겨 올 방 { head, names, meta, parts } 을 돌려준다 (없으면 null, 못 읽으면 throw)
function sqlStore(storage, importOld) {
  const sql = storage.sql;
  sql.exec('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)');
  sql.exec('CREATE TABLE IF NOT EXISTS parts (pid INTEGER PRIMARY KEY, data TEXT NOT NULL)');
  const get = (k) => { const r = sql.exec('SELECT v FROM kv WHERE k = ?', k).toArray()[0]; return r ? r.v : null; };
  const put = (k, v) => { sql.exec('INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v', k, v); };
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
      storage.transactionSync(() => {
        if (clear) sql.exec('DELETE FROM parts');
        for (const p of ps) sql.exec('INSERT INTO parts (pid, data) VALUES (?, ?) ON CONFLICT(pid) DO UPDATE SET data = excluded.data', p.pid, p.data);
        for (const pid of del) sql.exec('DELETE FROM parts WHERE pid = ?', pid);
        if (head !== null) { put('history', head); put('names', names); }
      });
    },
    async saveMeta(id, json) { put('meta', json); },
    async saveExtra(id, json) { put('extra', json); },
  };
}

// 예전 저장소(Render 때의 Postgres, Neon)에서 방 하나 읽기 → 덩어리 형식으로. DATABASE_URL 비밀값이 있을 때만 (옮겨 오는 기간에만 넣어 둔다)
async function readOld(dbUrl, id) {
  const { Client } = await import('pg');
  const info = core.parseDbUrl(dbUrl);
  const c = new Client({ connectionString: info.url, ssl: info.local ? false : { rejectUnauthorized: false }, connectionTimeoutMillis: 4000 });
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
core.setPlatform({
  hasStore: true,
  storeFor: (id) => stores.get(id) || null,
  term: (ws) => core.closed(ws),
  clientCloses: true,
  // 강퇴·밴: 다른 사람의 메시지를 처리하는 중에 닫으면 닫기 인사가 상대에게 안 가는 경우가 있다 (쉬는 소켓 방식). 화면은 'bye' 를 받으면 스스로 끊으므로 방에서만 바로 뺀다
  afterBye: (ws) => core.closed(ws),
  // 접속 정보를 소켓에 적어 둔다 (방 객체가 메모리에서 내려갔다 깨어나도 누가 어떤 접속인지 알 수 있게, 최대 2KB)
  attach: (ws) => { try { ws.serializeAttachment({ o: ws.owner, ipt: ws.ipt || '', z: ws.zOk || 0, ap: ws.approved ? 1 : 0 }); } catch {} },
});

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
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
    core.connect(server, { url, cip: { ip, local } }).finally(() => this.later());
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    if (msg === '{"type":"__evict"}' && this.env.LOCAL_DEV === '1') return this.testEvict();
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
  async testEvict() {
    const room = core.rooms.get(this.id);
    if (room) { for (const t of room.leaveTimers.values()) clearTimeout(t); await core.maintainRoom(this.id, room, true); }
    if (this.saveT) { clearTimeout(this.saveT); this.saveT = null; }
    core.dropRoom(this.id);
    const socks = this.ctx.getWebSockets();
    console.log(`방 ${this.id}: (시험) 다시 깨어남 (열린 접속 ${socks.length}개를 되살림)`);
    await core.restore(this.id, socks.map((ws) => [ws, ws.deserializeAttachment() || {}]));
  }

  // 바뀐 게 있으면 SAVE_MS 뒤 저장 (타이머가 도는 동안은 잠들지 않고, 다 저장하면 다시 잠들 수 있다)
  later() {
    if (this.saveT || !this.id) return;
    const room = core.rooms.get(this.id);
    if (!room || !(room.dirty || room.metaDirty || room.extraDirty)) return;
    this.saveT = setTimeout(async () => {
      this.saveT = null;
      try { await core.maintainRoom(this.id, room, false); } catch (e) { console.error('저장 점검 오류:', e.message); }
      this.later();
    }, core.SAVE_MS);
  }
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/ws') {                        // 실시간 접속 → 그 방의 객체로
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('websocket only', { status: 426 });
      const raw = url.searchParams.get('room') || 'lobby', id = core.ROOM_RE.test(raw) ? raw : 'lobby';
      return env.ROOM.get(env.ROOM.idFromName(id)).fetch(req);
    }
    if ((url.pathname === '/__test/seed' || url.pathname === '/__test/dump') && env.LOCAL_DEV === '1') {
      const raw = url.searchParams.get('room') || '', id = core.ROOM_RE.test(raw) ? raw : null;
      return id ? env.ROOM.get(env.ROOM.idFromName(id)).fetch(req) : new Response('bad room', { status: 400 });
    }
    if (url.pathname === '/version') return Response.json({ ...core.versionInfo(), host: 'cloudflare' }, { headers: { 'Cache-Control': 'no-store' } });
    if (url.pathname === '/' && !url.searchParams.get('room')) {   // 방 없이 오면 새 방 (new=1: 화면이 '이 방 링크를 보내 주세요' 안내를 띄운다)
      return new Response(null, { status: 302, headers: { Location: '/?room=' + core.newRoomId() + '&new=1', 'Cache-Control': 'no-store' } });
    }
    const res = await env.ASSETS.fetch(req);             // 화면(index.html·og.png): Cloudflare 가 압축·캐시
    // 첫 화면: 링크 미리보기 그림 주소(og:image)를 이 주소로 (미리보기는 절대 주소여야 한다. index.html 에는 Render 주소가 적혀 있다)
    if (url.pathname === '/' && (res.headers.get('Content-Type') || '').includes('text/html')) {
      return new HTMLRewriter().on('meta[property="og:image"]', { element(e) { e.setAttribute('content', url.origin + '/og.png'); } }).transform(res);
    }
    return res;
  },
};
