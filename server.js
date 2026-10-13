// Node 서버 (Render): express(화면) + ws(실시간) + Postgres(저장). 방·그림·방장 규칙은 core.js (Cloudflare 판 cf/worker.js 와 같이 쓴다)
const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');
const { WebSocketServer } = require('ws');
const core = require('./core');

const app = express();
const server = http.createServer(app);
// permessage-deflate: 글자 메시지(선·커서·채팅)를 압축해서 보낸다 (Render 무료 전송량 5GB 절약). 창을 작게(4KB) 잡아 접속당 메모리는 수십 KB.
// 이미 압축된 그림(바이너리)은 send(…, { compress: false }) 로 다시 압축하지 않는다.
const wss = new WebSocketServer({ server, maxPayload: core.MAX_FILL_BYTES,
  perMessageDeflate: { serverMaxWindowBits: 12, clientMaxWindowBits: 12, zlibDeflateOptions: { level: 6, memLevel: 5 }, threshold: 24, concurrencyLimit: 4 } });

// ---- 저장소: DATABASE_URL 이 있으면 Postgres (없으면 메모리만) ----
const dbInfo = core.parseDbUrl(process.env.DATABASE_URL);
const term = (ws) => ws.terminate();
if (dbInfo) {
  const { Pool } = require('pg');
  const pool = new Pool({ connectionString: dbInfo.url, ssl: dbInfo.local ? false : { rejectUnauthorized: false }, max: 4,
    idleTimeoutMillis: 20000, connectionTimeoutMillis: 8000, query_timeout: 20000, keepAlive: true });
  // 놀고 있던 연결을 DB 쪽에서 끊으면 pg 가 'error' 를 내보낸다. 처리하지 않으면 서버가 통째로 죽는다.
  pool.on('error', (e) => console.error('DB 연결 오류 (자동으로 다시 연결해요):', e.message));
  // 표: rooms(방마다 그림 목록 'p1:…'·이름·설정) + room_parts(그림 덩어리마다 'z2:'+base64)
  const store = {
    loadRow: async (id) => (await pool.query('SELECT history, names, meta FROM rooms WHERE id = $1', [id])).rows[0] || null,
    loadParts: async (id) => (await pool.query('SELECT pid, data FROM room_parts WHERE room_id = $1', [id])).rows,
    async saveRoom(id, { clear, put, del, head, names }) {     // 덩어리 쓰기·안 쓰는 덩어리 지우기·순서 목록 바꾸기는 한 트랜잭션
      const c = await pool.connect();
      let bad = false;
      try {
        await c.query('BEGIN');
        if (clear) await c.query('DELETE FROM room_parts WHERE room_id = $1', [id]);
        for (let i = 0; i < put.length; i += 100) {
          const ch = put.slice(i, i + 100), vals = [], args = [id];
          ch.forEach((p, k) => { vals.push(`($1, $${2 + k * 2}, $${3 + k * 2})`); args.push(p.pid, p.data); });
          await c.query(`INSERT INTO room_parts (room_id, pid, data) VALUES ${vals.join(', ')} ON CONFLICT (room_id, pid) DO UPDATE SET data = EXCLUDED.data`, args);
        }
        if (del.length) await c.query('DELETE FROM room_parts WHERE room_id = $1 AND pid = ANY($2::int[])', [id, del]);
        if (head !== null) {
          await c.query(
            'INSERT INTO rooms (id, history, names, updated_at) VALUES ($1, $2, $3, now()) ON CONFLICT (id) DO UPDATE SET history = $2, names = $3, updated_at = now()',
            [id, head, names]);
        }
        await c.query('COMMIT');
      } catch (e) { bad = true; try { await c.query('ROLLBACK'); } catch {} throw e; }
      finally { c.release(bad || undefined); }
    },
    // 방 설정은 행이 아직 없는 방도 저장되도록 UPSERT (그림은 건드리지 않는다)
    saveMeta: (id, json) => pool.query(
      'INSERT INTO rooms (id, history, names, meta, updated_at) VALUES ($1, $2, $3, $4, now()) ON CONFLICT (id) DO UPDATE SET meta = $4, updated_at = now()',
      [id, '[]', '{}', json]),
  };
  async function init() {                           // 표 만들기 + 읽기 확인
    try { await pool.query("CREATE TABLE IF NOT EXISTS rooms (id TEXT PRIMARY KEY, history TEXT NOT NULL, names TEXT NOT NULL DEFAULT '{}', meta TEXT NOT NULL DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT now())"); }
    catch (e) { console.error('표 만들기 실패:', e.message); }
    try { await pool.query('CREATE TABLE IF NOT EXISTS room_parts (room_id TEXT NOT NULL, pid INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (room_id, pid))'); }
    catch (e) { console.error('표 만들기 실패(room_parts):', e.message); }
    for (const col of ['names', 'meta']) {          // 이미 있던 표에는 새 컬럼만 덧붙인다 (기존 데이터는 그대로)
      try { await pool.query(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS ${col} TEXT NOT NULL DEFAULT '{}'`); }
      catch (e) { console.error(`컬럼 추가 실패(${col}):`, e.message); }
    }
    await pool.query('SELECT 1 FROM rooms LIMIT 1');   // 표를 읽을 수 있어야 '저장 가능'
    await pool.query('SELECT 1 FROM room_parts LIMIT 1');
  }
  core.setPlatform({ hasStore: true, storeFor: () => store, init, term });
} else core.setPlatform({ hasStore: false, term });

// 주기 점검(저장·빈 방 내리기)
setInterval(core.saveTick, core.SAVE_MS);
let stopping = false;
async function shutdown(why) {                           // Render 가 재시작·잠재우기 전에 보내는 신호: 저장하고 끝낸다
  if (stopping) return;
  stopping = true;
  console.log(`${why}: 마지막 저장 중…`);
  const guard = setTimeout(() => process.exit(0), 8000);
  try { await core.saveAll(true); } catch {}
  clearTimeout(guard);
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
// 테스트 전용: 윈도우는 SIGTERM 을 보내면 바로 죽어서(신호가 없다) 표준 입력의 'shutdown' 으로 같은 종료 절차를 시험한다
if (process.env.SHUTDOWN_ON_STDIN) process.stdin.on('data', (d) => { if (String(d).includes('shutdown')) shutdown('SIGTERM'); });
process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e));
process.on('uncaughtException', (e) => { console.error('uncaughtException:', e); core.saveAll(true).catch(() => {}); });   // 죽지 않고 계속하되, 일단 저장은 해 둔다

// ---- 이사 (MOVED_TO 가 있으면): 이 주소로 오는 사람을 새 주소로 보낸다 (Cloudflare 로 옮긴 뒤 Render 에 남겨 두는 안내) ----
// 브라우저에 저장된 '나'(비밀 키)·닉네임·설정·최근 방·자 위치를 주소 뒤 #mig= 로 함께 넘긴다 (# 뒤는 서버로 가지 않는다). 새 화면이 받아서 저장한다.
// 그림 저장·접속은 하지 않는다 (WebSocket 은 바로 닫음) → 전송량은 이 작은 페이지뿐.
// Render 위(Render 가 RENDER 환경변수를 넣는다)에서 MOVED_TO 를 따로 안 넣었으면 MOVED_DEFAULT 로 (Cloudflare 로 옮긴 뒤: Render 가 다시 켜져도
// 대시보드를 안 만지고 옛 주소가 새 주소로 보내게. 안 그러면 Render 가 예전처럼 따로 그림을 받는 서버로 살아나 그림이 갈라진다). 비어 있으면 이사 안 함
const MOVED_DEFAULT = 'https://draw.meongmungittong.workers.dev';
const MOVED_TO = String(process.env.MOVED_TO || (process.env.RENDER ? MOVED_DEFAULT : '')).replace(/\/+$/, '');
if (MOVED_TO) {
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
  const page = `<!DOCTYPE html><html lang="ko"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>같이 그리기 — 이사했어요</title><meta property="og:title" content="같이 그리기"><meta property="og:description" content="링크로 누구나 실시간으로 같이 그리는 무한 보드">
<meta property="og:image" content="${esc(MOVED_TO)}/og.png"><meta name="twitter:card" content="summary_large_image">
<style>body{margin:0;font-family:system-ui,sans-serif;background:#1b1d22;color:#e8e8e8;display:grid;place-items:center;min-height:100vh;text-align:center;padding:16px;box-sizing:border-box}a{color:#8ab4ff}</style></head>
<body><div><h2>같이 그리기가 새 주소로 이사했어요</h2><p id="m">잠시만요, 옮겨 가는 중…</p><p><a id="go" href="${esc(MOVED_TO)}/">새 주소로 가기</a></p></div>
<script>
(function () {
  var to = ${JSON.stringify(MOVED_TO)}, q = location.search || '', d = {};
  try {
    for (var i = 0; i < localStorage.length; i++) { var k = localStorage.key(i); if (k && k.indexOf('draw:') === 0) d[k] = localStorage.getItem(k); }
  } catch (e) {}
  var u = to + '/' + q;
  try { var j = JSON.stringify(d); if (j.length > 2) u += '#mig=' + btoa(unescape(encodeURIComponent(j))).replace(/\\+/g, '-').replace(/\\//g, '_').replace(/=+$/, ''); } catch (e) {}
  document.getElementById('go').href = u;
  location.replace(u);
})();
</script></body></html>`;
  app.get('/version', (req, res) => res.json({ ...core.versionInfo(), moved: MOVED_TO }));
  app.get('/og.png', (req, res) => res.redirect(301, MOVED_TO + '/og.png'));
  app.use((req, res) => { res.setHeader('Cache-Control', 'no-store'); res.type('html').send(page); });
  wss.on('connection', (ws) => { try { ws.send(JSON.stringify({ type: 'bye', code: 'moved', text: '새 주소로 이사했어요. 새로고침해 주세요.', to: MOVED_TO })); ws.close(4005, 'moved'); } catch {} });
}

// ---- 웹 서버 ----
app.get('/version', (req, res) => res.json(core.versionInfo()));
app.get('/auth/config', (req, res) => res.json({}));   // (계정 로그인은 Cloudflare 판에만: 화면이 계정 칸을 숨긴다)
app.get('/', (req, res, next) => {
  if (!req.query.room) return res.redirect('/?room=' + core.newRoomId() + '&new=1');   // 방 없이 오면 새 방 (new=1: 화면이 '이 방 링크를 보내 주세요' 안내를 띄운다)
  next();
});
// 화면(index.html, 약 210KB)은 미리 gzip 해 두고 보낸다 (약 4분의 1). 압축을 못 받는 브라우저는 아래 static 이 그대로 보낸다.
const indexGz = (() => {
  try {
    const raw = require('fs').readFileSync(path.join(__dirname, 'public', 'index.html'));
    return { gz: zlib.gzipSync(raw, { level: 9 }), etag: '"g' + crypto.createHash('sha1').update(raw).digest('base64url').slice(0, 20) + '"' };
  } catch { return null; }
})();
app.get(['/', '/index.html'], (req, res, next) => {
  if (!indexGz || !/\bgzip\b/.test(req.headers['accept-encoding'] || '')) return next();
  res.setHeader('Cache-Control', 'no-cache'); res.setHeader('ETag', indexGz.etag); res.setHeader('Vary', 'Accept-Encoding');
  if (req.headers['if-none-match'] === indexGz.etag) return res.status(304).end();
  res.setHeader('Content-Type', 'text/html; charset=UTF-8'); res.setHeader('Content-Encoding', 'gzip');
  res.end(indexGz.gz);
});
app.use(express.static(path.join(__dirname, 'public'), { setHeaders: (res) => res.setHeader('Cache-Control', 'no-cache') }));   // 올릴 때마다 새 화면이 바로 보이게 (매번 변경 여부를 확인)

wss.on('error', (e) => console.error('wss error:', e.message));

// 약 30초마다 ping: 응답이 없는(반쯤 끊긴) 소켓은 정리한다
const pingTimer = setInterval(() => {
  for (const c of wss.clients) {
    if (c.isAlive === false) { c.terminate(); continue; }
    c.isAlive = false;
    try { c.ping(); } catch {}
  }
}, core.PING_MS);
if (pingTimer.unref) pingTimer.unref();
wss.on('close', () => clearInterval(pingTimer));
const connTimer = setInterval(core.pruneConnRate, 60000);
if (connTimer.unref) connTimer.unref();

// Render 앞단(Cloudflare)이 넣어 주는 실제 접속 IP. 프록시 없이 이 컴퓨터에서 바로 붙은 접속(테스트)은 local
function clientIp(req) {
  const h = req.headers;
  const fwd = String(h['cf-connecting-ip'] || h['true-client-ip'] || String(h['x-forwarded-for'] || '').split(',')[0] || '').trim();
  const sock = String(req.socket.remoteAddress || '').replace(/^::ffff:/i, '');
  return { ip: (fwd || sock).replace(/%.*$/, '').replace(/^::ffff:/i, ''), local: !fwd && (sock === '127.0.0.1' || sock === '::1') };
}

wss.on('connection', (ws, req) => {
  ws.on('error', (e) => console.error('ws error:', e.message)); // 없으면 잘못된 프레임 하나에 서버가 죽는다
  if (MOVED_TO) return;                                // (이사한 뒤에는 위에서 안내하고 닫는다)
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw, isBinary) => core.message(ws, raw, isBinary));
  ws.on('close', () => core.closed(ws));
  core.connect(ws, { url: new URL(req.url, 'http://localhost'), cip: clientIp(req) });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('http://localhost:' + PORT));
