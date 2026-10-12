// 같이 그리기 서버 본체: 방·그림·방장·저장 규칙. 접속과 저장소는 바깥에서 붙인다 (setPlatform / connect / message / closed)
//  - server.js: Node (Render) — express + ws + Postgres
//  - cf/worker.js: Cloudflare Workers — 방 하나 = Durable Object 하나, 저장은 그 안의 SQLite
const crypto = require('crypto');
const zlib = require('zlib');
const { promisify } = require('util');

// 일반 메시지는 수백 바이트(최대 16KB). 칠하기('fill') 메시지만 더 클 수 있어서 소켓 한도는 넉넉히 두고, 아래 onRaw 에서 종류별로 다시 자른다.
const MAX_MSG_BYTES = 16 * 1024, MAX_FILL_BYTES = 128 * 1024;

// ---- 한도 (public/index.html 의 같은 이름 상수와 값이 같아야 한다) ----
const PEN_MAX = 2000;         // 펜 굵기 (멀리 축소해서도 굵게 그릴 수 있게)
const ERASER_R_MAX = 5000;    // 지우개 반지름
const FILL_CELL_MAX = 1000;   // 칠하기 칸 크기 상한 = 화면 1px 의 월드 크기 (0.1% 배율일 때 1000, 더 멀리 축소하면 화면이 채우기를 막는다). 칠하기 줄 굵기(칸 x 2)는 PEN_MAX 이하여야 한다
const MAX_STROKES_PER_ERASE = 300; // 획 지우개 한 번에 지울 수 있는 획 수
const FILL_THICK = 2;         // 칠하기 줄 두께 = 칸 크기 x 이 값 (줄끼리 겹쳐서 이음새가 안 보이게)
const MAX_FILL_SPANS = 3000;  // 한 번 칠하기에 들어갈 수 있는 가로줄 수
const MAX_HISTORY = +process.env.MAX_HISTORY || 500000;   // 방 하나에 둘 수 있는 선분 수 (테스트에서만 줄인다). 꽉 차면 서버 메모리 약 110MB, 압축 사본(D2) 약 2MB, 폰(4배 느린 CPU 흉내) 메모리 약 190MB
const PING_MS = +process.env.PING_MS || 30000;
const SID_RE = /^[0-9a-z]{1,12}$/;
const SID_CHARS = '0123456789abcdefghijklmnopqrstuvwxyz';
// ---- 방장 / 채팅 (2단계) ----
const MIN_MS = +process.env.MIN_MS || 60000;                   // 밴·채팅 금지의 '1분' (테스트에서만 줄인다)
const LEAVE_GRACE_MS = +process.env.LEAVE_GRACE_MS || 10000;   // 이 시간 안에 다시 들어오면 입장/퇴장 메시지를 내지 않는다
const LOCK_GRACE_MS = +process.env.LOCK_GRACE_MS || 120000;    // 방이 잠겨 있어도 방금 나간 사람은 다시 들어올 수 있다
const MAX_CHAT = 100, CHAT_MAX_CHARS = 200, REASON_MAX = 50, MAX_LIST = 200, BAN_MAX_MIN = 43200;
const BYE_CODE = { kicked: 4001, banned: 4002, locked: 4003, toomany: 4004 };
// ---- 분탕 방지 ----
const MAX_LOG = 300, MAX_MODS = 20;                             // 입장 기록(방마다 최근 300명) / 부방장 수
const IP_BAN_MAX_MS = 7 * 24 * 60 * 60 * 1000;                  // 밴할 때 같은 인터넷(IP)도 막는 기간은 최대 7일 (IP 는 바뀌고, 나중에 다른 사람이 쓸 수 있다)
const OWNER_SHARE = +process.env.OWNER_SHARE || 0.5;            // 방장·부방장이 아닌 한 사람이 쓸 수 있는 그림 용량 비율 (혼자 방을 가득 채워 아무도 못 그리게 하는 것 방지)
const CONN_BURST = +process.env.CONN_BURST || 40, CONN_PER_SEC = +process.env.CONN_PER_SEC || 1;   // 같은 IP 의 새 접속: 한꺼번에 40번, 그 뒤 초당 1번
const CONN_PER_IP = +process.env.CONN_PER_IP || 40;            // 같은 IP 에서 동시에 열 수 있는 접속 수
// 남의 그림을 덮을 수 있는 양 (방장·부방장은 제한 없음): 획 수 한꺼번에 30, 1분에 15 / 조각 수 한꺼번에 400, 1분에 200.
// 친구 방 기록으로 잰 보통 그리기: 획 하나가 남의 그림을 덮는 양은 99% 가 2획·24조각 이하.
const COVER_BURST = +process.env.COVER_BURST || 30, COVER_PER_MIN = +process.env.COVER_PER_MIN || 15;
const COVER_PIECES = +process.env.COVER_PIECES || 400, COVER_PIECES_PER_MIN = +process.env.COVER_PIECES_PER_MIN || 200;
// 칠하기가 남의 (색이 다른) 칠하기 조각을 덮을 수 있는 양: 한꺼번에 1000 조각, 1분에 500 씩. (친구 방 기록: 칠하기 101번 중 남의 칠하기를 덮은 건 0번)
const FILL_COVER = +process.env.FILL_COVER || 1000, FILL_COVER_PER_MIN = +process.env.FILL_COVER_PER_MIN || 500;
const SAVE_MS = +process.env.SAVE_MS || 5000;
const META_GAP_MS = +process.env.META_GAP_MS || 60000;   // 입장 기록만 바뀐 방 설정은 1분에 한 번만 저장 (밴·잠금 같은 설정은 바로 저장)
const CUR_FLUSH_MS = 100;                                // 커서 위치는 방마다 모아서 0.1초에 한 번 보낸다
let dbOut = 0;                                           // 서버가 켜진 뒤 DB 로 보낸 그림·설정 글자 수 (/version 의 dbKB, 전송량 확인용)
const ROOM_EMPTY_MS = +process.env.ROOM_EMPTY_MS || 10 * 60 * 1000;      // DB 에 저장해 둔 빈 방을 메모리에 남겨 두는 시간
const NO_DB_KEEP_MS = +process.env.NO_DB_KEEP_MS || 24 * 60 * 60 * 1000;  // DB 가 없거나 고장났을 땐 메모리가 유일한 사본이라 오래 둔다
const ROOM_RE = /^[A-Za-z0-9_-]{1,32}$/;
const ID_ALPHABET = 'abcdefghjkmnpqrstuvwxyz23456789';
const NAME_MAX = 12;
const rooms = new Map(); // roomId -> { clients, history, names, dirty, noSave, ready }

const r1 = (n) => Math.round(+n * 10) / 10;
const okNum = (v) => Number.isFinite(v) && Math.abs(v) <= 1e7;

// ---- 지우개·칠하기: 서버와 화면이 똑같은 함수를 써야 모두의 그림이 같아진다 (server.js와 index.html에 같은 내용) ----
// @@SHARED_BEGIN
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
    const piece = (a, b) => {
      const p = { x0: R1(s.x0 + dx * a), y0: R1(s.y0 + dy * a), x1: R1(s.x0 + dx * b), y1: R1(s.y0 + dy * b), color: s.color, size: s.size, o: s.o, sid: s.sid };
      if (s.f) p.f = 1;                                   // 칠하기 조각은 잘려도 칠하기 조각이다
      return p;
    };
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
    const m = s.size / 2;                                 // 범위 밖은 먼저 거른다 (획 이름을 만드는 것보다 훨씬 싸다)
    if (Math.max(s.x0, s.x1) + m < minX || Math.min(s.x0, s.x1) - m > maxX ||
        Math.max(s.y0, s.y1) + m < minY || Math.min(s.y0, s.y1) - m > maxY) continue;
    const k = s.o + '|' + s.sid;
    if (keys.has(k)) continue;
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
// 선분 목록에 합치기: 칠하기 조각(f)은 일반 선들 아래(목록 앞쪽, 먼저 칠한 것들 바로 뒤)에 끼우고, 일반 선은 맨 위(끝)에 붙인다.
// 이 규칙 덕분에 칠하기는 항상 선 밑에 깔리고, 나중에 칠한 색이 먼저 칠한 색 위에 온다.
function insertSegs(list, items) {
  let fs = null;
  for (const s of items) { if (s.f) (fs || (fs = [])).push(s); else list.push(s); }
  if (fs) { let n = 0; while (n < list.length && list[n].f) n++; list.splice(n, 0, ...fs); }
}
// 칠하기: 칸 크기(cell, 0.2의 배수)와 가로줄 목록([[줄, 시작칸, 끝칸], ...])을 선분으로 바꾼다. 칸 (i, j)의 중심은 ((i+0.5)*cell, (j+0.5)*cell).
// 줄 두께는 칸 크기보다 조금 굵어서(FILL_THICK) 줄끼리 겹쳐 이음새가 안 보이고, 가장자리는 경계선 밑으로 살짝 들어간다.
function fillSize(cell) { return Math.round(cell * FILL_THICK * 100) / 100; }
function fillSegs(o, sid, color, cell, rows) {
  const size = fillSize(cell), R = (n) => Math.round(n * 10) / 10, out = [];
  for (const r of rows) {
    const y = R((r[0] + 0.5) * cell);
    out.push({ x0: R((r[1] + 0.5) * cell), y0: y, x1: R((r[2] + 0.5) * cell), y1: y, color, size, o, sid, f: 1 });
  }
  return out;
}
// 그림 목록을 작게 담는 이진 형식 'D2' 풀기 (서버가 encodeSegs 로 만든다. 저장된 그림을 불러올 때 서버도 같은 함수를 쓴다).
// [D2][문자열 수, (길이, UTF-8)…][선분 수][선분…]. 선분: 표시 1바이트 + 필요한 값들 (정수는 7비트씩 이어 쓰는 수, 좌표는 0.1 단위 정수의 차이).
// 표시: 1 칠하기, 2 주인·획 바뀜, 4 색·굵기 바뀜, 8 앞 선분 끝에서 이어짐, 16 좌표를 실수 그대로, 32 굵기를 실수 그대로
function decodeSegs(u) {
  const r = d2Reader(u);
  if (!r) return null;
  const out = new Array(r.count);
  for (let i = 0; i < out.length; i++) out[i] = r.next();
  r.end();
  return out;
}
// 하나씩 꺼내는 읽개 (서버는 만든 D2 를 원래 그림과 조금씩 나눠 비교할 때 쓴다: 한꺼번에 풀어 메모리를 두 배로 쓰지 않게)
function d2Reader(u) {
  if (!u || u.length < 2 || u[0] !== 68 || u[1] !== 50) return null;
  const n = u.length, dv = new DataView(u.buffer, u.byteOffset, u.byteLength);
  let p = 2;
  const cut = () => new Error('D2: 잘린 데이터');
  const vu = () => { let x = 0, m = 1, b; do { if (p >= n) throw cut(); b = u[p++]; x += (b & 127) * m; m *= 128; } while (b & 128); return x; };
  const vs = () => { const z = vu(); return z % 2 ? -(z + 1) / 2 : z / 2; };
  const f64 = () => { if (p + 8 > n) throw cut(); const v = dv.getFloat64(p, true); p += 8; return v; };
  const td = new TextDecoder(), S = new Array(vu());
  for (let i = 0; i < S.length; i++) { const l = vu(); if (p + l > n) throw cut(); S[i] = td.decode(u.subarray(p, p + l)); p += l; }
  const count = vu();
  let o = '', sid = '', color = '', size = 0, px = 0, py = 0, X = 0, Y = 0;
  return {
    count,
    next() {
      if (p >= n) throw cut();
      const fl = u[p++];
      if (fl & 2) { o = S[vu()]; sid = S[vu()]; }
      if (fl & 4) { color = S[vu()]; size = fl & 32 ? f64() : vu() / 100; }
      let x0, y0, x1, y1;
      if (fl & 16) {
        if (fl & 8) { x0 = px; y0 = py; } else { x0 = f64(); y0 = f64(); }
        x1 = f64(); y1 = f64();
      } else {
        let X0 = X, Y0 = Y;
        if (fl & 8) { x0 = px; y0 = py; } else { X0 = X + vs(); Y0 = Y + vs(); x0 = X0 / 10; y0 = Y0 / 10; }
        const X1 = X0 + vs(), Y1 = Y0 + vs(); x1 = X1 / 10; y1 = Y1 / 10;
      }
      if (o === undefined || sid === undefined || color === undefined) throw new Error('D2: 잘못된 문자열 번호');
      const s = { x0, y0, x1, y1, color, size, o, sid };
      if (fl & 1) s.f = 1;
      px = x1; py = y1; X = Math.round(x1 * 10); Y = Math.round(y1 * 10);
      return s;
    },
    end() { if (p !== n) throw new Error('D2: 남는 데이터'); },
  };
}
// 덩어리 이름: D2 바이트의 64비트 해시를 36진수 14글자로 (서버와 화면이 같은 함수: 화면이 보관해 둔 덩어리가 그대로인지 확인한다)
function blockHash(u) {
  let h1 = 0xdeadbeef ^ u.length, h2 = 0x41c6ce57 ^ u.length;
  for (let i = 0; i < u.length; i++) { const c = u[i]; h1 = Math.imul(h1 ^ c, 2654435761); h2 = Math.imul(h2 ^ c, 1597334677); }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return (h2 >>> 0).toString(36).padStart(7, '0') + (h1 >>> 0).toString(36).padStart(7, '0');
}
// @@SHARED_END

// ---- 방 그림 보관: 선분을 덩어리(BLOCK 개 안팎)로 나눠 두고, 덩어리마다 범위(선 굵기 포함)를 기억한다 ----
// 지우개는 지우개 범위와 겹치는 덩어리만 계산한다 (그림이 많은 방에서 지우개를 움직일 때마다 전체를 훑으면 무료 서버(CPU 0.1개)가 버티지 못한다).
// 순서는 '덩어리 순서 + 덩어리 안 순서' = 원래 목록 순서. 계산은 위 공용 함수를 덩어리 배열에 그대로 쓴다 (선분마다 따로 계산되므로 결과가 같다).
// 그림을 바꾸는 곳은 모두 hist* 함수를 거친다: 바뀔 때마다 room.ver 가 올라 압축 사본(room.zc)이 낡은 것으로 처리된다.
const BLOCK = 512;
function growBlock(b, s) {
  const m = s.size / 2;
  const xa = Math.min(s.x0, s.x1) - m, xb = Math.max(s.x0, s.x1) + m, ya = Math.min(s.y0, s.y1) - m, yb = Math.max(s.y0, s.y1) + m;
  if (xa < b.x0) b.x0 = xa;
  if (xb > b.x1) b.x1 = xb;
  if (ya < b.y0) b.y0 = ya;
  if (yb > b.y1) b.y1 = yb;
}
// rev: 그 자리에서 바뀔 때마다(뒤에 붙이기·칠하기 끼우기) 오른다. pid/saved: DB 에 저장된 덩어리 번호와 그때의 rev (없으면 아직 저장 안 됨).
// 다른 바꾸기는 모두 새 덩어리를 만든다(blockOf) → 저장 안 된 덩어리로 보고 새 번호로 저장, 옛 번호는 지운다.
function blockOf(a) { const b = { a, x0: Infinity, y0: Infinity, x1: -Infinity, y1: -Infinity, rev: 0 }; for (const s of a) growBlock(b, s); return b; }
function histBump(room) { room.ver++; room.flat = null; }
function histChanged(room) { histBump(room); room.dirty = true; }
function histSet(room, arr) {                           // 목록 통째로 바꾸기 (불러오기·전체 지우기 등). 저장 표시는 부르는 쪽에서
  room.hb = [];
  for (let i = 0; i < arr.length; i += BLOCK) room.hb.push(blockOf(arr.slice(i, i + BLOCK)));
  room.hn = arr.length;
  histBump(room);
}
function histArr(room) {                                // 한 줄로 이은 목록 (바뀌기 전까지 다시 만들지 않는다)
  if (!room.flat) {
    const out = new Array(room.hn);
    let i = 0;
    for (const b of room.hb) for (const s of b.a) out[i++] = s;
    room.flat = out;
  }
  return room.flat;
}
function splitBlock(room, i) {                          // 너무 커진 덩어리는 나눈다
  const a = room.hb[i].a;
  if (a.length <= BLOCK * 2) return;
  const parts = [];
  for (let k = 0; k < a.length; k += BLOCK) parts.push(blockOf(a.slice(k, k + BLOCK)));
  room.hb.splice(i, 1, ...parts);
}
function appendSegs(room, list) {                       // 맨 끝에 붙이기
  for (const s of list) {
    let b = room.hb[room.hb.length - 1];
    if (!b || b.a.length >= BLOCK) { b = blockOf([]); room.hb.push(b); }
    b.a.push(s); b.rev++; growBlock(b, s);
  }
  room.hn += list.length;
}
function histPush(room, s) { appendSegs(room, [s]); histChanged(room); }
// 공용 insertSegs 와 같은 자리에 넣는다: 칠하기 조각(f)은 목록에서 처음 나오는 일반 선 바로 앞(= 앞쪽 칠하기들 뒤), 일반 선은 맨 끝
function histInsert(room, items) {
  const fs = [], ls = [];
  for (const s of items) (s.f ? fs : ls).push(s);
  if (fs.length) {
    let bi = -1, at = -1;
    for (let i = 0; i < room.hb.length && bi < 0; i++) { const k = room.hb[i].a.findIndex((s) => !s.f); if (k >= 0) { bi = i; at = k; } }
    if (bi < 0) appendSegs(room, fs);
    else {
      const b = room.hb[bi];
      b.a.splice(at, 0, ...fs); b.rev++;
      for (const s of fs) growBlock(b, s);
      room.hn += fs.length;
      splitBlock(room, bi);
    }
  }
  if (ls.length) appendSegs(room, ls);
  histChanged(room);
}
const capsuleBox = (x0, y0, x1, y1, r) => { const rr = Math.min(Math.max(r, 1), ERASER_R_MAX); return { xa: Math.min(x0, x1) - rr, xb: Math.max(x0, x1) + rr, ya: Math.min(y0, y1) - rr, yb: Math.max(y0, y1) + rr }; };
const blockOut = (b, c) => b.x1 < c.xa || b.x0 > c.xb || b.y1 < c.ya || b.y0 > c.yb;   // 덩어리 안 모든 선이 지우개 범위 밖 (공용 함수의 선분별 검사와 같은 기준)
// 부분 지우개: 바뀌었으면 true. 결과가 너무 커지면(잘게 잘려서) 하지 않는다.
function histErase(room, owner, x0, y0, x1, y1, r) {
  const c = capsuleBox(x0, y0, x1, y1, r), ch = [];
  let n = room.hn;
  for (let i = 0; i < room.hb.length; i++) {
    const b = room.hb[i];
    if (blockOut(b, c)) continue;
    const res = eraseCapsule(b.a, owner, x0, y0, x1, y1, r);
    if (res) { ch.push([i, res]); n += res.length - b.a.length; }
  }
  if (!ch.length || n > MAX_HISTORY * 1.5) return false;
  for (let k = ch.length - 1; k >= 0; k--) {           // 뒤에서부터 바꿔야 앞의 번호가 그대로다
    const [i, res] = ch[k];
    if (res.length) { room.hb[i] = blockOf(res); splitBlock(room, i); } else room.hb.splice(i, 1);
  }
  room.hn = n;
  histChanged(room);
  return true;
}
// 획 지우개: 지우개에 닿은 획 [주인, sid] 목록 (목록 순서대로 최대 max 개)
function histHit(room, owner, x0, y0, x1, y1, r, max) {
  const c = capsuleBox(x0, y0, x1, y1, r), keys = new Map();
  for (const b of room.hb) {
    if (blockOut(b, c)) continue;
    for (const k of hitStrokes(b.a, owner, x0, y0, x1, y1, r, max)) {
      const key = k[0] + '|' + k[1];
      if (keys.has(key)) continue;
      if (keys.size >= max) return [...keys.values()];
      keys.set(key, k);
    }
  }
  return [...keys.values()];
}
// 획 통째로 빼기 (공용 removeStrokes 와 같은 기준). 바뀌었으면 true.
function histRemoveStrokes(room, keys) {
  const set = new Set(keys.map((k) => k[0] + '|' + k[1])), owners = new Set(keys.map((k) => k[0]));
  let changed = false;
  for (let i = room.hb.length - 1; i >= 0; i--) {
    const b = room.hb[i];
    if (!b.a.some((s) => s.sid && owners.has(s.o) && set.has(s.o + '|' + s.sid))) continue;
    const a = b.a.filter((s) => !(s.sid && owners.has(s.o) && set.has(s.o + '|' + s.sid)));
    room.hn -= b.a.length - a.length; changed = true;
    if (a.length) room.hb[i] = blockOf(a); else room.hb.splice(i, 1);
  }
  if (changed) histChanged(room);
  return changed;
}
// 같은 값(주인·sid·좌표)의 선분을 하나씩 빼기 (공용 removeExactSegs 와 같은 기준: 목록에서 먼저 나오는 것부터). 바뀌었으면 true.
function histRemoveExact(room, items) {
  const key = (s) => s.o + '|' + s.sid + '|' + s.x0 + '|' + s.y0 + '|' + s.x1 + '|' + s.y1;
  const need = new Map(), owners = new Set();
  for (const s of items) { need.set(key(s), (need.get(key(s)) || 0) + 1); owners.add(s.o); }
  const ch = [];
  for (let i = 0; i < room.hb.length; i++) {
    const a = room.hb[i].a;
    let out = null;
    for (let k = 0; k < a.length; k++) {
      const s = a[k];
      if (owners.has(s.o)) {                            // (다른 사람 선분은 키가 같을 수 없다)
        const kk = key(s), c = need.get(kk);
        if (c) { need.set(kk, c - 1); if (!out) out = a.slice(0, k); continue; }
      }
      if (out) out.push(s);
    }
    if (out) ch.push([i, out]);
  }
  if (!ch.length) return false;
  for (let k = ch.length - 1; k >= 0; k--) {
    const [i, a] = ch[k];
    room.hn -= room.hb[i].a.length - a.length;
    if (a.length) room.hb[i] = blockOf(a); else room.hb.splice(i, 1);
  }
  histChanged(room);
  return true;
}
function histReplace(room, arr) { histSet(room, arr); room.dirty = true; }   // 드문 작업(전체 지우기·불러오기 복구)은 한 줄 목록으로 계산해서 통째로 바꾼다
// 조건에 맞는 선분만 남기기 (사람별 지우기). 안 바뀐 덩어리는 그대로 둬서 저장할 때 다시 보내지 않는다. 바뀌었으면 true.
function histFilter(room, keep) {
  let changed = false;
  for (let i = room.hb.length - 1; i >= 0; i--) {
    const b = room.hb[i];
    if (b.a.every(keep)) continue;
    const a = b.a.filter(keep);
    room.hn -= b.a.length - a.length; changed = true;
    if (a.length) room.hb[i] = blockOf(a); else room.hb.splice(i, 1);
  }
  if (changed) histChanged(room);
  return changed;
}

// ---- 방 그림 압축 사본 (zlib): DB 저장과 새로 들어온 사람에게 보내기에 같이 쓴다. 그림이 바뀌지 않았으면 다시 만들지 않는다 ----
// 그림 10만 조각이면 JSON 이 약 11MB 인데 압축하면 약 1.5MB. (무료 서버는 한 달 전송량이 5GB 라 들어올 때마다 11MB 를 보내면 금방 바닥난다)
// JSON 은 덩어리별로 만들면서 중간중간 쉬어서(setImmediate) 큰 방에서도 서버가 한 번에 오래 멈추지 않는다.
const deflateP = promisify(zlib.deflate), inflateP = promisify(zlib.inflate);
// 'D2' 만들기 (decodeSegs 의 반대). 선분을 여러 번에 나눠 넣고(add) 끝에 finish. 이 형식에 담을 수 없는 선분이 있으면 add 가 false.
// (선분에 모르는 값이 더 붙어 있으면 그 값은 빠진다 — 쓰는 곳이 없다. 아는 값은 makeSnap 이 풀어서 하나하나 확인한다)
function segEncoder(total) {
  let buf = new Uint8Array(Math.max(1024, total * 8)), p = 0;
  const idx = new Map(), strs = [];
  const need = (k) => { if (p + k > buf.length) { const nb = new Uint8Array(Math.max(buf.length * 2, p + k)); nb.set(buf.subarray(0, p)); buf = nb; } };
  const vu = (x) => { need(10); while (x >= 128) { buf[p++] = (x % 128) | 128; x = Math.floor(x / 128); } buf[p++] = x; };
  const vs = (x) => vu(x < 0 ? -2 * x - 1 : 2 * x);
  const f64 = (v) => { need(8); new DataView(buf.buffer).setFloat64(p, v, true); p += 8; };
  const si = (s) => { let i = idx.get(s); if (i === undefined) { i = strs.length; strs.push(s); idx.set(s, i); } return i; };
  const q10 = (v) => { const k = Math.round(v * 10); return k / 10 === v && Math.abs(k) < 4e15 ? k : null; };   // 0.1 단위로 정확히 나타낼 수 있으면 그 정수
  let o = null, sid = null, color = null, size = NaN, px = 0, py = 0, X = 0, Y = 0, n = 0;
  return {
    add(list) {
      for (const s of list) {
        if ((s.f !== undefined && s.f !== 1) || typeof s.o !== 'string' || typeof s.sid !== 'string' || typeof s.color !== 'string' || ![s.x0, s.y0, s.x1, s.y1, s.size].every(Number.isFinite)) return false;
        let fl = s.f === 1 ? 1 : 0;
        const nk = s.o !== o || s.sid !== sid, nst = s.color !== color || s.size !== size;
        const sz = Math.round(s.size * 100), rawSize = !(sz / 100 === s.size && sz >= 0 && sz < 4e15);
        const joined = n > 0 && s.x0 === px && s.y0 === py;
        const A = joined ? X : q10(s.x0), B = joined ? Y : q10(s.y0), C = q10(s.x1), D = q10(s.y1);
        const raw = A === null || B === null || C === null || D === null;
        if (nk) fl |= 2;
        if (nst) fl |= 4 | (rawSize ? 32 : 0);
        if (joined) fl |= 8;
        if (raw) fl |= 16;
        need(1); buf[p++] = fl;
        if (nk) { vu(si(s.o)); vu(si(s.sid)); o = s.o; sid = s.sid; }
        if (nst) { vu(si(s.color)); if (rawSize) f64(s.size); else vu(sz); color = s.color; size = s.size; }
        if (raw) { if (!joined) { f64(s.x0); f64(s.y0); } f64(s.x1); f64(s.y1); }
        else { if (!joined) { vs(A - X); vs(B - Y); } vs(C - A); vs(D - B); }
        px = s.x1; py = s.y1; X = Math.round(s.x1 * 10); Y = Math.round(s.y1 * 10); n++;
      }
      return true;
    },
    finish() {
      const body = buf.subarray(0, p), te = new TextEncoder(), head = [];
      const hv = (x) => { while (x >= 128) { head.push((x % 128) | 128); x = Math.floor(x / 128); } head.push(x); };
      head.push(68, 50); hv(strs.length);
      for (const s of strs) { const b = te.encode(s); hv(b.length); for (const c of b) head.push(c); }
      hv(n);
      const out = new Uint8Array(head.length + body.length);
      out.set(head, 0); out.set(body, head.length);
      return out;
    },
  };
}
const sameSeg = (a, b) => a.x0 === b.x0 && a.y0 === b.y0 && a.x1 === b.x1 && a.y1 === b.y1 && a.color === b.color && a.size === b.size && a.o === b.o && a.sid === b.sid && a.f === b.f;
// 그림 전체의 압축본 (room.ver 로 캐시). 'D2' 로 만들고, 풀어서 원래와 하나하나 같은지 확인한 뒤에만 쓴다 (다르면 예전 JSON 형식으로).
// fmt: 'z2' (D2 를 zlib 압축) 또는 'z1' (JSON 을 zlib 압축). 저장소에는 fmt + ':' + base64 로 저장한다.
function makeSnap(room) {
  if (room.zc && room.zc.ver === room.ver) return Promise.resolve(room.zc);
  if (room.zp && room.zp.ver === room.ver) return room.zp.p;
  const ver = room.ver, parts = room.hb.map((b) => b.a.slice());   // 지금 모습을 붙잡아 둔다 (선분 객체 자체는 바뀌지 않는다)
  const p = (async () => {
    let cpu = 0, mark = Date.now();
    const pause = async () => { if (Date.now() - mark >= 8) { cpu += Date.now() - mark; await new Promise((r) => setImmediate(r)); mark = Date.now(); } };
    let total = 0; for (const a of parts) total += a.length;
    const enc = segEncoder(total);
    let ok = true;
    for (const a of parts) { if (!enc.add(a)) { ok = false; break; } await pause(); }
    let z = null;
    if (ok) {
      const bin = enc.finish();
      let same = false;
      try {                                                         // 하나씩 풀어 원래 선분과 비교 (덩어리마다 잠깐 쉬어 다른 방이 멈추지 않게)
        const rd = d2Reader(bin);
        same = !!rd && rd.count === total;
        for (const a of parts) { if (!same) break; for (const s of a) if (!sameSeg(s, rd.next())) { same = false; break; } await pause(); }
        if (same) rd.end();
      } catch (e) { same = false; console.error('D2 확인 실패:', e.message); }
      if (same) { cpu += Date.now() - mark; z = { ver, fmt: 'z2', buf: await deflateP(bin, { level: 6 }) }; mark = Date.now(); }
      else console.error('D2 로 담은 그림이 원래와 달라요: 이 방은 예전 형식(JSON)으로 저장해요');
    }
    if (!z) z = await jsonSnap(parts, ver, pause);
    cpu += Date.now() - mark;
    room.snapMs = cpu;
    if (room.ver === ver) room.zc = z;
    return z;
  })();
  room.zp = { ver, p };
  p.then(() => { if (room.zp && room.zp.p === p) room.zp = null; }, () => { if (room.zp && room.zp.p === p) room.zp = null; });
  return p;
}
// 예전 형식: JSON 을 zlib 압축 (옛 화면(z=1)에 보낼 때와, D2 에 담을 수 없을 때)
async function jsonSnap(parts, ver, pause) {
  const strs = [];
  for (const a of parts) { if (a.length) strs.push(JSON.stringify(a).slice(1, -1)); await pause(); }
  return { ver, fmt: 'z1', buf: await deflateP('[' + strs.join(',') + ']', { level: 1 }) };
}
function makeSnapJson(room) {                                      // 옛 화면(z=1)용 JSON 압축본 (room.ver 로 따로 캐시)
  if (room.zc && room.zc.ver === room.ver && room.zc.fmt === 'z1') return Promise.resolve(room.zc);
  if (room.zj && room.zj.ver === room.ver) return Promise.resolve(room.zj);
  const ver = room.ver, parts = room.hb.map((b) => b.a.slice());
  return jsonSnap(parts, ver, async () => {}).then((z) => { if (room.ver === ver) room.zj = z; return z; });
}

function newRoomId() {
  let s = '';
  for (let i = 0; i < 10; i++) s += ID_ALPHABET[crypto.randomInt(ID_ALPHABET.length)];
  return s;
}

// ---- 저장소: 바깥에서 setPlatform 으로 붙인다. 없으면 메모리에만 (서버가 재시작되거나 잠들면 그림이 사라진다) ----
// storeFor(방 id) → { loadRow(id) → { history, names, meta, extra } | null, loadParts(id) → [{ pid, data }],
//   saveRoom(id, { clear, put: [{ pid, data }], del: [pid], head, names }) — 한 트랜잭션 (clear: 그 방 덩어리를 먼저 모두 지움, head 가 null 이면 방 행은 그대로),
//   saveMeta(id, json), saveExtra?(id, json) — 채팅 같은 덤 (Cloudflare 는 방이 자주 잠들었다 깨어나서 채팅도 저장한다) }
// init(): 표 만들기·읽기 확인 (안 되면 throw). term(ws): 접속을 바로 끊기. attach(ws): 접속 정보가 바뀌었을 때 (Cloudflare 는 소켓에 적어 둔다). track(promise): 뒤에서 도는 일
// dbState: 'none'(저장소 없음) | 'connecting' | 'ok' | 'error'(연결·저장 실패 — 계속 다시 시도한다)
let dbState = 'none';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// clientCloses: 강퇴·밴 때 서버가 닫지 않고 화면이 'bye' 를 받아 닫게 한다 (Cloudflare 의 쉬는 소켓은 다른 접속의 메시지를 처리하는 중에 닫으면
//   닫기 인사가 상대에게 가지 않고, 그 뒤 화면이 닫으려 해도 끝나지 않는다 — 로컬 workerd 에서 확인). afterBye(ws): bye 를 보낸 직후
let P = { hasStore: false, storeFor: () => null, init: async () => {}, term: (ws) => ws.close(), attach: () => {}, track: () => {}, afterBye: () => {}, clientCloses: false };
let dbReady = Promise.resolve();
function setPlatform(p) {
  P = { ...P, ...p };
  if (P.hasStore) { dbState = 'connecting'; dbReady = initDb(); }
  else console.log('DATABASE_URL 없음: 서버가 재시작되거나 잠들면 그림이 사라져요');
}
// 연결 주소 정리: 따옴표·공백을 없애고, 주소 안의 sslmode 같은 옵션은 뺀다(아래 ssl 설정이 주소 옵션에 덮어써지지 않게). 로컬 DB 인지도 판단한다.
function parseDbUrl(raw) {
  const s0 = String(raw || '').trim().replace(/^["']+|["']+$/g, '').trim();
  if (!s0) return null;
  let s = s0, local = false;
  try {
    const u = new URL(s0);
    local = /^(localhost|127\.0\.0\.1|\[::1\])$/.test(u.hostname) || u.searchParams.get('sslmode') === 'disable';
    for (const k of ['sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'channel_binding', 'uselibpqcompat']) u.searchParams.delete(k);
    s = u.toString();
  } catch {}
  return { url: s, local };
}
function setDbState(st) {
  if (!P.hasStore || dbState === st) return;
  console.log(`DB 상태: ${dbState} -> ${st}`);
  dbState = st;
  for (const room of rooms.values()) broadcastAll(room, roomStateMsg(room));   // 화면의 '저장 꺼짐' 표시를 바로 갱신한다
}
let dbInit = null;
function initDb() {                                  // 표 만들기 + 읽기 확인. 실패하면 'error' 로 두고 나중에 다시 시도한다
  if (dbInit) return dbInit;
  dbInit = (async () => {
    try {
      await P.init();
      setDbState('ok');
      console.log('DB 연결됨: 그림이 영구 저장돼요');
    } catch (e) {
      setDbState('error');
      console.error('DB를 쓸 수 없어요 (그림이 저장되지 않아요. 계속 다시 시도해요):', e.message);
    } finally { dbInit = null; }
  })();
  return dbInit;
}

// 저장소에서 읽은 행(+ 덩어리들)을 그림으로. 형식이 틀리면 throw
async function decodeStored(row, partRows) {
  // 그림은 'z2:' + base64(zlib 압축 D2) 로 저장한다. 예전 방의 'z1:'(zlib 압축 JSON) 과 JSON 그대로도 읽는다.
  let history, zbuf = null, zfmt = null;
  const h = typeof row.history === 'string' ? row.history : '';
  if (h.startsWith('p1:')) {                                    // 덩어리로 나눠 저장한 방 (22차부터)
    const m = /^p1:(\d+):([\d,-]*)$/.exec(h);
    if (!m) throw new Error('덩어리 목록 형식이 아니에요');
    const order = unpackPids(m[2]), byPid = new Map();
    for (const x of partRows || []) byPid.set(Number(x.pid), x.data);
    const parts = [];
    history = [];
    for (const pid of order) {
      if (!byPid.has(pid)) throw new Error(`덩어리 ${pid} 가 없어요`);
      const segs = await decodePart(byPid.get(pid));
      parts.push({ pid, n: segs.length });
      for (const s of segs) history.push(s);
    }
    if (history.length !== +m[1]) throw new Error('조각 수가 저장된 수와 달라요');
    return { history, names: JSON.parse(row.names || '{}'), meta: JSON.parse(row.meta || '{}'), parts, pids: [...byPid.keys()], head: h, namesRaw: row.names, extra: row.extra || null };
  }
  if (h.startsWith('z2:')) { zbuf = Buffer.from(h.slice(3), 'base64'); zfmt = 'z2'; history = decodeSegs(new Uint8Array(await inflateP(zbuf))); if (!history) throw new Error('D2 형식이 아니에요'); }
  else if (h.startsWith('z1:')) { zbuf = Buffer.from(h.slice(3), 'base64'); zfmt = 'z1'; history = JSON.parse((await inflateP(zbuf)).toString('utf8')); }
  else history = JSON.parse(row.history);
  return { history, names: JSON.parse(row.names || '{}'), meta: JSON.parse(row.meta || '{}'), zbuf, zfmt, pids: (partRows || []).map((x) => Number(x.pid)), head: null, namesRaw: row.names, extra: row.extra || null };
}
// 다른 저장소의 방(예전 형식 포함)을 덩어리 형식으로 (Cloudflare 로 옮겨 올 때): { head, names, meta, parts: [{ pid, data }] }
async function packRoom(row, partRows) {
  const d = await decodeStored(row, partRows);
  const h = cleanHistory(d.history), parts = [], pids = [];
  for (let i = 0; i < h.length; i += BLOCK) { const pid = pids.length + 1; parts.push({ pid, data: await encodePart(h.slice(i, i + BLOCK)) }); pids.push(pid); }
  return { head: 'p1:' + h.length + ':' + packPids(pids), names: typeof row.names === 'string' ? row.names : '{}', meta: typeof row.meta === 'string' ? row.meta : '{}', parts };
}
// 방 하나 불러오기. DB 가 잠깐 안 될 수 있어서(무료 DB 는 잠들어 있다 깨어난다) 몇 번 다시 시도한다.
// 끝내 못 읽으면 failed: 읽지 못한 채로 저장하면 기존 그림을 덮어쓰게 되므로 그 방은 저장하지 않는다(나중에 DB 가 돌아오면 recoverRoom 이 다시 읽어 합친다).
async function loadRoom(id, attempts = 3) {
  const empty = { history: [], names: {}, meta: null };
  const st = P.storeFor(id);
  if (!st) return empty;
  let row = null, partRows = null, ok = false;
  for (let a = 1; a <= attempts && !ok; a++) {
    try {
      await dbReady;
      if (dbState === 'error') await initDb();
      row = (await st.loadRow(id)) || null;
      if (row && typeof row.history === 'string' && row.history.startsWith('p1:')) partRows = await st.loadParts(id);
      ok = true;
      setDbState('ok');
    } catch (e) {
      console.error(`불러오기 실패 (${a}/${attempts}):`, e.message);
      if (a < attempts) await sleep(800 * a);
    }
  }
  if (!ok) { setDbState('error'); return { ...empty, failed: true }; }
  if (!row) return empty;
  try {
    return await decodeStored(row, partRows);
  } catch (e) {
    console.error('저장된 데이터를 읽을 수 없어요 (이 방은 저장하지 않아요):', e.message);
    return { ...empty, failed: true, corrupt: true };
  }
}

// 같은 방의 저장은 순서대로 한다 (먼저 시작한 오래된 내용이 나중의 새 내용을 덮어쓰지 않게)
function enqueue(room, fn) { room.q = (room.q || Promise.resolve()).then(fn, fn); return room.q; }
// 덩어리 하나 담기: 'z2:' + base64(zlib(D2)). 풀어서 원래와 하나하나 같을 때만 D2, 아니면 'z1:' (JSON 압축)
async function encodePart(a) {
  const enc = segEncoder(a.length);
  if (enc.add(a)) {
    const bin = enc.finish();
    let same = false;
    try { const back = decodeSegs(bin); same = !!back && back.length === a.length && a.every((s, i) => sameSeg(s, back[i])); } catch {}
    if (same) return 'z2:' + (await deflateP(bin, { level: 6 })).toString('base64');
    console.error('D2 로 담은 덩어리가 원래와 달라요: 이 덩어리는 예전 형식(JSON)으로 저장해요');
  }
  return 'z1:' + (await deflateP(JSON.stringify(a), { level: 6 })).toString('base64');
}
async function decodePart(d) {
  if (typeof d !== 'string') throw new Error('덩어리 형식이 아니에요');
  if (d.startsWith('z2:')) { const a = decodeSegs(new Uint8Array(await inflateP(Buffer.from(d.slice(3), 'base64')))); if (!a) throw new Error('D2 형식이 아니에요'); return a; }
  if (d.startsWith('z1:')) { const a = JSON.parse((await inflateP(Buffer.from(d.slice(3), 'base64'))).toString('utf8')); if (!Array.isArray(a)) throw new Error('덩어리 형식이 아니에요'); return a; }
  throw new Error('덩어리 형식이 아니에요');
}
// 덩어리 번호 순서를 짧게: 이어지는 번호는 '처음-끝' (보통 거의 순서대로라 그림이 커져도 몇 글자)
function packPids(a) {
  const out = [];
  for (let i = 0; i < a.length;) { let j = i; while (j + 1 < a.length && a[j + 1] === a[j] + 1) j++; out.push(j > i ? a[i] + '-' + a[j] : String(a[i])); i = j + 1; }
  return out.join(',');
}
function unpackPids(t) {
  const out = [];
  for (const p of t ? t.split(',') : []) {
    const m = /^(\d+)(?:-(\d+))?$/.exec(p);
    if (!m) throw new Error('덩어리 목록 형식이 아니에요');
    const a = +m[1], b = m[2] === undefined ? a : +m[2];
    if (b < a || b - a > 1e6) throw new Error('덩어리 목록 형식이 아니에요');
    for (let k = a; k <= b; k++) out.push(k);
  }
  return out;
}
// 불러온 방이 DB 에 어떤 덩어리로 저장돼 있는지 기억한다 (다음 저장 때 바뀐 것만 쓰고, 안 쓰는 번호는 지운다)
function adoptSaved(room, d) {
  room.savedPids = new Set(d.pids || []);
  room.nextPid = 1; for (const p of room.savedPids) if (p >= room.nextPid) room.nextPid = p + 1;
  room.savedHead = d.head || null;
  room.savedNames = typeof d.namesRaw === 'string' ? d.namesRaw : null;
}
// 덩어리 순서대로 그림 넣기 (저장된 덩어리 그대로 → 저장할 때 다시 보내지 않는다)
function histSetParts(room, arr, parts) {
  room.hb = [];
  let i = 0;
  for (const p of parts) { const b = blockOf(arr.slice(i, i + p.n)); b.pid = p.pid; b.saved = 0; room.hb.push(b); i += p.n; }
  room.hn = arr.length;
  histBump(room);
}
// force: 바로 저장 (마지막 사람이 나갈 때·종료 신호·전체 지우기 등). 아니면 SAVE_MS 간격으로.
// 바뀐 덩어리(새로 생겼거나 그 자리에서 바뀐 것)만 DB 에 쓴다: 그림을 그리는 동안은 보통 마지막 덩어리 하나(수 KB).
// (21차까지는 5초마다 그림 전체(수백 KB~수 MB)를 DB 로 보내서 Render 무료 전송량 5GB 를 이틀 만에 다 썼다)
// 덩어리 쓰기·안 쓰는 덩어리 지우기·순서 목록 바꾸기는 한 트랜잭션: 중간에 실패하면 DB 는 앞의 저장 그대로다.
function saveRoom(id, room, force) {
  const st = P.storeFor(id);
  if (!st || !room.dirty || room.noSave) return Promise.resolve();
  if (!force && Date.now() - (room.lastSave || 0) < SAVE_MS) return Promise.resolve();
  return enqueue(room, async () => {
    if (!room.dirty || room.noSave) return;           // 앞선 저장이 이미 최신 내용을 저장했을 수 있다
    room.dirty = false; room.lastSave = Date.now();
    try {
      if (room.hb.length > 2 * Math.ceil(room.hn / BLOCK) + 8) histSet(room, histArr(room));   // 지우개로 잘게 쪼개진 덩어리는 가끔 다시 묶는다 (그때만 전체를 다시 쓴다)
      if (!room.savedPids) adoptSaved(room, {});
      // 이 시점의 그림을 붙잡는다 (그사이 바뀐 건 dirty 가 다시 켜져 다음에 저장된다)
      const order = [], put = [], keep = new Set();
      let n = 0;
      for (const b of room.hb) {
        n += b.a.length;
        if (b.pid !== undefined && b.saved === b.rev) { order.push(b.pid); keep.add(b.pid); continue; }
        const pid = b.pid !== undefined ? b.pid : room.nextPid++;
        put.push({ b, pid, rev: b.rev, a: b.a.slice() });
        order.push(pid); keep.add(pid);
      }
      const del = [...room.savedPids].filter((p) => !keep.has(p));
      const head = 'p1:' + n + ':' + packPids(order), names = JSON.stringify(room.names);
      if (!put.length && !del.length && head === room.savedHead && names === room.savedNames) return;
      let mark = Date.now();
      for (const p of put) {
        p.data = await encodePart(p.a);
        if (Date.now() - mark >= 8) { await new Promise((r) => setImmediate(r)); mark = Date.now(); }
      }
      const rowChanged = head !== room.savedHead || names !== room.savedNames;
      // 처음 덩어리로 저장하는 방(clear)은 혹시 남은 옛 덩어리를 먼저 지운다
      await st.saveRoom(id, { clear: !room.savedHead, put: put.map((p) => ({ pid: p.pid, data: p.data })), del, head: rowChanged ? head : null, names });
      for (const p of put) { p.b.pid = p.pid; p.b.saved = p.rev; }
      room.savedPids = keep; room.savedHead = head; room.savedNames = names;
      dbOut += put.reduce((t, p) => t + p.data.length, 0) + head.length + names.length;
      setDbState('ok');
    } catch (e) {
      room.dirty = true;
      setDbState('error');
      console.error('저장 실패 (다시 시도해요):', e.message);
    }
  });
}

// ---- 닉네임 ----
function cleanNick(s) { return Array.from(cleanNick.raw(s)).slice(0, NAME_MAX).join(''); }
cleanNick.raw = (s) => String(s || '')                          // 보이지 않는 글자·줄 바꿈을 빼고 공백을 하나로
  .normalize('NFC')
  .replace(/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/gu, '')
  .replace(/\s+/g, ' ')
  .trim();

const TITLE_MAX = 30;                                            // 방 이름 최대 글자 수
function cleanTitle(s) { return Array.from(cleanNick.raw(s)).slice(0, TITLE_MAX).join(''); }
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
  for (const b of room.hb) for (const s of b.a) alive.add(s.o);
  for (const c of room.clients) alive.add(c.owner);
  for (const o of Object.keys(room.names)) {
    if (!alive.has(o)) { delete room.names[o]; room.dirty = true; }
  }
}

// 불러오기에 실패했던 방: DB 가 돌아오면 다시 읽어서, 그동안 새로 그린 것과 합친다 (저장돼 있던 그림은 그대로 두고 새 그림을 그 위에 올린다)
async function recoverRoom(id, room) {
  if (!room.loadFailed || room.corrupt || room.recovering || !P.storeFor(id)) return;
  room.recovering = true;
  try {
    const d = await loadRoom(id, 1);
    if (d.failed) { if (d.corrupt) room.corrupt = true; return; }
    const mem = histArr(room), h = cleanHistory(d.history);
    assignLegacySids(h);
    insertSegs(h, mem);
    adoptSaved(room, d);
    histReplace(room, h);
    room.names = { ...(isPlainObj(d.names) ? d.names : {}), ...room.names };
    room.meta = normalizeMeta(d.meta);
    room.noSave = false; room.loadFailed = false; room.dirty = true;
    console.log(`방 ${id}: 저장돼 있던 그림을 다시 불러와 합쳤어요`);
    for (const c of room.clients) sendHistory(c, room);
    broadcastAll(room, { type: 'names', names: room.names });
    broadcastAll(room, roomStateMsg(room));
    for (const c of [...room.clients]) {                 // 이제 방장·밴·잠금 정보를 알 수 있으니 다시 확인한다
      const v = admit(room, id, c.owner, c.ipt);
      if (v) { byeAndClose(c, v); continue; }
      send(c, meMsg(room, c.owner));
      if (isRoomOwner(room, c.owner)) send(c, adminMsg(room));
    }
  } finally { room.recovering = false; }
}

// 방 하나 점검: 저장 → (실패했던 방 복구) → 오래 비어 있던 방을 메모리에서 내림
async function maintainRoom(id, room, force) {
  if (!room.loaded) return;                            // 아직 불러오는 중인 방은 건드리지 않는다
  pruneNames(room);
  purgeMeta(room);
  if (room.loadFailed) await recoverRoom(id, room);
  await saveRoom(id, room, force || room.clients.size === 0);   // 아무도 없는 방은 기다릴 이유가 없다
  if (force || room.clients.size === 0 || Date.now() - (room.metaAt || 0) >= META_GAP_MS) await saveMeta(id, room);   // (입장 기록만 바뀐 건 1분에 한 번)
  await saveExtra(id, room);
  const clean = !P.storeFor(id) || (!room.dirty && !room.metaDirty && !room.extraDirty);   // 저장소가 없으면 dirty 표시는 의미가 없다
  const keep = dbState === 'ok' ? ROOM_EMPTY_MS : NO_DB_KEEP_MS;  // 저장된 빈 방은 곧 내리고, 메모리가 유일한 사본일 땐 오래 둔다
  if (room.clients.size === 0 && Date.now() - room.lastActive >= keep && clean && !room.loadFailed && rooms.get(id) === room) { rooms.delete(id); console.log(`방 ${id}: 비어서 메모리에서 내림${P.hasStore ? ' (DB 에 저장돼 있어요)' : ''}`); }
}
// 모든 방 점검 (Node: SAVE_MS 마다, 종료할 때). 겹쳐 돌지 않고 순서대로 돈다.
let saveBusy = false, saveChain = Promise.resolve();
async function runSaveAll(force) {
  saveBusy = true;
  try {
    if (P.hasStore && dbState === 'error') await initDb();     // 연결이 끊겨 있었다면 먼저 복구해 본다
    for (const [id, room] of [...rooms]) await maintainRoom(id, room, force);
  } catch (e) { console.error('저장 점검 오류:', e.message); }
  finally { saveBusy = false; }
}
const saveAll = (force) => (saveChain = saveChain.then(() => runSaveAll(force), () => runSaveAll(force)));
const saveTick = () => { if (!saveBusy) saveAll(); };

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

const isPlainObj = (o) => !!o && typeof o === 'object' && !Array.isArray(o);
// DB 에서 읽은 선분 중 쓸 수 없는 것(손상된 값)은 버린다. 정상적인 선분은 그대로 둔다.
const isSeg = (s) => !!s && typeof s === 'object' && [s.x0, s.y0, s.x1, s.y1, s.size].every(Number.isFinite);
const cleanHistory = (h) => (Array.isArray(h) ? h.filter(isSeg) : []);
function getRoom(id) {
  let room = rooms.get(id);
  if (!room) {
    room = { id, clients: new Set(), hb: [], hn: 0, ver: 0, flat: null, zc: null, zp: null, snapMs: 0, lastSave: 0,
      names: {}, meta: normalizeMeta(null), dirty: false, metaDirty: false, noSave: false,
      loaded: false, loadFailed: false, corrupt: false, recovering: false, lastActive: Date.now(), q: null,
      chat: [], chatSeq: 0, leaveTimers: new Map(), recent: new Map(), rate: new Map(), cover: new Map(), fcover: new Map() };
    const r = room;
    r.ready = loadRoom(id).then((d) => {
      const h = cleanHistory(d.history);
      const legacy = assignLegacySids(h);
      adoptSaved(r, d);
      if (d.parts && !legacy && h.length === d.history.length) histSetParts(r, h, d.parts);
      else { histSet(r, h); if (d.parts) r.dirty = true; }        // (덩어리로 저장된 방인데 고친 게 있으면 새로 저장)
      if (legacy) r.dirty = true;
      else if (d.zbuf && Array.isArray(d.history) && h.length === d.history.length) r.zc = { ver: r.ver, fmt: d.zfmt, buf: d.zbuf };   // 저장돼 있던 압축본이 지금 그림과 똑같으면 그대로 보낸다
      r.names = isPlainObj(d.names) ? d.names : {};
      r.loadFailed = !!d.failed; r.corrupt = !!d.corrupt;
      r.noSave = r.loadFailed;
      r.meta = normalizeMeta(d.meta);
      if (d.extra) restoreExtra(r, d.extra);
      r.loaded = true;
    });
    rooms.set(id, room);
  }
  room.lastActive = Date.now();
  return room;
}

function broadcast(room, sender, obj) {
  const data = JSON.stringify(obj);
  for (const c of room.clients) if (c !== sender) sendRaw(c, data);
}
// 압축한 그림을 보내는 중인 사람(ws.hold)에게 갈 메시지는 모아 뒀다가 그림 다음에 순서대로 보낸다
function sendRaw(ws, data) { if (ws.readyState !== 1) return; if (ws.hold) ws.hold.push(data); else ws.send(data); }
// 그림 전체 보내기: 압축을 받을 수 있는 화면(z=1)에는 압축본을 바이너리로, 아니면(옛 브라우저·작은 그림) JSON 으로
const Z_MIN = 2000;
// ---- 덩어리 맞추기 (z=3): 화면이 이미 가진 덩어리 이름 목록을 보내면, 서버는 화면에 없는 덩어리만 보낸다 ----
// 들어올 때·다시 접속할 때 그림 전체(친구 방 약 270KB) 대신 바뀐 덩어리만 (보통 몇 KB). 화면은 덩어리를 IndexedDB 에 보관한다.
// 덩어리 = 서버 그림 보관 단위(hb, 512개 안팎, 저장 단위와 같음). 이름 = 그 D2 바이트의 blockHash: 내용이 같으면 서버를 다시 켜도 같은 이름.
// 'B3' 메시지 (zlib): [B3][요청 번호][덩어리 수][덩어리마다: 짝수 2i = 화면이 보낸 목록의 i 번째를 그대로 / 홀수 2L+1 = 새 덩어리: 이름 14바이트 + D2 L바이트]
// 순서: 그림을 붙잡는 순간부터 이 화면에 갈 메시지는 모아 두었다가(ws.hold) B3 다음에 보낸다 (그림에 이미 들어간 변경이 두 번 적용되지 않게).
const HASH_RE = /^[0-9a-z]{14}$/, SYNC_MAX = 4000, SYNC_WAIT_MS = 4000;
const cleanHashes = (h) => (Array.isArray(h) ? h.slice(0, SYNC_MAX).filter((x) => typeof x === 'string' && HASH_RE.test(x)) : []);
const syncId = (v) => (Number.isInteger(v) && v >= 0 && v < 1e9 ? v : 0);
const syncCapture = (room) => room.hb.map((b) => ({ b, rev: b.rev, a: b.a.slice() }));
function blockD2(c) {                                    // 덩어리의 D2 와 이름 (덩어리에 기억해 두고, 그 자리에서 바뀌면 다시 만든다). D2 로 정확히 담을 수 없으면 null
  const b = c.b;
  if (b.d2 && b.d2rev === c.rev) return b;
  const enc = segEncoder(c.a.length);
  if (!enc.add(c.a)) return null;
  const d2 = enc.finish();
  let back = null;
  try { back = decodeSegs(d2); } catch {}
  if (!back || back.length !== c.a.length || !c.a.every((s, i) => sameSeg(s, back[i]))) return null;
  const e = { d2, h: blockHash(d2) };
  if (b.rev === c.rev) { b.d2 = d2; b.h = e.h; b.d2rev = c.rev; }
  return e;
}
async function buildSync(cap, have, id) {
  const idx = new Map();
  have.forEach((h, i) => { if (!idx.has(h)) idx.set(h, i); });
  const out = [66, 51], chunks = [];
  const vu = (arr, x) => { while (x >= 128) { arr.push((x % 128) | 128); x = Math.floor(x / 128); } arr.push(x); };
  vu(out, id); vu(out, cap.length);
  chunks.push(Buffer.from(out));
  let mark = Date.now(), reused = 0, sent = 0;
  for (const c of cap) {
    const e = blockD2(c);
    if (!e) return null;
    const i = idx.get(e.h), hd = [];
    if (i !== undefined) { vu(hd, 2 * i); chunks.push(Buffer.from(hd)); reused++; }
    else { vu(hd, 2 * e.d2.length + 1); for (let k = 0; k < 14; k++) hd.push(e.h.charCodeAt(k)); chunks.push(Buffer.from(hd), Buffer.from(e.d2.buffer, e.d2.byteOffset, e.d2.byteLength)); sent++; }
    if (Date.now() - mark >= 8) { await new Promise((r) => setImmediate(r)); mark = Date.now(); }
  }
  return { buf: await deflateP(Buffer.concat(chunks), { level: 6 }), reused, sent };
}
// cap 을 붙잡을 때 ws.hold 를 시작했다: 만들어서 보내고, 모아 둔 메시지를 보낸다
function finishSync(ws, have, id, cap) {
  P.track(buildSync(cap, have, id).then((r) => {
    if (ws.readyState !== 1) return;
    if (r) { ws.send(r.buf, { compress: false }); syncStats.n++; syncStats.reused += r.reused; syncStats.sent += r.sent; return; }
    ws.send(JSON.stringify({ type: 'history', data: cap.flatMap((c) => c.a) }));   // D2 로 담을 수 없는 덩어리가 있는 방: 예전 방식 (화면은 보관한 덩어리를 버린다)
  }, (e) => { console.error('덩어리 맞추기 실패:', e && e.message); try { P.term(ws); } catch {} })
    .then(() => { const q = ws.hold; ws.hold = null; if (q && ws.readyState === 1) for (const d of q) ws.send(d); if (ws.syncNext) { const n = ws.syncNext; ws.syncNext = null; onSyncMsg(ws, ws.room, n.h, n.id); } }));
}
const syncStats = { n: 0, reused: 0, sent: 0 };
// 들어올 때: 지금 그림을 붙잡고(이때부터 이 화면에 갈 메시지는 모아 둔다) 화면의 덩어리 목록을 기다린다 (먼저 와 있으면 바로)
function startSync(ws, room) {
  if (ws.readyState !== 1 || ws.hold) return;
  ws.hold = [];
  const cap = syncCapture(room);
  const go = (p) => { clearTimeout(ws.syncT); ws.syncGo = null; finishSync(ws, p.h, p.id, cap); };
  if (ws.pendingSync) { const p = ws.pendingSync; ws.pendingSync = null; go(p); return; }
  ws.syncGo = go;
  ws.syncT = setTimeout(() => { if (ws.syncGo && ws.readyState === 1) ws.syncGo({ h: [], id: 0 }); }, SYNC_WAIT_MS);   // 목록이 안 오면 전체를 보낸다 (그사이 끊겼으면 그만)
}
function onSyncMsg(ws, room, h, id) {
  if (ws.syncGo) { ws.syncGo({ h, id }); return; }        // 들어올 때 기다리던 목록
  if (ws.hold) { ws.syncNext = { h, id }; return; }        // 보내는 중이면 끝난 뒤 (가장 마지막 요청만)
  const now = Date.now();
  if (now - (ws.lastSyncAt || 0) < 1500) { ws.syncNext = { h, id }; clearTimeout(ws.syncT); ws.syncT = setTimeout(() => { const n = ws.syncNext; ws.syncNext = null; if (n && ws.readyState === 1) onSyncMsg(ws, ws.room, n.h, n.id); }, 1500); return; }
  ws.lastSyncAt = now;
  ws.hold = [];
  finishSync(ws, h, id, syncCapture(room));
}
function sendHistory(ws, room) {
  if (ws.readyState !== 1 || ws.hold) return;            // (이미 보내는 중이면 그게 최신 상태를 알려 준다)
  if (!ws.zOk || room.hn < Z_MIN || room.zFail) { send(ws, { type: 'history', data: histArr(room) }); return; }
  const z2 = ws.zOk >= 2;                                            // z=2·3: 'D2' 도 풀 수 있는 화면 / z=1: 옛 화면 (JSON 압축만)
  if (z2 && room.zc && room.zc.ver === room.ver) { ws.send(room.zc.buf, { compress: false }); return; }
  ws.hold = [];
  P.track((z2 ? makeSnap(room) : makeSnapJson(room)).then((z) => { if (ws.readyState === 1) ws.send(z.buf, { compress: false }); }, (e) => {
    console.error('그림 압축 실패:', e && e.message); room.zFail = true; ws.hold = null; try { P.term(ws); } catch {}   // 다시 접속하면 JSON 으로 받는다
  }).then(() => { const q = ws.hold; ws.hold = null; if (q && ws.readyState === 1) for (const d of q) ws.send(d); }));
}
const broadcastAll = (room, obj) => broadcast(room, null, obj);
// 커서: 사람마다 마지막 위치만 모아 두었다가 CUR_FLUSH_MS 마다 한 메시지로 ({type:'curs', l:[주인, x, y, …]}, 자기 커서는 빼고)
// (예전에는 움직일 때마다 다른 모든 사람에게 따로 보냈다: 사람 수의 제곱으로 늘어나는 전송량)
function queueCur(room, o, x, y) {
  (room.curs || (room.curs = new Map())).set(o, [x, y]);
  if (!room.curT) room.curT = setTimeout(() => flushCurs(room), CUR_FLUSH_MS);
}
function flushCurs(room) {
  room.curT = null;
  const q = room.curs;
  if (!q || !q.size) return;
  room.curs = new Map();
  const cache = new Map();
  for (const c of room.clients) {
    let data = cache.get(c.owner);
    if (data === undefined) {
      const l = [];
      for (const [o, p] of q) if (o !== c.owner) l.push(o, p[0], p[1]);
      data = l.length ? JSON.stringify({ type: 'curs', l }) : null;
      cache.set(c.owner, data);
    }
    if (data) sendRaw(c, data);
  }
}
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
const send = (ws, obj) => { if (ws.readyState === 1) sendRaw(ws, JSON.stringify(obj)); };
const isGuest = (o) => /^g[0-9a-f]{10}$/.test(String(o));              // 키 없는 접속('g' + 16진수 10글자)은 방장이 될 수 없다 (키로 만든 ID 는 10글자라 'g' 로 시작해도 손님이 아니다)
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

const IPT_RE = /^[A-Za-z0-9_-]{12}$/;
const tagList = (a) => (Array.isArray(a) ? a.filter((x) => typeof x === 'string' && IPT_RE.test(x)).slice(0, 3) : []);
const numOr = (v, d) => (Number.isFinite(+v) ? +v : d);
function normalizeMeta(j) {
  const m = { owner: null, title: '', locked: false, gate: false, noCover: false, bans: {}, mutes: {}, spectators: {}, mods: {}, log: {} };
  if (!j || typeof j !== 'object') return m;
  m.title = cleanTitle(j.title);
  if (typeof j.owner === 'string' && OWNER_RE.test(j.owner)) m.owner = j.owner;
  m.locked = j.locked === true;
  m.gate = j.gate === true;
  m.noCover = j.noCover === true;                                // 남의 그림 덮기 막기를 끈 방 (기본은 켬)
  const each = (src, max, fn) => { if (src && typeof src === 'object') for (const o of Object.keys(src).slice(0, max)) { const v = src[o]; if (v && typeof v === 'object' && OWNER_RE.test(o)) fn(o, v); } };
  for (const key of ['bans', 'mutes', 'spectators']) {
    each(j[key], MAX_LIST, (o, v) => {
      const e = m[key][o] = { until: numOr(v.until, 0), name: cleanNick(v.name) || '알 수 없음', reason: cleanReason(v.reason) };
      if (key === 'bans') { e.at = numOr(v.at, 0); e.ip = tagList(v.ip); e.ipUntil = numOr(v.ipUntil, 0); e.tries = Math.max(0, numOr(v.tries, 0) | 0); e.ipWanted = v.ipWanted === true; }
    });
  }
  each(j.mods, MAX_MODS, (o, v) => { m.mods[o] = { name: cleanNick(v.name) || '알 수 없음' }; });
  each(j.log, MAX_LOG, (o, v) => {
    const e = m.log[o] = { name: cleanNick(v.name) || '알 수 없음', names: (Array.isArray(v.names) ? v.names : []).map(cleanNick).filter(Boolean).slice(0, 5),
      first: numOr(v.first, 0), last: numOr(v.last, 0), n: Math.max(0, numOr(v.n, 0) | 0), ip: tagList(v.ip) };
    if (v.wait) e.wait = 1;
  });
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
function saveMeta(id, room) {
  const st = P.storeFor(id);
  if (!st || room.noSave || !room.metaDirty) return Promise.resolve();
  return enqueue(room, async () => {
    if (room.noSave || !room.metaDirty) return;
    room.metaDirty = false; room.metaAt = Date.now();
    try {
      await st.saveMeta(id, metaStr(room));
      setDbState('ok');
    } catch (e) {
      room.metaDirty = true;
      setDbState('error');
      console.error('방 설정 저장 실패 (다시 시도해요):', e.message);
    }
  });
}
// 채팅 같은 덤 (저장소가 saveExtra 를 줄 때만: Cloudflare 는 방이 쉬면 메모리에서 내려가서 채팅 기록도 저장해 둔다)
function saveExtra(id, room) {
  const st = P.storeFor(id);
  if (!st || !st.saveExtra) { room.extraDirty = false; return Promise.resolve(); }   // (Node 판은 채팅을 저장하지 않는다)
  if (room.noSave || !room.extraDirty) return Promise.resolve();
  return enqueue(room, async () => {
    if (!room.extraDirty) return;
    room.extraDirty = false;
    try { await st.saveExtra(id, JSON.stringify({ chat: room.chat, seq: room.chatSeq })); }
    catch (e) { room.extraDirty = true; console.error('채팅 저장 실패 (다시 시도해요):', e.message); }
  });
}
function restoreExtra(room, raw) {
  try {
    const x = JSON.parse(raw);
    if (Array.isArray(x.chat)) room.chat = x.chat.slice(-MAX_CHAT);
    if (Number.isInteger(x.seq)) room.chatSeq = x.seq;
  } catch {}
}
function metaStr(room) { const s = JSON.stringify(room.meta); dbOut += s.length; return s; }
function persistMeta(room) { room.metaDirty = true; saveMeta(room.id, room); }

// 부방장: 방장이 없을 때도 분탕을 막을 수 있게 강퇴·밴·채팅 금지·관전·그림 지우기·잠금을 맡긴다 (방장 위임·전체 지우기·부방장 지정은 방장만)
const isMod = (room, o) => !room.noSave && !!room.meta.mods[o];
const isStaff = (room, o) => isRoomOwner(room, o) || isMod(room, o);
// '새로 온 사람은 보기만'(gate)이 켜진 방에서 처음 온 사람은 방장·부방장이 허락할 때까지 그릴 수 없다
const isWaiting = (room, o) => room.meta.gate && !!(room.meta.log[o] && room.meta.log[o].wait) && !isStaff(room, o);
const isSpect = (room, o) => !!room.meta.spectators[o] || isWaiting(room, o);
const roomStateMsg = (room) => ({ type: 'roomstate', owner: room.noSave ? null : room.meta.owner, locked: room.meta.locked, persist: dbState === 'ok' && !room.noSave, max: MAX_HISTORY,
  mods: room.noSave ? [] : Object.keys(room.meta.mods), gate: room.meta.gate, cover: !room.meta.noCover, share: OWNER_SHARE, title: room.meta.title || '' });   // persist: 이 방의 그림이 DB 에 저장되는 중인가
function meMsg(room, o, now = Date.now()) {
  const mu = room.meta.mutes[o];
  return { type: 'me', muted: mu && (mu.until === 0 || mu.until > now) ? { until: mu.until, now, reason: mu.reason || '' } : null, spectator: isSpect(room, o), wait: isWaiting(room, o) };
}
function adminMsg(room) {
  const now = Date.now();
  const L = (src) => Object.keys(src).map((o) => ({ o, name: src[o].name, until: src[o].until, reason: src[o].reason || '' }));
  const bans = Object.keys(room.meta.bans).map((o) => { const b = room.meta.bans[o]; return { o, name: b.name, until: b.until, reason: b.reason || '', ipBlock: b.ip.length > 0 && b.ipUntil > now, ipLater: !!b.ipWanted && !b.ip.length, tries: b.tries || 0 }; });
  const log = Object.keys(room.meta.log).map((o) => { const e = room.meta.log[o]; return { o, name: e.name, names: e.names, first: e.first, last: e.last, n: e.n, on: hasOwner(room, o), wait: isWaiting(room, o) }; });   // (IP 표시는 보내지 않는다)
  return { type: 'admin', now, locked: room.meta.locked, gate: room.meta.gate, cover: !room.meta.noCover, bans, mutes: L(room.meta.mutes), spectators: L(room.meta.spectators), mods: Object.keys(room.meta.mods).map((o) => ({ o, name: room.meta.mods[o].name })), log };
}
function sendAdmin(room) { let m = null; for (const c of room.clients) if (isStaff(room, c.owner)) send(c, m || (m = adminMsg(room))); }
// 입장 기록: 사람(주인 ID)마다 이름·예전 이름·처음/마지막 입장·횟수·최근 IP 표시(되돌릴 수 없게 바꾼 값, 같은 인터넷 밴용). 방장·부방장만 본다.
function logVisit(room, ws, name, entered) {
  if (room.id === 'lobby') return;
  const now = Date.now(), L = room.meta.log, o = ws.owner;
  let e = L[o];
  if (!e) {
    e = L[o] = { name, names: [], first: now, last: now, n: 0, ip: [] };
    if (room.meta.gate && !isStaff(room, o)) e.wait = 1;
    const keys = Object.keys(L);
    if (keys.length > MAX_LOG) {                                   // 가득 차면 접속 중이 아닌 사람 중 가장 오래전에 온 사람부터 뺀다
      keys.sort((a, b) => L[a].last - L[b].last);
      for (const k of keys) { if (Object.keys(L).length <= MAX_LOG) break; if (k !== o && !hasOwner(room, k)) delete L[k]; }
    }
  }
  e.last = now; if (entered) e.n++;
  logName(room, o, name);
  if (ws.ipt) e.ip = [ws.ipt, ...e.ip.filter((x) => x !== ws.ipt)].slice(0, 3);
  room.metaDirty = true;                                          // (5초마다 도는 저장 점검이 저장한다)
}
function logName(room, o, name) {
  const e = room.meta.log[o];
  if (!e || !name || e.name === name) return;
  e.names = [e.name, ...e.names.filter((x) => x !== name && x !== e.name)].slice(0, 5);   // 이름을 바꿔 가며 분탕치는 사람도 알아볼 수 있게
  e.name = name; room.metaDirty = true;
}
function sendMe(room, o) { for (const c of room.clients) if (c.owner === o) send(c, meMsg(room, o)); }

function pushChat(room, m) {
  m.id = ++room.chatSeq; m.t = Date.now();
  room.chat.push(m); room.extraDirty = true;
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
  ws.noLeaveMsg = true; ws.bye = true;                  // (이 뒤로 이 접속이 보내는 메시지는 받지 않는다)
  if (!P.clientCloses) try { ws.close(BYE_CODE[info.code], info.code); } catch {}   // (Cloudflare: 화면이 'bye' 를 받고 스스로 닫는다 — 아래 setPlatform 설명)
  P.afterBye(ws);                                       // (Cloudflare: 닫기 인사가 상대에게 안 갈 수 있어 방에서 바로 뺀다 — 화면은 'bye' 를 받으면 스스로 끊는다)
  // Render 앞단(Cloudflare)을 거치면 닫기 인사가 끝나지 않아 접속이 30초 넘게 남아 있다: 잠시 뒤 끊어 버린다 (화면은 'bye' 를 받자마자 스스로 끝낸다)
  const t = setTimeout(() => { try { P.term(ws); } catch {} }, 1500);
  if (t.unref) t.unref();
}

// 접속 승인: 방장 지정 → 밴 → 같은 인터넷 밴 → 잠금 (거부하면 안내 정보를 돌려준다). ipt: 접속한 곳의 IP 표시
function admit(room, roomId, owner, ipt) {
  const now = Date.now();
  purgeMeta(room, now);
  if (roomId !== 'lobby' && !room.noSave && !room.meta.owner && !isGuest(owner)) {
    room.meta.owner = owner;                            // 방장 정보가 없는 방에 처음 도착한 사람이 방장이 된다
    persistMeta(room);
  }
  if (isRoomOwner(room, owner)) return null;            // 방장은 밴·잠금과 무관하게 항상 입장
  const tried = (b) => { b.tries = (b.tries || 0) + 1; room.metaDirty = true; sendAdmin(room); };   // 밴된 뒤 다시 들어오려 한 횟수 (방장이 볼 수 있게)
  const b = room.meta.bans[owner];
  if (b && (b.until === 0 || b.until > now)) {
    if (b.ipWanted && ipt && !b.ip.includes(ipt)) {               // 밴할 때 IP 를 몰랐던 사람(기록이 생기기 전에만 왔던 사람): 다시 오면 그 인터넷도 막는다
      b.ip = [ipt, ...b.ip].slice(0, 3); b.ipUntil = b.until === 0 ? now + IP_BAN_MAX_MS : Math.min(b.until, now + IP_BAN_MAX_MS); room.metaDirty = true;
    }
    tried(b); return { code: 'banned', text: '방장에 의해 밴되었습니다' + (b.reason ? `(${b.reason})` : ''), until: b.until, reason: b.reason || '' }; }
  // 밴된 사람이 브라우저를 바꾸거나 기록을 지워 새 사람으로 돌아오는 것: 같은 인터넷(IP)에서 처음 오는 사람은 막는다.
  // 밴 전부터 이 방에 오던 사람(같은 와이파이의 친구 등)과 부방장은 막지 않는다.
  if (ipt && !isMod(room, owner)) {
    const e = room.meta.log[owner];
    for (const bo of Object.keys(room.meta.bans)) {
      const bb = room.meta.bans[bo];
      if (!bb.ip.includes(ipt) || !(bb.ipUntil > now) || !(bb.until === 0 || bb.until > now) || (e && e.first < bb.at)) continue;
      tried(bb);
      return { code: 'banned', text: '같은 인터넷(와이파이)에서 밴된 사람이 있어 들어올 수 없어요. 아는 사이라면 방장에게 말해 주세요.', until: bb.until === 0 ? bb.ipUntil : Math.min(bb.until, bb.ipUntil), reason: '' };
    }
  }
  if (room.meta.locked && !hasOwner(room, owner) && !((room.recent.get(owner) || 0) > now - LOCK_GRACE_MS)) return { code: 'locked', text: '방장이 새 접속을 막아 두었어요. 잠시 뒤 다시 시도해 주세요.' };
  return null;
}
function approve(ws, room, wantName) {
  const owner = ws.owner;
  ws.approved = true;
  const wasPresent = hasOwner(room, owner), back = room.leaveTimers.get(owner);   // back: 10초 안에 다시 들어옴(앱 전환 등) — 입장/퇴장 메시지를 내지 않는다
  room.clients.add(ws);
  room.recent.set(owner, Date.now());
  const name = setName(room, owner, wantName);
  const firstTime = !room.meta.log[owner];
  logVisit(room, ws, name, !back && !wasPresent);
  send(ws, { type: 'hello', me: owner });
  if (ws.zOk !== 3) sendHistory(ws, room);
  send(ws, { type: 'names', names: room.names });
  send(ws, roomStateMsg(room));
  send(ws, meMsg(room, owner));
  sendAdmin(room);                                      // 방장·부방장에게 입장 기록·접속 표시를 바로 (들어온 사람이 방장·부방장이면 그 사람도 받는다)
  send(ws, { type: 'chats', list: room.chat });
  if (ws.zOk === 3) startSync(ws, room);                  // (그림은 화면의 덩어리 목록을 받은 뒤: 이 뒤로 이 화면에 갈 메시지는 그림 다음에)
  broadcastAll(room, { type: 'name', o: owner, name });
  announce(room);
  if (back) { clearTimeout(back); room.leaveTimers.delete(owner); }
  else if (!wasPresent) sysChat(room, isWaiting(room, owner) && firstTime ? `${name}님이 처음 들어왔어요 (방장·부방장이 허락하면 그릴 수 있어요)` : `${name}님이 들어왔어요`);
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
  const boss = isRoomOwner(room, ws.owner);
  if (!boss && !isMod(room, ws.owner)) return cmdErr(ws, '권한이 없습니다');
  const cmd = msg.cmd;
  const MOD_CMDS = ['kick', 'ban', 'unban', 'mute', 'unmute', 'spectate', 'clearuser', 'lock', 'gate', 'cover', 'title'];   // 부방장이 할 수 있는 것
  if (!boss && !MOD_CMDS.includes(cmd)) return cmdErr(ws, '방장만 할 수 있어요');
  const TARGET_CMDS = ['kick', 'ban', 'unban', 'mute', 'unmute', 'spectate', 'transfer', 'clearuser', 'mod'];
  let target = null;
  if (TARGET_CMDS.includes(cmd)) {
    target = typeof msg.target === 'string' && OWNER_RE.test(msg.target) ? msg.target : null;
    if (!target) return cmdErr(ws, '대상이 올바르지 않아요');
    if (target === room.meta.owner) return cmdErr(ws, '방장에게는 쓸 수 없는 명령이에요');
    if (!boss && isMod(room, target)) return cmdErr(ws, '부방장에게는 쓸 수 없어요');
  }
  const online = target ? [...room.clients].filter((c) => c.owner === target) : [];
  const logged = target ? room.meta.log[target] : null;
  // 지금 없어도 입장 기록에 있거나 이 방에 그림이 있는 사람(기록이 생기기 전에만 왔던 사람)이면 밴·채팅 금지·관전을 걸 수 있다
  const known = online.length > 0 || !!logged || (!!target && (!!room.names[target] || histArr(room).some((s) => s.o === target)));
  const nm = target ? (room.names[target] || (logged || room.meta.bans[target] || room.meta.mutes[target] || room.meta.spectators[target] || room.meta.mods[target] || {}).name || '알 수 없음') : '';
  const who = boss ? '방장' : '부방장';
  const reason = cleanReason(msg.reason);
  const minutes = msg.minutes;
  if (cmd === 'kick') {
    if (!online.length) return cmdErr(ws, '접속 중인 사용자가 아니에요');
    for (const c of online) byeAndClose(c, { code: 'kicked', text: `${who}에 의해 강퇴되었습니다` + (reason ? `(${reason})` : ''), reason });
    sysChat(room, `${nm}님이 강퇴되었습니다`);
  } else if (cmd === 'ban') {
    if (!known) return cmdErr(ws, '접속 중이거나 입장 기록·그림이 있는 사용자만 밴할 수 있어요');
    if (!checkMinutes(minutes)) return cmdErr(ws, '밴 시간이 올바르지 않아요 (1~43200분 또는 영구)');
    if (Object.keys(room.meta.bans).length >= MAX_LIST && !room.meta.bans[target]) return cmdErr(ws, '밴 목록이 가득 찼어요');
    const until = minutes === 0 ? 0 : now + minutes * MIN_MS;
    // ip: 같은 인터넷(IP)에서 새로 오는 사람도 막기 (밴된 사람이 새 브라우저로 돌아오는 것 방지). 지금 접속한 곳 + 입장 기록의 최근 IP
    const ip = msg.ip === true ? [...new Set([...online.map((c) => c.ipt), ...(logged ? logged.ip : [])].filter(Boolean))].slice(0, 3) : [];
    room.meta.bans[target] = { until, name: nm, reason, at: now, ip, ipUntil: ip.length ? (until === 0 ? now + IP_BAN_MAX_MS : Math.min(until, now + IP_BAN_MAX_MS)) : 0, tries: 0, ipWanted: msg.ip === true };
    delete room.meta.mods[target];                                // (방장이 부방장을 밴하면 부방장도 해제)
    persistMeta(room);
    for (const c of online) byeAndClose(c, { code: 'banned', text: `${who}에 의해 밴되었습니다` + (reason ? `(${reason})` : ''), until, reason });
    sysChat(room, minutes === 0 ? `${nm}님이 영구 밴되었습니다` : `${nm}님이 ${fmtDur(minutes)} 동안 밴되었습니다`);
    broadcastAll(room, roomStateMsg(room));
    sendAdmin(room);
  } else if (cmd === 'unban') {
    if (!room.meta.bans[target]) return cmdErr(ws, '밴 목록에 없는 사용자예요');
    delete room.meta.bans[target]; persistMeta(room);
    sysChat(room, `${nm}님의 밴이 해제되었습니다`); sendAdmin(room);
  } else if (cmd === 'mute') {
    if (!known) return cmdErr(ws, '접속 중이거나 입장 기록에 있는 사용자가 아니에요');
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
    let text;
    if (msg.on === true) {
      if (!known) return cmdErr(ws, '접속 중이거나 입장 기록에 있는 사용자가 아니에요');
      if (Object.keys(room.meta.spectators).length >= MAX_LIST && !room.meta.spectators[target]) return cmdErr(ws, '관전 목록이 가득 찼어요');
      room.meta.spectators[target] = { until: 0, name: nm, reason: '' };
      text = `${nm}님이 관전(보기만 가능)으로 바뀌었습니다`;
    } else {                                                       // 관전 해제 = 그리기 허락 ('새로 온 사람은 보기만' 대기도 함께 푼다)
      const waiting = !!(logged && logged.wait);
      if (!room.meta.spectators[target] && !waiting) return cmdErr(ws, '관전 목록에 없는 사용자예요');
      text = room.meta.spectators[target] ? `${nm}님의 그리기 제한이 해제되었습니다` : `${nm}님이 그리기를 허락받았어요`;
      delete room.meta.spectators[target]; if (logged) delete logged.wait;
    }
    persistMeta(room); sendMe(room, target);
    sysChat(room, text);
    sendAdmin(room);
  } else if (cmd === 'lock') {
    room.meta.locked = msg.on === true; persistMeta(room);
    broadcastAll(room, roomStateMsg(room));
    sysChat(room, room.meta.locked ? '방이 잠겼습니다 (새 접속을 막아요)' : '방 잠금이 해제되었습니다');
    sendAdmin(room);
  } else if (cmd === 'gate') {                                     // 새로 온 사람은 보기만 (방장·부방장이 허락하면 그리기)
    room.meta.gate = msg.on === true; persistMeta(room);
    broadcastAll(room, roomStateMsg(room));
    for (const o of new Set([...room.clients].map((c) => c.owner))) sendMe(room, o);   // 기다리던 사람들의 상태가 바뀔 수 있다
    sysChat(room, room.meta.gate ? '이제 처음 온 사람은 방장·부방장이 허락해야 그릴 수 있어요' : '이제 처음 온 사람도 바로 그릴 수 있어요');
    sendAdmin(room);
  } else if (cmd === 'title') {                                    // 방 이름 (빈 글자 = 지우기)
    const t = cleanTitle(msg.title);
    if (t === room.meta.title) return;
    room.meta.title = t; persistMeta(room);
    broadcastAll(room, roomStateMsg(room));
    sysChat(room, t ? `방 이름이 "${t}"(으)로 바뀌었어요` : '방 이름을 지웠어요');
  } else if (cmd === 'cover') {                                    // 남의 그림 덮기 막기 켜기·끄기
    room.meta.noCover = msg.on !== true; persistMeta(room);
    broadcastAll(room, roomStateMsg(room));
    sysChat(room, room.meta.noCover ? '남의 그림 덮기 막기가 꺼졌어요' : '이제 다른 사람 그림을 한꺼번에 많이 덮으면 막아요');
    sendAdmin(room);
  } else if (cmd === 'mod') {                                      // 부방장 지정·해제 (방장만)
    if (msg.on === true) {
      if (!known || isGuest(target)) return cmdErr(ws, '접속 중이거나 입장 기록에 있는 사용자만 부방장이 될 수 있어요');
      if (room.meta.bans[target]) return cmdErr(ws, '밴된 사용자는 부방장이 될 수 없어요');
      if (Object.keys(room.meta.mods).length >= MAX_MODS && !room.meta.mods[target]) return cmdErr(ws, `부방장은 ${MAX_MODS}명까지예요`);
      room.meta.mods[target] = { name: nm };
      delete room.meta.spectators[target]; if (logged) delete logged.wait;
    } else {
      if (!room.meta.mods[target]) return cmdErr(ws, '부방장이 아니에요');
      delete room.meta.mods[target];
    }
    persistMeta(room);
    broadcastAll(room, roomStateMsg(room));
    sendMe(room, target);
    sysChat(room, msg.on === true ? `${nm}님이 부방장이 되었습니다` : `${nm}님의 부방장이 해제되었습니다`);
    sendAdmin(room);
    if (msg.on !== true) for (const c of online) send(c, { type: 'admin', none: true });   // 해제된 사람 화면에서 관리 정보를 지운다
  } else if (cmd === 'transfer') {
    if (!online.length || isGuest(target)) return cmdErr(ws, '접속 중인 다른 사용자에게만 넘길 수 있어요');
    const old = ws.owner, oldName = room.names[old] || '방장';
    room.meta.owner = target; delete room.meta.mods[target]; persistMeta(room);
    broadcastAll(room, roomStateMsg(room));
    sendMe(room, old); sendMe(room, target); sendAdmin(room);
    sysChat(room, `${oldName}님이 ${nm}님에게 방장을 넘겼습니다`);
  } else if (cmd === 'clearall') {
    histReplace(room, []);
    broadcastAll(room, { type: 'clearall' });
    saveRoom(roomId, room, true);
    sysChat(room, '방장이 모든 그림을 지웠습니다');
  } else if (cmd === 'clearuser') {
    if (histFilter(room, (s) => s.o !== target)) { broadcastAll(room, { type: 'clear', o: target }); saveRoom(roomId, room, true); }
    sysChat(room, `${nm}님의 그림이 모두 지워졌습니다`);
  } else {
    cmdErr(ws, '알 수 없는 명령이에요');
  }
}
// 방장 되찾기: 이사하면서 예전 브라우저의 '나'를 못 넘긴 방장이 쓴다. 코드는 서버 비밀값 OWNER_CLAIM (8글자 이상일 때만 켜짐). 틀리면 1분에 3번까지.
function claimOwner(ws, room, msg) {
  const code = String(process.env.OWNER_CLAIM || '');
  const now = Date.now();
  ws.claimTries = (ws.claimTries || []).filter((t) => t > now - 60000);
  if (code.length < 8 || room.noSave || room.id === 'lobby' || isGuest(ws.owner)) return cmdErr(ws, '방장 되찾기를 쓸 수 없어요');
  if (ws.claimTries.length >= 3) return cmdErr(ws, '잠시 뒤에 다시 해 주세요');
  if (typeof msg.code !== 'string' || msg.code !== code) { ws.claimTries.push(now); return cmdErr(ws, '방장 되찾기 코드가 맞지 않아요'); }
  if (room.meta.owner === ws.owner) return;
  room.meta.owner = ws.owner; delete room.meta.mods[ws.owner]; delete room.meta.bans[ws.owner]; delete room.meta.spectators[ws.owner];
  persistMeta(room);
  broadcastAll(room, roomStateMsg(room));
  for (const o of new Set([...room.clients].map((c) => c.owner))) sendMe(room, o);
  sendAdmin(room);
  sysChat(room, `${room.names[ws.owner] || '누군가'}님이 방장을 되찾았어요`);
  console.log(`방 ${room.id}: 방장 되찾기 코드로 방장이 바뀜`);
}
function undoAllow(ws, n) {                           // 이전/되돌리기로 한꺼번에 넣고 빼는 선분: 초당 500개, 한꺼번에는 6000개까지 (큰 칠하기를 지운 뒤 되돌려도 막히지 않게)
  const now = Date.now();
  ws.undoTok = Math.min(6000, ws.undoTok + (now - ws.undoAt) * 0.5); ws.undoAt = now;
  const c = Math.max(1, n | 0);
  if (ws.undoTok < c) return false;
  ws.undoTok -= c;
  return true;
}
function fillAllow(ws) {                              // 칠하기: 초당 약 4번, 한꺼번에는 8번까지 (한 번이 크고 무거워서 따로 센다)
  const now = Date.now();
  ws.fillTok = Math.min(8, ws.fillTok + (now - ws.fillAt) * 0.004); ws.fillAt = now;
  if (ws.fillTok < 1) return false;
  ws.fillTok -= 1;
  return true;
}
// 칠하기 메시지 검사: 올바르면 정리된 값, 아니면 null. 칸 크기는 0.2 의 배수여야 한다 (좌표가 0.1 단위로 딱 떨어지게).
function checkFill(m) {
  if (typeof m.sid !== 'string' || !SID_RE.test(m.sid)) return null;
  if (typeof m.color !== 'string' || !/^#[0-9a-fA-F]{6}$/.test(m.color)) return null;
  const cell = +m.cell;
  if (!Number.isFinite(cell) || cell < 0.2 - 1e-9 || cell > FILL_CELL_MAX ||Math.abs(cell * 5 - Math.round(cell * 5)) > 1e-6) return null;
  const c = Math.round(cell * 5) / 5;
  if (!Array.isArray(m.rows) || !m.rows.length || m.rows.length > MAX_FILL_SPANS) return null;
  const lim = Math.floor(1e7 / c) - 1;
  const rows = [];
  for (const r of m.rows) {
    if (!Array.isArray(r) || r.length !== 3) return null;
    const j = r[0], a = r[1], b = r[2];
    if (!Number.isInteger(j) || !Number.isInteger(a) || !Number.isInteger(b) || a > b || Math.abs(j) > lim || Math.abs(a) > lim || Math.abs(b) > lim) return null;
    rows.push([j, a, b]);
  }
  return { sid: m.sid, color: m.color.toLowerCase(), cell: c, rows };
}
// 접속 단위 전체 메시지 상한 (도배로 서버 CPU 를 점유하는 것 방지). 초과분은 조용히 버린다.
// 한 사람(방장·부방장 제외)이 그림 용량의 OWNER_SHARE 넘게 쓰지 못하게: 방이 그 비율만큼 찼을 때만 센다 (사람별 수는 2초마다 다시 세고 그사이 추가분을 더한다)
function shareFull(ws, room, owner, add) {
  const cap = Math.floor(MAX_HISTORY * OWNER_SHARE);
  if (room.hn + add <= cap || isStaff(room, owner)) return false;
  const t = Date.now();
  if (!room.own || t - room.own.t > 2000) { const m = new Map(); for (const b of room.hb) for (const s of b.a) m.set(s.o, (m.get(s.o) || 0) + 1); room.own = { t, m }; }
  const c = room.own.m.get(owner) || 0;
  if (c + add > cap) { notice(ws, 'share', '한 사람은 이 방 그림 용량의 ' + Math.round(OWNER_SHARE * 100) + '%까지만 쓸 수 있어요. 내 그림을 조금 지우고 그려 주세요.'); return true; }
  room.own.m.set(owner, c + add);
  return false;
}
// 남의 그림 덮기 막기: 방장·부방장이 아닌 사람의 선이 다른 사람의 획을 짧은 시간에 많이 덮으면 그 뒤 선은 받지 않는다 (흰색 큰 붓으로 그림을 싹 덮는 분탕).
// '덮었다' = 내 선분이 남의 획에 닿고, 내 선이 그 획보다 1.5배 넘게 굵고, 색이 다를 때 (가는 선끼리 지나가거나 같은 색으로 이어 그리는 건 세지 않는다).
// 한 획이 같은 남의 조각을 여러 번 지나가도 한 번만 센다. 획 수와 조각 수 두 가지 한도를 모두 지켜야 하고(위 COVER_*), 한 획의 일부가 막히면 그 획의 나머지도 막는다.
// 덩어리 안을 8개씩 나눈 작은 범위 [xa, xb, ya, yb, 가장 가는 굵기] (이어진 선분은 한 획이라 범위가 좁다). 덩어리 배열·길이가 바뀌면 다시 만든다.
const SUB = 8;
const SKEY = Symbol('stroke');                                      // 선분에 붙여 두는 '주인|획' 이름 (JSON 저장·전송에는 안 들어간다)
function subBoxes(b) {
  const a = b.a;
  if (b.sb && b.sb.arr === a && b.sb.len === a.length) return b.sb;
  const n = Math.ceil(a.length / SUB), box = new Float64Array(n * 5);
  for (let k = 0; k < n; k++) {
    let xa = Infinity, xb = -Infinity, ya = Infinity, yb = -Infinity, ms = Infinity;
    for (let i = k * SUB, e = Math.min(a.length, i + SUB); i < e; i++) {
      const t = a[i], m = t.size / 2;
      xa = Math.min(xa, t.x0 - m, t.x1 - m); xb = Math.max(xb, t.x0 + m, t.x1 + m); ya = Math.min(ya, t.y0 - m, t.y1 - m); yb = Math.max(yb, t.y0 + m, t.y1 + m);
      if (t.size < ms) ms = t.size;
    }
    box.set([xa, xb, ya, yb, ms], k * 5);
  }
  return (b.sb = { arr: a, len: a.length, n, box });
}
function coverHits(room, owner, s, capS, capP, seen) {          // s 가 덮는 남의 획(이름)과 조각. seen: 이 획이 이미 덮은 조각(빼고 센다). 한도를 넘으면 그만 센다
  const r = s.size / 2, edx = s.x1 - s.x0, edy = s.y1 - s.y0, eL = Math.sqrt(edx * edx + edy * edy), keys = new Set(), pieces = [];
  const xa = Math.min(s.x0, s.x1) - r, xb = Math.max(s.x0, s.x1) + r, ya = Math.min(s.y0, s.y1) - r, yb = Math.max(s.y0, s.y1) + r;
  for (const b of room.hb) {
    if (b.x1 < xa || b.x0 > xb || b.y1 < ya || b.y0 > yb) continue;
    const sb = subBoxes(b), box = sb.box, a = b.a;
    for (let k = 0; k < sb.n; k++) {
      const q = k * 5;
      if (box[q + 1] < xa || box[q] > xb || box[q + 3] < ya || box[q + 2] > yb || s.size <= box[q + 4] * 1.5) continue;   // 범위 밖이거나 내 선보다 덜 가는 선만 있음
      for (let i = k * SUB, e = Math.min(a.length, i + SUB); i < e; i++) {
        const t = a[i];
        if (t.o === owner || t.color === s.color || s.size <= t.size * 1.5 || (seen && seen.has(t))) continue;
        const m = t.size / 2;
        if (Math.max(t.x0, t.x1) + m < xa || Math.min(t.x0, t.x1) - m > xb || Math.max(t.y0, t.y1) + m < ya || Math.min(t.y0, t.y1) - m > yb) continue;
        if (!erInterval(t, s.x0, s.y0, edx, edy, eL, r + m)) continue;
        pieces.push(t); keys.add(t[SKEY] || (t[SKEY] = t.o + '|' + t.sid));
        if (keys.size > capS || pieces.length > capP) return { keys, pieces };
      }
    }
  }
  return { keys, pieces };
}
function coverBlocked(ws, room, owner, s) {
  if (room.meta.noCover || isStaff(room, owner)) return false;
  const now = Date.now();
  // 획마다 따로 기억한다 (대칭자처럼 두 획의 선분이 번갈아 와도 각 획의 '이미 덮은 것'·'막힘'이 이어지게. 최근 8획까지)
  const covs = ws.covs || (ws.covs = new Map());
  let c = covs.get(s.sid), q = s;
  if (!c) {                                                         // 새 획: 첫 선분(점)은 꼭 본다
    c = { sid: s.sid, keys: new Set(), seen: new Set(), x: s.x0, y: s.y0 };
    covs.set(s.sid, c);
    if (covs.size > 8) covs.delete(covs.keys().next().value);
  } else {                                                            // 같은 획: 붓 굵기의 절반 넘게 움직였을 때만, 지난번 본 곳부터 여기까지를 한꺼번에 본다 (서버 계산을 줄이려고)
    if (c.no) return true;                                          // 이 획은 이미 막혔다: 나머지도 받지 않는다
    if (Math.hypot(s.x1 - c.x, s.y1 - c.y) < s.size / 2) return false;
    q = { x0: c.x, y0: c.y, x1: s.x1, y1: s.y1, size: s.size, color: s.color };
  }
  let b = room.cover.get(owner);
  if (!b) room.cover.set(owner, (b = { st: COVER_BURST, pc: COVER_PIECES, at: now, said: 0 }));
  const dt = (now - b.at) / 60000;
  b.st = Math.min(COVER_BURST, b.st + dt * COVER_PER_MIN); b.pc = Math.min(COVER_PIECES, b.pc + dt * COVER_PIECES_PER_MIN); b.at = now;
  const h = coverHits(room, owner, q, Math.floor(b.st) + c.keys.size + 1, Math.floor(b.pc) + 1, c.seen);
  let ns = 0;
  for (const k of h.keys) if (!c.keys.has(k)) ns++;                // (이 획이 이미 덮은 남의 획은 다시 세지 않는다)
  if (ns > b.st || h.pieces.length > b.pc) {
    c.no = true;
    notice(ws, 'cover', '다른 사람 그림을 한꺼번에 너무 많이 덮고 있어요. 그 선은 그려지지 않았어요. 잠시 뒤에 다시 그려 주세요.');
    if (now - b.said > 120000) { b.said = now; sysChat(room, `${room.names[owner] || '누군가'}님의 선이 다른 사람 그림을 너무 많이 덮어서 막았어요`); }   // 방장·부방장이 알 수 있게
    return true;
  }
  b.st -= ns; b.pc -= h.pieces.length;
  for (const k of h.keys) c.keys.add(k);
  for (const t of h.pieces) c.seen.add(t);
  c.x = s.x1; c.y = s.y1;
  return false;
}
// 남의 칠하기 덮기 막기: 새 칠하기 줄이 다른 사람의 (색이 다른) 칠하기 조각에 닿는 수를 센다. 칠하기는 늘 선 아래라 선은 덮을 수 없다.
// 줄 j 의 가운데 y = (j + 0.5) x 칸, 굵기 = 칸 x 2. 조각마다 겹칠 수 있는 줄 번호만 찾아 본다.
function fillCoverBlocked(ws, room, owner, list, cell) {
  if (room.meta.noCover || isStaff(room, owner) || !list.length) return false;
  const color = list[0].color, rows = new Map();
  let xa = Infinity, xb = -Infinity, ya = Infinity, yb = -Infinity;
  for (const r of list) {
    const j = Math.round(r.y0 / cell - 0.5), m = r.size / 2;
    (rows.get(j) || rows.set(j, []).get(j)).push([Math.min(r.x0, r.x1) - m, Math.max(r.x0, r.x1) + m]);
    xa = Math.min(xa, r.x0 - m, r.x1 - m); xb = Math.max(xb, r.x0 + m, r.x1 + m); ya = Math.min(ya, r.y0 - m); yb = Math.max(yb, r.y0 + m);
  }
  const now = Date.now();
  let b = room.fcover.get(owner);
  if (!b) room.fcover.set(owner, (b = { pc: FILL_COVER, at: now, said: 0 }));
  b.pc = Math.min(FILL_COVER, b.pc + (now - b.at) / 60000 * FILL_COVER_PER_MIN); b.at = now;
  let n = 0;
  for (const blk of room.hb) {
    if (blk.x1 < xa || blk.x0 > xb || blk.y1 < ya || blk.y0 > yb) continue;
    const sb = subBoxes(blk), box = sb.box, a = blk.a;
    for (let k = 0; k < sb.n; k++) {
      const q = k * 5;
      if (box[q + 1] < xa || box[q] > xb || box[q + 3] < ya || box[q + 2] > yb) continue;
      for (let i = k * SUB, e = Math.min(a.length, i + SUB); i < e; i++) {
        const t = a[i];
        if (!t.f || t.o === owner || t.color === color) continue;
        const ht = t.size / 2, tx0 = Math.min(t.x0, t.x1) - ht, tx1 = Math.max(t.x0, t.x1) + ht, ty = (t.y0 + t.y1) / 2;
        if (tx1 < xa || tx0 > xb || ty + ht < ya || ty - ht > yb) continue;
        let hit = false;
        for (let j = Math.ceil((ty - ht - cell) / cell - 0.5), j1 = Math.floor((ty + ht + cell) / cell - 0.5); j <= j1 && !hit; j++) {
          const sp = rows.get(j);
          if (sp) for (const [x0, x1] of sp) if (x0 < tx1 && x1 > tx0) { hit = true; break; }
        }
        if (hit && ++n > b.pc) {
          notice(ws, 'coverfill', '다른 사람이 칠한 곳을 한꺼번에 너무 많이 덮고 있어요. 이 칠하기는 저장되지 않았어요. 잠시 뒤에 다시 해 주세요.');
          if (now - b.said > 120000) { b.said = now; sysChat(room, `${room.names[owner] || '누군가'}님의 칠하기가 다른 사람 그림을 너무 많이 덮어서 막았어요`); }
          return true;
        }
      }
    }
  }
  b.pc -= n;
  return false;
}
function globalAllow(ws) {
  const now = Date.now();
  ws.gTok = Math.min(1000, ws.gTok + (now - ws.gAt) * 0.8);
  ws.gAt = now;
  if (ws.gTok < 1) return false;
  ws.gTok -= 1;
  return true;
}

// 어느 파일이 올라갔는지 바로 확인하는 용도 (화면의 설정 맨 아래에 '서버 버전'으로 보인다). db: none(저장소 없음) | connecting | ok | error
const versionInfo = () => ({ stage: 3, date: '2026-10-13', rev: 41, db: dbState, dbKB: Math.round(dbOut / 1024), sync: syncStats });

// 들어온 원본 메시지의 크기·종류를 먼저 거른다. 칠하기('fill')만 16KB 를 넘을 수 있고, 그 외의 큰 메시지는 읽지도 않고 버린다.
// 칠하기는 JSON 을 읽기 전에 속도 제한부터 확인한다 (무거운 메시지로 서버를 괴롭히지 못하게).
function onRaw(ws, room, roomId, owner, raw, isBinary) {
  if (isBinary) return;
  const head = raw.toString('utf8', 0, 60);
  const isFill = head.startsWith('{"type":"fill",');
  if (raw.length > MAX_MSG_BYTES && !isFill && !(head.startsWith('{"type":"sync",') && raw.length <= MAX_FILL_BYTES)) return;   // (덩어리 목록도 클 수 있다)
  if (isFill) {
    if (!fillAllow(ws)) { const m = /"sid":"([0-9a-z]{1,12})"/.exec(head); send(ws, { type: 'fillno', sid: m ? m[1] : null, reason: 'rate' }); return; }
    ws.fillPaid = true;                                   // 속도 제한은 여기서 이미 냈다 (onMessage 가 한 번 더 세지 않게)
  }
  try { onMessage(ws, room, roomId, owner, raw); } finally { ws.fillPaid = false; }
}
const DRAW_TYPES = new Set(['seg', 'clear', 'erase', 'erasestroke', 'cancelstroke', 'addsegs', 'removesegs', 'fill']);
function onMessage(ws, room, roomId, owner, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch { return; }
  if (!msg || typeof msg !== 'object') return;
  if (msg.type === 'b') {                               // 화면이 모아 보낸 메시지들 (선·커서·지우개, 0.08초마다): 하나씩 처리한다 (한도도 하나씩)
    if (!Array.isArray(msg.l)) return;
    for (const m of msg.l.slice(0, BATCH_MAX)) if (m && typeof m === 'object' && m.type !== 'b' && m.type !== 'fill') handleMsg(ws, room, roomId, owner, m);
    return;
  }
  handleMsg(ws, room, roomId, owner, msg);
}
const BATCH_MAX = 200;
function handleMsg(ws, room, roomId, owner, msg) {
  try {
    if (!globalAllow(ws)) return;
    const now = Date.now();
    if (DRAW_TYPES.has(msg.type) && isSpect(room, owner)) {   // 관전(보기만 가능)으로 바뀐 사람·허락을 기다리는 사람은 그리기를 할 수 없다
      notice(ws, 'spectator', isWaiting(room, owner) ? '방장·부방장이 허락하면 그릴 수 있어요 (지금은 보기만)' : '방장이 그리기를 제한했어요 (보기만 가능)');
      if (msg.type === 'fill') send(ws, { type: 'fillno', sid: typeof msg.sid === 'string' && SID_RE.test(msg.sid) ? msg.sid : null, reason: 'spectator' });   // 보낸 화면이 방금 칠한 걸 되돌릴 수 있게
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
      if (room.hn >= MAX_HISTORY) { notice(ws, 'full', '그림이 가득 찼어요. 일부를 지우고 다시 그려 주세요.'); return; }
      if (shareFull(ws, room, owner, 1) || coverBlocked(ws, room, owner, s)) return;
      histPush(room, s);
      broadcast(room, ws, { type: 'seg', ...s });
    } else if (msg.type === 'clear') {
      // 내가 그린 선만 지운다 (다른 사람 그림은 그대로)
      histFilter(room, (s) => s.o !== owner);
      broadcast(room, ws, { type: 'clear', o: owner });
      saveRoom(roomId, room, true);
    } else if (msg.type === 'erase') {
      // 지우개: 기본은 내가 그린 선만. 방장은 all 로 남의 선도 지울 수 있다. 잘린 결과를 다른 사람 화면도 같은 함수로 계산한다.
      if (!allowSeg(ws)) return;
      const x0 = r1(msg.x0), y0 = r1(msg.y0), x1 = r1(msg.x1), y1 = r1(msg.y1), r = r1(msg.r);
      if (![x0, y0, x1, y1, r].every(okNum)) return;
      const all = msg.all === true && isRoomOwner(room, owner);
      if (!histErase(room, all ? null : owner, x0, y0, x1, y1, r)) return;
      broadcast(room, ws, { type: 'erase', o: all ? null : owner, x0, y0, x1, y1, r });
    } else if (msg.type === 'erasestroke') {
      // 획 지우개: 경로에 닿은 획 전체를 지우고, 지워진 획 목록을 모두에게 알린다 (받는 쪽은 재계산하지 않는다)
      if (!allowSeg(ws)) return;
      const x0 = r1(msg.x0), y0 = r1(msg.y0), x1 = r1(msg.x1), y1 = r1(msg.y1), r = r1(msg.r);
      if (![x0, y0, x1, y1, r].every(okNum)) return;
      const all = msg.all === true && isRoomOwner(room, owner);
      const q = Number.isInteger(msg.q) && msg.q >= 0 && msg.q < 1e9 ? msg.q : 0;
      const keys = histHit(room, all ? null : owner, x0, y0, x1, y1, r, MAX_STROKES_PER_ERASE);
      if (keys.length) {
        histRemoveStrokes(room, keys);
        broadcastAll(room, { type: 'strokesgone', keys, by: owner, q });
      } else send(ws, { type: 'strokesgone', keys: [], by: owner, q }); // 보낸 사람이 예상과 다른지 확인할 수 있게
    } else if (msg.type === 'cancelstroke') {
      // 잘못 시작된 내 획 취소 (모두의 화면과 저장본에서 지움)
      if (!allowSeg(ws)) return;
      if (typeof msg.sid !== 'string' || !SID_RE.test(msg.sid)) return;
      const keys = [[owner, msg.sid]];
      if (histRemoveStrokes(room, keys)) broadcastAll(room, { type: 'strokesgone', keys, by: owner, cancel: true });
    } else if (msg.type === 'addsegs') {
      // 되돌리기(redo)·지우개 이전(undo)으로 내 선분을 다시 넣는다 (최대 100개, 주인은 항상 나). 칠하기 조각(f)은 선 아래로 들어간다.
      if (!Array.isArray(msg.segs) || !undoAllow(ws, Math.min(msg.segs.length, 100))) return;
      const list = [];
      for (const m of msg.segs.slice(0, 100)) {
        if (!m || typeof m !== 'object') continue;
        const f = m.f === 1;
        const s = { x0: r1(m.x0), y0: r1(m.y0), x1: r1(m.x1), y1: r1(m.y1), color: String(m.color).slice(0, 9), size: Math.min(Math.max(+m.size || 4, f ? 0.1 : 1), PEN_MAX), o: owner, sid: m.sid };
        if (f) s.f = 1;
        if (![s.x0, s.y0, s.x1, s.y1].every(okNum) || typeof s.sid !== 'string' || !SID_RE.test(s.sid)) continue;
        if (room.hn + list.length >= MAX_HISTORY) { notice(ws, 'full', '그림이 가득 찼어요. 일부를 지우고 다시 그려 주세요.'); break; }
        if (!f && coverBlocked(ws, room, owner, s)) break;      // (되돌리기로 큰 선을 바로 넣어 막기를 피하지 못하게)
        list.push(s);
      }
      if (!list.length || shareFull(ws, room, owner, list.length)) return;
      histInsert(room, list);
      broadcast(room, ws, { type: 'segs', list });
    } else if (msg.type === 'fill') {
      // 칠하기: 화면에서 계산한 가로줄 목록을 받아 검사하고, 선분(f)으로 바꿔 선 아래에 넣는다. 한 번에 통째로 들어가거나 통째로 거절된다.
      const sid = typeof msg.sid === 'string' && SID_RE.test(msg.sid) ? msg.sid : null;
      const no = (reason) => send(ws, { type: 'fillno', sid, reason });
      if (ws.fillPaid) ws.fillPaid = false; else if (!fillAllow(ws)) return no('rate');
      const f = checkFill(msg);
      if (!f) return no('bad');
      if (room.hn + f.rows.length > MAX_HISTORY) { notice(ws, 'full', '그림이 가득 찼어요. 일부를 지우고 다시 그려 주세요.'); return no('full'); }
      if (shareFull(ws, room, owner, f.rows.length)) return no('share');
      const fsegs = fillSegs(owner, f.sid, f.color, f.cell, f.rows);
      if (fillCoverBlocked(ws, room, owner, fsegs, f.cell)) return no('cover');
      histInsert(room, fsegs);
      broadcast(room, ws, { type: 'fill', o: owner, sid: f.sid, color: f.color, cell: f.cell, rows: f.rows });
    } else if (msg.type === 'removesegs') {
      // 이전(undo): 내 선분 중 값이 같은 것을 정확히 지운다 (잘린 조각 되돌리기)
      if (!Array.isArray(msg.segs) || !undoAllow(ws, Math.min(msg.segs.length, 100))) return;
      const items = [];
      for (const m of msg.segs.slice(0, 100)) {
        if (!m || typeof m !== 'object' || typeof m.sid !== 'string' || !SID_RE.test(m.sid)) continue;
        const s = { x0: r1(m.x0), y0: r1(m.y0), x1: r1(m.x1), y1: r1(m.y1), o: owner, sid: m.sid };
        if ([s.x0, s.y0, s.x1, s.y1].every(okNum)) items.push(s);
      }
      if (!items.length) return;
      if (!histRemoveExact(room, items)) return;
      broadcast(room, ws, { type: 'unsegs', list: items });
    } else if (msg.type === 'resync') {
      if (now - ws.lastSync < 2000) return;
      ws.lastSync = now;
      sendHistory(ws, room);
    } else if (msg.type === 'sync') {                     // 덩어리 맞추기 (z=3): 들어올 때, 그리고 화면이 서버 그림에 다시 맞추고 싶을 때
      if (ws.zOk !== 3) return;
      onSyncMsg(ws, room, cleanHashes(msg.h), syncId(msg.id));
    } else if (msg.type === 'ping') {
      send(ws, { type: 'pong' });                         // 화면이 다시 보일 때 연결이 살아 있는지 확인 (그림 전체를 다시 받지 않는다)
    } else if (msg.type === 'cur') {
      if (now - ws.lastCur < 25) return;
      ws.lastCur = now;
      const x = r1(msg.x), y = r1(msg.y);
      if (!okNum(x) || !okNum(y)) return;
      queueCur(room, owner, x, y);
    } else if (msg.type === 'curhide') {
      if (room.curs) room.curs.delete(owner);
      broadcast(room, ws, { type: 'curhide', o: owner });
    } else if (msg.type === 'nick') {
      if (now - ws.lastNick < 300) return;
      ws.lastNick = now;
      const name = setName(room, owner, msg.name);
      logName(room, owner, name);
      broadcastAll(room, { type: 'name', o: owner, name });
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
    } else if (msg.type === 'claim') {                   // 방장 되찾기 (서버 비밀값 OWNER_CLAIM 과 같은 코드)
      claimOwner(ws, room, msg);
    }
  } catch (e) {
    console.error('메시지 처리 오류:', e.message);
  }
}

function onClose(ws, room, roomId, owner) {
  if (!ws.approved) return;
  room.clients.delete(ws);
  if (room.curs) room.curs.delete(owner);                           // (모아 둔 커서가 나간 사람 커서를 다시 띄우지 않게)
  broadcastAll(room, { type: 'curhide', o: owner });
  announce(room);
  if (!hasOwner(room, owner)) {
    room.recent.set(owner, Date.now());
    const e = room.meta.log[owner];
    if (e) { e.last = Date.now(); room.metaDirty = true; }
    sendAdmin(room);                                                   // (입장 기록의 '접속 중' 표시)
    if (!ws.noLeaveMsg) {                                              // 강퇴·밴은 이미 시스템 메시지가 있으니 퇴장 메시지는 생략
      const nm = room.names[owner] || '누군가';
      room.leaveTimers.set(owner, setTimeout(() => {
        room.leaveTimers.delete(owner);
        if (!hasOwner(room, owner)) sysChat(room, `${nm}님이 나갔어요`);
      }, LEAVE_GRACE_MS));
    }
  }
  if (room.clients.size === 0) {                        // 마지막 사람이 나가면 바로 저장한다. 메모리에서 내리는 건 주기 점검(runSaveAll)이 맡는다.
    room.lastActive = Date.now();
    pruneNames(room);
    saveRoom(roomId, room);
    saveMeta(roomId, room);
  }
}

// ---- 접속한 곳(IP): 같은 인터넷 밴과 접속 수 제한에만 쓴다. 원래 IP 는 저장하지 않고, 서버만 아는 비밀 값으로 되돌릴 수 없게 바꾼 표시(ipt)만 쓴다 ----
let ipKey = null;                                                  // (처음 쓸 때 만든다: Cloudflare 는 전역에서 난수를 만들 수 없다)
const IP_KEY = () => ipKey || (ipKey = crypto.createHash('sha256').update('ip-tag:' + (process.env.IP_SALT || process.env.DATABASE_URL || crypto.randomBytes(16).toString('hex'))).digest());
function ip6Prefix(ip) {                                           // IPv6 는 앞 64비트만 (기기마다·시간마다 바뀌는 뒷부분은 뺀다)
  const [head, tail] = ip.split('::');
  const h = head ? head.split(':') : [], t = tail === undefined ? null : tail ? tail.split(':') : [];
  const parts = t ? [...h, ...Array(Math.max(0, 8 - h.length - t.length)).fill('0'), ...t] : h;
  return parts.slice(0, 4).map((x) => (parseInt(x, 16) || 0).toString(16)).join(':');
}
function ipTag(ip) {
  if (!ip) return '';
  const key = ip.includes(':') ? ip6Prefix(ip.toLowerCase()) : ip;
  return crypto.createHmac('sha256', IP_KEY()).update(key).digest('base64url').slice(0, 12);
}
const connRate = new Map();                                        // IP 표시 -> { tok, at }
function connAllow(ipt) {                                          // 같은 곳에서 새 접속을 너무 빨리 많이 열지 못하게 (사람을 잔뜩 만들어 분탕치는 것 방지)
  const t = Date.now(), r = connRate.get(ipt) || { tok: CONN_BURST, at: t };
  r.tok = Math.min(CONN_BURST, r.tok + (t - r.at) / 1000 * CONN_PER_SEC); r.at = t;
  connRate.set(ipt, r);
  if (r.tok < 1) return false;
  r.tok -= 1; return true;
}
function pruneConnRate() { const t = Date.now(); for (const [k, r] of connRate) if (r.tok + (t - r.at) / 1000 * CONN_PER_SEC >= CONN_BURST) connRate.delete(k); }   // (1분마다: 다 찬 기록은 지운다)

// ---- 접속 ----
// 바깥(server.js / cf/worker.js)이 소켓을 받으면 connect(ws, { url, cip: { ip, local } }), 메시지가 오면 message, 닫히면 closed.
const sockets = new Set();                             // 지금 열린 모든 접속 (같은 IP 접속 수 세기)
function initSock(ws, owner) {
  const t0 = Date.now();
  ws.owner = owner;
  ws.approved = false;
  ws.tokens = 400; ws.tokAt = t0; ws.gTok = 1000; ws.gAt = t0; ws.cmdTok = 5; ws.cmdAt = t0; ws.undoTok = 6000; ws.undoAt = t0; ws.fillTok = 8; ws.fillAt = t0; ws.fillPaid = false;
  ws.lastCur = 0; ws.lastNick = 0; ws.lastSync = 0;
  ws.isAlive = true;
}
let pruneAt = 0;
async function connect(ws, { url, cip }) {
  if (Date.now() - pruneAt > 60000) { pruneAt = Date.now(); pruneConnRate(); }   // (Cloudflare 에는 1분마다 도는 타이머가 없다)
  sockets.add(ws);
  ws.ipt = cip.local ? '' : ipTag(cip.ip);
  if (ws.ipt) {
    let same = 0;
    for (const c of sockets) if (c.ipt === ws.ipt) same++;
    if (same > CONN_PER_IP || !connAllow(ws.ipt)) { byeAndClose(ws, { code: 'toomany', text: '같은 인터넷에서 접속이 너무 많아요. 잠시 뒤 다시 들어와 주세요.' }); return; }
  }

  const rawRoom = url.searchParams.get('room') || 'lobby';
  const roomId = ROOM_RE.test(rawRoom) ? rawRoom : 'lobby';
  const room = getRoom(roomId);                        // (방의 마지막 활동 시각도 여기서 갱신된다)

  // 브라우저가 보낸 비밀 키를 해시해 '주인 ID'로 쓴다. 키가 없으면 이번 접속에서만 주인.
  ws.zOk = ({ 1: 1, 2: 2, 3: 3 })[url.searchParams.get('z')] || 0;   // 화면이 압축된 그림(바이너리)을 풀 수 있다: 1 = JSON 압축, 2 = D2 도, 3 = 덩어리 맞추기(B3)도
  const key = url.searchParams.get('k') || '';
  const owner = /^[A-Za-z0-9_-]{16,64}$/.test(key)
    ? crypto.createHash('sha256').update(key).digest('base64url').slice(0, 10)
    : 'g' + crypto.randomBytes(5).toString('hex');
  initSock(ws, owner);
  ws.room = room; ws.roomId = roomId;
  P.attach(ws);

  try { await room.ready; } catch (e) { console.error('방 불러오기 오류:', e.message); try { ws.close(1011, 'room load error'); } catch {} return; }
  if (ws.readyState !== 1) return;
  const verdict = admit(room, roomId, owner, ws.ipt);  // 1) 방장 지정  2) 밴  3) 같은 인터넷 밴  4) 잠금
  if (verdict) { byeAndClose(ws, verdict); return; }   // 거부: clients 에 넣지 않고 입장·접속자 수 알림도 하지 않는다
  approve(ws, room, url.searchParams.get('n'));        // 승인: 초기 동기화
  P.attach(ws);
}
// 승인 전·강퇴/밴 뒤에 도착한 메시지는 무시한다
function message(ws, raw, isBinary) {
  if (ws.approved && !ws.bye) return onRaw(ws, ws.room, ws.roomId, ws.owner, raw, isBinary);
  // 덩어리 목록은 화면이 열리자마자 보내서 승인보다 먼저 올 수 있다: 기억해 두었다가 승인 때 쓴다
  if (!ws.bye && ws.zOk === 3 && !isBinary && raw.length <= MAX_FILL_BYTES && raw.toString('utf8', 0, 20).startsWith('{"type":"sync",')) {
    try { const m = JSON.parse(raw); ws.pendingSync = { h: cleanHashes(m.h), id: syncId(m.id) }; } catch {}
  }
}
function closed(ws) {                                   // (두 번 불려도 한 번만: Cloudflare 는 bye 뒤에 바로 부르고, 진짜 닫힘도 나중에 온다)
  if (ws.gone) return;
  ws.gone = true;
  sockets.delete(ws);
  if (ws.room) onClose(ws, ws.room, ws.roomId, ws.owner);
  ws.approved = false; P.attach(ws);
}
// Cloudflare: 쉬다가 메모리에서 내려갔던 방이 다시 깨어날 때, 열려 있던 접속을 다시 붙인다 (아무 메시지도 보내지 않는다).
// list: [[ws, { o: 주인, ipt, z, ap: 승인됨 }]]
async function restore(roomId, list) {
  const room = getRoom(roomId);
  await room.ready;
  for (const [ws, a] of list) {
    sockets.add(ws);
    initSock(ws, a.o);
    ws.ipt = a.ipt || ''; ws.zOk = a.z || 0; ws.room = room; ws.roomId = roomId;
    if (a.ap && ws.readyState === 1) { ws.approved = true; room.clients.add(ws); }
    else { sockets.delete(ws); try { ws.close(1012, 'restart'); } catch {} }   // 승인 전에 잠들었던 접속은 다시 들어오게 한다
  }
  return room;
}
function dropRoom(id) { rooms.delete(id); }            // (Cloudflare: 방이 새로 깨어날 때 예전 메모리를 버린다)

module.exports = {
  setPlatform, connect, message, closed, restore, dropRoom, getRoom, packRoom, parseDbUrl, maintainRoom, saveAll, saveTick, pruneConnRate, versionInfo, newRoomId, ipTag,
  rooms, sockets, get dbState() { return dbState; },
  MAX_FILL_BYTES, PING_MS, SAVE_MS, ROOM_RE, MAX_HISTORY,
};
