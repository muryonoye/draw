const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const rooms = new Map();
const MAX_HISTORY = 200000;

app.get('/', (req, res, next) => {
  if (!req.query.room) return res.redirect('/?room=' + Math.random().toString(36).slice(2, 8));
  next();
});
app.use(express.static(path.join(__dirname, 'public')));

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const roomId = (url.searchParams.get('room') || 'lobby').slice(0, 32);
  if (!rooms.has(roomId)) rooms.set(roomId, { clients: new Set(), history: [] });
  const room = rooms.get(roomId);
  room.clients.add(ws);
  ws.send(JSON.stringify({ type: 'history', data: room.history }));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'seg') {
      const s = {
        x0: +msg.x0, y0: +msg.y0, x1: +msg.x1, y1: +msg.y1,
        color: String(msg.color).slice(0, 9),
        size: Math.min(Math.max(+msg.size || 4, 1), 60),
      };
      if (room.history.length < MAX_HISTORY) room.history.push(s);
      broadcast(room, ws, { type: 'seg', ...s });
    } else if (msg.type === 'clear') {
      room.history = [];
      broadcast(room, ws, { type: 'clear' });
    }
  });

  ws.on('close', () => {
    room.clients.delete(ws);
    if (room.clients.size === 0) {
      setTimeout(() => {
        const r = rooms.get(roomId);
        if (r && r.clients.size === 0) rooms.delete(roomId);
      }, 10 * 60 * 1000);
    }
  });
});

function broadcast(room, sender, obj) {
  const data = JSON.stringify(obj);
  for (const c of room.clients) if (c !== sender && c.readyState === 1) c.send(data);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('http://localhost:' + PORT));
