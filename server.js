// OmniAgent Group Chat — real-time relay server
// Minimal WebSocket relay: broadcasts messages between clients in the same
// room and keeps a short in-memory history so late joiners can catch up.
// No database, no accounts — just a dumb pipe, same spirit as the terminal server.

const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const MAX_HISTORY = 200; // messages kept per room, in memory only (lost on restart)

// Optional shared secret. Set RELAY_KEY as an env var to require
// clients to connect with wss://host/groupchat?key=yoursecret
const RELAY_KEY = process.env.RELAY_KEY || '';

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('OmniAgent Group Chat relay is running.\n');
});

const wss = new WebSocketServer({ server, path: '/groupchat' });

// roomId -> { clients: Set<ws>, history: Array<message> }
const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, { clients: new Set(), history: [] });
  return rooms.get(roomId);
}

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(roomId, obj, exceptWs) {
  const room = rooms.get(roomId);
  if (!room) return;
  for (const client of room.clients) {
    if (client !== exceptWs) send(client, obj);
  }
}

wss.on('connection', (ws, req) => {
  if (RELAY_KEY) {
    const url = new URL(req.url, 'http://x');
    if (url.searchParams.get('key') !== RELAY_KEY) {
      ws.close(4001, 'unauthorized');
      return;
    }
  }

  ws.joinedRooms = new Set();

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || !msg.type) return;

    if (msg.type === 'join' && msg.roomId) {
      const room = getRoom(msg.roomId);
      room.clients.add(ws);
      ws.joinedRooms.add(msg.roomId);
      send(ws, { type: 'history', roomId: msg.roomId, messages: room.history.slice(-100) });
      broadcast(msg.roomId, { type: 'presence', roomId: msg.roomId, event: 'join', userId: msg.userId, userName: msg.userName }, ws);
      return;
    }

    if (msg.type === 'leave' && msg.roomId) {
      const room = rooms.get(msg.roomId);
      if (room) room.clients.delete(ws);
      ws.joinedRooms.delete(msg.roomId);
      return;
    }

    if (msg.type === 'message' && msg.roomId && msg.message) {
      const room = getRoom(msg.roomId);
      room.history.push(msg.message);
      if (room.history.length > MAX_HISTORY) room.history.shift();
      broadcast(msg.roomId, { type: 'message', roomId: msg.roomId, message: msg.message }, ws);
      return;
    }

    if (msg.type === 'typing' && msg.roomId) {
      broadcast(msg.roomId, { type: 'typing', roomId: msg.roomId, userId: msg.userId, userName: msg.userName }, ws);
      return;
    }
  });

  ws.on('close', () => {
    for (const roomId of ws.joinedRooms) {
      const room = rooms.get(roomId);
      if (room) room.clients.delete(ws);
    }
  });
});

server.listen(PORT, () => {
  console.log(`OmniAgent Group Chat relay listening on :${PORT}`);
});
