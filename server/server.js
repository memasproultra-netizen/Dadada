'use strict';
/**
 * Ascension — сервер: раздаёт игру по HTTP и работает как relay для онлайн-кооператива.
 * Без зависимостей (только встроенный Node.js >= 18). Запуск: node server/server.js
 *
 * Протокол совпадает с клиентом игры (v95+):
 *   клиент -> {t:'hello', room, role:'host'|'guest'}
 *   сервер -> {t:'hello-ok', peer:bool} | {t:'err', msg}
 *   сервер -> {t:'peer-joined'} / {t:'peer-left'}
 *   всё остальное пересылается второму игроку комнаты как есть.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');

const PORT = +process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const MAX_PAYLOAD = 8 * 1024 * 1024;     // инвентарь гостя приходит картинкой — запас с головой
const MAX_ROOMS = +process.env.MAX_ROOMS || 500;
const MAX_BACKLOG = 16 * 1024 * 1024;    // если получатель не успевает принимать — отключаем
const HEARTBEAT_MS = 25000;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon', '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.txt': 'text/plain; charset=utf-8',
};

const rooms = new Map();   // имя -> {host: Conn|null, guest: Conn|null}
const conns = new Set();

// ---------------------------------------------------------------- HTTP (статика)
const gzCache = new Map(); // файл -> {mtime, buf}
function serveStatic(req, res) {
  let urlPath;
  try { urlPath = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (e) { res.writeHead(400); return res.end('bad request'); }
  if (urlPath === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (urlPath === '/stats') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ rooms: rooms.size, connections: conns.size }));
  }
  if (urlPath === '/') urlPath = '/index.html';
  const file = path.normalize(path.join(PUBLIC_DIR, urlPath));
  if (!file.startsWith(PUBLIC_DIR + path.sep)) { res.writeHead(403); return res.end('forbidden'); }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('not found'); }
    const ext = path.extname(file).toLowerCase();
    const headers = { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=86400' };
    const wantGz = /\bgzip\b/.test(req.headers['accept-encoding'] || '') && /^\.(html|js|css|json|svg|txt)$/.test(ext);
    if (!wantGz) { headers['Content-Length'] = st.size; res.writeHead(200, headers); return fs.createReadStream(file).pipe(res); }
    const hit = gzCache.get(file);
    const send = buf => { headers['Content-Encoding'] = 'gzip'; headers['Content-Length'] = buf.length; headers['Vary'] = 'Accept-Encoding'; res.writeHead(200, headers); res.end(buf); };
    if (hit && hit.mtime === st.mtimeMs) return send(hit.buf);
    fs.readFile(file, (e2, data) => {
      if (e2) { res.writeHead(500); return res.end('error'); }
      zlib.gzip(data, { level: 9 }, (e3, buf) => { if (e3) { res.writeHead(500); return res.end('error'); } gzCache.set(file, { mtime: st.mtimeMs, buf }); send(buf); });
    });
  });
}

// ---------------------------------------------------------------- WebSocket (RFC 6455, минимум)
function wsFrame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) h = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(len, 6); }
  return Buffer.concat([h, payload]);
}

class Conn {
  constructor(socket) {
    this.s = socket; this.buf = Buffer.alloc(0); this.parts = []; this.partsLen = 0; this.partsOp = 0;
    this.alive = true; this.closed = false; this.room = null; this.role = null;
    socket.setNoDelay(true);
    socket.on('data', d => this.onData(d));
    socket.on('close', () => this.cleanup());
    socket.on('error', () => this.cleanup());
    conns.add(this);
    this.helloTimer = setTimeout(() => { if (!this.room) this.close(1008); }, 15000);
  }
  sendRaw(buf) {
    if (this.closed) return;
    if (this.s.writableLength > MAX_BACKLOG) return this.close(1013);
    try { this.s.write(buf); } catch (e) { this.cleanup(); }
  }
  sendText(str) { this.sendRaw(wsFrame(0x1, Buffer.from(str, 'utf8'))); }
  sendJSON(o) { this.sendText(JSON.stringify(o)); }
  close(code) {
    if (this.closed) return;
    const p = Buffer.alloc(2); p.writeUInt16BE(code || 1000, 0);
    try { this.s.write(wsFrame(0x8, p)); this.s.end(); } catch (e) { }
    this.cleanup();
    setTimeout(() => { try { this.s.destroy(); } catch (e) { } }, 1000).unref();
  }
  onData(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
      let len = b[1] & 0x7f, off = 2;
      if (!masked) return this.close(1002);
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2) > 0) return this.close(1009); len = b.readUInt32BE(6); off = 10; }
      if (len > MAX_PAYLOAD) return this.close(1009);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      this.onFrame(fin, op, payload);
      if (this.closed) return;
    }
  }
  onFrame(fin, op, payload) {
    if (op === 0x8) return this.close(1000);
    if (op === 0x9) return this.sendRaw(wsFrame(0xA, payload));
    if (op === 0xA) { this.alive = true; return; }
    if (op === 0x1 || op === 0x2) { this.parts = [payload]; this.partsLen = payload.length; this.partsOp = op; }
    else if (op === 0x0) { this.parts.push(payload); this.partsLen += payload.length; if (this.partsLen > MAX_PAYLOAD) return this.close(1009); }
    else return this.close(1002);
    if (!fin) return;
    const msg = this.parts.length === 1 ? this.parts[0] : Buffer.concat(this.parts); this.parts = []; this.partsLen = 0;
    if (this.partsOp === 0x1) this.onMessage(msg);
  }
  onMessage(buf) {
    this.alive = true;
    if (!this.room) {                       // первое сообщение должно быть hello
      let m; try { m = JSON.parse(buf.toString('utf8')); } catch (e) { return this.close(1003); }
      if (!m || m.t !== 'hello') return this.close(1008);
      return hello(this, m);
    }
    const r = rooms.get(this.room); if (!r) return;
    const other = this.role === 'host' ? r.guest : r.host;
    if (other) other.sendRaw(wsFrame(0x1, buf));          // пересылаем как есть, без разбора JSON
  }
  cleanup() {
    if (this.closed) return; this.closed = true; clearTimeout(this.helloTimer); conns.delete(this);
    if (this.room) {
      const r = rooms.get(this.room);
      if (r && r[this.role] === this) {
        r[this.role] = null;
        const other = this.role === 'host' ? r.guest : r.host;
        if (other) other.sendJSON({ t: 'peer-left' });
        if (!r.host && !r.guest) rooms.delete(this.room);
      }
    }
  }
}

function hello(c, m) {
  const role = m.role === 'host' || m.role === 'guest' ? m.role : null;
  const name = String(m.room == null ? 'ascension' : m.room).replace(/[\u0000-\u001f]/g, '').trim().toUpperCase().slice(0, 32) || 'ASCENSION';
  if (!role) { c.sendJSON({ t: 'err', msg: 'Unknown role' }); return c.close(1008); }
  let r = rooms.get(name);
  if (!r) {
    if (rooms.size >= MAX_ROOMS) { c.sendJSON({ t: 'err', msg: 'Server is full, try later' }); return c.close(1013); }
    r = { host: null, guest: null }; rooms.set(name, r);
  }
  if (r[role]) {
    c.sendJSON({ t: 'err', msg: role === 'host' ? 'This room already has a host — pick another room code' : 'This room already has a guest' });
    return c.close(1008);
  }
  r[role] = c; c.room = name; c.role = role; clearTimeout(c.helloTimer);
  const other = role === 'host' ? r.guest : r.host;
  c.sendJSON({ t: 'hello-ok', peer: !!other });
  if (other) other.sendJSON({ t: 'peer-joined' });
}

// ---------------------------------------------------------------- запуск
const server = http.createServer(serveStatic);
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket' || req.headers['sec-websocket-version'] !== '13') {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return;
  }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  new Conn(socket);
});
// Ping-pong: убираем «мёртвые» соединения и не даём прокси хостингов рвать молчащие сокеты
setInterval(() => {
  for (const c of conns) { if (!c.alive) { c.close(1001); continue; } c.alive = false; c.sendRaw(wsFrame(0x9, Buffer.alloc(0))); }
}, HEARTBEAT_MS).unref();

if (require.main === module) {
  server.listen(PORT, HOST, () => {
    console.log('Ascension server: http://localhost:' + PORT + '  (WebSocket relay на том же порту)');
  });
}
module.exports = { server, rooms, conns };
