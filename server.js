'use strict';
/*
 * Onyx WrestleFest — online multiplayer relay server (v2)
 * -------------------------------------------------------
 * Upgraded from the ChatGPT v1 relay. The wire protocol is a SUPERSET of
 * v1: every v1 message ('join', 'input', 'frame', 'joined',
 * 'player_joined', 'player_left', 'error') still works unchanged, so the
 * existing game client keeps working. v2 adds:
 *
 *   - Server-generated room codes        {type:'create', host:true}
 *   - Session tokens + reconnection      'joined' now carries a token;
 *                                        {type:'rejoin', room, token}
 *                                        reclaims a held slot (45 s hold)
 *                                        for guests AND the host. A host
 *                                        drop no longer kills the room.
 *   - Lobby roster broadcasts            {type:'roster', players:[...]},
 *                                        'player_away' / 'player_back',
 *                                        'host_away' / 'host_back'
 *   - Sequenced inputs                   guest 'input' may carry seq;
 *                                        forwarded so the host can drop
 *                                        stale packets.
 *   - Sequenced, timestamped frames      host 'frame' is re-broadcast with
 *                                        seq + server ts; frames are
 *                                        DROPPED for guests whose socket
 *                                        is backed up (stale video is
 *                                        worse than a skipped frame).
 *   - Optional state channel             {type:'state'} host -> guests,
 *                                        for compact JSON snapshots as an
 *                                        alternative to JPEG frames.
 *   - Generic event channel              {type:'event', event, data}
 *                                        host -> all guests, guest -> host
 *                                        (tagged with the sender's slot).
 *   - App-level ping                     {type:'ping', t} -> {type:'pong', t}
 *                                        so clients can show real RTT.
 *
 * The host runs the authoritative game (as in v1). This server is a dumb,
 * validating relay: it never simulates gameplay.
 *
 * Run:  npm install && npm start        (PORT env, default 8080)
 */
const WebSocket = require('ws');
const crypto = require('crypto');

const port = Number(process.env.PORT || 8080);
const wss = new WebSocket.Server({ port, maxPayload: 210000 });

const MAX_ROOMS = 200;
const MAX_GUESTS = 5;
const SLOT_HOLD_MS = 45000;          // how long a dropped slot stays reserved
const FRAME_BACKUP_LIMIT = 512000;   // skip frames for sockets backed up past this
const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const INPUT_KEYS = ['left', 'right', 'up', 'down', 'punch', 'kick',
  'grapple', 'whip', 'fin', 'run', 'tag', 'apron', 'start', 'guard', 'counter'];

const rooms = new Map(); // code -> room
// room = { code, host: slotRec|null, guests: Map<slot, slotRec>, frameSeq }
// slotRec = { ws: WebSocket|null, token: string, awaySince: number|null }

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(msg)); } catch { /* socket dying; close handler cleans up */ }
  }
}
function newToken() { return crypto.randomBytes(18).toString('hex'); }
function newCode() {
  for (let tries = 0; tries < 50; tries++) {
    let code = '';
    for (let i = 0; i < 6; i++) code += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    if (!rooms.has(code)) return code;
  }
  return null;
}
function roster(room) {
  const players = [];
  if (room.host) players.push({ slot: 0, connected: !!room.host.ws });
  for (const [slot, rec] of room.guests) players.push({ slot, connected: !!rec.ws });
  return players.sort((a, b) => a.slot - b.slot);
}
function broadcastRoster(room) {
  const msg = { type: 'roster', room: room.code, players: roster(room) };
  if (room.host && room.host.ws) send(room.host.ws, msg);
  for (const rec of room.guests.values()) if (rec.ws) send(rec.ws, msg);
}
function broadcastGuests(room, msg) {
  for (const rec of room.guests.values()) if (rec.ws) send(rec.ws, msg);
}
function slotRecOf(room, slot) {
  return slot === 0 ? room.host : room.guests.get(slot) || null;
}
function roomIsDead(room) {
  const now = Date.now();
  const anyConnected = (room.host && room.host.ws) ||
    [...room.guests.values()].some(r => r.ws);
  if (anyConnected) return false;
  const recs = [room.host, ...room.guests.values()].filter(Boolean);
  return recs.every(r => r.awaySince && now - r.awaySince > SLOT_HOLD_MS);
}
function destroyRoom(room, reason) {
  if (room.host && room.host.ws) { room.host.ws.room = null; room.host.ws.slot = null; }
  for (const rec of room.guests.values()) {
    if (rec.ws) { send(rec.ws, { type: 'error', message: reason }); rec.ws.room = null; rec.ws.slot = null; }
  }
  rooms.delete(room.code);
}
/* periodic sweep: expire held slots nobody reclaimed, reap dead rooms */
setInterval(() => {
  const now = Date.now();
  for (const room of [...rooms.values()]) {
    for (const [slot, rec] of [...room.guests]) {
      if (!rec.ws && rec.awaySince && now - rec.awaySince > SLOT_HOLD_MS) {
        room.guests.delete(slot);
        if (room.host && room.host.ws) send(room.host.ws, { type: 'player_left', slot });
        broadcastRoster(room);
      }
    }
    if (room.host && !room.host.ws && room.host.awaySince &&
        now - room.host.awaySince > SLOT_HOLD_MS) {
      destroyRoom(room, 'Host did not return — room closed');
      continue;
    }
    if (roomIsDead(room)) rooms.delete(room.code);
  }
}, 5000);

function bindSlot(ws, room, slot, rec, resumed) {
  rec.ws = ws; rec.awaySince = null;
  ws.room = room.code; ws.slot = slot; ws.token = rec.token;
  send(ws, { type: 'joined', slot, room: room.code, token: rec.token, resumed: !!resumed, players: roster(room) });
}
function handleJoin(ws, m) {
  if (ws.room) { send(ws, { type: 'error', message: 'Already joined a room' }); return; }
  const wantHost = m.host === true;
  let code = String(m.room || '').toUpperCase();
  if (m.type === 'create') {
    if (!wantHost) { send(ws, { type: 'error', message: 'Only a host can create a room' }); return; }
    code = newCode();
    if (!code) { send(ws, { type: 'error', message: 'Could not allocate a room code' }); return; }
  } else if (!/^[A-Z0-9]{1,12}$/.test(code)) {
    send(ws, { type: 'error', message: 'Invalid room code' }); return;
  }
  let room = rooms.get(code);
  if (wantHost) {
    if (room && room.host) {
      send(ws, { type: 'error', message: room.host.ws ? 'Room already hosted' : 'Room is held for its host — use rejoin' });
      return;
    }
    if (!room) {
      if (rooms.size >= MAX_ROOMS) { send(ws, { type: 'error', message: 'Server is full' }); return; }
      room = { code, host: null, guests: new Map(), frameSeq: 0 };
      rooms.set(code, room);
    }
    const rec = { ws: null, token: newToken(), awaySince: null };
    room.host = rec;
    bindSlot(ws, room, 0, rec, false);
    broadcastRoster(room);
    return;
  }
  /* guest join */
  if (!room || !room.host) { send(ws, { type: 'error', message: 'Host is not connected' }); return; }
  let slot = 0;
  for (let i = 1; i <= MAX_GUESTS; i++) { if (!room.guests.has(i)) { slot = i; break; } }
  if (!slot) { send(ws, { type: 'error', message: 'Room full' }); return; }
  const rec = { ws: null, token: newToken(), awaySince: null };
  room.guests.set(slot, rec);
  bindSlot(ws, room, slot, rec, false);
  if (room.host.ws) send(room.host.ws, { type: 'player_joined', slot });
  broadcastRoster(room);
}
function handleRejoin(ws, m) {
  if (ws.room) { send(ws, { type: 'error', message: 'Already joined a room' }); return; }
  const code = String(m.room || '').toUpperCase();
  const room = rooms.get(code);
  if (!room) { send(ws, { type: 'error', message: 'Room not found' }); return; }
  const token = String(m.token || '');
  /* host slot? */
  if (room.host && room.host.token === token && token) {
    if (room.host.ws) { send(ws, { type: 'error', message: 'Host is already connected' }); return; }
    bindSlot(ws, room, 0, room.host, true);
    broadcastGuests(room, { type: 'host_back' });
    broadcastRoster(room);
    return;
  }
  for (const [slot, rec] of room.guests) {
    if (rec.token === token && token) {
      if (rec.ws) { send(ws, { type: 'error', message: 'That player is already connected' }); return; }
      bindSlot(ws, room, slot, rec, true);
      if (room.host && room.host.ws) send(room.host.ws, { type: 'player_back', slot });
      broadcastRoster(room);
      return;
    }
  }
  send(ws, { type: 'error', message: 'Session not found — join as a new player' });
}
function detach(ws, /* explicit */ isLeave) {
  const code = ws.room, slot = ws.slot;
  ws.room = null; ws.slot = null; ws.token = null;
  if (!code) return;
  const room = rooms.get(code);
  if (!room) return;
  const rec = slotRecOf(room, slot);
  if (!rec || rec.ws !== ws) return;
  if (isLeave) {
    /* explicit leave: free the slot immediately, no hold */
    if (slot === 0) { destroyRoom(room, 'Host left the room'); return; }
    room.guests.delete(slot);
    if (room.host && room.host.ws) send(room.host.ws, { type: 'player_left', slot });
    broadcastRoster(room);
    return;
  }
  /* unexpected drop: hold the slot for reconnection */
  rec.ws = null; rec.awaySince = Date.now();
  if (slot === 0) {
    broadcastGuests(room, { type: 'host_away' });
  } else if (room.host && room.host.ws) {
    send(room.host.ws, { type: 'player_away', slot });
  }
  broadcastRoster(room);
}

wss.on('connection', ws => {
  ws.room = null; ws.slot = null; ws.token = null;
  ws.isAlive = true; ws.rateStart = Date.now(); ws.rateCount = 0; ws.frameStart = Date.now(); ws.frameCount = 0;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    const now = Date.now();
    if (now - ws.rateStart >= 1000) { ws.rateStart = now; ws.rateCount = 0; }
    if (++ws.rateCount > 120) { send(ws, { type: 'error', message: 'Rate limit exceeded' }); return; }
    if (raw.length > 200000) return;
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.type === 'ping') { send(ws, { type: 'pong', t: m.t }); return; }
    if (m.type === 'join' || m.type === 'create') { handleJoin(ws, m); return; }
    if (m.type === 'rejoin') { handleRejoin(ws, m); return; }
    if (m.type === 'leave') { detach(ws, true); return; }

    const room = rooms.get(ws.room);
    if (!room || m.room !== ws.room) return;
    const isHost = ws.slot === 0 && room.host && room.host.ws === ws;

    if (m.type === 'input' && ws.slot > 0 && room.guests.get(ws.slot) && room.guests.get(ws.slot).ws === ws) {
      if (!room.host || !room.host.ws) return; // host away: inputs drop silently
      const input = {};
      for (const key of INPUT_KEYS) input[key] = m.input && m.input[key] === true;
      const out = { type: 'input', slot: ws.slot, input };
      if (Number.isFinite(m.seq)) out.seq = m.seq;
      send(room.host.ws, out);
    } else if (m.type === 'frame' && isHost && typeof m.data === 'string' &&
               m.data.length < 200000 && /^data:image\/jpeg;base64,/.test(m.data)) {
      if (now - ws.frameStart >= 1000) { ws.frameStart = now; ws.frameCount = 0; }
      if (++ws.frameCount > 30) return; // host frame budget: 30/s
      room.frameSeq++;
      const out = JSON.stringify({ type: 'frame', data: m.data, seq: room.frameSeq, ts: now });
      for (const rec of room.guests.values()) {
        if (!rec.ws || rec.ws.readyState !== WebSocket.OPEN) continue;
        if (rec.ws.bufferedAmount > FRAME_BACKUP_LIMIT) continue; // drop stale video for slow guests
        try { rec.ws.send(out); } catch { /* dying socket; close handler cleans up */ }
      }
    } else if (m.type === 'state' && isHost && m.data !== undefined) {
      /* compact snapshot channel: relayed verbatim (string or object) */
      broadcastGuests(room, { type: 'state', data: m.data, ts: now });
    } else if (m.type === 'event' && typeof m.event === 'string' && m.event.length <= 64) {
      if (isHost) broadcastGuests(room, { type: 'event', event: m.event, data: m.data });
      else if (ws.slot > 0 && room.host && room.host.ws) {
        send(room.host.ws, { type: 'event', slot: ws.slot, event: m.event, data: m.data });
      }
    }
  });
  ws.on('close', () => detach(ws, false));
  ws.on('error', () => detach(ws, false));
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));
console.log('Onyx WrestleFest multiplayer relay v2 listening on', port);
