const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const PORT = process.env.PORT || 10000;

app.use(express.static(__dirname));
app.get('/health', (_req, res) => res.json({ ok: true, game: 'LITERYTHYM' }));

const queue = [];
const clients = new Map(); // ws -> { matchId, score, finished }
const matches = new Map();
let nextMatch = 1;
const send = (ws, obj) => { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); };
const removeFromQueue = ws => { const i = queue.indexOf(ws); if (i !== -1) queue.splice(i, 1); };
function queuePlayer(ws) {
  removeFromQueue(ws);
  // Remove stale connections from the front of the queue.
  while (queue.length && queue[0].readyState !== WebSocket.OPEN) queue.shift();
  const opponent = queue.shift();
  if (!opponent) { queue.push(ws); send(ws, { type: 'queued' }); return; }
  if (opponent === ws) { queue.push(ws); send(ws, { type: 'queued' }); return; }
  const matchId = 'match-' + (nextMatch++);
  const match = { id: matchId, players: [opponent, ws], scores: new Map([[opponent, 0], [ws, 0]]), finished: new Set(), started: false, timer: null };
  matches.set(matchId, match);
  clients.get(opponent).matchId = matchId;
  clients.get(ws).matchId = matchId;
  send(opponent, { type: 'matchFound', matchId });
  send(ws, { type: 'matchFound', matchId });
  match.timer = setTimeout(() => {
    if (!matches.has(matchId)) return;
    match.started = true;
    send(opponent, { type: 'start', matchId, countdown: 3 });
    send(ws, { type: 'start', matchId, countdown: 3 });
  }, 3000);
}
function finishMatch(match) {
  if (match.finished.size < 2) return;
  const [a, b] = match.players;
  const sa = match.scores.get(a) || 0, sb = match.scores.get(b) || 0;
  send(a, { type: 'result', matchId: match.id, yourScore: sa, opponentScore: sb });
  send(b, { type: 'result', matchId: match.id, yourScore: sb, opponentScore: sa });
  for (const ws of match.players) {
    const state = clients.get(ws);
    if (state) { state.matchId = null; state.score = 0; state.finished = false; }
  }
  clearTimeout(match.timer);
  matches.delete(match.id);
}
function disconnect(ws) {
  removeFromQueue(ws);
  const state = clients.get(ws);
  if (state && state.matchId) {
    const match = matches.get(state.matchId);
    if (match) {
      clearTimeout(match.timer);
      const other = match.players.find(p => p !== ws);
      send(other, { type: 'opponentLeft' });
      matches.delete(match.id);
      for (const p of match.players) { const s = clients.get(p); if (s) { s.matchId = null; s.finished = false; s.score = 0; } }
    }
  }
  clients.delete(ws);
}

wss.on('connection', ws => {
  clients.set(ws, { matchId: null, score: 0, finished: false, lastScoreAt: 0 });
  send(ws, { type: 'connected' });
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', message: 'Invalid message.' }); }
    const state = clients.get(ws);
    if (!state) return;
    if (msg.type === 'queue') {
      if (state.matchId) return send(ws, { type: 'error', message: 'You are already in a match.' });
      queuePlayer(ws);
    } else if (msg.type === 'cancelQueue') {
      removeFromQueue(ws);
    } else if (msg.type === 'score') {
      const match = matches.get(state.matchId);
      if (!match || !match.started || match.id !== msg.matchId || match.finished.has(ws)) return;
      const now = Date.now();
      if (now - state.lastScoreAt < 180) return;
      state.lastScoreAt = now;
      // Casual prototype: scores are relayed for the live display; final scores are client-reported.
      const score = Math.max(0, Math.min(100000000, Math.floor(Number(msg.score) || 0)));
      state.score = score;
      match.scores.set(ws, score);
      const other = match.players.find(p => p !== ws);
      send(other, { type: 'score', matchId: match.id, score });
    } else if (msg.type === 'finished') {
      const match = matches.get(state.matchId);
      if (!match || !match.started || match.id !== msg.matchId) return;
      const score = Math.max(0, Math.min(100000000, Math.floor(Number(msg.score) || 0)));
      state.score = score;
      match.scores.set(ws, score);
      match.finished.add(ws);
      const other = match.players.find(p => p !== ws);
      send(other, { type: 'score', matchId: match.id, score });
      finishMatch(match);
    }
  });
  ws.on('close', () => disconnect(ws));
  ws.on('error', () => disconnect(ws));
});

server.listen(PORT, '0.0.0.0', () => console.log(`LITERYTHYM server listening on ${PORT}`));
