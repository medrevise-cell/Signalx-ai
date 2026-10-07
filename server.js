import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const PORT = process.env.PORT || 10000;
const DERIV_URL = 'wss://ws.binaryws.com/websockets/v3';

app.use(express.json());
app.use(express.static('public'));
app.get('/api/health', (_, res) => res.json({ ok: true, service: 'signalx-ai-suite', mode: 'research' }));

function derivRequest(payload, timeout = 12000) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(DERIV_URL);
    const timer = setTimeout(() => { ws.close(); reject(new Error('Deriv request timeout')); }, timeout);
    ws.on('open', () => ws.send(JSON.stringify(payload)));
    ws.on('message', raw => {
      const data = JSON.parse(raw.toString());
      if (data.error) { clearTimeout(timer); ws.close(); reject(new Error(data.error.message || 'Deriv error')); return; }
      if (data.msg_type === 'history' || data.msg_type === 'active_symbols' || data.msg_type === 'tick') {
        clearTimeout(timer); ws.close(); resolve(data);
      }
    });
    ws.on('error', err => { clearTimeout(timer); reject(err); });
  });
}

app.get('/api/symbols', async (_, res) => {
  try { res.json(await derivRequest({ active_symbols: 'brief', product_type: 'basic', req_id: 1 })); }
  catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/api/history', async (req, res) => {
  const symbol = String(req.query.symbol || '1HZ100V');
  const count = Math.min(Math.max(Number(req.query.count || 1000), 100), 10000);
  try {
    const data = await derivRequest({ ticks_history: symbol, count, end: 'latest', style: 'ticks', req_id: 2 });
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

wss.on('connection', (client) => {
  let upstream;
  client.on('message', raw => {
    try {
      const { symbol = '1HZ100V' } = JSON.parse(raw.toString());
      if (upstream) upstream.close();
      upstream = new WebSocket(DERIV_URL);
      upstream.on('open', () => upstream.send(JSON.stringify({ ticks: symbol, subscribe: 1, req_id: 10 })));
      upstream.on('message', msg => {
        if (client.readyState === WebSocket.OPEN) client.send(msg.toString());
      });
      upstream.on('error', e => { if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify({ error: e.message })); });
    } catch (e) { if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify({ error: e.message })); }
  });
  client.on('close', () => upstream?.close());
});

server.on('upgrade', (request, socket, head) => {
  if (request.url === '/live') wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request));
  else socket.destroy();
});

server.listen(PORT, () => console.log(`SignalX listening on ${PORT}`));
