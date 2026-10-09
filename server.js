
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import path from 'path';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

const PORT = process.env.PORT || 10000;
const DERIV_URL = 'wss://ws.binaryws.com/websockets/v3';

const ALLOWED_SYMBOLS = new Set([
  '1HZ100V',
  '1HZ75V',
  '1HZ50V',
  '1HZ25V',
  '1HZ10V'
]);

app.use(express.json());
app.use(express.static(path.join(process.cwd(), 'public')));

// Serve index.html from the repository root.
app.get('/', (_req, res) => {
  res.sendFile(path.join(process.cwd(), 'index.html'));
});

// Health check.
app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'signalx-ai-suite',
    mode: 'research',
    tradingEnabled: false
  });
});

// Request data from Deriv's public WebSocket API.
function derivRequest(payload, timeout = 15000) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(DERIV_URL);
    let settled = false;

    const timer = setTimeout(() => {
      finish(new Error('Deriv request timed out.'));
    }, timeout);

    function finish(error, data) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);

      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      } else if (ws.readyState === WebSocket.CONNECTING) {
        ws.terminate();
      }

      if (error) reject(error);
      else resolve(data);
    }

    ws.on('open', () => {
      ws.send(JSON.stringify(payload), error => {
        if (error) finish(error);
      });
    });

    ws.on('message', raw => {
      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch {
        finish(new Error('Invalid response from Deriv.'));
        return;
      }

      if (data.error) {
        finish(new Error(data.error.message || 'Deriv API error.'));
        return;
      }

      if (
        data.msg_type === 'history' ||
        data.msg_type === 'active_symbols' ||
        data.msg_type === 'tick'
      ) {
        finish(null, data);
      }
    });

    ws.on('error', error => finish(error));

    ws.on('close', () => {
      if (!settled) {
        finish(new Error('Deriv connection closed unexpectedly.'));
      }
    });
  });
}

// List available Deriv symbols.
app.get('/api/symbols', async (_req, res) => {
  try {
    const data = await derivRequest({
      active_symbols: 'brief',
      product_type: 'basic',
      req_id: 1
    });

    res.json(data);
  } catch (error) {
    res.status(502).json({
      error: error.message
    });
  }
});

// Retrieve historical ticks.
app.get('/api/history', async (req, res) => {
  const symbol = String(req.query.symbol || '1HZ100V');
  const requestedCount = Number(req.query.count || 1000);

  if (!ALLOWED_SYMBOLS.has(symbol)) {
    return res.status(400).json({
      error: 'Unsupported symbol.'
    });
  }

  if (!Number.isFinite(requestedCount)) {
    return res.status(400).json({
      error: 'Tick count must be a number.'
    });
  }

  const count = Math.min(
    Math.max(Math.floor(requestedCount), 100),
    10000
  );

  try {
    const data = await derivRequest({
      ticks_history: symbol,
      count,
      end: 'latest',
      style: 'ticks',
      req_id: 2
    });

    res.json(data);
  } catch (error) {
    res.status(502).json({
      error: error.message
    });
  }
});

// Live tick streaming endpoint: /live
wss.on('connection', client => {
  let upstream = null;

  client.on('message', raw => {
    let request;

    try {
      request = JSON.parse(raw.toString());
    } catch {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({
          error: 'Send a valid JSON message.'
        }));
      }
      return;
    }

    const symbol = String(request.symbol || '1HZ100V');

    if (!ALLOWED_SYMBOLS.has(symbol)) {
      client.send(JSON.stringify({
        error: 'Unsupported symbol.'
      }));
      return;
    }

    if (upstream) {
      upstream.close();
      upstream = null;
    }

    const feed = new WebSocket(DERIV_URL);
    upstream = feed;

    feed.on('open', () => {
      if (feed !== upstream) return;

      feed.send(JSON.stringify({
        ticks: symbol,
        subscribe: 1,
        req_id: 10
      }));
    });

    feed.on('message', rawMessage => {
      if (
        feed === upstream &&
        client.readyState === WebSocket.OPEN
      ) {
        client.send(rawMessage.toString());
      }
    });

    feed.on('error', error => {
      if (
        feed === upstream &&
        client.readyState === WebSocket.OPEN
      ) {
        client.send(JSON.stringify({
          error: error.message
        }));
      }
    });

    feed.on('close', () => {
      if (feed === upstream) upstream = null;
    });
  });

  client.on('close', () => {
    if (upstream) {
      upstream.close();
      upstream = null;
    }
  });

  client.on('error', () => {
    if (upstream) upstream.close();
  });
});

// Accept WebSocket upgrades only at /live.
server.on('upgrade', (request, socket, head) => {
  const pathname = new URL(
    request.url,
    'http://localhost'
  ).pathname;

  if (pathname !== '/live') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, ws => {
    wss.emit('connection', ws, request);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`SignalX AI listening on port ${PORT}`);
});
