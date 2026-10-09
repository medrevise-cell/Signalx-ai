
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

app.get('/', (_req, res) => {
  res.sendFile(path.join(process.cwd(), 'index.html'));
});

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'signalx-ai-suite',
    mode: 'research',
    tradingEnabled: false,
    liveEndpoint: '/live',
    supportedSymbols: [...ALLOWED_SYMBOLS]
  });
});

function getErrorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

// One-shot public market-data request.
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
      error: getErrorMessage(error)
    });
  }
});

app.get('/api/history', async (req, res) => {
  const symbol = String(req.query.symbol || '1HZ100V');
  const requestedCount = Number(req.query.count || 500);

  if (!ALLOWED_SYMBOLS.has(symbol)) {
    return res.status(400).json({
      error: 'Unsupported symbol.',
      supportedSymbols: [...ALLOWED_SYMBOLS]
    });
  }

  if (!Number.isFinite(requestedCount)) {
    return res.status(400).json({
      error: 'Tick count must be a number.'
    });
  }

  const count = Math.min(
    Math.max(Math.floor(requestedCount), 1),
    1000
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
      error: getErrorMessage(error)
    });
  }
});

function sendJSON(client, data) {
  if (client.readyState === WebSocket.OPEN) {
    client.send(JSON.stringify(data));
  }
}

wss.on('connection', (client, request) => {
  let upstream = null;
  let currentSymbol = null;
  let closed = false;

  function stopFeed() {
    const oldFeed = upstream;
    upstream = null;
    currentSymbol = null;

    if (
      oldFeed &&
      oldFeed.readyState !== WebSocket.CLOSED
    ) {
      oldFeed.close();
    }
  }

  function startFeed(rawSymbol) {
    const symbol = String(rawSymbol || '1HZ100V');

    if (!ALLOWED_SYMBOLS.has(symbol)) {
      sendJSON(client, {
        type: 'feed_error',
        error: 'Unsupported symbol.',
        supportedSymbols: [...ALLOWED_SYMBOLS]
      });
      return;
    }

    if (
      upstream &&
      currentSymbol === symbol &&
      upstream.readyState !== WebSocket.CLOSED
    ) {
      return;
    }

    stopFeed();
    currentSymbol = symbol;

    const feed = new WebSocket(DERIV_URL);
    upstream = feed;

    sendJSON(client, {
      type: 'feed_status',
      status: 'connecting',
      symbol
    });

    feed.on('open', () => {
      if (closed || feed !== upstream) return;

      console.log(`Deriv connected; subscribing to ${symbol}`);

      feed.send(JSON.stringify({
        ticks: symbol,
        subscribe: 1,
        req_id: 10
      }), error => {
        if (error && feed === upstream) {
          sendJSON(client, {
            type: 'feed_error',
            error: getErrorMessage(error),
            symbol
          });
        }
      });
    });

    feed.on('message', rawMessage => {
      if (closed || feed !== upstream) return;
      if (client.readyState !== WebSocket.OPEN) return;

      let data;

      try {
        data = JSON.parse(rawMessage.toString());
      } catch {
        sendJSON(client, {
          type: 'feed_error',
          error: 'Received invalid JSON from Deriv.',
          symbol
        });
        return;
      }

      // Forward subscription errors visibly.
      if (data.error) {
        console.error('Deriv subscription error:', data.error);

        sendJSON(client, {
          type: 'feed_error',
          error: data.error.message || 'Deriv rejected the request.',
          code: data.error.code || null,
          symbol
        });
        return;
      }

      if (data.msg_type === 'tick' && data.tick) {
        console.log(`Tick ${symbol}: ${data.tick.quote}`);

        // Preserve Deriv's original JSON quote and precision metadata.
        sendJSON(client, {
          ...data,
          type: 'market_tick',
          symbol
        });
        return;
      }

      // Forward subscription confirmations and other responses.
      sendJSON(client, {
        ...data,
        type: data.msg_type === 'tick' ? 'market_tick' : 'deriv_message',
        symbol
      });
    });

    feed.on('error', error => {
      if (feed !== upstream || closed) return;

      console.error('Deriv WebSocket error:', error.message);

      sendJSON(client, {
        type: 'feed_error',
        error: error.message || 'Deriv connection failed.',
        symbol
      });
    });

    feed.on('close', (code, reason) => {
      if (feed !== upstream || closed) return;

      upstream = null;
      currentSymbol = null;

      console.log(
        `Deriv feed closed for ${symbol}: ${code} ${reason.toString()}`
      );

      sendJSON(client, {
        type: 'feed_status',
        status: 'disconnected',
        symbol,
        code,
        reason: reason.toString()
      });
    });
  }

  // IMPORTANT FIX:
  // Read the symbol from /live?symbol=1HZ100V
  // and start the subscription immediately.
  const url = new URL(
    request.url,
    'http://localhost'
  );

  const querySymbol = url.searchParams.get('symbol');

  startFeed(querySymbol || '1HZ100V');

  // Also support browsers that send { "symbol": "1HZ100V" }.
  client.on('message', raw => {
    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch {
      sendJSON(client, {
        type: 'feed_error',
        error: 'Send a valid JSON message.'
      });
      return;
    }

    if (message.symbol) {
      startFeed(message.symbol);
    }
  });

  client.on('close', () => {
    closed = true;
    stopFeed();
  });

  client.on('error', error => {
    console.error('Browser WebSocket error:', error.message);
    closed = true;
    stopFeed();
  });
});

server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, 'http://localhost');

  if (url.pathname !== '/live') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, ws => {
    wss.emit('connection', ws, request);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`SignalX AI listening on port ${PORT}`);
  console.log('Trading is disabled; public market-data research only.');
});
