
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import path from 'path';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

const PORT = process.env.PORT || 10000;
const DERIV_APP_ID = String(process.env.DERIV_APP_ID || '').trim();

const DERIV_URL = DERIV_APP_ID
  ? `wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(DERIV_APP_ID)}`
  : null;

const ALLOWED_SYMBOLS = new Set([
  '1HZ100V',
  '1HZ75V',
  '1HZ50V',
  '1HZ25V',
  '1HZ10V'
]);

app.use(express.json());
app.use(express.static(path.join(process.cwd(), 'public')));

// Serve the existing dashboard from the repository root.
app.get('/', (_req, res) => {
  res.sendFile(path.join(process.cwd(), 'index.html'));
});

// Health and configuration status.
app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'signalx-ai-suite',
    mode: 'research',
    tradingEnabled: false,
    derivConfigured: Boolean(DERIV_URL),
    liveEndpoint: '/live',
    supportedSymbols: [...ALLOWED_SYMBOLS]
  });
});

function getErrorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function requireDerivConfiguration(res) {
  if (DERIV_URL) return true;

  res.status(503).json({
    error: 'Deriv API is not configured.',
    detail: 'Add DERIV_APP_ID in Render Environment and redeploy.',
    tradingEnabled: false
  });

  return false;
}

// Make a single public market-data request.
function derivRequest(payload, timeout = 15000) {
  return new Promise((resolve, reject) => {
    if (!DERIV_URL) {
      reject(new Error('DERIV_APP_ID is not configured.'));
      return;
    }

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
        finish(new Error('Invalid JSON response from Deriv.'));
        return;
      }

      if (data.error) {
        finish(new Error(
          `${data.error.code || 'Deriv API error'}: ${
            data.error.message || 'Request rejected.'
          }`
        ));
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

    ws.on('close', (code, reason) => {
      if (!settled) {
        finish(new Error(
          `Deriv closed the connection (${code}): ${reason.toString()}`
        ));
      }
    });
  });
}

// Public market-symbol list.
app.get('/api/symbols', async (_req, res) => {
  if (!requireDerivConfiguration(res)) return;

  try {
    const data = await derivRequest({
      active_symbols: 'brief',
      product_type: 'basic',
      req_id: 1
    });

    res.json(data);
  } catch (error) {
    console.error('Symbol request failed:', getErrorMessage(error));

    res.status(502).json({
      error: 'Could not retrieve Deriv symbols.',
      detail: getErrorMessage(error)
    });
  }
});

// Historical tick data.
app.get('/api/history', async (req, res) => {
  if (!requireDerivConfiguration(res)) return;

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
    console.error('History request failed:', getErrorMessage(error));

    res.status(502).json({
      error: 'Could not retrieve historical ticks.',
      detail: getErrorMessage(error)
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

    if (!oldFeed) return;

    if (oldFeed.readyState === WebSocket.OPEN) {
      oldFeed.close();
    } else if (oldFeed.readyState === WebSocket.CONNECTING) {
      oldFeed.terminate();
    }
  }

  function startFeed(rawSymbol) {
    const symbol = String(rawSymbol || '1HZ100V').trim();

    if (closed) return;

    if (!DERIV_URL) {
      sendJSON(client, {
        type: 'feed_error',
        error: 'Deriv is not configured. Add DERIV_APP_ID in Render.',
        code: 'MISSING_APP_ID'
      });
      return;
    }

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
      (
        upstream.readyState === WebSocket.OPEN ||
        upstream.readyState === WebSocket.CONNECTING
      )
    ) {
      return;
    }

    stopFeed();

    currentSymbol = symbol;

    let feed;

    try {
      feed = new WebSocket(DERIV_URL);
    } catch (error) {
      currentSymbol = null;

      sendJSON(client, {
        type: 'feed_error',
        error: getErrorMessage(error),
        symbol
      });

      return;
    }

    upstream = feed;

    sendJSON(client, {
      type: 'feed_status',
      status: 'connecting',
      symbol
    });

    const subscriptionTimer = setTimeout(() => {
      if (
        feed === upstream &&
        feed.readyState !== WebSocket.CLOSED
      ) {
        console.error(`No tick confirmation received for ${symbol}.`);

        sendJSON(client, {
          type: 'feed_error',
          error: 'Timed out waiting for Deriv subscription response.',
          symbol
        });
      }
    }, 15000);

    let receivedTick = false;

    feed.on('open', () => {
      if (closed || feed !== upstream) return;

      console.log(`Deriv connected; subscribing to ${symbol}`);

      sendJSON(client, {
        type: 'feed_status',
        status: 'connected',
        symbol,
        message: 'Connected to Deriv; waiting for market data.'
      });

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

      if (data.error) {
        clearTimeout(subscriptionTimer);

        console.error(
          'Deriv subscription error:',
          data.error.code,
          data.error.message
        );

        sendJSON(client, {
          type: 'feed_error',
          error: data.error.message || 'Deriv rejected the request.',
          code: data.error.code || null,
          symbol
        });

        return;
      }

      if (data.msg_type === 'tick' && data.tick) {
        if (!receivedTick) {
          receivedTick = true;
          clearTimeout(subscriptionTimer);

          console.log(`First tick received for ${symbol}.`);
        }

        // Keep the original Deriv response fields intact so
        // existing frontend code can read msg_type and tick.quote.
        sendJSON(client, {
          ...data,
          type: 'market_tick',
          symbol
        });

        return;
      }

      if (
        data.msg_type === 'tick' ||
        data.msg_type === 'subscription'
      ) {
        clearTimeout(subscriptionTimer);
      }

      // Forward subscription confirmations and other responses.
      sendJSON(client, {
        ...data,
        type: 'deriv_message',
        symbol
      });
    });

    feed.on('error', error => {
      if (feed !== upstream || closed) return;

      clearTimeout(subscriptionTimer);

      console.error('Deriv WebSocket error:', error.message);

      sendJSON(client, {
        type: 'feed_error',
        error: error.message || 'Deriv connection failed.',
        symbol
      });
    });

    feed.on('close', (code, reason) => {
      clearTimeout(subscriptionTimer);

      if (feed !== upstream || closed) return;

      upstream = null;
      currentSymbol = null;

      const detail = reason.toString();

      console.log(
        `Deriv feed closed for ${symbol}: ${code} ${detail}`
      );

      sendJSON(client, {
        type: 'feed_status',
        status: 'disconnected',
        symbol,
        code,
        reason: detail
      });
    });
  }

  // Subscribe immediately using the URL symbol.
  // Example: /live?symbol=1HZ100V
  const url = new URL(request.url, 'http://localhost');
  startFeed(url.searchParams.get('symbol') || '1HZ100V');

  // Allow the frontend to change markets after connecting.
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

// Accept WebSocket upgrades only on /live.
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
  console.log(`Deriv API configured: ${Boolean(DERIV_URL)}`);
  console.log('Research mode only. Real-money trading is disabled.');
});
