const path = require('path');
const express = require('express');
const rabbitmq = require('./rabbitmq');

const app = express();
const PORT = Number(process.env.PORT || 3000);
const PUBLIC_DIR = path.join(__dirname, 'public');
const MAX_MESSAGES = Number(process.env.MESSAGE_LIMIT || 100);

const messages = [];
const sseClients = new Set();

function createId() {
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function normalizeMessage(payload) {
  if (typeof payload === 'string') {
    return {
      id: createId(),
      text: payload,
      createdAt: new Date().toISOString(),
    };
  }

  if (!payload || typeof payload !== 'object') {
    return {
      id: createId(),
      text: String(payload),
      createdAt: new Date().toISOString(),
    };
  }

  return {
    id: payload.id || createId(),
    text: typeof payload.text === 'string' ? payload.text : JSON.stringify(payload),
    createdAt: payload.createdAt || new Date().toISOString(),
  };
}

function broadcast(eventName, data) {
  const payload = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;

  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch (error) {
      sseClients.delete(client);
    }
  }
}

function storeMessage(payload) {
  const message = normalizeMessage(payload);
  messages.push(message);

  if (messages.length > MAX_MESSAGES) {
    messages.splice(0, messages.length - MAX_MESSAGES);
  }

  broadcast('message', message);
  return message;
}

async function startServer() {
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  app.use(express.static(PUBLIC_DIR));

  app.get('/', (req, res) => {
    res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
  });

  app.get('/api/messages', (req, res) => {
    res.json(messages);
  });

  app.get('/api/health', (req, res) => {
    res.json({
      ok: true,
      port: PORT,
      messageCount: messages.length,
    });
  });

  app.post('/api/messages', async (req, res) => {
    const text = typeof req.body.message === 'string' ? req.body.message.trim() : '';

    if (!text) {
      return res.status(400).json({
        ok: false,
        error: 'Message is required.',
      });
    }

    const payload = {
      id: createId(),
      text,
      createdAt: new Date().toISOString(),
    };

    try {
      await rabbitmq.publishMessage(payload);
      return res.status(202).json({
        ok: true,
        queued: true,
        message: payload,
      });
    } catch (error) {
      return res.status(503).json({
        ok: false,
        error: `RabbitMQ is unavailable: ${error.message}`,
      });
    }
  });

  app.get('/events', (req, res) => {
    res.status(200);
    res.set({
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });

    if (typeof res.flushHeaders === 'function') {
      res.flushHeaders();
    }

    sseClients.add(res);
    res.write(`event: snapshot\ndata: ${JSON.stringify(messages)}\n\n`);

    req.on('close', () => {
      sseClients.delete(res);
    });
  });

  await rabbitmq.startConsumer((payload) => {
    const message = storeMessage(payload);
    console.log(`[RabbitMQ] message received: ${message.text}`);
  }).catch((error) => {
    console.warn(`[RabbitMQ] consumer not started yet: ${error.message}`);
  });

  const server = app.listen(PORT, () => {
    console.log(`Express app running on http://localhost:${PORT}`);
    console.log(`RabbitMQ queue: ${rabbitmq.queueName}`);
  });

  const shutdown = async () => {
    server.close();
    await rabbitmq.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) {
  startServer().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

module.exports = {
  app,
  startServer,
};
