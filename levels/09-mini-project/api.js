/**
 * Mission 9 — Boss: tiny API that creates orders and publishes order.created.
 * Run: node levels/09-mini-project/api.js
 */
import http from 'http';
import { connect } from '../../shared/connection.js';
import { randomUUID } from 'crypto';

const EXCHANGE = 'rabitplay-orders';
const PORT = 3000;

let channel;
let connection;

async function ensureChannel() {
  if (!channel) {
    const conn = await connect();
    connection = conn.connection;
    channel = conn.channel;
    await channel.assertExchange(EXCHANGE, 'topic', { durable: false });
  }
  return channel;
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'POST' && req.url === '/orders') {
    let body = '';
    for await (const chunk of req) body += chunk;
    let payload;
    try {
      payload = JSON.parse(body || '{}');
    } catch {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid JSON' }));
      return;
    }
    const item = payload.item || 'Unknown';
    const orderId = randomUUID();
    const ch = await ensureChannel();
    ch.publish(EXCHANGE, 'order.created', Buffer.from(JSON.stringify({ orderId, item })));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ orderId, item }));
    return;
  }
  res.writeHead(404);
  res.end();
});

server.listen(PORT, () => {
  console.log('API listening on http://localhost:%d', PORT);
  console.log('POST /orders with body { "item": "Widget" }');
});
