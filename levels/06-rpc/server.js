/**
 * Mission 6 — RPC: server consumes requests, computes fib(n), sends response to replyTo.
 * Run: node levels/06-rpc/server.js
 */
import { connect } from '../../shared/connection.js';

const QUEUE = 'rabitplay-rpc';

function fib(n) {
  if (n <= 1) return n;
  return fib(n - 1) + fib(n - 2);
}

async function main() {
  const { channel } = await connect();
  await channel.assertQueue(QUEUE, { durable: false });
  channel.prefetch(1);

  console.log('RPC server waiting for requests...');

  channel.consume(QUEUE, (msg) => {
    if (!msg) return;
    const n = parseInt(msg.content.toString(), 10);
    const replyTo = msg.properties.replyTo;
    const correlationId = msg.properties.correlationId;
    if (!replyTo || !correlationId) {
      channel.nack(msg, false, false);
      return;
    }
    const result = fib(n);
    channel.sendToQueue(replyTo, Buffer.from(String(result)), {
      correlationId,
    });
    channel.ack(msg);
    console.log('fib(%d) = %d', n, result);
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
