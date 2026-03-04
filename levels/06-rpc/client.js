/**
 * Mission 6 — RPC: client sends n, waits for fib(n) response.
 * Run: node levels/06-rpc/client.js (after starting server.js)
 */
import { connect } from '../../shared/connection.js';
import { randomUUID } from 'crypto';

const QUEUE = 'rabitplay-rpc';

async function main() {
  const { connection, channel } = await connect();
  const replyQueue = await channel.assertQueue('', { exclusive: true });
  const correlationId = randomUUID();
  const n = parseInt(process.argv[2] || '10', 10);

  return new Promise((resolve, reject) => {
    let consumerTag;

    const timeout = setTimeout(() => {
      if (consumerTag) channel.cancel(consumerTag).catch(() => {});
      reject(new Error('RPC timeout'));
    }, 10000);

    const handler = (msg) => {
      if (!msg || msg.properties.correlationId !== correlationId) return;
      clearTimeout(timeout);
      const result = msg.content.toString();
      const done = () => {
        console.log('fib(%d) = %s', n, result);
        channel.close().then(() => connection.close()).then(resolve);
      };
      if (consumerTag) channel.cancel(consumerTag).then(done).catch(done);
      else done();
    };

    channel.consume(replyQueue.queue, handler, { noAck: true }).then((res) => {
      consumerTag = res.consumerTag;
      channel.sendToQueue(QUEUE, Buffer.from(String(n)), {
        replyTo: replyQueue.queue,
        correlationId,
      });
    });
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
