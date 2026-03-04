/**
 * Mission 9 — Boss: consumes order.created, "reserves", publishes order.reserved.
 * Run: node levels/09-mini-project/worker-reserve.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-orders';

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'order.created');
  channel.prefetch(1);

  console.log('Reserve worker waiting for order.created...');

  channel.consume(q.queue, async (msg) => {
    if (!msg) return;
    const { orderId, item } = JSON.parse(msg.content.toString());
    console.log('Reserving order', orderId, item);
    await sleep(500);
    channel.publish(EXCHANGE, 'order.reserved', Buffer.from(JSON.stringify({ orderId, item })));
    channel.ack(msg);
    console.log('Reserved', orderId);
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
