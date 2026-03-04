/**
 * Mission 9 — Boss: consumes order.reserved, "ships", publishes order.shipped.
 * Run: node levels/09-mini-project/worker-ship.js
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
  await channel.bindQueue(q.queue, EXCHANGE, 'order.reserved');
  channel.prefetch(1);

  console.log('Ship worker waiting for order.reserved...');

  channel.consume(q.queue, async (msg) => {
    if (!msg) return;
    const { orderId, item } = JSON.parse(msg.content.toString());
    console.log('Shipping order', orderId, item);
    await sleep(500);
    channel.publish(EXCHANGE, 'order.shipped', Buffer.from(JSON.stringify({ orderId, item })));
    channel.ack(msg);
    console.log('Shipped', orderId);
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
