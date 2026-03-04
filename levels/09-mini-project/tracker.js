/**
 * Mission 9 — Boss: consumes order.# and logs every order event.
 * Run: node levels/09-mini-project/tracker.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-orders';

async function main() {
  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'order.#');

  console.log('Tracker listening for order.#...');

  channel.consume(q.queue, (msg) => {
    if (msg) {
      const key = msg.fields.routingKey;
      const payload = msg.content.toString();
      console.log('[Tracker]', key, '->', payload);
      channel.ack(msg);
    }
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
