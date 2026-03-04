/**
 * Mission 5 — Topics: consumer bound to order.*.eu (EU orders only).
 * Run: node levels/05-topics/consumer-order-eu.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-topics';

async function main() {
  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'order.*.eu');

  console.log('Order-EU consumer waiting for order.*.eu events...');

  channel.consume(q.queue, (msg) => {
    if (msg) {
      console.log('EU order [%s]:', msg.fields.routingKey, msg.content.toString());
      channel.ack(msg);
    }
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
