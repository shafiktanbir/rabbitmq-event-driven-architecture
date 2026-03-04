/**
 * Mission 5 — Topics: consumer bound to user.# (all user events).
 * Run: node levels/05-topics/consumer-user.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-topics';

async function main() {
  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'user.#');

  console.log('User consumer waiting for user.* events...');

  channel.consume(q.queue, (msg) => {
    if (msg) {
      console.log('User event [%s]:', msg.fields.routingKey, msg.content.toString());
      channel.ack(msg);
    }
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
