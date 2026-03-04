/**
 * Mission 3 — Fan-out: consumer that prints logs to console.
 * Run: node levels/03-fanout/consumer-console.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-logs';

async function main() {
  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE, '');

  console.log('Console consumer waiting for logs...');

  channel.consume(q.queue, (msg) => {
    if (msg) {
      console.log('Console:', msg.content.toString());
      channel.ack(msg);
    }
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
