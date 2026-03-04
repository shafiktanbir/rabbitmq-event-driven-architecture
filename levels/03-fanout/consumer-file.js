/**
 * Mission 3 — Fan-out: consumer that "saves" logs to file (logs the line).
 * Run: node levels/03-fanout/consumer-file.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-logs';

async function main() {
  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE, '');

  console.log('File consumer waiting for logs...');

  channel.consume(q.queue, (msg) => {
    if (msg) {
      console.log('Saved to file:', msg.content.toString());
      channel.ack(msg);
    }
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
