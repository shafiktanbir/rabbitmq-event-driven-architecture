/**
 * Mission 4 — Direct: consumer bound to "info" only.
 * Run: node levels/04-direct/consumer-info.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-logs-direct';

async function main() {
  const { channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'direct', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'info');

  console.log('Info consumer waiting for [info] logs...');

  channel.consume(q.queue, (msg) => {
    if (msg) {
      console.log('Log [info]:', msg.content.toString());
      channel.ack(msg);
    }
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
