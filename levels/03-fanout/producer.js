/**
 * Mission 3 — Fan-out: publish to exchange; all bound queues get a copy.
 * Run: node levels/03-fanout/producer.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-logs';

async function main() {
  const { connection, channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: false });

  const msg = `Log at ${new Date().toISOString()}`;
  channel.publish(EXCHANGE, '', Buffer.from(msg));
  console.log('Published:', msg);

  await channel.close();
  await connection.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
