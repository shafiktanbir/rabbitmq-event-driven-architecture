/**
 * Mission 4 — Direct: publish with routing keys (error, info, warning).
 * Run: node levels/04-direct/producer.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-logs-direct';

async function main() {
  const { connection, channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'direct', { durable: false });

  const logs = [
    { key: 'info', msg: 'User logged in' },
    { key: 'error', msg: 'Database connection failed' },
    { key: 'info', msg: 'Request completed' },
    { key: 'warning', msg: 'High memory usage' },
    { key: 'error', msg: 'Payment declined' },
  ];
  for (const { key, msg } of logs) {
    channel.publish(EXCHANGE, key, Buffer.from(msg));
    console.log('Published [%s]:', key, msg);
  }

  await channel.close();
  await connection.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
