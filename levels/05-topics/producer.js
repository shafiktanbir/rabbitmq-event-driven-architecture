/**
 * Mission 5 — Topics: publish with dot-separated routing keys.
 * Run: node levels/05-topics/producer.js
 */
import { connect } from '../../shared/connection.js';

const EXCHANGE = 'rabitplay-topics';

async function main() {
  const { connection, channel } = await connect();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: false });

  const events = [
    { key: 'user.signup', msg: 'Alice joined' },
    { key: 'order.created.eu', msg: 'Order #101 EU' },
    { key: 'user.profile.updated', msg: 'Alice updated profile' },
    { key: 'order.created.us', msg: 'Order #102 US' },
    { key: 'backend.error', msg: 'DB timeout' },
    { key: 'order.shipped.eu', msg: 'Order #101 shipped' },
  ];
  for (const { key, msg } of events) {
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
