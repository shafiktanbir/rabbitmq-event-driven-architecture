/**
 * Mission 7 — Reliability: durable queue, persistent messages, publish confirms.
 * Run: node levels/07-reliability/producer.js
 */
import { connect } from '../../shared/connection.js';

const QUEUE = 'rabitplay-reliable';

async function main() {
  const { connection, channel } = await connect();
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.confirmSelect();

  const messages = ['persistent one', 'persistent two', 'persistent three'];
  for (const msg of messages) {
    channel.sendToQueue(QUEUE, Buffer.from(msg), {
      persistent: true,
      deliveryMode: 2,
    });
    console.log('Sent:', msg);
  }

  await new Promise((resolve, reject) => {
    channel.waitForConfirms().then(resolve).catch(reject);
  });

  await channel.close();
  await connection.close();
  console.log('All confirms received.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
