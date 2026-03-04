/**
 * Mission 1 — Hello: producer sends messages to the queue.
 * Run: node levels/01-hello/producer.js
 */
import { connect } from '../../shared/connection.js';

const QUEUE = 'rabitplay-hello';

async function main() {
  const { connection, channel } = await connect();
  await channel.assertQueue(QUEUE, { durable: false });

  const messages = ['First', 'Second', 'Third'];
  for (const msg of messages) {
    channel.sendToQueue(QUEUE, Buffer.from(msg));
    console.log('Sent:', msg);
  }

  await channel.close();
  await connection.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
