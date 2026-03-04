/**
 * Mission 8 — Dead letters: send to main queue; one message will be nack'd and go to DLQ.
 * Run: node levels/08-dead-letters/producer.js
 */
import { connect } from '../../shared/connection.js';

const MAIN_QUEUE = 'rabitplay-main';
const DLX = 'rabitplay-dlx';
const DLQ = 'rabitplay-dlq';

async function main() {
  const { connection, channel } = await connect();

  await channel.assertExchange(DLX, 'direct', { durable: false });
  await channel.assertQueue(DLQ, { durable: false });
  await channel.bindQueue(DLQ, DLX, 'dead');

  await channel.assertQueue(MAIN_QUEUE, {
    durable: false,
    deadLetterExchange: DLX,
    deadLetterRoutingKey: 'dead',
  });

  const messages = ['ok-1', 'fail', 'ok-2'];
  for (const msg of messages) {
    channel.sendToQueue(MAIN_QUEUE, Buffer.from(msg));
    console.log('Sent:', msg);
  }

  await channel.close();
  await connection.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
