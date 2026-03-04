/**
 * Mission 8 — Dead letters: main consumer; nack "fail" without requeue -> goes to DLQ.
 * Run: node levels/08-dead-letters/consumer.js
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

  console.log('Main consumer waiting... ("fail" will be nack\'d -> DLQ)');

  channel.consume(MAIN_QUEUE, (msg) => {
    if (!msg) return;
    const body = msg.content.toString();
    if (body === 'fail') {
      console.log('Rejecting (dead-letter):', body);
      channel.nack(msg, false, false);
      return;
    }
    console.log('Processed:', body);
    channel.ack(msg);
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
