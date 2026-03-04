/**
 * Mission 8 — Dead letters: consume from DLQ to inspect failed messages.
 * Run: node levels/08-dead-letters/consumer-dlq.js (after producer + main consumer have run)
 */
import { connect } from '../../shared/connection.js';

const DLQ = 'rabitplay-dlq';

async function main() {
  const { channel } = await connect();
  await channel.assertQueue(DLQ, { durable: false });

  console.log('DLQ consumer waiting for dead letters...');

  channel.consume(DLQ, (msg) => {
    if (msg) {
      console.log('Dead letter:', msg.content.toString());
      channel.ack(msg);
    }
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
