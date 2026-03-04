/**
 * Mission 7 — Reliability: manual ack; unacked messages are re-queued if consumer dies.
 * Run: node levels/07-reliability/consumer.js
 * Try: kill this process on the 2nd message (before ack) and restart — message re-delivered.
 */
import { connect } from '../../shared/connection.js';

const QUEUE = 'rabitplay-reliable';

async function main() {
  const { channel } = await connect();
  await channel.assertQueue(QUEUE, { durable: true });
  channel.prefetch(1);

  console.log('Reliability consumer waiting... (kill on 2nd msg to see re-queue)');

  let count = 0;
  channel.consume(QUEUE, (msg) => {
    if (!msg) return;
    count += 1;
    const body = msg.content.toString();
    console.log('Received [%d]:', count, body);

    if (count === 2) {
      console.log('Simulating crash: not acking message 2. Exit with Ctrl+C and restart consumer.');
      return;
    }
    channel.ack(msg);
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
