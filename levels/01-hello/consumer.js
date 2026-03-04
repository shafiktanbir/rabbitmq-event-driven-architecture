/**
 * Mission 1 — Hello: consumer receives messages from the queue.
 * Run: node levels/01-hello/consumer.js (leave running, then run producer in another terminal)
 */
import { connect } from '../../shared/connection.js';

const QUEUE = 'rabitplay-hello';

async function main() {
  const { channel } = await connect();
  await channel.assertQueue(QUEUE, { durable: false });

  console.log('Waiting for messages on queue:', QUEUE);

  channel.consume(QUEUE, (msg) => {
    if (msg) {
      console.log('Received:', msg.content.toString());
      channel.ack(msg);
    }
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
