/**
 * Mission 2 — Work queue: worker consumes tasks, simulates work (slow for "slow" tasks).
 * Run multiple: node levels/02-work-queue/worker.js (in separate terminals)
 */
import { connect } from '../../shared/connection.js';

const QUEUE = 'rabitplay-tasks';

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const { channel } = await connect();
  await channel.assertQueue(QUEUE, { durable: false });
  channel.prefetch(1);

  console.log('Worker ready. Waiting for tasks...');

  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;
    const task = msg.content.toString();
    const isSlow = task.includes('slow');
    console.log('Processing:', task);
    if (isSlow) await sleep(2000);
    console.log('Done:', task);
    channel.ack(msg);
  }, { noAck: false });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
