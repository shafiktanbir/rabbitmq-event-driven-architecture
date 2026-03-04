/**
 * Mission 2 — Work queue: producer sends tasks (some fast, some slow).
 * Run: node levels/02-work-queue/producer.js
 */
import { connect } from '../../shared/connection.js';

const QUEUE = 'rabitplay-tasks';

async function main() {
  const { connection, channel } = await connect();
  await channel.assertQueue(QUEUE, { durable: false });

  const tasks = [
    'task 1',
    'task 2 slow',
    'task 3',
    'task 4',
    'task 5 slow',
    'task 6',
  ];
  for (const task of tasks) {
    channel.sendToQueue(QUEUE, Buffer.from(task));
    console.log('Sent:', task);
  }

  await channel.close();
  await connection.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
