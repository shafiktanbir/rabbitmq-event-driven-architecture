/**
 * Mission 0 — Setup: connect, create channel, declare queue, send one message.
 * Run: node levels/00-setup/send.js
 */
import amqp from 'amqplib';

const QUEUE = 'rabitplay-hello';

async function main() {
  const connection = await amqp.connect('amqp://guest:guest@localhost:5672');
  const channel = await connection.createChannel();
  await channel.assertQueue(QUEUE, { durable: false });
  const msg = 'Hello RabitPlay!';
  channel.sendToQueue(QUEUE, Buffer.from(msg));
  console.log('Sent:', msg);
  await channel.close();
  await connection.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
