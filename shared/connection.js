/**
 * Reusable RabbitMQ connection + channel helper.
 * Use after Level 0 when you want to focus on patterns, not boilerplate.
 *
 * @param {string} [url] - AMQP URL (default: amqp://guest:guest@localhost:5672)
 * @returns {Promise<{ connection: import('amqplib').Connection, channel: import('amqplib').Channel }>}
 */
export async function connect(url = 'amqp://guest:guest@localhost:5672') {
  const amqp = await import('amqplib');
  const connection = await amqp.default.connect(url);
  const channel = await connection.createChannel();
  return { connection, channel };
}
