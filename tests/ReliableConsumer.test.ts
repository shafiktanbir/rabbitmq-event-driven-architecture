import { jest } from '@jest/globals';
import amqplib from 'amqplib';
import { ConnectionManager } from '../src/connection/ConnectionManager.js';
import { ReliableConsumer } from '../src/consumer/ReliableConsumer.js';
import { MockConnection, MockChannel, createMockConsumeMessage } from './mocks/amqplib.mock.js';

describe('ReliableConsumer', () => {
  let connectionManager: ConnectionManager;
  let mockConnection: MockConnection;

  beforeEach(async () => {
    mockConnection = new MockConnection();
    jest.spyOn(amqplib, 'connect').mockImplementation(async () => {
      return mockConnection as any;
    });

    connectionManager = new ConnectionManager();
    await connectionManager.connect();
  });

  afterEach(async () => {
    await connectionManager.close();
    jest.restoreAllMocks();
  });

  it('subscribes with prefetch backpressure and acks message on successful processing', async () => {
    const consumer = new ReliableConsumer(connectionManager, {
      queue: 'orders.process',
      prefetch: 10,
    });

    const handler = jest.fn().mockResolvedValue({ processed: true });
    const consumerTag = await consumer.consume(handler);
    expect(consumerTag).toBeDefined();

    const channel = mockConnection.channels[0];
    expect(channel.prefetchValue).toBe(10);

    const message = createMockConsumeMessage({ orderId: 'ord-123' }, { messageId: 'm-1' });

    // Simulate incoming message from RabbitMQ
    channel.simulateIncomingMessage(consumerTag, message);

    // Wait for async processing
    await new Promise((r) => setTimeout(r, 20));

    expect(handler).toHaveBeenCalledWith(
      { orderId: 'ord-123' },
      message,
      expect.objectContaining({ messageId: 'm-1', retryCount: 0 })
    );

    expect(channel.acknowledgedMessages).toContain(message);
    await consumer.stop();
  });

  it('catches processing errors, routes through retry pipeline, and acks original message', async () => {
    const consumer = new ReliableConsumer(connectionManager, {
      queue: 'orders.process',
    });

    const handler = jest.fn().mockRejectedValue(new Error('Downstream DB query failed'));
    const consumerTag = await consumer.consume(handler);
    const channel = mockConnection.channels[0];

    const message = createMockConsumeMessage({ orderId: 'ord-error-1' }, {
      messageId: 'm-err-1',
      headers: { 'x-retry-count': 0 },
    });

    const failureHandledPromise = new Promise<any>((resolve) => {
      consumer.once('failureHandled', resolve);
    });

    channel.simulateIncomingMessage(consumerTag, message);

    const handledResult = await failureHandledPromise;
    expect(handledResult.action).toBe('retry_10s');
    expect(handledResult.retryCount).toBe(1);
    expect(handledResult.targetQueue).toBe('orders.retry.10s');

    // Message must be acknowledged from main queue so it does not block next messages
    expect(channel.acknowledgedMessages).toContain(message);

    // Retry message published to DLX
    expect(channel.publishedMessages.length).toBe(1);
    expect(channel.publishedMessages[0].exchange).toBe('orders.retry.dlx');
    expect(channel.publishedMessages[0].routingKey).toBe('orders.retry.10s');

    await consumer.stop();
  });

  it('automatically resubscribes to queue upon connection restoration', async () => {
    const consumer = new ReliableConsumer(connectionManager, {
      queue: 'orders.process',
    });

    const handler = jest.fn().mockResolvedValue(true);
    await consumer.consume(handler);

    expect(mockConnection.channels.length).toBe(1);

    // Simulate connection drop and reconnect
    const resubscribingPromise = new Promise((resolve) => consumer.once('resubscribing', resolve));
    const subscribedPromise = new Promise((resolve) => consumer.once('subscribed', resolve));

    connectionManager.emit('reconnected', { connection: mockConnection });

    await resubscribingPromise;
    await subscribedPromise;

    // A new channel was created for the consumer after reconnect
    expect(mockConnection.channels.length).toBe(2);

    await consumer.stop();
  });
});
