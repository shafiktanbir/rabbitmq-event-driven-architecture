import { jest } from '@jest/globals';
import { ConnectionManager } from '../src/connection/ConnectionManager.js';
import { ReliablePublisher } from '../src/publisher/ReliablePublisher.js';
import { MockConnection } from './mocks/amqplib.mock.js';
import amqplib from 'amqplib';

describe('ReliablePublisher', () => {
  let connectionManager: ConnectionManager;
  let mockConnection: MockConnection;
  let publisher: ReliablePublisher;

  beforeEach(async () => {
    mockConnection = new MockConnection();
    jest.spyOn(amqplib, 'connect').mockImplementation(async () => {
      return mockConnection as any;
    });

    connectionManager = new ConnectionManager();
    await connectionManager.connect();
    publisher = new ReliablePublisher(connectionManager);
  });

  afterEach(async () => {
    publisher.close();
    await connectionManager.close();
    jest.restoreAllMocks();
  });

  it('publishes messages with persistent deliveryMode 2 and unique messageId on broker ACK', async () => {
    const payload = { orderId: 'ord-100', amount: 49.99 };
    const result = await publisher.publish('orders.exchange', 'orders.process', payload, {
      messageId: 'custom-msg-id-1',
    });

    expect(result.confirmed).toBe(true);
    expect(result.messageId).toBe('custom-msg-id-1');
    expect(result.exchange).toBe('orders.exchange');
    expect(result.routingKey).toBe('orders.process');

    const confirmChannel = mockConnection.confirmChannels[0];
    expect(confirmChannel.publishedMessages.length).toBe(1);

    const published = confirmChannel.publishedMessages[0];
    expect(published.exchange).toBe('orders.exchange');
    expect(published.routingKey).toBe('orders.process');
    expect(published.options?.deliveryMode).toBe(2);
    expect(published.options?.persistent).toBe(true);
    expect(published.options?.messageId).toBe('custom-msg-id-1');
    expect(JSON.parse(published.content.toString())).toEqual(payload);
  });

  it('rejects publication when broker NACKs the message', async () => {
    await connectionManager.getConfirmChannel();
    const confirmChannel = mockConnection.confirmChannels[0];
    confirmChannel.willNack = true;

    await expect(
      publisher.publish('orders.exchange', 'orders.process', { test: true })
    ).rejects.toThrow('Publisher confirm NACKed by broker: Simulated broker NACK');

    expect(publisher.getInFlightCount()).toBe(0);
  });

  it('rejects publication when broker confirm times out', async () => {
    await connectionManager.getConfirmChannel();
    const confirmChannel = mockConnection.confirmChannels[0];
    confirmChannel.willHang = true;

    await expect(
      publisher.publish('orders.exchange', 'orders.process', { test: true }, { timeoutMs: 50 })
    ).rejects.toThrow('Publisher confirm timed out after 50ms');

    expect(publisher.getInFlightCount()).toBe(0);
  });

  it('guarantees zero dropped messages by rejecting in-flight messages when broker disconnects', async () => {
    await connectionManager.getConfirmChannel();
    const confirmChannel = mockConnection.confirmChannels[0];
    confirmChannel.willHang = true;

    const publishPromise = publisher.publish(
      'orders.exchange',
      'orders.process',
      { data: 1 },
      { timeoutMs: 5000 }
    );
    expect(publisher.getInFlightCount()).toBe(1);

    // Simulate sudden broker disconnect
    mockConnection.simulateDisconnect();

    await expect(publishPromise).rejects.toThrow('Broker disconnected while awaiting publisher confirm');
    expect(publisher.getInFlightCount()).toBe(0);
  });

  it('handles flow control backpressure and waits for drain event when channel buffer is saturated', async () => {
    await connectionManager.getConfirmChannel();
    const confirmChannel = mockConnection.confirmChannels[0];
    confirmChannel.willBlockFlow = true;

    let drainListened = false;
    confirmChannel.on('newListener', (event) => {
      if (event === 'drain') {
        drainListened = true;
      }
    });

    const result = await publisher.publish('orders.exchange', 'orders.process', { fast: 'data' });
    expect(result.confirmed).toBe(true);
    expect(drainListened).toBe(true);
  });
});
