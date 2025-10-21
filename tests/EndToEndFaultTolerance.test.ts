import { jest } from '@jest/globals';
import amqplib from 'amqplib';
import { ConnectionManager } from '../src/connection/ConnectionManager.js';
import { ReliablePublisher } from '../src/publisher/ReliablePublisher.js';
import { RetryTopology } from '../src/retry/RetryTopology.js';
import { RetryManager } from '../src/retry/RetryManager.js';
import { InMemoryDeduplicationStore } from '../src/idempotency/DeduplicationStore.js';
import { withIdempotency } from '../src/idempotency/withIdempotency.js';
import { MockConnection, createMockConsumeMessage } from './mocks/amqplib.mock.js';

describe('End-to-End Enterprise Messaging & Fault-Tolerance Lifecycle', () => {
  let connectionManager: ConnectionManager;
  let mockConnection: MockConnection;
  let store: InMemoryDeduplicationStore;

  beforeEach(async () => {
    mockConnection = new MockConnection();
    jest.spyOn(amqplib, 'connect').mockImplementation(async () => {
      return mockConnection as any;
    });

    connectionManager = new ConnectionManager();
    await connectionManager.connect();
    store = new InMemoryDeduplicationStore();
  });

  afterEach(async () => {
    store.destroy();
    await connectionManager.close();
    jest.restoreAllMocks();
  });

  it('proves zero dropped messages, progressive retry progression, poison pill quarantine, and duplicate dropping', async () => {
    const channel = await connectionManager.getChannel();
    await RetryTopology.setup(channel);

    const publisher = new ReliablePublisher(connectionManager);
    const retryManager = new RetryManager();

    // 1. Publish order event with publisher confirms
    const orderPayload = { orderId: 'ORD-PROD-2026', total: 450.0 };
    const publishResult = await publisher.publish(
      'orders.exchange',
      'orders.process',
      orderPayload,
      { idempotencyKey: 'idemp-ord-2026' }
    );

    expect(publishResult.confirmed).toBe(true);
    expect(publishResult.messageId).toBe('idemp-ord-2026');

    // 2. Setup Idempotent Business Logic Consumer
    let executionCount = 0;
    const businessLogic = jest.fn().mockImplementation(async (data: any) => {
      executionCount++;
      return { success: true, orderId: data.orderId };
    });

    const consumerHandler = withIdempotency(businessLogic, { store });

    const incomingMsg = createMockConsumeMessage(orderPayload, {
      messageId: 'idemp-ord-2026',
      headers: { 'x-retry-count': 0 },
    });

    // 3. Process message for the first time
    const result1: any = await consumerHandler(orderPayload, incomingMsg, {
      messageId: 'idemp-ord-2026',
      retryCount: 0,
    });

    expect(result1).toEqual({ success: true, orderId: 'ORD-PROD-2026' });
    expect(executionCount).toBe(1);

    // 4. Duplicate message arrives (e.g. at-least-once delivery redelivery from broker)
    const duplicateMsg = createMockConsumeMessage(orderPayload, {
      messageId: 'idemp-ord-2026',
      headers: { 'x-retry-count': 0 },
    });

    const result2: any = await consumerHandler(orderPayload, duplicateMsg, {
      messageId: 'idemp-ord-2026',
      retryCount: 0,
    });

    // VERIFICATION: Business logic execution count must remain 1!
    expect(executionCount).toBe(1);
    expect(result2.duplicate).toBe(true);
    expect(result2.cachedResult).toEqual({ success: true, orderId: 'ORD-PROD-2026' });

    // 5. Test Poison-Pill Progression through Retry Queues to Quarantine DLQ
    const poisonPayload = { orderId: 'POISON-999', malformed: true };
    const poisonMsg0 = createMockConsumeMessage(poisonPayload, {
      messageId: 'poison-msg-1',
      headers: { 'x-retry-count': 0 },
    });

    // Attempt 1 fails -> routes to 10s retry queue
    const retry1 = await retryManager.handleFailure(
      channel,
      poisonMsg0,
      new Error('Corrupted customer record')
    );
    expect(retry1.action).toBe('retry_10s');
    expect(retry1.targetQueue).toBe('orders.retry.10s');
    expect(retry1.retryCount).toBe(1);

    // Attempt 2 fails -> routes to 60s retry queue
    const poisonMsg1 = createMockConsumeMessage(poisonPayload, {
      messageId: 'poison-msg-1',
      headers: { 'x-retry-count': 1 },
    });
    const retry2 = await retryManager.handleFailure(
      channel,
      poisonMsg1,
      new Error('Corrupted customer record')
    );
    expect(retry2.action).toBe('retry_60s');
    expect(retry2.targetQueue).toBe('orders.retry.60s');
    expect(retry2.retryCount).toBe(2);

    // Attempt 3 fails (maxRetries = 2 exceeded) -> quarantined in poison DLQ
    const poisonMsg2 = createMockConsumeMessage(poisonPayload, {
      messageId: 'poison-msg-1',
      headers: { 'x-retry-count': 2 },
    });
    const poisonFinal = await retryManager.handleFailure(
      channel,
      poisonMsg2,
      new Error('Corrupted customer record')
    );
    expect(poisonFinal.action).toBe('poison_dlq');
    expect(poisonFinal.targetQueue).toBe('orders.poison.dlq');
    expect(poisonFinal.retryCount).toBe(3);
  });
});
