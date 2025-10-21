import { MockChannel, createMockConsumeMessage } from './mocks/amqplib.mock.js';
import { RetryTopology, DEFAULT_RETRY_CONFIG } from '../src/retry/RetryTopology.js';
import { RetryManager } from '../src/retry/RetryManager.js';

describe('RetryTopology and Dead Letter Exchange', () => {
  let mockChannel: MockChannel;

  beforeEach(() => {
    mockChannel = new MockChannel();
  });

  it('declares the complete progressive retry exchange and queue topology', async () => {
    const config = await RetryTopology.setup(mockChannel as any);

    expect(config.mainExchange).toBe('orders.exchange');
    expect(config.retryExchange).toBe('orders.retry.dlx');
    expect(config.retry10sQueue).toBe('orders.retry.10s');
    expect(config.retry60sQueue).toBe('orders.retry.60s');
    expect(config.poisonDlqQueue).toBe('orders.poison.dlq');

    // Verify exchanges
    expect(mockChannel.exchanges.get('orders.exchange')?.type).toBe('direct');
    expect(mockChannel.exchanges.get('orders.retry.dlx')?.type).toBe('direct');

    // Verify main queue dead-letter routing to DLX
    const mainQ = mockChannel.queues.get('orders.process');
    expect(mainQ).toBeDefined();
    expect(mainQ?.options?.arguments?.['x-dead-letter-exchange']).toBe('orders.retry.dlx');
    expect(mainQ?.options?.arguments?.['x-dead-letter-routing-key']).toBe('orders.retry.10s');

    // Verify 10s retry queue TTL and dead letter target
    const retry10sQ = mockChannel.queues.get('orders.retry.10s');
    expect(retry10sQ).toBeDefined();
    expect(retry10sQ?.options?.arguments?.['x-message-ttl']).toBe(10000);
    expect(retry10sQ?.options?.arguments?.['x-dead-letter-exchange']).toBe('orders.exchange');
    expect(retry10sQ?.options?.arguments?.['x-dead-letter-routing-key']).toBe('orders.process');

    // Verify 60s retry queue TTL and dead letter target
    const retry60sQ = mockChannel.queues.get('orders.retry.60s');
    expect(retry60sQ).toBeDefined();
    expect(retry60sQ?.options?.arguments?.['x-message-ttl']).toBe(60000);
    expect(retry60sQ?.options?.arguments?.['x-dead-letter-exchange']).toBe('orders.exchange');
    expect(retry60sQ?.options?.arguments?.['x-dead-letter-routing-key']).toBe('orders.process');

    // Verify poison pill DLQ (permanent holding, no TTL)
    const dlq = mockChannel.queues.get('orders.poison.dlq');
    expect(dlq).toBeDefined();
    expect(dlq?.options?.arguments?.['x-message-ttl']).toBeUndefined();
  });

  describe('RetryManager progressive routing and poison pill quarantine', () => {
    let retryManager: RetryManager;

    beforeEach(() => {
      retryManager = new RetryManager();
    });

    it('routes initial failure (retry count 0) to orders.retry.10s with updated headers and acks original message', async () => {
      const msg = createMockConsumeMessage({ orderId: 'ord-fail-1' }, {
        messageId: 'msg-f1',
        headers: {}, // retry count defaults to 0
      });

      const failureError = new Error('Database connection timed out');
      const result = await retryManager.handleFailure(mockChannel as any, msg, failureError);

      expect(result.action).toBe('retry_10s');
      expect(result.retryCount).toBe(1);
      expect(result.targetQueue).toBe('orders.retry.10s');
      expect(result.errorReason).toBe('Database connection timed out');

      // Original message must be acknowledged so it leaves the active queue
      expect(mockChannel.acknowledgedMessages).toContain(msg);

      // Verify message routed to DLX
      expect(mockChannel.publishedMessages.length).toBe(1);
      const published = mockChannel.publishedMessages[0];
      expect(published.exchange).toBe('orders.retry.dlx');
      expect(published.routingKey).toBe('orders.retry.10s');
      expect(published.options?.headers?.['x-retry-count']).toBe(1);
      expect(published.options?.headers?.['x-failure-reason']).toBe('Database connection timed out');
      expect(published.options?.headers?.['x-first-failed-at']).toBeDefined();
    });

    it('routes second failure (retry count 1) to orders.retry.60s with updated headers', async () => {
      const firstFailedAt = Date.now() - 15000;
      const msg = createMockConsumeMessage({ orderId: 'ord-fail-2' }, {
        messageId: 'msg-f2',
        headers: {
          'x-retry-count': 1,
          'x-first-failed-at': firstFailedAt,
        },
      });

      const failureError = new Error('Payment gateway 503 Unavailable');
      const result = await retryManager.handleFailure(mockChannel as any, msg, failureError);

      expect(result.action).toBe('retry_60s');
      expect(result.retryCount).toBe(2);
      expect(result.targetQueue).toBe('orders.retry.60s');

      expect(mockChannel.acknowledgedMessages).toContain(msg);

      const published = mockChannel.publishedMessages[0];
      expect(published.exchange).toBe('orders.retry.dlx');
      expect(published.routingKey).toBe('orders.retry.60s');
      expect(published.options?.headers?.['x-retry-count']).toBe(2);
      expect(published.options?.headers?.['x-first-failed-at']).toBe(firstFailedAt);
      expect(published.options?.headers?.['x-failure-reason']).toBe('Payment gateway 503 Unavailable');
    });

    it('routes third failure (retry count 2 >= maxRetries) to poison-pill DLQ (orders.poison.dlq)', async () => {
      const msg = createMockConsumeMessage({ orderId: 'ord-corrupt-data' }, {
        messageId: 'msg-corrupt',
        headers: {
          'x-retry-count': 2,
          'x-first-failed-at': Date.now() - 75000,
        },
      });

      const failureError = new Error('Unparseable schema: poison pill payload');
      const result = await retryManager.handleFailure(mockChannel as any, msg, failureError);

      expect(result.action).toBe('poison_dlq');
      expect(result.retryCount).toBe(3);
      expect(result.targetQueue).toBe('orders.poison.dlq');

      expect(mockChannel.acknowledgedMessages).toContain(msg);

      const published = mockChannel.publishedMessages[0];
      expect(published.exchange).toBe('orders.retry.dlx');
      expect(published.routingKey).toBe('orders.poison.dlq');
      expect(published.options?.headers?.['x-retry-count']).toBe(3);
      expect(published.options?.headers?.['x-poisoned']).toBe(true);
      expect(published.options?.headers?.['x-failure-reason']).toBe('Unparseable schema: poison pill payload');
    });
  });
});
