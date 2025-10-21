import { jest } from '@jest/globals';
import { InMemoryDeduplicationStore } from '../src/idempotency/DeduplicationStore.js';
import { withIdempotency } from '../src/idempotency/withIdempotency.js';
import { createMockConsumeMessage } from './mocks/amqplib.mock.js';

describe('Idempotency and Deduplication Engine', () => {
  describe('InMemoryDeduplicationStore', () => {
    let store: InMemoryDeduplicationStore;

    beforeEach(() => {
      store = new InMemoryDeduplicationStore({
        defaultTtlMs: 5000,
        maxEntries: 3,
        cleanupIntervalMs: 1000,
      });
    });

    afterEach(() => {
      store.destroy();
    });

    it('handles atomic setIfAbsent and prevents concurrent duplication', async () => {
      const first = await store.setIfAbsent('order-key-1');
      expect(first).toBe(true);

      const duplicate = await store.setIfAbsent('order-key-1');
      expect(duplicate).toBe(false);
    });

    it('tracks completed status and cached result', async () => {
      await store.setIfAbsent('order-key-2');
      await store.markCompleted('order-key-2', { paymentStatus: 'AUTHORIZED' });

      const record = await store.get('order-key-2');
      expect(record?.status).toBe('COMPLETED');
      expect(record?.result).toEqual({ paymentStatus: 'AUTHORIZED' });
    });

    it('clears lock on markFailed so retry queue can reprocess', async () => {
      await store.setIfAbsent('order-key-3');
      await store.markFailed('order-key-3', 'Connection timeout');

      // Key should now be available again for retry
      const retryAcquired = await store.setIfAbsent('order-key-3');
      expect(retryAcquired).toBe(true);
    });

    it('evicts expired records after TTL passes', async () => {
      jest.useFakeTimers();

      await store.setIfAbsent('expire-key', 500);
      expect(await store.get('expire-key')).not.toBeNull();

      jest.advanceTimersByTime(600);

      expect(await store.get('expire-key')).toBeNull();
      jest.useRealTimers();
    });

    it('enforces LRU max capacity and evicts oldest entry', async () => {
      await store.setIfAbsent('k1');
      await store.setIfAbsent('k2');
      await store.setIfAbsent('k3');
      expect(store.size()).toBe(3);

      // Inserting 4th entry exceeds maxEntries (3) -> oldest ('k1') should be evicted
      await store.setIfAbsent('k4');
      expect(store.size()).toBe(3);
      expect(await store.get('k1')).toBeNull();
      expect(await store.get('k4')).not.toBeNull();
    });
  });

  describe('withIdempotency middleware', () => {
    it('executes business handler on first receipt and drops duplicate messages on redelivery', async () => {
      const store = new InMemoryDeduplicationStore();
      const mockBusinessLogic = jest.fn().mockImplementation(async (order: any) => {
        return { processedOrderId: order.id, amountCharged: order.amount };
      });

      const onDuplicate = jest.fn<(msg: any, record: any) => void>();

      const consumer = withIdempotency(mockBusinessLogic, {
        store,
        onDuplicate,
      });

      const message1 = createMockConsumeMessage({ id: 'ord-999', amount: 120 }, {
        messageId: 'uuid-unique-999',
      });

      const metadata = {
        messageId: 'uuid-unique-999',
        retryCount: 0,
      };

      // 1. First execution
      const result1 = await consumer({ id: 'ord-999', amount: 120 }, message1, metadata);
      expect(mockBusinessLogic).toHaveBeenCalledTimes(1);
      expect(result1).toEqual({ processedOrderId: 'ord-999', amountCharged: 120 });
      expect(onDuplicate).not.toHaveBeenCalled();

      // 2. Redelivered / duplicate message with identical messageId
      const message2 = createMockConsumeMessage({ id: 'ord-999', amount: 120 }, {
        messageId: 'uuid-unique-999',
      });

      const result2: any = await consumer({ id: 'ord-999', amount: 120 }, message2, metadata);

      // CRITICAL ASSERTION: Business handler must NOT be executed a second time
      expect(mockBusinessLogic).toHaveBeenCalledTimes(1);
      expect(result2.duplicate).toBe(true);
      expect(result2.cachedResult).toEqual({ processedOrderId: 'ord-999', amountCharged: 120 });
      expect(onDuplicate).toHaveBeenCalledTimes(1);

      store.destroy();
    });

    it('releases lock when handler throws an error so retry queue is not blocked', async () => {
      const store = new InMemoryDeduplicationStore();
      let attempts = 0;

      const flakyHandler = jest.fn().mockImplementation(async () => {
        attempts++;
        if (attempts === 1) {
          throw new Error('Temporary downstream 500 error');
        }
        return { success: true };
      });

      const consumer = withIdempotency(flakyHandler, { store });

      const msg = createMockConsumeMessage({ test: 1 }, { messageId: 'flaky-msg-1' });
      const metadata = { messageId: 'flaky-msg-1', retryCount: 0 };

      // First attempt fails
      await expect(consumer({ test: 1 }, msg, metadata)).rejects.toThrow('Temporary downstream 500 error');

      // Second attempt (via retry queue) should now be permitted to execute
      const retryResult = await consumer({ test: 1 }, msg, { ...metadata, retryCount: 1 });
      expect(retryResult).toEqual({ success: true });
      expect(flakyHandler).toHaveBeenCalledTimes(2);

      store.destroy();
    });
  });
});
