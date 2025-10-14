import type { ConsumeMessage } from 'amqplib';
import type { MessageHandler, IdempotencyOptions, MessageMetadata, IdempotencyStore } from '../types/index.js';
import { InMemoryDeduplicationStore } from './DeduplicationStore.js';

export function defaultKeyExtractor(msg: ConsumeMessage): string | undefined {
  const headers = msg.properties.headers || {};
  return (
    msg.properties.messageId ||
    (headers['x-idempotency-key'] as string) ||
    (headers['idempotencyKey'] as string)
  );
}

export function withIdempotency<T = unknown>(
  handler: MessageHandler<T>,
  options: IdempotencyOptions = {}
): MessageHandler<T> {
  const store: IdempotencyStore = options.store || new InMemoryDeduplicationStore();
  const keyExtractor = options.keyExtractor || defaultKeyExtractor;
  const ttlMs = options.ttlMs ?? 24 * 60 * 60 * 1000;

  return async (content: T, rawMessage: ConsumeMessage, metadata: MessageMetadata): Promise<unknown> => {
    const key = keyExtractor(rawMessage);

    // If message does not have an idempotency key, execute handler directly
    if (!key) {
      return handler(content, rawMessage, metadata);
    }

    const existingRecord = await store.get(key);

    if (existingRecord && existingRecord.status === 'COMPLETED') {
      // Duplicate message detected! Drop business logic execution
      if (options.onDuplicate) {
        await options.onDuplicate(rawMessage, existingRecord);
      }
      return {
        duplicate: true,
        key,
        message: 'Duplicate message dropped; business logic was not re-executed',
        cachedResult: existingRecord.result,
      };
    }

    // Attempt atomic lock / record insertion
    const acquired = await store.setIfAbsent(key, ttlMs);
    if (!acquired) {
      // Another consumer might be processing this message concurrently or in-flight
      const current = await store.get(key);
      if (current && current.status === 'COMPLETED') {
        if (options.onDuplicate) {
          await options.onDuplicate(rawMessage, current);
        }
        return {
          duplicate: true,
          key,
          cachedResult: current.result,
        };
      }
    }

    try {
      const result = await handler(content, rawMessage, metadata);
      await store.markCompleted(key, result);
      return result;
    } catch (error) {
      // Release idempotency lock so retry queue can re-attempt processing
      await store.markFailed(key, (error as Error).message);
      throw error;
    }
  };
}
