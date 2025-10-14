import type { Channel } from 'amqplib';
import type { RetryConfig } from '../types/index.js';

export const DEFAULT_RETRY_CONFIG: RetryConfig = {
  mainExchange: 'orders.exchange',
  mainQueue: 'orders.process',
  mainRoutingKey: 'orders.process',
  retryExchange: 'orders.retry.dlx',
  retry10sQueue: 'orders.retry.10s',
  retry60sQueue: 'orders.retry.60s',
  poisonDlqQueue: 'orders.poison.dlq',
  maxRetries: 2,
};

export class RetryTopology {
  public static async setup(
    channel: Channel,
    customConfig: Partial<RetryConfig> = {}
  ): Promise<RetryConfig> {
    const config: RetryConfig = { ...DEFAULT_RETRY_CONFIG, ...customConfig };

    // 1. Assert Main Exchange (direct)
    await channel.assertExchange(config.mainExchange, 'direct', { durable: true });

    // 2. Assert Dead Letter Exchange (direct)
    await channel.assertExchange(config.retryExchange, 'direct', { durable: true });

    // 3. Assert Main Processing Queue with DLX configured
    await channel.assertQueue(config.mainQueue, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': config.retryExchange,
        'x-dead-letter-routing-key': config.retry10sQueue,
      },
    });
    await channel.bindQueue(config.mainQueue, config.mainExchange, config.mainRoutingKey);

    // 4. Assert Progressive Retry Queue 1 (10s TTL)
    // When message expires (10,000ms), RabbitMQ routes it back to the main exchange & queue
    await channel.assertQueue(config.retry10sQueue, {
      durable: true,
      arguments: {
        'x-message-ttl': 10000,
        'x-dead-letter-exchange': config.mainExchange,
        'x-dead-letter-routing-key': config.mainRoutingKey,
      },
    });
    await channel.bindQueue(config.retry10sQueue, config.retryExchange, config.retry10sQueue);

    // 5. Assert Progressive Retry Queue 2 (60s TTL)
    // When message expires (60,000ms), RabbitMQ routes it back to the main exchange & queue
    await channel.assertQueue(config.retry60sQueue, {
      durable: true,
      arguments: {
        'x-message-ttl': 60000,
        'x-dead-letter-exchange': config.mainExchange,
        'x-dead-letter-routing-key': config.mainRoutingKey,
      },
    });
    await channel.bindQueue(config.retry60sQueue, config.retryExchange, config.retry60sQueue);

    // 6. Assert Poison Pill DLQ (permanent holding, no TTL)
    await channel.assertQueue(config.poisonDlqQueue, {
      durable: true,
    });
    await channel.bindQueue(config.poisonDlqQueue, config.retryExchange, config.poisonDlqQueue);

    return config;
  }
}
