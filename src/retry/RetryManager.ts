import type { Channel, ConsumeMessage } from 'amqplib';
import type { RetryConfig, MessageMetadata } from '../types/index.js';
import { DEFAULT_RETRY_CONFIG } from './RetryTopology.js';

export interface RetryResult {
  action: 'retry_10s' | 'retry_60s' | 'poison_dlq';
  retryCount: number;
  targetQueue: string;
  errorReason: string;
}

export class RetryManager {
  private config: RetryConfig;

  constructor(customConfig: Partial<RetryConfig> = {}) {
    this.config = { ...DEFAULT_RETRY_CONFIG, ...customConfig };
  }

  public extractMetadata(message: ConsumeMessage): MessageMetadata {
    const headers = message.properties.headers || {};
    const retryCount = Number(headers['x-retry-count'] ?? 0);
    const messageId = message.properties.messageId || (headers['x-idempotency-key'] as string) || 'unknown';
    const correlationId = message.properties.correlationId;
    const firstFailedAt = headers['x-first-failed-at'] ? Number(headers['x-first-failed-at']) : undefined;
    const lastFailedAt = headers['x-last-failed-at'] ? Number(headers['x-last-failed-at']) : undefined;
    const errorReason = headers['x-failure-reason'] ? String(headers['x-failure-reason']) : undefined;
    const poisoned = Boolean(headers['x-poisoned'] ?? false);

    return {
      messageId,
      correlationId,
      retryCount,
      firstFailedAt,
      lastFailedAt,
      errorReason,
      poisoned,
    };
  }

  public async handleFailure(
    channel: Channel,
    message: ConsumeMessage,
    error: Error
  ): Promise<RetryResult> {
    const metadata = this.extractMetadata(message);
    const currentRetryCount = metadata.retryCount;
    const now = Date.now();
    const firstFailedAt = metadata.firstFailedAt || now;

    let action: 'retry_10s' | 'retry_60s' | 'poison_dlq';
    let targetQueue: string;
    let nextRetryCount = currentRetryCount + 1;
    let isPoisoned = false;

    if (currentRetryCount === 0) {
      action = 'retry_10s';
      targetQueue = this.config.retry10sQueue;
    } else if (currentRetryCount === 1) {
      action = 'retry_60s';
      targetQueue = this.config.retry60sQueue;
    } else {
      action = 'poison_dlq';
      targetQueue = this.config.poisonDlqQueue;
      isPoisoned = true;
    }

    const updatedHeaders = {
      ...(message.properties.headers || {}),
      'x-retry-count': nextRetryCount,
      'x-original-exchange': message.fields.exchange || this.config.mainExchange,
      'x-original-routing-key': message.fields.routingKey || this.config.mainRoutingKey,
      'x-first-failed-at': firstFailedAt,
      'x-last-failed-at': now,
      'x-failure-reason': error.message,
      'x-failure-stack': error.stack || '',
      'x-poisoned': isPoisoned,
    };

    const publishOptions = {
      ...message.properties,
      headers: updatedHeaders,
      persistent: true,
      deliveryMode: 2,
    };

    // Route to DLX with appropriate routing key
    channel.publish(
      this.config.retryExchange,
      targetQueue,
      message.content,
      publishOptions
    );

    // Acknowledge the original message from the current queue to prevent redelivery loop
    channel.ack(message);

    return {
      action,
      retryCount: nextRetryCount,
      targetQueue,
      errorReason: error.message,
    };
  }
}
